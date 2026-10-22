#!/usr/bin/env bash
#
# W23 - the allocation's life: a deadline, a retry, a slot that goes back, and the players who
# never arrived (ADRs 0032, 0033, 0034).
#
# What it proves:
#   1. a confirmed match with no server to run it is **rejected at its deadline**, with a reason,
#      and its players come back to the queue **with the score they already had** - not the back
#      of the line;
#   2. a slot that frees **before** the deadline is taken by the match that was already waiting,
#      instead of that match dissolving for lack of a retry;
#   3. a match a server took and **never started** gives the slot back (`start_timeout`), and so
#      does a match on a server that **stopped heartbeating** (`server_failed`);
#   4. `GET /v1/matches/{matchId}/connection` answers 200 with an address and a short-lived signed
#      token **only** when there is a server; 409 `not_allocated` / 409 `match_rejected` /
#      409 `match_finished` / 404 otherwise - and the 404 is the same for a stranger and for a
#      match that does not exist;
#   5. the connection token is scoped: `scope=game-connect`, `matchId`, `team`, 120 s - a
#      credential for one match, not the player's hour-long identity;
#   6. `started` and `connected` are the game server's reports: both idempotent, a connection only
#      after the start and only for the roster;
#   7. one absent player is **marked** and the match keeps running for the ones who showed up; a
#      match where **nobody** connected is rejected (`all_no_show`) and gives the slot back.
#
# The two halves run in different processes on purpose: the orchestrator owns the deadlines
# (AllocationWatchdog) and the engine owns the requeue (MatchRequeueSweeper), so a scenario that
# only checked the database status would miss a rejected match whose players never came back.
#
# Usage: bash requests/lifecycle/verify_lifecycle.sh
# Preconditions: docker redis+postgres+kafka+schema-registry, auth-service on 8081, the tree built
#                (mvn -o -B install -DskipTests), nothing else using 8099/9102/8087/9096.
# It stops any engine and any orchestrator it finds (it owns the pairing loop and the consumer
# group) and starts its own API on 8099. ~4 minutes.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AUTH="${AUTH:-http://localhost:8081}"
API_PORT="${API_PORT:-8099}"
API="http://localhost:${API_PORT}"
API_METRICS_PORT="${API_METRICS_PORT:-9201}"
API_METRICS="http://localhost:${API_METRICS_PORT}/metrics"
ENGINE_METRICS_PORT="${ENGINE_METRICS_PORT:-9102}"
ENGINE_METRICS="http://localhost:${ENGINE_METRICS_PORT}/metrics"
ORCHESTRATOR_PORT="${ORCHESTRATOR_PORT:-8087}"
ORCHESTRATOR="http://localhost:${ORCHESTRATOR_PORT}"
ORCH_METRICS_PORT="${ORCH_METRICS_PORT:-9096}"
ORCH_METRICS="http://localhost:${ORCH_METRICS_PORT}/metrics"
ORCH_TOKEN="${ORCH_TOKEN:-w23-game-server-token}"

# The deadline is the sprint; these are the numbers the watchdog is started with so the scenario
# can watch it happen instead of waiting half a minute per phase.
ALLOCATION_TIMEOUT="${ALLOCATION_TIMEOUT:-8}"
START_TIMEOUT="${START_TIMEOUT:-20}"
CONNECT_WINDOW="${CONNECT_WINDOW:-10}"
SERVER_TTL="${SERVER_TTL:-120}"

REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
MM="$REPO_ROOT/requests/mm.sh"
OUT="/tmp/w23-out.json"
API_LOG="${API_LOG:-/tmp/w23-api.log}"
ENGINE_LOG="${ENGINE_LOG:-/tmp/w23-engine.log}"
ORCH_LOG="${ORCH_LOG:-/tmp/w23-orchestrator.log}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
gt()   { "$MM" gt "$1" "$2"; }
eq()   { "$MM" eq "$1" "$2"; }

redis() { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }
psql()  { docker exec -i "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -tAc "$1" 2>/dev/null; }

metric() { # <scrape-url> <series prefix, without the closing brace> -> value
  "$MM" metric "$1" "$2"
}
metric_or_zero() { local value; value="$(metric "$1" "$2")"; printf '%s' "${value:-0}"; }

free_port() { # <port>
  local holders
  holders="$(lsof -ti "tcp:$1" 2>/dev/null || true)"
  if [ -n "$holders" ]; then
    echo "$holders" | xargs kill -9 2>/dev/null || true
    sleep 1
  fi
}

wait_http() { # <url> <seconds>
  local deadline=$((SECONDS + $2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS "$1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

stop_services() {
  for pid_file in /tmp/w23-api.pid /tmp/w23-engine.pid /tmp/w23-orchestrator.pid; do
    if [ -f "$pid_file" ]; then
      kill -9 "$(cat "$pid_file")" 2>/dev/null || true
      rm -f "$pid_file"
    fi
  done
  # The engine owns the pairing loop and the orchestrator owns the MATCH_CONFIRMED consumer group:
  # a leftover of either would pair the arrivals while they are being counted, or take half the
  # events, and the failure would look like the watchdog's.
  pkill -f MatchmakingEngineApplication 2>/dev/null || true
  pkill -f MatchOrchestratorApplication 2>/dev/null || true
}
trap 'clean_state; stop_services' EXIT

start_api() {
  free_port "$API_PORT"
  free_port "$API_METRICS_PORT"
  (
    cd "$REPO_ROOT" || exit 1
    PORT="$API_PORT" METRICS_PORT="$API_METRICS_PORT" INSTANCE_ID=api-w23 \
      AUTH_BASE_URL="$AUTH" \
      mvn -o -q -pl services/matchmaking-api exec:java \
        -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication \
        </dev/null >"$API_LOG" 2>&1 &
    echo $! >/tmp/w23-api.pid
  )
  wait_http "$API/health" 120
}

start_engine() {
  free_port "$ENGINE_METRICS_PORT"
  (
    cd "$REPO_ROOT" || exit 1
    METRICS_PORT="$ENGINE_METRICS_PORT" SERVER_TTL_SECONDS="$SERVER_TTL" \
      mvn -o -q -pl services/matchmaking-engine exec:java \
        -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication \
        </dev/null >"$ENGINE_LOG" 2>&1 &
    echo $! >/tmp/w23-engine.pid
  )
  wait_http "$ENGINE_METRICS" 180
}

start_orchestrator() {
  free_port "$ORCHESTRATOR_PORT"
  free_port "$ORCH_METRICS_PORT"
  (
    cd "$REPO_ROOT" || exit 1
    ORCHESTRATOR_PORT="$ORCHESTRATOR_PORT" METRICS_PORT="$ORCH_METRICS_PORT" \
      GAME_SERVER_TOKEN="$ORCH_TOKEN" SERVER_TTL_SECONDS="$SERVER_TTL" \
      ALLOCATION_TIMEOUT_SECONDS="$ALLOCATION_TIMEOUT" \
      START_TIMEOUT_SECONDS="$START_TIMEOUT" \
      CONNECT_WINDOW_SECONDS="$CONNECT_WINDOW" \
      mvn -o -q -pl services/match-orchestrator exec:java \
        -Dexec.mainClass=com.victor.matchmaking.orchestrator.MatchOrchestratorApplication \
        </dev/null >"$ORCH_LOG" 2>&1 &
    echo $! >/tmp/w23-orchestrator.pid
  )
  wait_http "$ORCHESTRATOR/health" 180
}

login() { # <playerId> -> token
  curl -fsS -X POST "$AUTH/v1/auth/login" -H 'content-type: application/json' \
    -d "{\"playerId\":\"$1\"}" \
    | "$MM" json - token
}

probe_to() { # <out-file> <queue-subcommand and flags...>
  local out="$1"
  shift
  "$MM" queue "$@" >"$out" 2>/tmp/w23-probe.err
  local code=$?
  if [ $code -ne 0 ]; then
    printf '        probe error: %s\n' "$(tail -3 /tmp/w23-probe.err)"
  fi
  return $code
}

probe() { probe_to /tmp/w23-probe.json "$@"; }

field() { "$MM" json "$OUT" "$1"; }
field_of() { "$MM" json "$1" "$2"; }

orchestrator_call() { # <method> <path> [json] -> status in $STATUS, body in $OUT
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    STATUS="$(curl -sS -X "$method" "$ORCHESTRATOR$path" -H "authorization: Bearer $ORCH_TOKEN" \
      -H 'content-type: application/json' -d "$body" -o "$OUT" -w '%{http_code}')"
  else
    STATUS="$(curl -sS -X "$method" "$ORCHESTRATOR$path" -H "authorization: Bearer $ORCH_TOKEN" \
      -o "$OUT" -w '%{http_code}')"
  fi
}

register_server() { # <serverId> <capacity>
  orchestrator_call PUT "/v1/game-servers/$1" \
    "{\"region\":\"NA\",\"gameModes\":[\"1v1\"],\"capacity\":$2,\"endpoint\":\"10.0.0.7:7777\"}"
}

connection() { # <matchId> <token> -> status in $STATUS, body in $OUT
  STATUS="$(curl -sS "$API/v1/matches/$1/connection" -H "authorization: Bearer $2" \
    -o "$OUT" -w '%{http_code}')"
}

jwt_claim() { # <token> <claim> -> value ("" when absent)
  "$MM" jwt "$1" "$2"
}

allocation_status()  { psql "SELECT COALESCE(status, '') FROM match_allocations WHERE match_id = '$1'"; }
allocation_reason()  { psql "SELECT COALESCE(rejection_reason, '') FROM match_allocations WHERE match_id = '$1'"; }
allocation_server()  { psql "SELECT COALESCE(server_id, '') FROM match_allocations WHERE match_id = '$1'"; }
allocation_field()   { psql "SELECT COALESCE($2::text, '') FROM match_allocations WHERE match_id = '$1'"; }
server_allocated()   { psql "SELECT allocated FROM game_servers WHERE server_id = '$1'"; }
roster_tickets()     { psql "SELECT ticket_id FROM match_roster WHERE match_id = '$1' AND ticket_id IS NOT NULL ORDER BY player_id"; }
roster_players()     { psql "SELECT player_id FROM match_roster WHERE match_id = '$1' ORDER BY player_id"; }
player_no_show()     { psql "SELECT COALESCE(no_show_at::text, '') FROM match_roster WHERE match_id = '$1' AND player_id = '$2'"; }

queue_depth() { redis eval "local n = 0 for _, k in ipairs(redis.call('keys', 'queue:1v1:NA*')) do n = n + redis.call('zcard', k) end return n" 0; }
pending_count() { redis eval "return #redis.call('keys', 'match-pending:*')" 0; }
pending_matches() { redis eval "local keys = redis.call('keys', 'match-pending:*') table.sort(keys) local out = {} for _, k in ipairs(keys) do out[#out + 1] = string.gsub(k, '^match%-pending:', '') end return out" 0; }
match_players() { # <matchId>
  redis get "match-pending:$1" | "$MM" json-array - players playerId
}

ticket_queued_millis() { # <ticketId> -> epoch millis, "" when the key is gone
  local raw
  raw="$(redis get "ticket:$1")"
  if [ -n "$raw" ]; then
    "$MM" epoch-millis "$(printf '%s' "$raw" | "$MM" json - queuedAt)"
  fi
}

score_of() { # <ticketId> -> score as a float ("" when the member is absent)
  local raw
  raw="$(redis zscore queue:1v1:NA "$1")"
  if [ -n "$raw" ]; then
    "$MM" num "$raw"
  fi
}

score_matches() { # <ticketId> <expectedMillis> -> 0 when the score is the original queuedAt
  local actual
  actual="$(score_of "$1")"
  if [ -z "$actual" ]; then
    return 1
  fi
  "$MM" approx "$actual" "$2" 1.0
}

wait_member() { # <ticketId> <seconds>
  local deadline=$((SECONDS + $2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -n "$(score_of "$1")" ]; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

wait_allocation() { # <matchId> <status> <seconds>
  local deadline=$((SECONDS + $3))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(allocation_status "$1")" = "$2" ]; then
      return 0
    fi
    sleep 0.2
  done
  [ "$(allocation_status "$1")" = "$2" ]
}

wait_pending() { # <count> <seconds>
  local deadline=$((SECONDS + $2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(pending_count)" -ge "$1" ]; then
      return 0
    fi
    sleep 0.2
  done
  [ "$(pending_count)" -ge "$1" ]
}

wait_server_allocated() { # <serverId> <allocated> <seconds>
  local deadline=$((SECONDS + $3))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(server_allocated "$1")" = "$2" ]; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

wait_requeued() { # <matchId> <seconds>
  local deadline=$((SECONDS + $2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -n "$(allocation_field "$1" requeued_at)" ]; then
      return 0
    fi
    sleep 0.3
  done
  return 1
}

wait_no_show() { # <matchId> <playerId> <seconds>
  local deadline=$((SECONDS + $3))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -n "$(player_no_show "$1" "$2")" ]; then
      return 0
    fi
    sleep 0.3
  done
  return 1
}

clean_state() {
  redis eval "for _, k in ipairs(redis.call('keys', 'queue:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  redis eval "for _, k in ipairs(redis.call('keys', 'ticket:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  redis eval "for _, k in ipairs(redis.call('keys', 'match-pending:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  redis eval "for _, k in ipairs(redis.call('keys', 'player-pending:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  # The engine names a match `match-<uuid>`, so the run is identified by its players: every match
  # this scenario caused has a roster row for a `w23-` player.
  psql "DELETE FROM match_allocations a USING match_roster r WHERE r.match_id = a.match_id AND r.player_id LIKE 'w23-%'" >/dev/null
  psql "DELETE FROM match_allocations WHERE server_id LIKE 'gs-w23-%'" >/dev/null
  psql "DELETE FROM game_servers WHERE server_id LIKE 'gs-w23-%'" >/dev/null
}

# A phase starts with an empty queue: a ticket left CONFIRMING (its probe gone) would be requeued
# by the 30 s sweep in the middle of the next phase, and a requeued member would pair before the
# players the phase is about. `ticket:*` goes too, so the sweep reports the skip instead of
# resurrecting a case the next phase is not testing.
reset_queue() {
  redis eval "for _, k in ipairs(redis.call('keys', 'queue:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  redis eval "for _, k in ipairs(redis.call('keys', 'ticket:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  redis eval "for _, k in ipairs(redis.call('keys', 'match-pending:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  redis eval "for _, k in ipairs(redis.call('keys', 'player-pending:*')) do redis.call('del', k) end return 1" 0 >/dev/null
}

enqueue_pair() { # <player-a> <player-b> <wait-seconds> -> /tmp/w23-a.json, /tmp/w23-b.json
  local token_a token_b
  token_a="$(login "$1")"
  token_b="$(login "$2")"
  probe_to /tmp/w23-a.json enqueue --api "$API" --token "$token_a" --player "$1" --region NA \
    --redis-port 6379 --wait "$3" &
  local pid_a=$!
  probe_to /tmp/w23-b.json enqueue --api "$API" --token "$token_b" --player "$2" --region NA \
    --redis-port 6379 --wait "$3" &
  local pid_b=$!
  wait "$pid_a"
  wait "$pid_b"
}

play_pair() { # <player-a> <player-b> -> /tmp/w23-a.json, /tmp/w23-b.json (both confirmed)
  local token_a token_b
  token_a="$(login "$1")"
  token_b="$(login "$2")"
  probe_to /tmp/w23-a.json play --api "$API" --token "$token_a" --player "$1" --region NA --redis-port 6379 &
  local pid_a=$!
  probe_to /tmp/w23-b.json play --api "$API" --token "$token_b" --player "$2" --region NA --redis-port 6379 &
  local pid_b=$!
  wait "$pid_a"
  wait "$pid_b"
}

require_match() { # <matchId> <what>
  if [ -z "$1" ]; then
    fail "the scenario cannot continue: $2 did not form (services logs: $ENGINE_LOG / $ORCH_LOG)"
    printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
    exit 1
  fi
}

confirm() { # <matchId> <player>
  probe confirm --redis-port 6379 --match-id "$1" --player "$2" >/dev/null
}

confirm_pending_match() { # <matchId> -> the roster confirms, in parallel
  local player
  for player in $(match_players "$1"); do
    confirm "$1" "$player" &
  done
  wait
}

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
for container in "$REDIS_CONTAINER:redis" "$POSTGRES_CONTAINER:postgres"; do
  name="${container%%:*}"
  label="${container##*:}"
  if docker ps --format '{{.Names}}' | grep -q "^${name}$"; then
    ok "$label is running ($name)"
  else
    fail "$label is not running (docker compose up -d redis postgres kafka schema-registry)"
  fi
done
if docker ps --format '{{.Names}}' | grep -q "kafka"; then
  ok "Kafka is running (MATCH_CONFIRMED is how a confirmed match reaches the allocator)"
else
  fail "Kafka is not running"
fi
if docker ps --format '{{.Names}}' | grep -q "schema-registry"; then
  ok "Schema Registry is running (the engine resolves the schema id at publish time)"
else
  fail "Schema Registry is not running (see runbook item 3)"
fi
if wait_http "$AUTH/.well-known/jwks.json" 5; then
  ok "auth-service answers on 8081 (it signs the connection token)"
else
  fail "auth-service is not answering on 8081"
fi
if [ -f "services/match-orchestrator/target/classes/com/victor/matchmaking/orchestrator/MatchOrchestratorApplication.class" ] \
  && [ -f "services/matchmaking-api/target/classes/com/victor/matchmaking/api/MatchmakingApiApplication.class" ]; then
  ok "the tree is built (mvn -o -B install -DskipTests, runbook item 1)"
else
  fail "the services are not built"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. the stack this scenario owns"
# ---------------------------------------------------------------------------
stop_services
clean_state
if start_api; then
  ok "API on $API_PORT (scrape $API_METRICS_PORT), wired to the auth service"
else
  fail "the API did not start (see $API_LOG)"
fi
if start_engine; then
  ok "engine on metrics $ENGINE_METRICS_PORT (the only pairing loop and requeue sweep running)"
else
  fail "the engine did not start (see $ENGINE_LOG)"
fi
if start_orchestrator; then
  ok "orchestrator on $ORCHESTRATOR_PORT (allocation timeout ${ALLOCATION_TIMEOUT}s, start timeout ${START_TIMEOUT}s, connect window ${CONNECT_WINDOW}s)"
else
  fail "the orchestrator did not start (see $ORCH_LOG)"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

PLAYERS_JSON="$("$MM" queue players --count 16 --prefix w23-)"
PLAYERS=($(printf '%s' "$PLAYERS_JSON" | "$MM" json-array - players))
note "16 players share one skill: ${PLAYERS[*]:0:3} ..."

# ---------------------------------------------------------------------------
step "2. an allocated match: the address, the token and its scope"
# ---------------------------------------------------------------------------
register_server gs-w23-a 1
if [ "$STATUS" = "200" ] && [ "$(field free)" = "1" ]; then
  ok "gs-w23-a registered with one slot (free 1)"
else
  fail "registering gs-w23-a answered $STATUS ($(cat "$OUT"))"
fi

play_pair "${PLAYERS[0]}" "${PLAYERS[1]}"
MATCH_A="$(field_of /tmp/w23-a.json matchId)"
if [ -n "$MATCH_A" ] && [ "$(field_of /tmp/w23-a.json confirmed)" = "True" ]; then
  ok "the first pair formed and confirmed ($MATCH_A)"
else
  fail "the first pair did not confirm (see /tmp/w23-a.json)"
fi
if wait_allocation "$MATCH_A" ALLOCATED 20; then
  ok "and it is ALLOCATED: a server took it"
else
  fail "expected ALLOCATED, found '$(allocation_status "$MATCH_A")'"
fi
if [ "$(allocation_server "$MATCH_A")" = "gs-w23-a" ] && [ "$(server_allocated gs-w23-a)" = "1" ]; then
  ok "the allocation names gs-w23-a and the pool's slot is taken (allocated 1)"
else
  fail "allocation server '$(allocation_server "$MATCH_A")', pool allocated '$(server_allocated gs-w23-a)'"
fi

TOKEN_A="$(login "${PLAYERS[0]}")"
TEAM_A="$(psql "SELECT team FROM match_roster WHERE match_id = '$MATCH_A' AND player_id = '${PLAYERS[0]}'")"
connection "$MATCH_A" "$TOKEN_A"
if [ "$STATUS" = "200" ] && [ "$(field serverAddress)" = "10.0.0.7:7777" ] \
  && [ "$(field status)" = "ALLOCATED" ] && [ "$(field team)" = "$TEAM_A" ]; then
  ok "GET /v1/matches/{id}/connection answers 200 with the server's address and the team ($TEAM_A)"
else
  fail "the connection info answered $STATUS ($(cat "$OUT"))"
fi
CONNECTION_TOKEN="$(field connectionToken)"
if [ "$(jwt_claim "$CONNECTION_TOKEN" scope)" = "game-connect" ] \
  && [ "$(jwt_claim "$CONNECTION_TOKEN" matchId)" = "$MATCH_A" ] \
  && [ "$(jwt_claim "$CONNECTION_TOKEN" team)" = "$TEAM_A" ] \
  && [ "$(jwt_claim "$CONNECTION_TOKEN" sub)" = "${PLAYERS[0]}" ]; then
  ok "the token is scoped: scope=game-connect, matchId=$MATCH_A, team=$TEAM_A, sub=${PLAYERS[0]}"
else
  fail "the connection token's claims are wrong: $(jwt_claim "$CONNECTION_TOKEN" scope)/$(jwt_claim "$CONNECTION_TOKEN" matchId)/$(jwt_claim "$CONNECTION_TOKEN" team)"
fi
EXPIRES_IN="$(field expiresInSeconds)"
EXPIRES_AT_UNIX="$(jwt_claim "$CONNECTION_TOKEN" exp)"
TTL_FROM_TOKEN="$((EXPIRES_AT_UNIX - $(date +%s)))"
if [ "$EXPIRES_IN" = "120" ] && "$MM" within "$TTL_FROM_TOKEN" 110 120; then
  ok "it lives 120 s, not an hour ($EXPIRES_IN s; exp is $TTL_FROM_TOKEN s away)"
else
  fail "the token's TTL is $EXPIRES_IN and exp is $TTL_FROM_TOKEN s away, expected ~120"
fi
STRANGER_TOKEN="$(login w23-stranger)"
connection "$MATCH_A" "$STRANGER_TOKEN"
if [ "$STATUS" = "404" ]; then
  ok "a player outside the match gets 404, the same answer a match that does not exist gets"
else
  fail "a stranger got $STATUS ($(cat "$OUT"))"
fi
if gt "$(metric_or_zero "$API_METRICS" 'matchmaking_connection_requests_total{result="served"')" 0; then
  ok "the API counted the served request (matchmaking_connection_requests_total{result=\"served\"})"
else
  fail "the served counter did not move"
fi
# A match that is really running: started by its server and joined by both players. Without the
# start it would hit the 20 s start deadline, and without the connections the 10 s no-show window
# would reject it - either way before step 4 gets to release it.
orchestrator_call POST "/v1/game-servers/gs-w23-a/allocations/$MATCH_A/started"
if [ "$STATUS" = "200" ] && [ "$(field status)" = "STARTED" ]; then
  ok "MATCH_A is started by its server (the game server's report, not an inference)"
else
  fail "started on MATCH_A answered $STATUS ($(cat "$OUT"))"
fi
for player in $(roster_players "$MATCH_A"); do
  orchestrator_call POST "/v1/game-servers/gs-w23-a/allocations/$MATCH_A/players/$player/connected"
  if [ "$STATUS" != "200" ]; then
    fail "connecting $player to MATCH_A answered $STATUS ($(cat "$OUT"))"
  fi
done
ok "both players of MATCH_A connected: the match is running and the watchdog leaves it alone"

# ---------------------------------------------------------------------------
step "3. no server to run it: the deadline rejects the match (no_capacity)"
# ---------------------------------------------------------------------------
# The W22 capacity gate refuses to *form* a match into a full pool, so the way to get a confirmed
# match with nowhere to run is the other half of that same design: capacity is a snapshot, and a
# slot can vanish between the formation and the confirmation. The pair forms with a free slot, the
# slot is taken away, and only then do the players confirm - which is exactly the race ADR 0031
# accepted, used on purpose here.
register_server gs-w23-a 2
enqueue_pair "${PLAYERS[2]}" "${PLAYERS[3]}" 10
MATCH_B="$(field_of /tmp/w23-a.json matchId)"
require_match "$MATCH_B" "the second pair"
if [ -n "$MATCH_B" ]; then
  ok "the second pair formed ($MATCH_B) and is waiting for its confirmation"
else
  fail "the second pair did not form (see /tmp/w23-a.json: $(cat /tmp/w23-a.json))"
fi
register_server gs-w23-a 1
note "capacity dropped to 1 (MATCH_A holds it) before the confirmation: the allocator has no slot"
for player in $(match_players "$MATCH_B"); do
  confirm "$MATCH_B" "$player" &
done
wait
if wait_allocation "$MATCH_B" PENDING_ALLOCATION 20; then
  ok "the confirmed match waits in PENDING_ALLOCATION: no server took it"
else
  fail "expected PENDING_ALLOCATION, found '$(allocation_status "$MATCH_B")'"
fi
TOKEN_B="$(login "${PLAYERS[2]}")"
connection "$MATCH_B" "$TOKEN_B"
if [ "$STATUS" = "409" ] && [ "$(field error)" = "not_allocated" ]; then
  ok "the connection info says 409 not_allocated: the client knows to wait, not to give up"
else
  fail "expected 409 not_allocated, got $STATUS ($(cat "$OUT"))"
fi

TICKETS_B=($(roster_tickets "$MATCH_B"))
if [ "${#TICKETS_B[@]}" = "2" ]; then
  QUEUED_MILLIS_A="$(ticket_queued_millis "${TICKETS_B[0]}")"
  QUEUED_MILLIS_B="$(ticket_queued_millis "${TICKETS_B[1]}")"
  note "the two tickets are ${TICKETS_B[*]} with queuedAt ${QUEUED_MILLIS_A}/${QUEUED_MILLIS_B} (epoch ms)"
else
  QUEUED_MILLIS_A=""
  QUEUED_MILLIS_B=""
  fail "the roster of $MATCH_B does not hold two ticket ids"
fi

if wait_allocation "$MATCH_B" REJECTED $((ALLOCATION_TIMEOUT + 20)); then
  ok "the watchdog rejects it at its ${ALLOCATION_TIMEOUT}s deadline instead of waiting forever"
else
  fail "the match is still '$(allocation_status "$MATCH_B")' after the deadline"
fi
if [ "$(allocation_reason "$MATCH_B")" = "no_capacity" ] \
  && [ -n "$(allocation_field "$MATCH_B" rejected_at)" ]; then
  ok "the reason is no_capacity and the rejection is timestamped"
else
  fail "reason '$(allocation_reason "$MATCH_B")', rejected_at '$(allocation_field "$MATCH_B" rejected_at)'"
fi
if [ "${#TICKETS_B[@]}" = "2" ] && wait_member "${TICKETS_B[0]}" 10 && wait_member "${TICKETS_B[1]}" 10; then
  ok "the engine's sweep put both players back in the queue"
else
  fail "the tickets did not come back to the queue (depth $(queue_depth))"
fi
if [ "${#TICKETS_B[@]}" = "2" ] \
  && score_matches "${TICKETS_B[0]}" "$QUEUED_MILLIS_A" \
  && score_matches "${TICKETS_B[1]}" "$QUEUED_MILLIS_B"; then
  ok "with the score they already had: the wait is not thrown away (the queue is ordered by queuedAt)"
else
  fail "the requeued score is not the original queuedAt (${TICKETS_B[0]:-}: '$(score_of "${TICKETS_B[0]:-}")' vs '$QUEUED_MILLIS_A')"
fi
TICKET_B_P2="$(psql "SELECT ticket_id FROM match_roster WHERE match_id = '$MATCH_B' AND player_id = '${PLAYERS[2]}'")"
REQUEUED_TICKET_STATUS=""
if [ -n "$TICKET_B_P2" ]; then
  REQUEUED_TICKET_STATUS="$(curl -fsS "$API/v1/matchmaking/tickets/$TICKET_B_P2" \
    -H "authorization: Bearer $TOKEN_B" | "$MM" json - status)"
fi
if [ -n "$TICKET_B_P2" ] && [ "$REQUEUED_TICKET_STATUS" = "SEARCHING" ]; then
  ok "and the ticket is SEARCHING again, not CONFIRMED: the player can be paired by anyone"
else
  fail "the requeued ticket is not SEARCHING"
fi
if wait_requeued "$MATCH_B" 10; then
  ok "the match is marked as requeued, so the next sweep leaves it alone"
else
  fail "requeued_at is still null after the sweep"
fi
# The scrape renders counters as doubles (1.0, not 1): compare as numbers, not as text.
if gt "$(metric_or_zero "$ENGINE_METRICS" 'matchmaking_matches_requeued_total{mode="1v1",region="NA"')" 0 \
  && gt "$(metric_or_zero "$ENGINE_METRICS" 'matchmaking_players_requeued_total{mode="1v1",region="NA"')" 1; then
  ok "the engine counted 1 match and 2 players requeued (matchmaking_matches_requeued_total)"
else
  fail "requeue counters are $(metric_or_zero "$ENGINE_METRICS" 'matchmaking_matches_requeued_total')/$(metric_or_zero "$ENGINE_METRICS" 'matchmaking_players_requeued_total')"
fi
sleep 1
connection "$MATCH_B" "$TOKEN_B"
if [ "$STATUS" = "409" ] && [ "$(field error)" = "match_rejected" ] \
  && [ "$(field reason)" = "no_capacity" ]; then
  ok "the connection info now says 409 match_rejected with the reason (no_capacity)"
else
  fail "expected 409 match_rejected/no_capacity, got $STATUS ($(cat "$OUT"))"
fi
if gt "$(metric_or_zero "$ORCH_METRICS" 'matchmaking_match_rejections_total{reason="no_capacity"')" 0; then
  ok "the rejection is counted by reason (matchmaking_match_rejections_total{reason=\"no_capacity\"})"
else
  fail "the rejection counter did not move"
fi
# The number the ADR quotes: how long the players were out of the queue because of the decision.
# It is the engine's own timer (rejection instant -> requeued_at), not the scenario's stopwatch.
DELAY_MEAN="$("$MM" ratio "$(metric_or_zero "$ENGINE_METRICS" 'matchmaking_requeue_delay_seconds_sum{mode="1v1",region="NA"')" \
  "$(metric_or_zero "$ENGINE_METRICS" 'matchmaking_requeue_delay_seconds_count{mode="1v1",region="NA"')" 3)"
if "$MM" between "$DELAY_MEAN" 0 3; then
  ok "the requeue landed ${DELAY_MEAN}s after the rejection (matchmaking_requeue_delay_seconds)"
else
  fail "the measured rejection->requeue delay is ${DELAY_MEAN}s, expected under 3"
fi

# ---------------------------------------------------------------------------
step "4. the retry: a slot that frees before the deadline is used"
# ---------------------------------------------------------------------------
orchestrator_call POST "/v1/game-servers/gs-w23-a/allocations/$MATCH_A/release"
if [ "$STATUS" = "200" ] && [ "$(server_allocated gs-w23-a)" = "0" ]; then
  ok "releasing MATCH_A gives its slot back (allocated 0)"
else
  fail "release answered $STATUS with allocated '$(server_allocated gs-w23-a)'"
fi
connection "$MATCH_A" "$TOKEN_A"
if [ "$STATUS" = "409" ] && [ "$(field error)" = "match_finished" ]; then
  ok "the released match answers 409 match_finished (nobody is sent to a server for it)"
else
  fail "expected 409 match_finished for the released match, got $STATUS ($(cat "$OUT"))"
fi
BEFORE_RETRIES="$(metric_or_zero "$ORCH_METRICS" 'matchmaking_allocation_retries_total{result="allocated"')"
# A slot exists again, and the two requeued players are the head of the queue: the engine pairs
# them without anybody re-enqueueing (a second ticket per player is the bug this avoids).
register_server gs-w23-a 1
MATCH_C=""
if wait_pending 1 15; then
  MATCH_C="$(pending_matches | head -1)"
  require_match "$MATCH_C" "the requeued pair"
  ok "the requeued pair is paired again the moment a slot exists ($MATCH_C)"
else
  fail "the requeued pair did not pair after the slot came back (depth $(queue_depth))"
fi
require_match "$MATCH_C" "the requeued pair"
# The slot vanishes before the confirmation - the match is confirmed with nowhere to run.
register_server gs-w23-a 0
for player in $(match_players "$MATCH_C"); do
  confirm "$MATCH_C" "$player" &
done
wait
if wait_allocation "$MATCH_C" PENDING_ALLOCATION 20; then
  ok "the confirmed match waits: the allocator found no slot"
else
  fail "expected PENDING_ALLOCATION, found '$(allocation_status "$MATCH_C")'"
fi
# Still inside the 8 s deadline: the slot comes back, and the retry is what has to use it.
SLOT_BACK_AT=$("$MM" now)
register_server gs-w23-a 1
if wait_allocation "$MATCH_C" ALLOCATED 15; then
  RETRY_SECONDS="$("$MM" elapsed "$SLOT_BACK_AT")"
  ok "the retry allocates it ${RETRY_SECONDS}s after the slot came back: a slot that frees before the deadline is a match, not a rejection"
else
  fail "MATCH_C is '$(allocation_status "$MATCH_C")' although the slot came back before the deadline"
fi
if gt "$(metric_or_zero "$ORCH_METRICS" 'matchmaking_allocation_retries_total{result="allocated"')" "$BEFORE_RETRIES"; then
  ok "the successful retry is counted ($BEFORE_RETRIES -> $(metric_or_zero "$ORCH_METRICS" 'matchmaking_allocation_retries_total{result="allocated"'}))"
else
  fail "the retry counter did not move"
fi

# ---------------------------------------------------------------------------
step "5. the server took it and never started it: start_timeout"
# ---------------------------------------------------------------------------
if wait_allocation "$MATCH_C" REJECTED $((START_TIMEOUT + 20)); then
  ok "MATCH_C, allocated but never started, is rejected at the ${START_TIMEOUT}s start deadline"
else
  fail "MATCH_C is still '$(allocation_status "$MATCH_C")' after the start deadline"
fi
if [ "$(allocation_reason "$MATCH_C")" = "start_timeout" ]; then
  ok "the reason is start_timeout: the server is the one that failed"
else
  fail "expected start_timeout, found '$(allocation_reason "$MATCH_C")'"
fi
if wait_server_allocated gs-w23-a 0 10; then
  ok "the slot is back in the pool (allocated 0): a match that never ran does not hold capacity"
else
  fail "gs-w23-a still holds '$(server_allocated gs-w23-a)' slots"
fi
if wait_requeued "$MATCH_C" 10; then
  ok "and its players are requeued, so they are not lost with the slot"
else
  fail "MATCH_C was rejected but not requeued"
fi
note "the queue is reset for the next phase (a stale member would pair first)"
reset_queue

# ---------------------------------------------------------------------------
step "6. a server that stops heartbeating: server_failed"
# ---------------------------------------------------------------------------
reset_queue
orchestrator_call DELETE /v1/game-servers/gs-w23-a
register_server gs-w23-b 1
if [ "$(server_allocated gs-w23-b)" = "0" ]; then
  ok "gs-w23-b registered (and gs-w23-a drained): one live slot in NA"
else
  fail "gs-w23-b allocated '$(server_allocated gs-w23-b)' on registration"
fi
play_pair "${PLAYERS[4]}" "${PLAYERS[5]}"
MATCH_E="$(field_of /tmp/w23-a.json matchId)"
require_match "$MATCH_E" "the pair of the stale-server phase"
if wait_allocation "$MATCH_E" ALLOCATED 20 && [ "$(allocation_server "$MATCH_E")" = "gs-w23-b" ]; then
  ok "the pair is allocated on gs-w23-b ($MATCH_E)"
else
  fail "MATCH_E is '$(allocation_status "$MATCH_E")' on '$(allocation_server "$MATCH_E")'"
fi
# The server dies without releasing: its heartbeat stops and nobody tells the allocator.
psql "UPDATE game_servers SET last_seen = now() - make_interval(secs => $((SERVER_TTL * 6))) WHERE server_id = 'gs-w23-b'" >/dev/null
if wait_allocation "$MATCH_E" REJECTED 20; then
  ok "the watchdog rejects the match once the server is past its ${SERVER_TTL}s TTL"
else
  fail "MATCH_E is still '$(allocation_status "$MATCH_E")' after the server went stale"
fi
if [ "$(allocation_reason "$MATCH_E")" = "server_failed" ]; then
  ok "the reason is server_failed: the release that never arrived is replaced, not waited for"
else
  fail "expected server_failed, found '$(allocation_reason "$MATCH_E")'"
fi
if [ "$(server_allocated gs-w23-b)" = "0" ]; then
  ok "and its slot is given back (allocated 0), even though the server never released it"
else
  fail "gs-w23-b still holds '$(server_allocated gs-w23-b)' slots"
fi
if wait_requeued "$MATCH_E" 10; then
  ok "with its players requeued (requeued_at set)"
else
  fail "MATCH_E was rejected but not requeued"
fi

# ---------------------------------------------------------------------------
step "7. started and connected: the game server's two reports"
# ---------------------------------------------------------------------------
reset_queue
register_server gs-w23-c 2
play_pair "${PLAYERS[6]}" "${PLAYERS[7]}"
MATCH_F="$(field_of /tmp/w23-a.json matchId)"
require_match "$MATCH_F" "the pair of the started/connected phase"
if wait_allocation "$MATCH_F" ALLOCATED 20; then
  ok "MATCH_F is ALLOCATED on the fresh server ($MATCH_F)"
else
  fail "MATCH_F is '$(allocation_status "$MATCH_F")'"
fi
P_F1="${PLAYERS[6]}"
P_F2="${PLAYERS[7]}"
orchestrator_call POST "/v1/game-servers/gs-w23-c/allocations/$MATCH_F/players/$P_F1/connected"
if [ "$STATUS" = "409" ] && [ "$(field error)" = "invalid_transition" ]; then
  ok "a connection before the start is refused (409 invalid_transition)"
else
  fail "expected 409 invalid_transition, got $STATUS ($(cat "$OUT"))"
fi
orchestrator_call POST "/v1/game-servers/gs-w23-c/allocations/$MATCH_F/started"
STARTED_AT="$(field startedAt)"
if [ "$STATUS" = "200" ] && [ "$(field status)" = "STARTED" ] && [ "$(field transition)" = "APPLIED" ] \
  && [ -n "$STARTED_AT" ]; then
  ok "started moves the allocation to STARTED and stamps the instant"
else
  fail "started answered $STATUS ($(cat "$OUT"))"
fi
orchestrator_call POST "/v1/game-servers/gs-w23-c/allocations/$MATCH_F/started"
if [ "$STATUS" = "200" ] && [ "$(field transition)" = "ALREADY" ] && [ "$(field startedAt)" = "$STARTED_AT" ]; then
  ok "a repeated started is idempotent and does not move the instant"
else
  fail "the second started answered $STATUS with '$(field transition)'/'$(field startedAt)'"
fi
orchestrator_call POST "/v1/game-servers/gs-w23-c/allocations/$MATCH_F/players/$P_F1/connected"
CONNECTED_AT="$(field connectedAt)"
if [ "$STATUS" = "200" ] && [ "$(field transition)" = "APPLIED" ] && [ -n "$CONNECTED_AT" ]; then
  ok "$P_F1 is connected (connected_at stamped)"
else
  fail "connected answered $STATUS ($(cat "$OUT"))"
fi
orchestrator_call POST "/v1/game-servers/gs-w23-c/allocations/$MATCH_F/players/$P_F1/connected"
if [ "$STATUS" = "200" ] && [ "$(field transition)" = "ALREADY" ] && [ "$(field connectedAt)" = "$CONNECTED_AT" ]; then
  ok "a reconnecting player is idempotent, and the first instant is the one that counts"
else
  fail "the second connected answered $STATUS with '$(field transition)'"
fi
orchestrator_call POST "/v1/game-servers/gs-w23-c/allocations/$MATCH_F/players/w23-not-in-it/connected"
if [ "$STATUS" = "404" ] && [ "$(field error)" = "player_not_in_match" ]; then
  ok "a player outside the roster is not connected (404 player_not_in_match)"
else
  fail "expected 404 player_not_in_match, got $STATUS ($(cat "$OUT"))"
fi
connection "$MATCH_F" "$(login "$P_F1")"
if [ "$STATUS" = "200" ] && [ "$(field status)" = "STARTED" ]; then
  ok "a STARTED match still hands out the address and the token (status STARTED)"
else
  fail "the connection info for the STARTED match answered $STATUS ($(cat "$OUT"))"
fi

# ---------------------------------------------------------------------------
step "8. no-show: one absent player is marked, a match nobody joined is rejected"
# ---------------------------------------------------------------------------
register_server gs-w23-c 2
if wait_no_show "$MATCH_F" "$P_F2" $((CONNECT_WINDOW + 15)); then
  ok "the player who never connected is marked (no_show_at) after the ${CONNECT_WINDOW}s window"
else
  fail "$P_F2 was not marked absent after the connect window"
fi
if [ -z "$(player_no_show "$MATCH_F" "$P_F1")" ]; then
  ok "the player who connected is not marked: the absent one pays, not the present one"
else
  fail "$P_F1 was marked absent although he connected"
fi
if [ "$(allocation_status "$MATCH_F")" = "STARTED" ]; then
  ok "and the match keeps running (still STARTED): one absent player does not dissolve it"
else
  fail "MATCH_F is '$(allocation_status "$MATCH_F")' after the no-show marking"
fi
if gt "$(metric_or_zero "$ORCH_METRICS" 'matchmaking_no_shows_total')" 0; then
  ok "the absence is counted (matchmaking_no_shows_total)"
else
  fail "the no-show counter did not move"
fi

register_server gs-w23-c 2
reset_queue
play_pair "${PLAYERS[8]}" "${PLAYERS[9]}"
MATCH_G="$(field_of /tmp/w23-a.json matchId)"
require_match "$MATCH_G" "the pair of the all-absent phase"
if wait_allocation "$MATCH_G" ALLOCATED 20; then
  ok "a second match for the all-absent case is allocated ($MATCH_G)"
else
  fail "MATCH_G is '$(allocation_status "$MATCH_G")'"
fi
orchestrator_call POST "/v1/game-servers/gs-w23-c/allocations/$MATCH_G/started"
if [ "$STATUS" = "200" ] && [ "$(field status)" = "STARTED" ]; then
  ok "it is started and nobody connects"
else
  fail "started on MATCH_G answered $STATUS ($(cat "$OUT"))"
fi
if wait_allocation "$MATCH_G" REJECTED $((CONNECT_WINDOW + 15)); then
  ok "at the connect window's end the match is rejected: nobody to play"
else
  fail "MATCH_G is '$(allocation_status "$MATCH_G")' after the window"
fi
if [ "$(allocation_reason "$MATCH_G")" = "all_no_show" ]; then
  ok "the reason is all_no_show (distinct from start_timeout and server_failed)"
else
  fail "expected all_no_show, found '$(allocation_reason "$MATCH_G")'"
fi
if wait_server_allocated gs-w23-c 1 10; then
  ok "its slot is back in the pool (only MATCH_F still holds one: allocated 1)"
else
  fail "gs-w23-c holds '$(server_allocated gs-w23-c)' slots, expected 1"
fi
if wait_requeued "$MATCH_G" 10; then
  ok "and its players are requeued: a no-show match is not a lost match"
else
  fail "MATCH_G was rejected but not requeued"
fi

# ---------------------------------------------------------------------------
step "9. the invariants, read from the database"
# ---------------------------------------------------------------------------
OVER="$(psql "SELECT count(*) FROM game_servers WHERE allocated > capacity")"
if [ "$OVER" = "0" ]; then
  ok "no server is over-committed (allocated <= capacity on every row)"
else
  fail "$OVER servers have allocated > capacity"
fi
ALLOCATED_ROWS="$(psql "SELECT count(*) FROM match_allocations WHERE status IN ('ALLOCATED', 'STARTED') AND server_id LIKE 'gs-w23-%'")"
ALLOCATED_SUM="$(psql "SELECT COALESCE(sum(allocated), 0) FROM game_servers WHERE server_id LIKE 'gs-w23-%'")"
if [ "$ALLOCATED_ROWS" = "$ALLOCATED_SUM" ]; then
  ok "the ALLOCATED rows and the pool's sum(allocated) agree ($ALLOCATED_ROWS)"
else
  fail "match_allocations has $ALLOCATED_ROWS ALLOCATED rows but the pool holds $ALLOCATED_SUM slots"
fi
UNEXPLAINED="$(psql "SELECT count(*) FROM match_allocations a WHERE status = 'REJECTED' AND (rejection_reason IS NULL OR rejected_at IS NULL) AND EXISTS (SELECT 1 FROM match_roster r WHERE r.match_id = a.match_id AND r.player_id LIKE 'w23-%')")"
if [ "$UNEXPLAINED" = "0" ]; then
  ok "every rejected match of the run says why and when (rejection_reason + rejected_at)"
else
  fail "$UNEXPLAINED rejected matches have no reason or no timestamp"
fi
CONTRADICTION="$(psql "SELECT count(*) FROM match_roster WHERE connected_at IS NOT NULL AND no_show_at IS NOT NULL")"
if [ "$CONTRADICTION" = "0" ]; then
  ok "no player is both connected and absent (the two states are exclusive)"
else
  fail "$CONTRADICTION roster rows are connected and no-show at the same time"
fi
ORPHANS="$(psql "SELECT count(*) FROM match_allocations a WHERE a.server_id LIKE 'gs-w23-%' AND NOT EXISTS (SELECT 1 FROM match_roster r WHERE r.match_id = a.match_id)")"
if [ "$ORPHANS" = "0" ]; then
  ok "every match of the run has its roster (the projection writes both in one transaction)"
else
  fail "$ORPHANS match rows have no roster: nobody could be requeued"
fi
TICKETLESS="$(psql "SELECT count(*) FROM match_roster r WHERE r.player_id LIKE 'w23-%' AND r.ticket_id IS NULL")"
if [ "$TICKETLESS" = "0" ]; then
  ok "every roster row of the run carries its ticket: a requeue can find it"
else
  fail "$TICKETLESS roster rows of the run carry no ticket id"
fi

# ---------------------------------------------------------------------------
step "10. cleanup"
# ---------------------------------------------------------------------------
clean_state
if [ "$(psql "SELECT count(*) FROM game_servers WHERE server_id LIKE 'gs-w23-%'")" = "0" ] \
  && [ "$(psql "SELECT count(*) FROM match_allocations a WHERE a.server_id LIKE 'gs-w23-%' OR EXISTS (SELECT 1 FROM match_roster r WHERE r.match_id = a.match_id AND r.player_id LIKE 'w23-%')")" = "0" ]; then
  ok "the scenario's pool rows and allocations are gone"
else
  fail "the scenario left rows behind"
fi
if [ "$(queue_depth)" = "0" ]; then
  ok "and the queue is empty (the services stop at exit)"
else
  fail "the queue still holds $(queue_depth) members"
fi

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
