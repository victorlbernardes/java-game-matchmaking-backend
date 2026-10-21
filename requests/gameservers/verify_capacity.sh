#!/usr/bin/env bash
#
# W22 - a confirmed match with no server to run it: the pool, the atomic allocation, and the
# capacity-aware pairing (ADR 0031).
#
# What it proves:
#   1. with **no pool registered**, nothing constrains matching - the behaviour of W1-W21 survives
#      the sprint (a deployment that never registered a game server is not a deployment that
#      refuses to match anybody). This phase runs in **SA**, and that is not decoration: the match
#      it leaves confirmed-and-waiting is exactly what W23's retry would spend the first slot of
#      the NA pool on, so the wait it asserts lives in a region the rest of the scenario never
#      fills - a region per question, instead of one region answering two;
#   2. with a pool that has **no room**, the engine forms nothing and the queue waits: the tickets
#      stay SEARCHING, no match is announced, and `matchmaking_pairing_suspended_total` says why;
#   3. a slot appears -> **exactly one** match forms, and once its players confirm the orchestrator
#      allocates it: `match_allocations` names the server, and the pool's `allocated`/`free` agree;
#   4. capacity is consumed by the allocation, so the next pair waits - and `release` gives the slot
#      back, exactly once (a repeated release cannot return a second slot);
#   5. two confirmed matches cannot take the same last slot: the loser stays PENDING_ALLOCATION and
#      the server is never over-committed;
#   6. a server that stops heartbeating leaves the pool by TTL, with no sweep running;
#   7. shrinking a server below what it is already running is refused (409), instead of silently
#      over-committing.
#
# The scenario plays the game servers too: every phase re-registers the ones it needs, which is what
# an agent's heartbeat does. That is not decoration - with SERVER_TTL_SECONDS=10 a server that is not
# re-registered really does leave the pool, and phase 6 depends on it.
#
# Preconditions: docker compose up -d redis postgres kafka schema-registry, auth-service on 8081,
# the tree built (`mvn -o -B install -DskipTests`, runbook item 1). The script stops any engine and
# any orchestrator it finds - it needs to own the pairing loop and the consumer group - and starts
# its own API (8098, metrics 9200), engine (metrics 9101) and orchestrator (8086, metrics 9094).
#
# Usage:
#   bash requests/gameservers/verify_capacity.sh
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AUTH="${AUTH:-http://localhost:8081}"
API="http://localhost:${API_PORT:-8098}"
ENGINE_METRICS="http://localhost:${ENGINE_METRICS_PORT:-9101}/metrics"
ORCHESTRATOR="http://localhost:${ORCHESTRATOR_PORT:-8086}"
ORCH_METRICS="http://localhost:${ORCH_METRICS_PORT:-9094}/metrics"
ORCH_TOKEN="${ORCH_TOKEN:-w22-game-server-token}"
# The TTL has to outlive a phase: since W23 a match on a server whose heartbeat stopped is
# rejected (`server_failed`) and its players are requeued, so a TTL shorter than a phase would
# dissolve the matches this scenario is watching. Phase 8 backdates `last_seen` instead of waiting
# the TTL out - the same technique W23's own scenario uses.
SERVER_TTL="${SERVER_TTL:-120}"
# W23 gave a confirmed match two deadlines (a server within ALLOCATION_TIMEOUT, a start within
# START_TIMEOUT). This scenario tests W22's properties - a match that waits for a slot, one that
# holds the slot while it runs - so both are set long: a phase has to outlive the other sprint's
# timers, not be rewritten by them. W23's own scenario runs them short and covers the deadlines.
ALLOCATION_TIMEOUT="${ALLOCATION_TIMEOUT:-600}"
START_TIMEOUT="${START_TIMEOUT:-600}"
REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
MM="$REPO_ROOT/requests/mm.sh"
OUT="/tmp/w22-out.json"
API_LOG="${API_LOG:-/tmp/w22-api.log}"
ENGINE_LOG="${ENGINE_LOG:-/tmp/w22-engine.log}"
ORCH_LOG="${ORCH_LOG:-/tmp/w22-orchestrator.log}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
gt()   { "$MM" gt "$1" "$2"; }
eq()   { "$MM" eq "$1" "$2"; }

redis()  { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }
psql()   { docker exec -i "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -tAc "$1" 2>/dev/null; }

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
  for pid_file in /tmp/w22-api.pid /tmp/w22-engine.pid /tmp/w22-orchestrator.pid; do
    if [ -f "$pid_file" ]; then
      kill -9 "$(cat "$pid_file")" 2>/dev/null || true
      rm -f "$pid_file"
    fi
  done
  # The engine owns the pairing loop and the orchestrator owns the consumer group: a leftover of
  # either would pair the arrivals while they are being counted, or take half the events.
  pkill -f MatchmakingEngineApplication 2>/dev/null || true
  pkill -f MatchOrchestratorApplication 2>/dev/null || true
}
trap stop_services EXIT

start_api() {
  free_port 8098
  free_port 9200
  (
    cd "$REPO_ROOT" || exit 1
    PORT=8098 METRICS_PORT=9200 INSTANCE_ID=api-w22 \
      mvn -o -q -pl services/matchmaking-api exec:java \
        -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication \
        </dev/null >"$API_LOG" 2>&1 &
    echo $! >/tmp/w22-api.pid
  )
  wait_http "$API/health" 120
}

start_engine() {
  free_port 9101
  (
    cd "$REPO_ROOT" || exit 1
    METRICS_PORT=9101 SERVER_TTL_SECONDS="$SERVER_TTL" \
      mvn -o -q -pl services/matchmaking-engine exec:java \
        -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication \
        </dev/null >"$ENGINE_LOG" 2>&1 &
    echo $! >/tmp/w22-engine.pid
  )
  wait_http "$ENGINE_METRICS" 180
}

start_orchestrator() {
  free_port 8086
  free_port 9094
  (
    cd "$REPO_ROOT" || exit 1
    ORCHESTRATOR_PORT=8086 METRICS_PORT=9094 GAME_SERVER_TOKEN="$ORCH_TOKEN" \
      SERVER_TTL_SECONDS="$SERVER_TTL" \
      ALLOCATION_TIMEOUT_SECONDS="$ALLOCATION_TIMEOUT" START_TIMEOUT_SECONDS="$START_TIMEOUT" \
      mvn -o -q -pl services/match-orchestrator exec:java \
        -Dexec.mainClass=com.victor.matchmaking.orchestrator.MatchOrchestratorApplication \
        </dev/null >"$ORCH_LOG" 2>&1 &
    echo $! >/tmp/w22-orchestrator.pid
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
  "$MM" queue "$@" >"$out" 2>/tmp/w22-probe.err
  local code=$?
  if [ $code -ne 0 ]; then
    printf '        probe error: %s\n' "$(tail -3 /tmp/w22-probe.err)"
  fi
  return $code
}

probe() { # <json-args...> -> writes $OUT
  probe_to "$OUT" "$@"
}

field() { # <key> -> the value from $OUT
  "$MM" json "$OUT" "$1"
}

field_of() { # <file> <key>
  "$MM" json "$1" "$2"
}

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

register_server() { # <serverId> <capacity> [gameMode]
  orchestrator_call PUT "/v1/game-servers/$1" \
    "{\"region\":\"NA\",\"gameModes\":[\"${3:-1v1}\"],\"capacity\":$2,\"endpoint\":\"10.0.0.7:7777\"}"
}

capacity_of() { # <gameMode> <region> -> body in $OUT, status in $STATUS
  orchestrator_call GET "/v1/capacity?gameMode=$1&region=$2"
}

pending_count() { redis eval "return #redis.call('keys', 'match-pending:*')" 0; }

pending_matches() { # every match waiting for a confirmation, newest key order
  redis eval "local keys = redis.call('keys', 'match-pending:*') table.sort(keys) local out = {} for _, k in ipairs(keys) do out[#out + 1] = string.gsub(k, '^match%-pending:', '') end return out" 0
}

match_players() { # <matchId> -> the roster's player ids, one per line
  redis get "match-pending:$1" | "$MM" json-array - players playerId | tr ' ' '\n'
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

queue_depth() { # players waiting in the 1v1 NA queue (all partitions)
  redis eval "local n = 0 for _, k in ipairs(redis.call('keys', 'queue:1v1:NA*')) do n = n + redis.call('zcard', k) end return n" 0
}

allocation_status() { psql "SELECT status FROM match_allocations WHERE match_id = '$1'"; }

forget_match() { # <matchId>
  # A confirmed match that never got a server now retries until its deadline (W23), which means it
  # takes the next free slot the pool offers - including the ones later phases create. Where this
  # scenario abandons such a match on purpose, the retry has to stop with it: the row (the roster
  # cascades) and the keys that name it go together.
  psql "DELETE FROM match_allocations WHERE match_id = '$1'" >/dev/null
  redis del "match-pending:$1" >/dev/null
  redis del "match-confirmed:$1" >/dev/null
  redis del "match-confirmed-claim:$1" >/dev/null
}
allocation_server() { psql "SELECT COALESCE(server_id, '') FROM match_allocations WHERE match_id = '$1'"; }
server_allocated() { psql "SELECT allocated FROM game_servers WHERE server_id = '$1'"; }

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

clean_state() {
  redis eval "for _, k in ipairs(redis.call('keys', 'queue:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  redis eval "for _, k in ipairs(redis.call('keys', 'ticket:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  redis eval "for _, k in ipairs(redis.call('keys', 'match-pending:*')) do redis.call('del', k) end return 1" 0 >/dev/null
  # A match confirmed with no pool is an allocation row with server_id NULL, which the server
  # prefix does not reach: W23's roster is what names the scenario's own matches (and the roster
  # itself is ON DELETE CASCADE, so deleting the allocation is enough).
  psql "DELETE FROM match_allocations WHERE match_id IN (SELECT match_id FROM match_roster WHERE player_id LIKE 'w22-%')" >/dev/null
  psql "DELETE FROM match_allocations WHERE server_id LIKE 'gs-w22-%'" >/dev/null
  psql "DELETE FROM game_servers WHERE server_id LIKE 'gs-w22-%'" >/dev/null
}

queue_region() { printf '%s' "${1:-NA}"; }

enqueue_pair() { # <player-a> <player-b> <wait-seconds> [region] -> /tmp/w22-a.json, /tmp/w22-b.json
  local token_a token_b region
  region="$(queue_region "${4:-NA}")"
  token_a="$(login "$1")"
  token_b="$(login "$2")"
  probe_to /tmp/w22-a.json enqueue --api "$API" --token "$token_a" --player "$1" --region "$region" \
    --redis-port 6379 --wait "$3" &
  local pid_a=$!
  probe_to /tmp/w22-b.json enqueue --api "$API" --token "$token_b" --player "$2" --region "$region" \
    --redis-port 6379 --wait "$3" &
  local pid_b=$!
  wait "$pid_a"
  wait "$pid_b"
}

play_pair() { # <player-a> <player-b> [region] -> /tmp/w22-a.json, /tmp/w22-b.json (both confirmed)
  local token_a token_b region
  region="$(queue_region "${3:-NA}")"
  token_a="$(login "$1")"
  token_b="$(login "$2")"
  probe_to /tmp/w22-a.json play --api "$API" --token "$token_a" --player "$1" --region "$region" --redis-port 6379 &
  local pid_a=$!
  probe_to /tmp/w22-b.json play --api "$API" --token "$token_b" --player "$2" --region "$region" --redis-port 6379 &
  local pid_b=$!
  wait "$pid_a"
  wait "$pid_b"
}

confirm() { # <matchId> <player>
  probe confirm --redis-port 6379 --match-id "$1" --player "$2" >/dev/null
}

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
if docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
  ok "$REDIS_CONTAINER is running"
else
  fail "$REDIS_CONTAINER is not running (docker compose up -d redis postgres kafka schema-registry)"
fi
if docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
  ok "$POSTGRES_CONTAINER is running (the pool lives there)"
else
  fail "$POSTGRES_CONTAINER is not running"
fi
if docker ps --format '{{.Names}}' | grep -q "kafka"; then
  ok "Kafka is running (MATCH_CONFIRMED is how a match reaches the allocator)"
else
  fail "Kafka is not running"
fi
if wait_http "$AUTH/.well-known/jwks.json" 5; then
  ok "auth-service answers on 8081 (the scenario uses the shared one: it needs no region claim)"
else
  fail "auth-service is not answering on 8081"
fi
if [ -f "services/match-orchestrator/target/classes/com/victor/matchmaking/orchestrator/MatchOrchestratorApplication.class" ]; then
  ok "the tree is built (mvn -o -B install -DskipTests, runbook item 1)"
else
  fail "services/match-orchestrator is not built"
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
  ok "API on 8098"
else
  fail "the API did not start (see $API_LOG)"
fi
if start_engine; then
  ok "engine on metrics 9101 (it is the only pairing loop running)"
else
  fail "the engine did not start (see $ENGINE_LOG)"
fi
if start_orchestrator; then
  ok "orchestrator on 8086 (metrics 9094, SERVER_TTL_SECONDS=$SERVER_TTL)"
else
  fail "the orchestrator did not start (see $ORCH_LOG)"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

PLAYERS_JSON="$("$MM" queue players --count 14 --prefix w22-)"
PLAYERS=($(printf '%s' "$PLAYERS_JSON" | "$MM" json-array - players))
note "14 players share one skill: ${PLAYERS[*]:0:3} ..."

# ---------------------------------------------------------------------------
step "2. no pool registered: nothing constrains matching (the W1-W21 default, in SA)"
# ---------------------------------------------------------------------------
STATUS="$(curl -sS -o "$OUT" -w '%{http_code}' "$ORCHESTRATOR/v1/capacity?gameMode=1v1&region=NA")"
if [ "$STATUS" = "401" ]; then
  ok "the pool's endpoints are internal: no game server token, no answer (401)"
else
  fail "without the token the capacity endpoint answered $STATUS, expected 401"
fi

capacity_of 1v1 NA
if [ "$STATUS" = "200" ] && [ "$(field authoritative)" = "False" ]; then
  ok "NA has no pool: capacity is unknown (authoritative=false), not zero"
else
  fail "an empty pool must read as unknown (status=$STATUS body=$(cat "$OUT"))"
fi

BEFORE_NO_CAPACITY="$(metric_or_zero "$ORCH_METRICS" 'matchmaking_allocations_total{result="no_capacity"')"
play_pair "${PLAYERS[0]}" "${PLAYERS[1]}" SA
MATCH_A="$(field_of /tmp/w22-a.json matchId)"
CONFIRMED_A="$(field_of /tmp/w22-a.json confirmed)"
if [ -n "$MATCH_A" ] && [ "$CONFIRMED_A" = "True" ]; then
  ok "with no pool the players pair and confirm as they did before W22"
else
  fail "an empty pool must not stop matching (see /tmp/w22-a.json: $(cat /tmp/w22-a.json))"
fi
if wait_allocation "$MATCH_A" PENDING_ALLOCATION 10; then
  ok "and the confirmed match is PENDING_ALLOCATION: no server took it (W23's retry looks in SA, and SA has no pool)"
else
  fail "expected the match to stay PENDING_ALLOCATION, found '$(allocation_status "$MATCH_A")'"
fi
AFTER_NO_CAPACITY="$(metric_or_zero "$ORCH_METRICS" 'matchmaking_allocations_total{result="no_capacity"')"
if gt "$AFTER_NO_CAPACITY" "$BEFORE_NO_CAPACITY"; then
  ok "the allocator counted the refusal ($BEFORE_NO_CAPACITY -> $AFTER_NO_CAPACITY)"
else
  fail "the no-capacity counter did not move ($BEFORE_NO_CAPACITY -> $AFTER_NO_CAPACITY)"
fi

# ---------------------------------------------------------------------------
step "3. a pool with no room: the queue waits instead of forming a match nobody can host"
# ---------------------------------------------------------------------------
register_server gs-w22-zero 0
if [ "$STATUS" = "200" ] && [ "$(field state)" = "READY" ] && [ "$(field free)" = "0" ]; then
  ok "gs-w22-zero registered with capacity 0 (state READY, free 0): a real server, full"
else
  fail "registering a server answered $STATUS ($(cat "$OUT"))"
fi

capacity_of 1v1 NA
if [ "$STATUS" = "200" ] && [ "$(field authoritative)" = "True" ] && [ "$(field free)" = "0" ] \
    && [ "$(field servers)" = "1" ]; then
  ok "NA now has a pool and it is authoritative: free 0, 1 server"
else
  fail "a registered pool must make capacity authoritative (status=$STATUS body=$(cat "$OUT"))"
fi

sleep 2
FREE_GAUGE="$(metric_or_zero "$ENGINE_METRICS" 'matchmaking_capacity_free{mode="1v1",region="NA"')"
if eq "$FREE_GAUGE" 0; then
  ok "the engine reads free=0 for 1v1/NA (matchmaking_capacity_free)"
else
  fail "the engine's capacity gauge reads '$FREE_GAUGE', expected 0"
fi

BEFORE_SUSPENSIONS="$(metric_or_zero "$ENGINE_METRICS" 'matchmaking_pairing_suspended_total{mode="1v1",region="NA"')"
enqueue_pair "${PLAYERS[2]}" "${PLAYERS[3]}" 4
sleep 2
if [ "$(queue_depth)" = "2" ]; then
  ok "both arrivals are in the queue (depth 2): they were admitted, not refused"
else
  fail "expected the two players waiting, queue depth is '$(queue_depth)'"
fi
if [ "$(pending_count)" = "0" ]; then
  ok "and no match was formed: the region has nowhere to run it"
else
  fail "a match was formed with no capacity: $(pending_matches)"
fi
TOKEN_C="$(login "${PLAYERS[2]}")"
TICKET_C="$(field_of /tmp/w22-a.json ticketId)"
probe ticket --api "$API" --token "$TOKEN_C" --ticket-id "$TICKET_C"
if [ "$(field ticketStatus)" = "SEARCHING" ]; then
  ok "the ticket is still SEARCHING: the wait continues, nothing was lost"
else
  fail "expected the waiting ticket to be SEARCHING, got '$(field ticketStatus)'"
fi
AFTER_SUSPENSIONS="$(metric_or_zero "$ENGINE_METRICS" 'matchmaking_pairing_suspended_total{mode="1v1",region="NA"')"
if gt "$AFTER_SUSPENSIONS" "$BEFORE_SUSPENSIONS"; then
  ok "the suspension is counted ($BEFORE_SUSPENSIONS -> $AFTER_SUSPENSIONS): the reason is visible"
else
  fail "the pairing suspension counter did not move ($BEFORE_SUSPENSIONS -> $AFTER_SUSPENSIONS)"
fi

# ---------------------------------------------------------------------------
step "4. a slot appears: exactly one match forms, and the allocator fills it"
# ---------------------------------------------------------------------------
register_server gs-w22-one 1
capacity_of 1v1 NA
if [ "$(field free)" = "1" ]; then
  ok "gs-w22-one offers one slot: capacity free 1"
else
  fail "expected free 1 after registering a server with capacity 1, got $(cat "$OUT")"
fi
if wait_pending 1 15; then
  ok "the players who were waiting formed a match within one poll of the slot appearing"
else
  fail "the queue did not pair after capacity appeared (depth $(queue_depth))"
fi
if [ "$(queue_depth)" = "0" ]; then
  ok "and they left the queue (depth 0)"
else
  fail "the queue still holds $(queue_depth) players"
fi

MATCH_WAITING="$(pending_matches | head -1)"
note "match formed: $MATCH_WAITING"
for player in $(match_players "$MATCH_WAITING"); do
  confirm "$MATCH_WAITING" "$player"
done
if wait_allocation "$MATCH_WAITING" ALLOCATED 15; then
  ok "the confirmed match was allocated (status ALLOCATED)"
else
  fail "the match was not allocated: '$(allocation_status "$MATCH_WAITING")' (log $ORCH_LOG)"
fi
if [ "$(allocation_server "$MATCH_WAITING")" = "gs-w22-one" ]; then
  ok "and the row names the server that took it (gs-w22-one)"
else
  fail "expected server gs-w22-one, got '$(allocation_server "$MATCH_WAITING")'"
fi
if wait_server_allocated gs-w22-one 1 10; then
  ok "the pool agrees: gs-w22-one allocated 1, free 0"
else
  fail "gs-w22-one allocated is '$(server_allocated gs-w22-one)'"
fi
if [ "$(metric_or_zero "$ORCH_METRICS" 'matchmaking_allocations_total{result="allocated"')" != "0" ]; then
  ok "the allocation counter moved (matchmaking_allocations_total{result=\"allocated\"})"
else
  fail "the allocation counter did not move"
fi

# ---------------------------------------------------------------------------
step "5. the slot is gone: the next pair waits"
# ---------------------------------------------------------------------------
enqueue_pair "${PLAYERS[4]}" "${PLAYERS[5]}" 4
sleep 2
if [ "$(pending_count)" = "0" ]; then
  ok "no match was formed: the only slot is running a match"
else
  fail "a second match was formed with no free slot: $(pending_matches)"
fi
if [ "$(queue_depth)" = "2" ]; then
  ok "the new pair waits in the queue"
else
  fail "expected 2 players waiting, depth is '$(queue_depth)'"
fi

# ---------------------------------------------------------------------------
step "6. release: the slot comes back, and only once"
# ---------------------------------------------------------------------------
register_server gs-w22-one 1 # the agent's heartbeat: the server is still here, still full
orchestrator_call POST "/v1/game-servers/gs-w22-one/allocations/$MATCH_WAITING/release"
if [ "$STATUS" = "200" ] && [ "$(field allocated)" = "0" ] && [ "$(field free)" = "1" ]; then
  ok "release returned the slot (allocated 0, free 1)"
else
  fail "release answered $STATUS ($(cat "$OUT"))"
fi
orchestrator_call POST "/v1/game-servers/gs-w22-one/allocations/$MATCH_WAITING/release"
if [ "$STATUS" = "404" ] && [ "$("$MM" json /tmp/w22-out.json error)" = "allocation_not_found" ]; then
  ok "a repeated release finds nothing to flip (404 allocation_not_found)"
else
  fail "the second release answered $STATUS ($(cat "$OUT"))"
fi
capacity_of 1v1 NA
if [ "$(field free)" = "1" ]; then
  ok "and the slot was returned exactly once (free 1, not 2)"
else
  fail "expected free 1 after two releases, got $(cat "$OUT")"
fi
if wait_pending 1 15 && [ "$(queue_depth)" = "0" ]; then
  ok "the pair that was waiting took the freed slot"
else
  fail "the queue did not pair after the release (depth $(queue_depth))"
fi
MATCH_SECOND="$(pending_matches | head -1)"
for player in $(match_players "$MATCH_SECOND"); do
  confirm "$MATCH_SECOND" "$player"
done
if wait_allocation "$MATCH_SECOND" ALLOCATED 15; then
  ok "and the allocator filled it again (ALLOCATED on gs-w22-one)"
else
  fail "the second match was not allocated: '$(allocation_status "$MATCH_SECOND")'"
fi

# ---------------------------------------------------------------------------
step "7. two confirmed matches, one slot: the allocator refuses to over-commit"
# ---------------------------------------------------------------------------
# A second server with room for two, two pairs formed while it has room, and then the room shrinks
# to one before the confirmations are answered - the race the transaction exists for.
register_server gs-w22-two 2
enqueue_pair "${PLAYERS[6]}" "${PLAYERS[7]}" 3
enqueue_pair "${PLAYERS[8]}" "${PLAYERS[9]}" 3
if wait_pending 2 15; then
  ok "two matches formed against the two slots the pool offered"
else
  fail "expected two matches, pending: $(pending_matches)"
fi
register_server gs-w22-two 1
capacity_of 1v1 NA
if [ "$(field free)" = "1" ]; then
  ok "the pool shrank to one free slot before anybody confirmed"
else
  fail "expected free 1 after shrinking gs-w22-two, got $(cat "$OUT")"
fi

MATCHES=($(pending_matches))
note "confirmed together: ${MATCHES[*]:-none}"
for match in "${MATCHES[@]}"; do
  for player in $(match_players "$match"); do
    confirm "$match" "$player"
  done
done
if [ "${#MATCHES[@]}" -gt 0 ]; then
  if wait_allocation "${MATCHES[0]}" ALLOCATED 15; then :; fi
fi
sleep 2

ALLOCATED_NOW=0
PENDING_NOW=0
for match in "${MATCHES[@]}"; do
  status="$(allocation_status "$match")"
  note "$match -> $status"
  [ "$status" = "ALLOCATED" ] && ALLOCATED_NOW=$((ALLOCATED_NOW + 1))
  [ "$status" = "PENDING_ALLOCATION" ] && PENDING_NOW=$((PENDING_NOW + 1))
done
if [ "$ALLOCATED_NOW" = "1" ] && [ "$PENDING_NOW" = "1" ]; then
  ok "exactly one of the two confirmations got the slot; the other stays PENDING_ALLOCATION"
else
  fail "expected 1 ALLOCATED + 1 PENDING_ALLOCATION, got $ALLOCATED_NOW + $PENDING_NOW"
fi
if [ "$(server_allocated gs-w22-two)" = "1" ]; then
  ok "gs-w22-two is not over-committed (allocated 1, capacity 1)"
else
  fail "gs-w22-two allocated is '$(server_allocated gs-w22-two)'"
fi
# The loser stays PENDING_ALLOCATION for as long as this scenario runs (the deadline is long on
# purpose), so its retry would take the first slot phase 8 frees. It is abandoned here, which is
# exactly the state the assertion above described.
for match in "${MATCHES[@]}"; do
  if [ "$(allocation_status "$match")" = "PENDING_ALLOCATION" ]; then
    forget_match "$match"
    note "abandoned on purpose: $match (its retry would take the next free slot)"
  fi
done

# ---------------------------------------------------------------------------
step "8. a server that stops heartbeating leaves the pool by TTL (no sweep runs)"
# ---------------------------------------------------------------------------
# The scenario is the agents: it re-registers the two servers that are running matches, so their
# only state is "full", and then registers a fresh one and says nothing more about it.
register_server gs-w22-one 1
register_server gs-w22-two 1
register_server gs-w22-ttl 1
capacity_of 1v1 NA
if [ "$(field free)" = "1" ]; then
  ok "the fresh server adds one slot (free 1)"
else
  fail "expected free 1 after registering gs-w22-ttl, got $(cat "$OUT")"
fi
# Backdate instead of waiting: what is under test is the pool's reading of `last_seen`, not the
# passage of time, and waiting $SERVER_TTL out would turn a 2 s check into two minutes.
psql "UPDATE game_servers SET last_seen = last_seen - interval '$((SERVER_TTL * 6)) seconds' WHERE server_id = 'gs-w22-ttl'" >/dev/null
note "gs-w22-ttl last_seen backdated by $((SERVER_TTL * 6))s (SERVER_TTL_SECONDS=$SERVER_TTL)"
sleep 1
capacity_of 1v1 NA
if [ "$(field free)" = "0" ]; then
  ok "the silent server is out of the pool: free dropped to 0 with nothing running a sweep"
else
  fail "expected free 0 after the TTL, got $(cat "$OUT")"
fi
orchestrator_call GET /v1/game-servers
if [ "$("$MM" json-element /tmp/w22-out.json servers serverId gs-w22-ttl state)" = "STALE" ]; then
  ok "and the listing derives STALE for it (the state is computed, never stored)"
else
  fail "expected gs-w22-ttl to be listed as STALE ($(cat "$OUT"))"
fi
enqueue_pair "${PLAYERS[10]}" "${PLAYERS[11]}" 3
sleep 2
if [ "$(pending_count)" = "0" ]; then
  ok "a pair arriving now waits: the pool has no room it can still offer"
else
  fail "expected no new match, pending: $(pending_matches)"
fi
register_server gs-w22-ttl 1
if wait_pending 1 15; then
  ok "and it pairs the moment the server speaks again"
else
  fail "the queue did not pair after the heartbeat came back"
fi
# Close the match this phase formed: the next phase needs a queue with nothing pending and nothing
# in flight, and a ticket left CONFIRMING would be requeued by the 30 s sweep in the middle of it.
TTL_MATCHES=($(pending_matches))
if [ "${#TTL_MATCHES[@]}" -gt 0 ]; then
  for player in $(match_players "${TTL_MATCHES[0]}"); do
    confirm "${TTL_MATCHES[0]}" "$player" &
  done
  wait
  if wait_allocation "${TTL_MATCHES[0]}" ALLOCATED 20; then
    ok "and it is allocated the moment the slot is back"
  else
    fail "the pair formed after the heartbeat was not allocated ($(allocation_status "${TTL_MATCHES[0]}"))"
  fi
fi

# ---------------------------------------------------------------------------
step "9. shrinking below what is allocated is refused"
# ---------------------------------------------------------------------------
orchestrator_call PUT /v1/game-servers/gs-w22-two \
  '{"region":"NA","gameModes":["1v1"],"capacity":0,"endpoint":"10.0.0.7:7777"}'
if [ "$STATUS" = "409" ] && [ "$("$MM" json /tmp/w22-out.json error)" = "capacity_below_allocated" ]; then
  ok "a server running a match cannot be shrunk to 0 (409 capacity_below_allocated)"
else
  fail "expected 409 capacity_below_allocated, got $STATUS ($(cat "$OUT"))"
fi
if [ "$(psql "SELECT capacity || '/' || allocated FROM game_servers WHERE server_id = 'gs-w22-two'")" = "1/1" ]; then
  ok "and the refused registration left the row untouched (capacity 1, allocated 1)"
else
  fail "gs-w22-two is now '$(psql "SELECT capacity || '/' || allocated FROM game_servers WHERE server_id = 'gs-w22-two'")'"
fi

# ---------------------------------------------------------------------------
step "10. a drained server takes no new match, and its heartbeat ends the drain"
# ---------------------------------------------------------------------------
# The TTL phase left the other servers full or stale, so the region's only free slot is this one.
register_server gs-w22-drain 1
capacity_of 1v1 NA
if [ "$STATUS" = "200" ] && [ "$(field free)" = "1" ]; then
  ok "gs-w22-drain registers with one slot (free 1)"
else
  fail "expected free 1 after registering gs-w22-drain, got $(cat "$OUT")"
fi
orchestrator_call DELETE /v1/game-servers/gs-w22-drain
if [ "$STATUS" = "200" ] && [ "$(field state)" = "DRAINING" ]; then
  ok "DELETE drains it: the derived state is DRAINING"
else
  fail "expected 200 DRAINING, got $STATUS ($(cat "$OUT"))"
fi
capacity_of 1v1 NA
if [ "$(field authoritative)" = "True" ] && [ "$(field free)" = "0" ]; then
  ok "a drained server offers no slot (free 0) and the region is still authoritative"
else
  fail "expected free 0 and authoritative, got $(cat "$OUT")"
fi
enqueue_pair "${PLAYERS[12]}" "${PLAYERS[13]}" 3
sleep 2
if [ "$(pending_count)" = "0" ] && [ "$(queue_depth)" -ge 2 ]; then
  ok "the pair waits: a drained server hosts nothing new, but nobody is refused"
else
  fail "a match was formed against a drained server (pending $(pending_count), depth $(queue_depth))"
fi
register_server gs-w22-drain 1
if wait_pending 1 15; then
  ok "the heartbeat clears the drain and the waiting pair is formed"
else
  fail "the pair did not form after the drain was cleared"
fi
DRAIN_MATCHES=($(pending_matches))
if [ "${#DRAIN_MATCHES[@]}" -gt 0 ]; then
  for player in $(match_players "${DRAIN_MATCHES[0]}"); do
    confirm "${DRAIN_MATCHES[0]}" "$player" &
  done
  wait
  if wait_allocation "${DRAIN_MATCHES[0]}" ALLOCATED 20; then
    ok "and it runs on the server that came back (server_id gs-w22-drain)"
  else
    fail "the pair formed after the drain was not allocated ($(allocation_status "${DRAIN_MATCHES[0]}"))"
  fi
  orchestrator_call POST "/v1/game-servers/gs-w22-drain/allocations/${DRAIN_MATCHES[0]}/release"
  if [ "$STATUS" = "200" ] && [ "$(server_allocated gs-w22-drain)" = "0" ]; then
    ok "release returns its slot (allocated 0)"
  else
    fail "release on gs-w22-drain answered $STATUS with allocated '$(server_allocated gs-w22-drain)'"
  fi
fi

# ---------------------------------------------------------------------------
step "11. the invariant, read from the database"
# ---------------------------------------------------------------------------
OVER="$(psql "SELECT count(*) FROM game_servers WHERE allocated > capacity")"
if [ "$OVER" = "0" ]; then
  ok "no server is over-committed (allocated <= capacity on every row)"
else
  fail "$OVER servers have allocated > capacity"
fi
ALLOCATED_ROWS="$(psql "SELECT count(*) FROM match_allocations WHERE status = 'ALLOCATED' AND server_id LIKE 'gs-w22-%'")"
ALLOCATED_SUM="$(psql "SELECT COALESCE(sum(allocated), 0) FROM game_servers WHERE server_id LIKE 'gs-w22-%'")"
if [ "$ALLOCATED_ROWS" = "$ALLOCATED_SUM" ]; then
  ok "the rows in ALLOCATED and the pool's sum(allocated) agree ($ALLOCATED_ROWS)"
else
  fail "match_allocations has $ALLOCATED_ROWS ALLOCATED rows but the pool holds $ALLOCATED_SUM slots"
fi
RELEASED="$(psql "SELECT count(*) FROM match_allocations WHERE status = 'RELEASED' AND match_id = '$MATCH_WAITING'")"
if [ "$RELEASED" = "1" ]; then
  ok "the released match kept its row (status RELEASED), which is what makes release idempotent"
else
  fail "expected the released match to be RELEASED, found $RELEASED"
fi
orchestrator_call GET /v1/game-servers
if gt "$("$MM" json-length /tmp/w22-out.json servers)" 2; then
  ok "the pool lists every server the scenario registered"
else
  fail "the listing does not hold the servers ($(cat "$OUT"))"
fi

# ---------------------------------------------------------------------------
step "12. cleanup"
# ---------------------------------------------------------------------------
psql "DELETE FROM match_allocations WHERE server_id LIKE 'gs-w22-%'" >/dev/null
psql "DELETE FROM game_servers WHERE server_id LIKE 'gs-w22-%'" >/dev/null
redis eval "for _, k in ipairs(redis.call('keys', 'queue:*')) do redis.call('del', k) end return 1" 0 >/dev/null
redis eval "for _, k in ipairs(redis.call('keys', 'match-pending:*')) do redis.call('del', k) end return 1" 0 >/dev/null
ok "the pool rows and the scenario's queue keys are gone (services stop at exit)"

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
