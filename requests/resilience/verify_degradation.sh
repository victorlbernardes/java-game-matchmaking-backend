#!/usr/bin/env bash
#
# W18 - the degradation matrix, tested with chaos (ADR 0024).
#
# What it proves, one dependency at a time, with the stack alive and traffic flowing:
#
#   1. Redis RESTARTS (the script cache is gone): the services keep working without being
#      restarted. Before: every enqueue answered `500 NOSCRIPT No matching script` forever,
#      and the matching loop stopped with it.
#   2. REDIS is DOWN: the API answers 503 with a Retry-After in milliseconds, never leaks the
#      internal address, keeps /health green and turns /health/ready red naming redis. Before:
#      `500 {"error":"Connection refused: localhost/127.0.0.1:6379"}` and /health said UP.
#   3. POSTGRES is DOWN: a confirmation does NOT move the hot state ahead of the source of
#      truth - no ticket reaches CONFIRMED for a match that was never written - and the same
#      confirmation is written when the database returns. Before: the tickets went CONFIRMED
#      and stayed there, with 0 rows and no way back to the queue.
#   4. KAFKA is DOWN: a confirmation still writes the match (Postgres) and parks the event in
#      the outbox; when the broker returns the event is published exactly once (ADR 0018).
#   5. The SCHEMA REGISTRY is DOWN when the engine starts: the confirmation is still written
#      and the event waits in the outbox until the registry answers (the schema id is resolved
#      at publish time, so a registry outage cannot refuse a match).
#
# Preconditions: `docker compose up -d zookeeper kafka schema-registry redis postgres`, auth
# service, API and engine running (engine with METRICS_PORT=9091), and the engine built after
# this change (`mvn -B install -DskipTests` - runbook item 1). Phases 3-5 stop containers; every
# phase starts them again, but an interrupted run leaves a dependency down (see the note at the
# end of each phase).
#
# Usage:
#   bash requests/resilience/verify_degradation.sh
#   SKIP_SCHEMA_PHASE=1 bash requests/resilience/verify_degradation.sh   # 4 phases, ~3 min
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
MM="$REPO_ROOT/requests/mm.sh"
PROBE() { "$MM" resilience "$@"; }
API="http://localhost:8080"
AUTH="http://localhost:8081"
API_METRICS="http://localhost:${API_METRICS_PORT:-9090}/metrics"
ENGINE_METRICS="http://localhost:${ENGINE_METRICS_PORT:-9091}/metrics"
ENGINE_LOG="${ENGINE_LOG:-/tmp/matchmaking-engine.log}"
REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-game-matchmaking-backend-kafka-1}"
REGISTRY_CONTAINER="${REGISTRY_CONTAINER:-game-matchmaking-backend-schema-registry-1}"
QUEUE_KEY="queue:1v1:NA"
REDIS_DOWN_PROBES="${REDIS_DOWN_PROBES:-20}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

redis() { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }
psql_c() { docker exec "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -At -c "$1" 2>/dev/null; }

wait_http() { # <url> <seconds>
  local waited=0
  while [ "$waited" -lt "$2" ]; do
    curl -fsS --max-time 3 "$1" >/dev/null 2>&1 && return 0
    sleep 1; waited=$((waited + 1))
  done
  return 1
}

wait_container() { # <container> <seconds> <command...>
  local waited=0
  while [ "$waited" -lt "$2" ]; do
    docker exec "$1" "${@:3}" >/dev/null 2>&1 && return 0
    sleep 1; waited=$((waited + 1))
  done
  return 1
}

json_field() { # <field> -> the value, from stdin ("" when absent)
  "$MM" json - "$1"
}

metric_value() { # <url> <metric> <label fragment> -> the sum, with an absent series read as 0
  "$MM" sum "$("$MM" metric-sum "$1" "$2" "$3")"
}

# --------------------------------------------------------------------------- preconditions
step "preconditions"
for url in "$API/health" "$AUTH/.well-known/jwks.json" "$ENGINE_METRICS" "$API/health/ready"; do
  if wait_http "$url" 5; then ok "reachable: $url"; else fail "not reachable: $url (start the stack)"; fi
done
for container in "$REDIS_CONTAINER" "$POSTGRES_CONTAINER" "$KAFKA_CONTAINER"; do
  if docker ps --format '{{.Names}}' | grep -q "^$container$"; then
    ok "$container is running"
  else
    fail "$container is not running"
  fi
done
[ "$FAIL" -eq 0 ] || { printf '\n== result: %d passed, %d failed (fix the preconditions)\n' "$PASS" "$FAIL"; exit 1; }
redis flushall >/dev/null

# --------------------------------------------------------------------------- 1. Redis restart
step "1. a Redis restart does not require restarting the services"
before_enqueue="$(PROBE enqueue --count 1 --prefix w18-restart-before)"
before_status="$(printf '%s' "$before_enqueue" | "$MM" json-first-key - statuses)"
[ "$before_status" = "201" ] && ok "enqueue works before the restart (201)" \
                              || fail "enqueue before the restart answered $before_status"

redis flushall >/dev/null
api_pid_before="$(pgrep -f MatchmakingApiApplication | head -1)"
engine_pid_before="$(pgrep -f MatchmakingEngineApplication | head -1)"
docker restart "$REDIS_CONTAINER" >/dev/null
wait_container "$REDIS_CONTAINER" 30 redis-cli ping
note "redis restarted: the script cache is empty ($(redis script list | wc -l | tr -d ' ') scripts loaded)"

after_enqueue="$(PROBE enqueue --count 1 --prefix w18-restart-after)"
after_status="$(printf '%s' "$after_enqueue" | "$MM" json-first-key - statuses)"
[ "$after_status" = "201" ] \
  && ok "enqueue after the restart answers 201 (the script reloads instead of 500 NOSCRIPT)" \
  || fail "enqueue after the restart answered $after_status: $(printf '%s' "$after_enqueue" | json_field bodies)"

# The pairing script has to reload too - and so does the confirmation subscription: a restart takes
# the script cache (reloaded lazily) and every Pub/Sub connection (rebuilt by RedisSubscriber) with
# it. A match formed, confirmed and written after the restart is the whole path working; before the
# fix this is where the failure was silent - the engine received nothing, logged nothing, and every
# match dissolved at its deadline.
redis flushall >/dev/null
match_json="$(PROBE drive --confirm --wait 30)"
match_id="$(printf '%s' "$match_json" | json_field matchId)"
if [ -z "$match_id" ]; then
  fail "the engine did not pair after the restart: $(printf '%s' "$match_json" | head -c 200)"
else
  ok "the engine still pairs after the restart (match $match_id formed)"
  tickets="$(printf '%s' "$match_json" | "$MM" json-values - tickets)"
  written=0
  waited=0
  while [ "$waited" -lt 30 ]; do
    if [ "$(PROBE row-count "$match_id")" = "1" ]; then written=1; break; fi
    sleep 1; waited=$((waited + 1))
  done
  if [ "$written" = "1" ]; then
    ok "the confirmation was received and written after the restart (${waited}s): the subscription was rebuilt"
    all_confirmed=1
    for ticket in $tickets; do
      [ "$(PROBE ticket-status "$ticket")" = "CONFIRMED" ] || all_confirmed=0
    done
    [ "$all_confirmed" = "1" ] && ok "and both tickets reached CONFIRMED with no service restarted" \
                               || fail "a ticket did not reach CONFIRMED after the restart"
  else
    fail "the engine received no confirmation after the Redis restart: the subscription died with the connection and every match dissolves at its deadline"
  fi
fi
[ "$api_pid_before" = "$(pgrep -f MatchmakingApiApplication | head -1)" ] \
  && ok "the API was never restarted (pid $api_pid_before)" || fail "the API process changed"
[ "$engine_pid_before" = "$(pgrep -f MatchmakingEngineApplication | head -1)" ] \
  && ok "the engine was never restarted (pid $engine_pid_before)" || fail "the engine process changed"
redis flushall >/dev/null

# --------------------------------------------------------------------------- 2. Redis down
step "2. Redis down: an honest, fast 503 (and a readiness that says so)"
docker stop "$REDIS_CONTAINER" >/dev/null
sleep 1

down_enqueue="$(PROBE enqueue --count "$REDIS_DOWN_PROBES" --prefix w18-redis-down)"
note "$(printf '%s' "$down_enqueue" | "$MM" json - statuses) p50 $(printf '%s' "$down_enqueue" | "$MM" json - p50Ms)ms p99 $(printf '%s' "$down_enqueue" | "$MM" json - p99Ms)ms max $(printf '%s' "$down_enqueue" | "$MM" json - maxMs)ms"
# Every arrival was a 503: one status in the histogram, and that status is 503.
all_503="no"
[ "$(printf '%s' "$down_enqueue" | "$MM" json-length - statuses)" = "1" ] \
  && [ "$(printf '%s' "$down_enqueue" | "$MM" json-first-key - statuses)" = "503" ] && all_503="yes"
[ "$all_503" = "yes" ] && ok "every arrival is refused with 503" || fail "not every arrival was a 503"
max_ms="$(printf '%s' "$down_enqueue" | "$MM" json - maxMs)"
"$MM" le "$max_ms" 2000 \
  && ok "the refusal is fast (slowest ${max_ms}ms < 2000ms): nothing hangs on a dead dependency" \
  || fail "a refusal took ${max_ms}ms, which looks like a wait, not a refusal"
body_503="$(printf '%s' "$down_enqueue" | "$MM" json - bodies.503)"
printf '%s' "$body_503" | grep -q '"dependency": *"redis"' \
  && ok "the body names the dependency: $body_503" || fail "the body does not name the dependency: $body_503"
printf '%s' "$body_503" | grep -qE 'localhost|127\.0\.0\.1|6379' \
  && fail "the body leaks the internal address: $body_503" \
  || ok "the body carries no internal address"

ticket_http="$(PROBE ticket-http-status ticket-w18-any)"
[ "$ticket_http" = "503" ] && ok "GET a ticket with Redis down is a 503" || fail "GET a ticket answered $ticket_http"

readiness="$(curl -sS --max-time 10 -w '\n%{http_code}' "$API/health/ready")"
readiness_code="$(printf '%s' "$readiness" | tail -1)"
readiness_body="$(printf '%s' "$readiness" | head -1)"
[ "$readiness_code" = "503" ] && ok "readiness is 503 with Redis down" || fail "readiness answered $readiness_code"
printf '%s' "$readiness_body" | grep -q '"redis":"DOWN"' \
  && ok "readiness names redis as the dependency that is down" \
  || fail "readiness did not name redis: $readiness_body"
liveness_code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$API/health")"
[ "$liveness_code" = "200" ] && ok "liveness stays 200: the process is fine, the dependency is not" \
                            || fail "liveness answered $liveness_code"

counter_after="$(metric_value "$API_METRICS" "matchmaking_dependency_errors_total" "redis")"
if [ "$counter_after" != "0" ] && [ -n "$counter_after" ]; then
  "$MM" ge "$counter_after" "$REDIS_DOWN_PROBES" \
    && ok "the refusals are counted per dependency ($counter_after on redis)" \
    || fail "the dependency counter reads $counter_after for $REDIS_DOWN_PROBES refusals"
else
  fail "the dependency counter is not exposed on $API_METRICS (reads '$counter_after')"
fi

docker start "$REDIS_CONTAINER" >/dev/null
wait_container "$REDIS_CONTAINER" 30 redis-cli ping
recovered="$(PROBE enqueue --count 1 --prefix w18-redis-back | "$MM" json-first-key - statuses)"
[ "$recovered" = "201" ] && ok "the first arrival after Redis returns is admitted (503 was a pause, not a loss)" \
                         || fail "enqueue after the recovery answered $recovered"
redis flushall >/dev/null
note "phase 2 leaves Redis running"

# --------------------------------------------------------------------------- 3. Postgres down
step "3. Postgres down: the hot state never runs ahead of the source of truth"
redis flushall >/dev/null
# The match is formed and the tickets are CONFIRMING BEFORE the database is stopped: that is what
# makes the assertion deterministic (nothing can have been written yet) and it is also the honest
# sequence - the database disappears between "the players said yes" and "the row is written".
match_json="$(PROBE drive --wait 30)"
match_id="$(printf '%s' "$match_json" | json_field matchId)"
if [ -z "$match_id" ]; then
  fail "no pending match formed before the database was stopped"
  ticket_a=""; ticket_b=""
else
  ticket_a="$(printf '%s' "$match_json" | "$MM" json-values - tickets | cut -d' ' -f1)"
  ticket_b="$(printf '%s' "$match_json" | "$MM" json-values - tickets | cut -d' ' -f2)"
  players="$(printf '%s' "$match_json" | "$MM" json-array - players)"
  ok "match $match_id formed and both tickets are CONFIRMING (${ticket_a}, ${ticket_b})"
fi

docker stop "$POSTGRES_CONTAINER" >/dev/null
sleep 2
if [ -n "$match_id" ]; then
  confirm_json="$(PROBE confirm "$match_id" $players)"
  published="$(printf '%s' "$confirm_json" | json_field published)"
  [ "${published:-0}" -ge 2 ] && ok "both confirmations were published while the database was down" \
                              || fail "the probe published ${published:-0} of 2 confirmations"
  # The claim key is the engine's own answer that it received the confirmation (the same key the
  # timeout sweep honours). Without this, "no CONFIRMED and no row" would also be true of a
  # confirmation that never arrived - which is exactly how the dead subscription hid itself.
  claimed=0
  waited=0
  while [ "$waited" -lt 10 ]; do
    [ "$(redis exists "match-confirmed-claim:$match_id")" = "1" ] && claimed=1 && break
    sleep 1; waited=$((waited + 1))
  done
  [ "$claimed" = "1" ] && ok "the engine processed the confirmation (it claimed the match)" \
                       || fail "the engine never claimed $match_id: the confirmation did not arrive"
fi
sleep 3

state_a="$(PROBE ticket-status "$ticket_a")"
state_b="$(PROBE ticket-status "$ticket_b")"
[ "$state_a" != "CONFIRMED" ] && [ "$state_b" != "CONFIRMED" ] \
  && ok "no ticket reached CONFIRMED while the write was failing ($state_a, $state_b)" \
  || fail "the hot state ran ahead of the durable fact ($state_a, $state_b)"
[ "$(PROBE row-count "$match_id")" = "0" ] \
  && ok "no match row exists (the source of truth refused the write)" \
  || fail "a row exists for a match whose write should have failed"
[ "$(PROBE pending-status "$match_id")" = "PRESENT" ] \
  && ok "the pending match is still there: the recovery works from it" \
  || fail "the pending match disappeared, so nothing can requeue the players"
grep -q "confirmation write failed matchId=$match_id" "$ENGINE_LOG" \
  && ok "the engine says what happened: confirmation write failed for $match_id" \
  || note "the engine log does not carry the failure line (check ENGINE_LOG=$ENGINE_LOG)"

history="$(PROBE history-status w18-history)"
history_status="$(printf '%s' "$history" | json_field status)"
[ "$history_status" = "503" ] \
  && ok "history with Postgres down is a 503 (not a 500 with the driver's message)" \
  || fail "history answered $history_status"
printf '%s' "$history" | grep -qE 'localhost|5432|refused' \
  && fail "the history error leaks the database address: $(printf '%s' "$history" | head -c 200)" \
  || ok "the history error carries no database address"
readiness="$(curl -sS --max-time 10 "$API/health/ready")"
printf '%s' "$readiness" | grep -q '"postgres":"DOWN"' \
  && ok "readiness names postgres as down: $readiness" || fail "readiness did not name postgres: $readiness"

note "starting Postgres again; the engine retries the confirmation it could not write..."
docker start "$POSTGRES_CONTAINER" >/dev/null
wait_container "$POSTGRES_CONTAINER" 30 pg_isready -U matchmaking
written=0
waited=0
while [ "$waited" -lt 60 ]; do
  if [ "$(PROBE row-count "$match_id")" = "1" ]; then written=1; break; fi
  sleep 2; waited=$((waited + 2))
done
[ "$written" = "1" ] \
  && ok "the confirmation was written when the database returned (${waited}s), so the players keep their match" \
  || fail "the match was never written after the database returned (${waited}s)"
[ "$(PROBE ticket-status "$ticket_a")" = "CONFIRMED" ] \
  && ok "and only then did the tickets reach CONFIRMED" \
  || fail "tickets are $(PROBE ticket-status "$ticket_a") after the write landed"
outbox="$(PROBE outbox-status "$match_id")"
note "outbox for the match: $outbox"
note "phase 3 leaves Postgres running"

# --------------------------------------------------------------------------- 4. Kafka down
step "4. Kafka down: confirmations stay durable, events wait in the outbox"
redis flushall >/dev/null
docker stop "$KAFKA_CONTAINER" >/dev/null
note "kafka stopped: the relay will retry with backoff and publish when it returns"
match_json="$(PROBE drive --confirm --wait 30)"
match_id="$(printf '%s' "$match_json" | json_field matchId)"
if [ -z "$match_id" ]; then
  fail "no pending match formed with Kafka down"
else
  waited=0
  while [ "$waited" -lt 30 ]; do
    if [ "$(PROBE row-count "$match_id")" = "1" ]; then break; fi
    sleep 1; waited=$((waited + 1))
  done
  [ "$(PROBE row-count "$match_id")" = "1" ] \
    && ok "the match is written in Postgres with the broker down (a broker is not the source of truth)" \
    || fail "no match row with Kafka down: the confirmation depended on the broker"
  outbox="$(PROBE outbox-status "$match_id")"
  printf '%s' "$outbox" | grep -q '"PENDING"' \
    && ok "the event is durably queued: outbox $outbox" \
    || fail "the event is not in the outbox: $outbox"
  ticket_a="$(printf '%s' "$match_json" | "$MM" json-values - tickets | cut -d' ' -f1)"
  [ "$(PROBE ticket-status "$ticket_a")" = "CONFIRMED" ] \
    && ok "the player's ticket is CONFIRMED: the match happened, only the announcement is late" \
    || fail "the ticket is $(PROBE ticket-status "$ticket_a")"
fi

docker start "$KAFKA_CONTAINER" >/dev/null
note "waiting for the broker to accept the relay's publishes (up to 90s)..."
published=0
waited=0
while [ "$waited" -lt 90 ]; do
  if printf '%s' "$(PROBE outbox-status "$match_id")" | grep -q '"PUBLISHED"'; then published=1; break; fi
  sleep 3; waited=$((waited + 3))
done
[ "$published" = "1" ] \
  && ok "the event was published within ${waited}s of the broker returning" \
  || fail "the event was not published after the broker returned (${waited}s): $(PROBE outbox-status "$match_id")"
printf '%s' "$(PROBE outbox-status "$match_id")" | grep -q '"PARKED"' \
  && fail "an event was parked: the retry gave up" \
  || ok "nothing was parked: retrying worked"
note "phase 4 leaves Kafka running"

# --------------------------------------------------------------------------- 5. Schema Registry
if [ "${SKIP_SCHEMA_PHASE:-0}" = "1" ]; then
  step "5. Schema Registry down: skipped (SKIP_SCHEMA_PHASE=1)"
else
  step "5. Schema Registry down at engine start: the registry cannot refuse a confirmation"
  note "the schema id is resolved when the event is published, and the serializer caches it per"
  note "topic: a fresh engine with the registry down is the honest way to ask the question."
  docker stop "$REGISTRY_CONTAINER" >/dev/null
  pkill -f MatchmakingEngineApplication
  sleep 3
  ( cd "$REPO_ROOT" && METRICS_PORT=9091 nohup mvn -o -B -q -pl services/matchmaking-engine exec:java \
      -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication \
      >> "$ENGINE_LOG" 2>&1 </dev/null & )
  if wait_http "$ENGINE_METRICS" 90; then
    ok "the engine starts with the registry down (a publish-time dependency, not a boot-time one)"
  else
    fail "the engine did not start with the registry down"
  fi
  redis flushall >/dev/null
  match_json="$(PROBE drive --confirm --wait 30)"
  match_id="$(printf '%s' "$match_json" | json_field matchId)"
  if [ -z "$match_id" ]; then
    fail "no pending match formed with the registry down"
  else
    waited=0
    while [ "$waited" -lt 30 ]; do
      if [ "$(PROBE row-count "$match_id")" = "1" ]; then break; fi
      sleep 1; waited=$((waited + 1))
    done
    [ "$(PROBE row-count "$match_id")" = "1" ] \
      && ok "the match is written with the registry down" \
      || fail "no match row with the registry down: the confirmation depended on the registry"
    outbox="$(PROBE outbox-status "$match_id")"
    printf '%s' "$outbox" | grep -q 'PENDING' \
      && ok "the event waits in the outbox: $outbox" \
      || note "outbox for the match: $outbox (already published, or parked)"
  fi
  docker start "$REGISTRY_CONTAINER" >/dev/null
  wait_http "http://localhost:8082/subjects" 60
  published=0
  waited=0
  while [ "$waited" -lt 90 ]; do
    if printf '%s' "$(PROBE outbox-status "$match_id")" | grep -q '"PUBLISHED"'; then published=1; break; fi
    sleep 3; waited=$((waited + 3))
  done
  [ "$published" = "1" ] \
    && ok "the event is published within ${waited}s of the registry returning" \
    || fail "the event was not published after the registry returned (${waited}s)"
  note "phase 5 leaves the registry running and the engine running (log: $ENGINE_LOG)"
fi

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
if [ "$FAIL" -ne 0 ]; then
  printf '   if a phase was interrupted, check that redis, postgres, kafka and the registry are up\n'
  printf '   (docker ps) and that the engine, API and auth service are running.\n'
  exit 1
fi
exit 0
