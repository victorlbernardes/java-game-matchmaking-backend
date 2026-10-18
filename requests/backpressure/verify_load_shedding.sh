#!/usr/bin/env bash
#
# W17 - what the system says no to, and that saying no keeps it up (ADR 0022, ADR 0023).
#
# Three refusals, three different reasons, three different places to stop:
#
#   1. rate limit (429): one authenticated player cannot turn the queue into a weapon. The bucket
#      lives in Redis (ratelimit:tickets:{playerId}), so the limit holds across API instances and
#      survives a restart, and the answer carries Retry-After + X-RateLimit-* for a client that
#      backs off.
#   2. queue cap (503): a queue deep enough to promise nobody a game admits nobody more. The check
#      is inside create.lua together with the write, so concurrent arrivals cannot each read a
#      depth below the cap and all decide to write (which is how a "cap" becomes a suggestion).
#   3. database backpressure: a database that is behind is not asked harder. The pairing pauses and
#      the confirmation waits for a write slot instead of opening a transaction; every confirmed
#      match is still written, and Postgres itself is asked to hold at most DB_MAX_IN_FLIGHT
#      transactions at a time. The database is made genuinely slow with a BEFORE INSERT trigger.
#
# Preconditions: `docker compose up -d` (redis, postgres), auth-service, matchmaking-api and
# matchmaking-engine running on the current build, the engine with METRICS_PORT=9091. The API's
# own numbers are authoritative - the scenario adopts the burst the API advertises and the capacity
# it reports, and only checks that the behaviour matches the number (see the notes below).
#
# Usage:
#   bash requests/backpressure/verify_load_shedding.sh
#   QUEUE_MAX_DEPTH=1000 DB_MAX_IN_FLIGHT=8 bash requests/backpressure/verify_load_shedding.sh
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
API="${API:-http://localhost:8080}"
AUTH="${AUTH:-http://localhost:8081}"
API_METRICS="${API_METRICS:-http://localhost:9090/metrics}"
ENGINE_METRICS="${ENGINE_METRICS:-http://localhost:9091/metrics}"
REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
ENGINE_LOG="${ENGINE_LOG:-/tmp/matchmaking-engine.log}"
ENGINE_START_TIMEOUT_S="${ENGINE_START_TIMEOUT_S:-90}"

# What the running services were started with (see requests/README.md). Only used to size the
# checks and to report a mismatch, never to assert a limit the server does not actually use.
RATE_LIMIT_REFILL_PER_SECOND="${RATE_LIMIT_REFILL_PER_SECOND:-5}"
QUEUE_MAX_DEPTH="${QUEUE_MAX_DEPTH:-1000}"
DB_MAX_IN_FLIGHT="${DB_MAX_IN_FLIGHT:-8}"
PG_SLEEP_SECONDS="${PG_SLEEP_SECONDS:-3}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

redis() { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }
psql_() { docker exec "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -At -c "$1" 2>/dev/null; }

metric_sum() { # <url> <metric> <label-fragment>
  curl -fsS --max-time 5 "$1" 2>/dev/null | python3 -c '
import sys
name, label_filter = sys.argv[1], sys.argv[2]
total, seen = 0.0, False
for line in sys.stdin:
    if line.startswith("#") or not line.startswith(name):
        continue
    head, _, value = line.rpartition(" ")
    if label_filter and label_filter not in head:
        continue
    try:
        total += float(value)
        seen = True
    except ValueError:
        pass
print(total if seen else "ABSENT")
' "$2" "$3"
}

token() {
  curl -fsS --max-time 10 -X POST "$AUTH/v1/auth/login" -H 'Content-Type: application/json' \
    -d "{\"playerId\":\"$1\",\"displayName\":\"$1\"}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])' 2>/dev/null
}

json_field() { python3 -c "import json,sys; print(json.load(sys.stdin).get('$1',''))" 2>/dev/null; }

cleanup_trigger() {
  psql_ "DROP TRIGGER IF EXISTS w17_slow_matches ON matches;" >/dev/null
  psql_ "DROP FUNCTION IF EXISTS w17_slow_matches();" >/dev/null
}
trap cleanup_trigger EXIT

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
if docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
  ok "$REDIS_CONTAINER is running"
else
  fail "$REDIS_CONTAINER is not running (docker compose up -d redis)"
fi
if docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
  ok "$POSTGRES_CONTAINER is running"
else
  fail "$POSTGRES_CONTAINER is not running (docker compose up -d postgres)"
fi
if curl -fsS --max-time 10 "$AUTH/.well-known/jwks.json" >/dev/null 2>&1; then
  ok "auth-service is up"
else
  fail "auth-service is not answering on $AUTH"
fi
if curl -fsS --max-time 10 "$API/health" >/dev/null 2>&1; then
  ok "matchmaking-api is up"
else
  fail "matchmaking-api is not answering on $API"
fi
in_flight_metric="$(metric_sum "$ENGINE_METRICS" matchmaking_db_writes_in_flight '')"
if [ "$in_flight_metric" != "ABSENT" ]; then
  ok "the engine exposes the W17 gauge (matchmaking_db_writes_in_flight)"
else
  fail "no matchmaking_db_writes_in_flight on $ENGINE_METRICS: is the engine running the W17 build with METRICS_PORT=9091?"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. rate limit: one player cannot spend the queue (ADR 0023)"
# ---------------------------------------------------------------------------
player="rl-$(date +%s)"
token_player="$(token "$player")"
if [ -z "$token_player" ]; then
  fail "could not log in the rate-limit player"
  printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
  exit 1
fi
redis del "ratelimit:tickets:$player" >/dev/null

REQUEST_COUNT="${REQUEST_COUNT:-60}"
allowed=0
rejected=0
server_5xx=0
limit_advertised=""
missing_retry_after=0
remaining_on_refusal="missing-refusal"
started="$(python3 -c 'import time; print(time.time())')"
for _ in $(seq 1 "$REQUEST_COUNT"); do
  headers="$(curl -sS --max-time 20 -D - -o /tmp/w17-rl-body -X POST "$API/v1/matchmaking/tickets" \
    -H "Authorization: Bearer $token_player" -H 'Content-Type: application/json' \
    -d '{"gameMode":"1v1","region":"NA","maxLatencyMs":80}' 2>/dev/null)"
  status="$(printf '%s' "$headers" | head -1 | awk '{print $2}')"
  if [ -z "$limit_advertised" ]; then
    limit_advertised="$(printf '%s' "$headers" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-ratelimit-limit"{print $2}')"
  fi
  case "$status" in
    200|201) allowed=$((allowed + 1)) ;;
    429)
      rejected=$((rejected + 1))
      retry_after="$(printf '%s' "$headers" | tr -d '\r' | awk -F': ' 'tolower($1)=="retry-after"{print $2}')"
      remaining="$(printf '%s' "$headers" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-ratelimit-remaining"{print $2}')"
      remaining_on_refusal="$remaining"
      if ! printf '%s' "$retry_after" | grep -qE '^[0-9]+$' || [ "$retry_after" -lt 1 ]; then
        missing_retry_after=$((missing_retry_after + 1))
      fi
      ;;
    5*) server_5xx=$((server_5xx + 1)) ;;
    *) : ;;
  esac
done
elapsed="$(python3 -c "import time; print(round(time.time() - $started, 3))")"

if [ -n "$limit_advertised" ]; then
  ok "the API advertises its burst: X-RateLimit-Limit=$limit_advertised"
else
  fail "no X-RateLimit-Limit header: the limiter is off, or the API is not the W17 build"
fi
if [ "$rejected" -ge 1 ]; then
  ok "$rejected of $REQUEST_COUNT requests were refused with 429 (the burst is not a licence)"
else
  fail "no 429 in $REQUEST_COUNT requests: the limiter admitted everything"
fi
if [ "$allowed" -ge 1 ]; then
  ok "$allowed requests were admitted before the limiter stepped in"
else
  fail "the limiter refused the first request as well: a burst of 0 is not a burst"
fi

# The bound: the burst plus whatever the refill earned during the loop. The window is a fraction
# of a second, so this is a tight check and not a formality.
if [ -n "$limit_advertised" ]; then
  bound_ok="$(python3 -c "
import math, sys
limit, refill, elapsed, allowed = int($limit_advertised), float($RATE_LIMIT_REFILL_PER_SECOND), float($elapsed), int($allowed)
budget = limit + math.ceil(elapsed * refill) + 1
print('yes' if allowed <= budget else 'no:%d>%d' % (allowed, budget))")"
  case "$bound_ok" in
    yes) ok "admitted $allowed <= burst $limit_advertised + refill over ${elapsed}s (a limit, not a hope)" ;;
    *)   fail "admitted ${bound_ok#no:} requests while the configured budget was the bound (the bucket leaked)" ;;
  esac
fi
if [ "$missing_retry_after" -eq 0 ] && [ "$rejected" -ge 1 ]; then
  ok "every 429 carried Retry-After (a refusal that does not say when to come back is a hang)"
else
  fail "$missing_retry_after refusals came without a usable Retry-After"
fi
if [ "$remaining_on_refusal" = "0" ]; then
  ok "the refusal reports X-RateLimit-Remaining: 0 (the header agrees with the decision)"
else
  fail "a refusal reported X-RateLimit-Remaining: $remaining_on_refusal (expected 0)"
fi
if [ "$server_5xx" -eq 0 ]; then
  ok "no request failed with 5xx: being throttled is an answer, not an error"
else
  fail "$server_5xx requests answered 5xx while being throttled"
fi

rate_limited_kpi="$(metric_sum "$API_METRICS" matchmaking_tickets_rejected_total 'reason="rate_limited"')"
if [ "$rate_limited_kpi" != "ABSENT" ] && python3 -c "import sys; sys.exit(0 if float('$rate_limited_kpi') >= $rejected else 1)"; then
  ok "the API counts them: matchmaking_tickets_rejected_total{reason=\"rate_limited\"}=$rate_limited_kpi"
else
  fail "the rate_limited rejection counter reads $rate_limited_kpi after $rejected refusals"
fi

# The bucket refills: after the Retry-After it reported, the same player is served again.
sleep "${RETRY_AFTER_SLEEP_S:-3}"
after_status="$(curl -sS --max-time 20 -o /tmp/w17-rl-body -w '%{http_code}' -X POST "$API/v1/matchmaking/tickets" \
  -H "Authorization: Bearer $token_player" -H 'Content-Type: application/json' \
  -d '{"gameMode":"1v1","region":"NA","maxLatencyMs":80}' 2>/dev/null)"
case "$after_status" in
  200|201) ok "after the Retry-After the same player is admitted again ($after_status): the bucket refills" ;;
  *)       fail "the player was still refused ($after_status) seconds after the Retry-After the API itself sent" ;;
esac

# ---------------------------------------------------------------------------
step "2. database backpressure: a slow database is not asked harder (ADR 0022)"
# ---------------------------------------------------------------------------
note "the database is slowed for real: BEFORE INSERT ON matches sleeps ${PG_SLEEP_SECONDS}s"
redis flushall >/dev/null
note "redis flushed: the rate-limit step's tickets are not part of this measurement"
psql_ "CREATE OR REPLACE FUNCTION w17_slow_matches() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN PERFORM pg_sleep(${PG_SLEEP_SECONDS}); RETURN NEW; END \$\$;" >/dev/null
psql_ "DROP TRIGGER IF EXISTS w17_slow_matches ON matches;" >/dev/null
psql_ "CREATE TRIGGER w17_slow_matches BEFORE INSERT ON matches FOR EACH ROW EXECUTE FUNCTION w17_slow_matches();" >/dev/null
trigger_installed="$(psql_ "SELECT count(*) FROM pg_trigger WHERE tgname = 'w17_slow_matches'")"
if [ "$trigger_installed" = "1" ]; then
  ok "the matches insert is slowed by a trigger (the measurement is real, not simulated in the client)"
else
  fail "the trigger was not installed; the run would measure nothing"
fi

burst_json="$(python3 "$REPO_ROOT/requests/backpressure/db_backpressure.py" \
  --players "${BACKPRESSURE_PLAYERS:-24}" --min-matches "${BACKPRESSURE_MATCHES:-12}" \
  --timeout "${BACKPRESSURE_TIMEOUT_S:-120}" 2>/tmp/w17-db-backpressure.err)"
burst_rc=$?
sed 's/^/        /' /tmp/w17-db-backpressure.err
field() { printf '%s' "$burst_json" | json_field "$1"; }
matches="$(field matches)"
written="$(field rows_written)"
max_inserts="$(field max_active_inserts)"
max_in_flight="$(field max_db_writes_in_flight)"
deferred_delta="$(field deferred_delta)"
burst_seconds="$(field burst_seconds)"
rows_after_cleanup="$(field rows_after_cleanup)"

if [ "$burst_rc" -eq 0 ] && [ "${matches:-0}" -ge "${BACKPRESSURE_MATCHES:-12}" ]; then
  ok "the burst is real: $matches matches formed and confirmed"
else
  fail "the burst did not form enough matches (rc=$burst_rc, json=$burst_json)"
fi
if [ "${written:-0}" = "$matches" ] && [ "${matches:-0}" -gt 0 ]; then
  ok "every confirmed match landed in Postgres ($written/$matches): the wait is a wait, not a loss"
else
  fail "$written of $matches confirmed matches were written"
fi
if [ "${max_inserts:-0}" -le "$DB_MAX_IN_FLIGHT" ] && [ "${max_inserts:-0}" -ge 1 ]; then
  ok "Postgres never ran more than $max_inserts inserts of matches at once (limit $DB_MAX_IN_FLIGHT)"
else
  fail "Postgres ran $max_inserts inserts of matches at once against a limit of $DB_MAX_IN_FLIGHT"
fi
if [ "${max_in_flight:-0}" -le "$DB_MAX_IN_FLIGHT" ] && [ "${max_in_flight:-0}" -ge 1 ]; then
  ok "the gauge agreed: matchmaking_db_writes_in_flight peaked at $max_in_flight"
else
  fail "matchmaking_db_writes_in_flight peaked at $max_in_flight (limit $DB_MAX_IN_FLIGHT)"
fi
if [ "${deferred_delta:-0}" -ge 1 ]; then
  ok "$deferred_delta confirmations found the database at its limit and waited instead of insisting"
else
  fail "no confirmation was ever deferred: the gate was never the constraint, so it proved nothing"
fi
slow_enough="$(python3 -c "print('yes' if float('${burst_seconds:-0}') >= float($PG_SLEEP_SECONDS) else 'no')")"
if [ "$slow_enough" = "yes" ]; then
  ok "the burst took ${burst_seconds}s: at least one ${PG_SLEEP_SECONDS}s wait really happened in the database"
else
  fail "the burst took only ${burst_seconds}s, less than one database wait: the trigger did not slow the writes"
fi
if [ "${rows_after_cleanup:-1}" = "0" ]; then
  ok "the scenario cleaned up after itself: 0 rows left in Postgres"
else
  fail "$rows_after_cleanup rows of this scenario are still in Postgres"
fi
if [ -f "$ENGINE_LOG" ]; then
  if grep -q "engine.backpressure confirmation deferred" "$ENGINE_LOG"; then
    ok "the engine said it: $(grep "engine.backpressure confirmation deferred" "$ENGINE_LOG" | tail -1)"
  else
    fail "no engine.backpressure line in $ENGINE_LOG (the counter moved, but the log should say why)"
  fi
else
  note "no engine log at $ENGINE_LOG (ENGINE_LOG=... to point at it); the counter and Postgres already proved the wait"
fi
cleanup_trigger

# ---------------------------------------------------------------------------
step "3. queue cap: a queue that can promise nothing admits nothing (ADR 0022)"
# ---------------------------------------------------------------------------
note "the engine is stopped for this step: with nothing draining, the cap is what answers"
if pgrep -f MatchmakingEngineApplication >/dev/null 2>&1; then
  pkill -f MatchmakingEngineApplication
  for _ in $(seq 1 30); do
    pgrep -f MatchmakingEngineApplication >/dev/null 2>&1 || break
    sleep 1
  done
fi
engine_was_running=0
if pgrep -f MatchmakingEngineApplication >/dev/null 2>&1; then
  fail "the engine is still running: the queue would drain and the cap would never be reached"
else
  engine_was_running=1
  ok "the engine is stopped"
fi

redis flushall >/dev/null
note "redis flushed: the queue starts empty"

arrivals="${ARRIVALS:-$((QUEUE_MAX_DEPTH + 100))}"
fill_json="$(python3 "$REPO_ROOT/requests/backpressure/fill_queue.py" --players "$arrivals" --concurrency 32 2>/tmp/w17-fill.err)"
fill_rc=$?
sed 's/^/        /' /tmp/w17-fill.err
ff() { printf '%s' "$fill_json" | json_field "$1"; }
accepted="$(ff accepted)"
shed="$(ff shed)"
other="$(ff other)"
capacity="$(ff capacity)"
queue_name="$(ff queue)"
depth="$(ff depth)"
retry_after="$(ff retry_after)"
replay_status="$(ff replay_status)"

if [ "$fill_rc" -eq 0 ] && [ "${shed:-0}" -ge 1 ]; then
  ok "$shed of $arrivals arrivals were shed with 503: the queue reached its cap"
else
  fail "nothing was shed (rc=$fill_rc, json=$fill_json): the cap was never reached, so nothing was proven"
fi
if [ "${other:-1}" = "0" ]; then
  ok "every other arrival was accepted: the only answer besides 2xx was the 503"
else
  fail "$other arrivals answered neither 2xx nor 503 (a refusal must be a decision, not an error)"
fi
if [ -n "$capacity" ] && [ "$capacity" = "$QUEUE_MAX_DEPTH" ]; then
  ok "the refusal names the limit it hit: capacity=$capacity on $queue_name"
else
  note "the API reports capacity=$capacity where this shell assumed QUEUE_MAX_DEPTH=$QUEUE_MAX_DEPTH"
  if [ -n "$capacity" ] && [ "$depth" = "$capacity" ]; then
    ok "the refusal names the limit it hit: capacity=$capacity on $queue_name (adopted from the API)"
  else
    fail "the 503 body did not carry a usable capacity/depth ($capacity/$depth)"
  fi
fi
if [ "${accepted:-0}" = "$capacity" ]; then
  ok "exactly the cap was admitted ($accepted), never more: the check and the write are one step"
else
  fail "$accepted arrivals were admitted against a capacity of $capacity"
fi
if [ "${depth:-0}" = "$capacity" ]; then
  ok "the depth in the refusal equals the capacity: the answer describes the state it refused from"
else
  fail "the 503 said depth=$depth while the capacity is $capacity"
fi
if printf '%s' "$retry_after" | grep -qE '^[0-9]+$' && [ "${retry_after:-0}" -ge 1 ]; then
  ok "the 503 carried Retry-After: $retry_after"
else
  fail "the 503 came without a Retry-After (got '$retry_after')"
fi
queued="$(redis zcard "$queue_name")"
if [ "$queued" = "$capacity" ]; then
  ok "redis agrees: $queue_name holds exactly $capacity members"
else
  fail "redis holds $queued members against a capacity of $capacity"
fi
queue_full_kpi="$(metric_sum "$API_METRICS" matchmaking_tickets_rejected_total 'reason="queue_full"')"
if [ "$queue_full_kpi" != "ABSENT" ] && python3 -c "import sys; sys.exit(0 if float('$queue_full_kpi') >= $shed else 1)"; then
  ok "the API counts them: matchmaking_tickets_rejected_total{reason=\"queue_full\"}=$queue_full_kpi"
else
  fail "the queue_full rejection counter reads $queue_full_kpi after $shed refusals"
fi
case "$replay_status" in
  200|201) ok "an accepted ticket's replay still answers ($replay_status) while the queue is full: the player did enqueue" ;;
  *)       fail "the replay of an accepted ticket answered $replay_status instead of the ticket it already has" ;;
esac

# Leave the stack as it was found: the scenario's own queue and tickets go, the engine comes back.
note "cleanup: the scenario's queue and tickets are removed, the engine is started again"
# The queue members ARE the ticket ids, so they are captured before the queue is deleted: the
# tickets themselves carry a 900 s TTL and would otherwise outlive the members as orphans. One
# DEL with every key beats a loop of `docker exec` calls by orders of magnitude (measured: ~2000
# round trips through the Docker daemon is minutes; one command is one).
scenario_tickets="$(redis zrange "$queue_name" 0 -1 | sed 's|^|ticket:|')"
scenario_idem="$(redis keys "idem:$(ff label)-*")"
redis del "$queue_name" $scenario_tickets $scenario_idem >/dev/null

if [ "$engine_was_running" = "1" ]; then
  ( cd "$REPO_ROOT" && METRICS_PORT=9091 nohup mvn -o -B -q -pl services/matchmaking-engine exec:java \
      -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication \
      > "$ENGINE_LOG" 2>&1 </dev/null & )
  started_engine=0
  for _ in $(seq 1 "$ENGINE_START_TIMEOUT_S"); do
    if [ "$(metric_sum "$ENGINE_METRICS" matchmaking_db_writes_in_flight '')" != "ABSENT" ]; then
      started_engine=1
      break
    fi
    sleep 1
  done
  if [ "$started_engine" = "1" ]; then
    ok "the engine is running again and scraping on $ENGINE_METRICS"
  else
    fail "the engine did not come back within ${ENGINE_START_TIMEOUT_S}s (log: $ENGINE_LOG)"
  fi
fi

resume_token="$(token "resume-$(date +%s)")"
resume_status="$(curl -sS --max-time 20 -o /tmp/w17-resume-body -w '%{http_code}' -X POST "$API/v1/matchmaking/tickets" \
  -H "Authorization: Bearer $resume_token" -H 'Content-Type: application/json' \
  -d '{"gameMode":"1v1","region":"NA","maxLatencyMs":80}' 2>/dev/null)"
case "$resume_status" in
  200|201) ok "with the queue empty again, a new arrival is admitted ($resume_status): the refusal was temporary" ;;
  *)       fail "a new arrival answered $resume_status after the queue was drained" ;;
esac

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
