#!/usr/bin/env bash
#
# W13 task 1 — the same event delivered twice must not be applied twice.
#
# Reproduces the replay an operator would do after fixing a projection bug: stop the
# consumer, rewind its group to the beginning of the topic, start it again. Every
# record is delivered a second time, and the effect must not happen again.
#
# The counts come from Postgres (the inbox and the projection), not from the logs:
# the log says what the consumer intended, the database says what persisted.
#
# Preconditions:
#   docker compose up -d            (kafka + postgres healthy)
#   mvn -B install -DskipTests      (the services resolve their modules from ~/.m2)
#   the orchestrator running, and at least one MATCH_CONFIRMED on the topic
#   (python3 requests/e2e/run_e2e.py creates one - scenario S11)
#
# This script stops the running orchestrator (pkill), rewinds the offsets, starts it
# again and leaves it running. Usage:
#   bash requests/kafka/verify_idempotent_consumer.sh
#   GROUP=match-orchestrator-2 bash requests/kafka/verify_idempotent_consumer.sh
#
# Exit code 0 = the replay produced no new effect and the log shows the duplicates.
set -uo pipefail

cd "$(dirname "$0")/../.." || exit 1

TOPIC="${TOPIC:-matchmaking.events.match-confirmed}"
GROUP="${GROUP:-match-orchestrator}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-game-matchmaking-backend-kafka-1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
BROKER_IN_CONTAINER="${BROKER_IN_CONTAINER:-localhost:29092}"
DB_CONTAINER_USER="${DB_CONTAINER_USER:-matchmaking}"
DB_CONTAINER_NAME="${DB_CONTAINER_NAME:-matchmaking}"
MAIN_CLASS="com.victor.matchmaking.orchestrator.MatchOrchestratorApplication"
CONSUMER_LOG="${CONSUMER_LOG:-/tmp/match-orchestrator-idempotency.log}"
# The replay is a few dozen records on one partition; a minute is generous, and the
# timeout is what keeps a stuck consumer from hanging the check forever.
CATCH_UP_TIMEOUT_S="${CATCH_UP_TIMEOUT_S:-120}"
WAIT_STEP_S=2

PASS=0
FAIL=0

ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
step() { printf '\n== %s\n' "$1"; }

psql_query() {
  docker exec "$POSTGRES_CONTAINER" psql -U "$DB_CONTAINER_USER" -d "$DB_CONTAINER_NAME" -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

# Current committed offset and log end offset of the group on our topic.
group_position() {
  docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BROKER_IN_CONTAINER" \
    --describe --group "$GROUP" 2>/dev/null | awk -v topic="$TOPIC" '$2 == topic { print $4" "$5" "$6 }'
}

lag() {
  group_position | awk '{ print $3 }'
}

members() {
  docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BROKER_IN_CONTAINER" \
    --describe --group "$GROUP" 2>/dev/null | awk -v topic="$TOPIC" '$2 == topic && $7 != "-" { print $7 }'
}

# ---------------------------------------------------------------------------
step "1. preconditions"
# ---------------------------------------------------------------------------
if pgrep -f "$MAIN_CLASS" > /dev/null; then
  ok "orchestrator is running (pid $(pgrep -f "$MAIN_CLASS" | tr '\n' ' '))"
else
  bad "orchestrator is not running - start it first:"
  echo "        mvn -pl services/match-orchestrator exec:java -Dexec.mainClass=$MAIN_CLASS &"
  echo "        (see the start order in requests/README.md)"
fi

allocations_before="$(psql_query "SELECT count(*) FROM match_allocations")"
processed_before="$(psql_query "SELECT count(*) FROM processed_events WHERE consumer_group = '$GROUP'")"
topic_records="$(group_position | awk '{ print $2 }')"

if [ -n "${allocations_before:-}" ] && [ -n "${processed_before:-}" ]; then
  ok "orchestrator projection readable: match_allocations=$allocations_before processed_events($GROUP)=$processed_before"
else
  bad "cannot read the orchestrator tables (run: docker compose up -d postgres)"
fi

if [ "${topic_records:-0}" -gt 0 ] 2>/dev/null; then
  ok "topic $TOPIC holds $topic_records record(s) to replay"
else
  bad "topic $TOPIC is empty - create a confirmed match first (python3 requests/e2e/run_e2e.py)"
fi

if [ "$allocations_before" -gt 0 ] 2>/dev/null; then
  ok "the consumer has already projected something to replay against"
else
  bad "nothing projected yet - run the E2E (scenario S11) before replaying"
fi

[ "$FAIL" -eq 0 ] || { echo; echo "ABORTED: fix the preconditions above."; exit 1; }

wait_for_empty_group() {
  for _ in $(seq 1 "$((CATCH_UP_TIMEOUT_S / WAIT_STEP_S))"); do
    [ -z "$(members)" ] && return 0
    sleep "$WAIT_STEP_S"
  done
  return 1
}

wait_for_catch_up() {
  for _ in $(seq 1 "$((CATCH_UP_TIMEOUT_S / WAIT_STEP_S))"); do
    [ "$(lag)" = "0" ] && return 0
    sleep "$WAIT_STEP_S"
  done
  return 1
}

# ---------------------------------------------------------------------------
step "2. rewind the group to the beginning of the topic"
# ---------------------------------------------------------------------------
before_position="$(group_position)"
echo "  group position before: offset=$(echo "$before_position" | awk '{print $1}') end=$(echo "$before_position" | awk '{print $2}') lag=$(echo "$before_position" | awk '{print $3}')"

# Offsets can only be reset for a group with no members.
pkill -f "$MAIN_CLASS" && ok "stopped the orchestrator" || bad "could not stop the orchestrator"
if wait_for_empty_group; then
  ok "consumer group $GROUP has no members"
else
  bad "consumer group $GROUP still has members after $CATCH_UP_TIMEOUT_S""s"
fi

reset_output="$(docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BROKER_IN_CONTAINER" \
  --reset-offsets --to-earliest --topic "$TOPIC" --group "$GROUP" --execute 2>&1)"
# The reset is checked on the group's position, not on the CLI's table: the table is
# padded into one line (no newlines between the header and the row), so parsing it
# would test the CLI's formatting instead of the offsets.
rewound=0
position_after_reset=""
for _ in $(seq 1 5); do
  position_after_reset="$(group_position)"
  [ "$(printf '%s' "$position_after_reset" | awk '{print $1}')" = "0" ] && { rewound=1; break; }
  sleep "$WAIT_STEP_S"
done
if [ "$rewound" = "1" ]; then
  ok "group rewound to the beginning: offset=0 end=$(printf '%s' "$position_after_reset" | awk '{print $2}')"
else
  bad "group was not rewound: ${position_after_reset:-<empty>} (cli: $(printf '%s' "$reset_output" | tr -s ' ' | tail -c 140))"
fi

# ---------------------------------------------------------------------------
step "3. start the consumer again and let it re-read the whole topic"
# ---------------------------------------------------------------------------
: > "$CONSUMER_LOG"
nohup mvn -pl services/match-orchestrator exec:java -Dexec.mainClass="$MAIN_CLASS" >> "$CONSUMER_LOG" 2>&1 &
echo "  consumer log: $CONSUMER_LOG"

# The window that matters is the consumer's, not the JVM's: the clock starts when it
# subscribes, so the measurement is "re-read N records", not "start a process".
subscribed_at=""
for _ in $(seq 1 "$((CATCH_UP_TIMEOUT_S / WAIT_STEP_S))"); do
  if grep -q "consumer.subscribed" "$CONSUMER_LOG"; then
    subscribed_at="$(date +%s)"
    break
  fi
  sleep "$WAIT_STEP_S"
done
if [ -n "$subscribed_at" ]; then
  ok "consumer restarted and subscribed"
else
  bad "consumer did not start (see $CONSUMER_LOG; did you run: mvn -B install -DskipTests ?)"
fi

if wait_for_catch_up; then
  replay_seconds="$(( $(date +%s) - ${subscribed_at:-$(date +%s)} ))"
  ok "group caught up (lag 0) ${replay_seconds}s after subscribing"
else
  bad "group did not catch up within ${CATCH_UP_TIMEOUT_S}s (lag $(lag))"
fi

# ---------------------------------------------------------------------------
step "4. the replay must not have changed the effect"
# ---------------------------------------------------------------------------
allocations_after="$(psql_query "SELECT count(*) FROM match_allocations")"
processed_after="$(psql_query "SELECT count(*) FROM processed_events WHERE consumer_group = '$GROUP'")"
duplicates="$(grep -c 'outcome=DUPLICATE' "$CONSUMER_LOG" 2>/dev/null || true)"
applied="$(grep -c 'outcome=APPLIED' "$CONSUMER_LOG" 2>/dev/null || true)"

echo "  match_allocations:  $allocations_before -> $allocations_after"
echo "  processed_events:   $processed_before -> $processed_after"
echo "  records replayed:   ${topic_records:-?}"
echo "  outcomes logged:    APPLIED=${applied:-0} DUPLICATE=${duplicates:-0}"

if [ "$allocations_after" = "$allocations_before" ]; then
  ok "the projection did not grow (no duplicated effect)"
else
  bad "the projection grew from $allocations_before to $allocations_after - the replay duplicated effects"
fi

if [ "$processed_after" = "$processed_before" ]; then
  ok "the inbox did not grow (every record was already processed)"
else
  bad "the inbox grew from $processed_before to $processed_after - records were not recognised"
fi

if [ "${duplicates:-0}" -gt 0 ] 2>/dev/null; then
  ok "the consumer recognised the redeliveries as duplicates ($duplicates record(s))"
else
  bad "no duplicate was logged: the replay did not reach the idempotency check"
fi

if [ "${applied:-0}" -eq 0 ] 2>/dev/null; then
  ok "no record was applied again (no new effect)"
else
  bad "$applied record(s) were applied again during the replay"
fi

# ---------------------------------------------------------------------------
step "5. the consumer is left running"
# ---------------------------------------------------------------------------
if pgrep -f "$MAIN_CLASS" > /dev/null; then
  ok "orchestrator running (stop it with: pkill -f $MAIN_CLASS)"
else
  bad "orchestrator is not running"
fi

printf '\n%s passed, %s failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
