#!/usr/bin/env bash
#
# W13 task 2 — a record that cannot be handled must not block the partition.
#
# Two failure shapes, both end to end against the real stack:
#
#   phase 1 — the payload never decodes. The consumer is handed bytes that are not a
#             MATCH_CONFIRMED (what a producer with a bug in front of the registry
#             would write).
#   phase 2 — the payload decodes but the effect cannot be applied. The projection row
#             for that match is already there, so the INSERT conflicts. This is a data
#             problem, not a wire problem, and it is the one a replay is most likely to
#             run into.
#
# Both must end the same way: three attempts, the original record on
# matchmaking.events.match-confirmed-dlq with the diagnosis in its headers, and the
# offset committed. The commit is the point of the whole exercise — a record that is
# retried forever, or left uncommitted, stalls every event behind it.
#
# What is asserted about the payload is not "something arrived": phase 1 checks the
# poisoned bytes are on the dead letter topic verbatim, and phase 2 checks the
# dead-lettered record still decodes through the Schema Registry into the same event it
# was. Keeping the bytes is only worth something if they are replayable.
#
# Preconditions:
#   docker compose up -d            (kafka + schema-registry + postgres healthy)
#   mvn -B install -DskipTests      (the services resolve their modules from ~/.m2)
#   the orchestrator running, with at least one event already projected
#   (requests/mm.sh e2e projects one in scenario S11)
#
# The dead letter topic is created here if it is missing: a service is not allowed to
# invent topics (partition count and retention are deliberate operational choices), so
# whoever deploys this service creates it - and this script does it for the local stack
# before it produces anything, because a dead letter that cannot be written is an
# uncommitted record, which is the failure it is supposed to prevent.
#
# This script stops the orchestrator for phase 2 (offsets can only be reset for a group
# with no members) and leaves it running at the end. Usage:
#   bash requests/kafka/verify_dlq.sh
#   GROUP=match-orchestrator-2 bash requests/kafka/verify_dlq.sh
#
# Exit code 0 = both failure shapes ended on the dead letter topic with the offset
# committed and no duplicated effect.
set -uo pipefail

# The harness: one jar, one language, no Python (see tools/scenarios).
MM="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/requests/mm.sh"

cd "$(dirname "$0")/../.." || exit 1

TOPIC="${TOPIC:-matchmaking.events.match-confirmed}"
DLQ_TOPIC="${DLQ_TOPIC:-${TOPIC}-dlq}"
PARTITION="${PARTITION:-0}"
GROUP="${GROUP:-match-orchestrator}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-game-matchmaking-backend-kafka-1}"
REGISTRY_CONTAINER="${REGISTRY_CONTAINER:-game-matchmaking-backend-schema-registry-1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
BROKER_IN_CONTAINER="${BROKER_IN_CONTAINER:-localhost:29092}"
# The protobuf consumer runs in the schema-registry container, which reaches the broker
# by its compose name and the registry on its own localhost.
REGISTRY_IN_CONTAINER="${REGISTRY_IN_CONTAINER:-http://localhost:8081}"
BROKER_FOR_REGISTRY="${BROKER_FOR_REGISTRY:-kafka:29092}"
DB_CONTAINER_USER="${DB_CONTAINER_USER:-matchmaking}"
DB_CONTAINER_NAME="${DB_CONTAINER_NAME:-matchmaking}"
MAIN_CLASS="com.victor.matchmaking.orchestrator.MatchOrchestratorApplication"
CONSUMER_LOG="${CONSUMER_LOG:-/tmp/match-orchestrator-dlq.log}"
# Payload of phase 1: not a Confluent-framed protobuf in any reading, and ASCII, so the
# bytes can be compared exactly through the console consumer.
POISON_PAYLOAD="${POISON_PAYLOAD:-NOT-A-PROTOBUF-PAYLOAD}"
# Three attempts at 250 ms doubling is under a second of retrying; the rest is startup
# and rebalance time.
WAIT_TIMEOUT_S="${WAIT_TIMEOUT_S:-120}"
WAIT_STEP_S=2
DLQ_DUMP="${DLQ_DUMP:-/tmp/match-orchestrator-dlq-dump.txt}"

PASS=0
FAIL=0

ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '  %s\n' "$1"; }

psql_query() {
  docker exec "$POSTGRES_CONTAINER" psql -U "$DB_CONTAINER_USER" -d "$DB_CONTAINER_NAME" -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

psql_write() {
  docker exec "$POSTGRES_CONTAINER" psql -U "$DB_CONTAINER_USER" -d "$DB_CONTAINER_NAME" -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

# Committed offset, log end offset and lag of the group on our topic.
group_position() {
  docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BROKER_IN_CONTAINER" \
    --describe --group "$GROUP" 2>/dev/null | awk -v topic="$TOPIC" '$2 == topic { print $4" "$5" "$6 }'
}

committed() { group_position | awk '{ print $1 }'; }
log_end()   { group_position | awk '{ print $2 }'; }
lag()       { group_position | awk '{ print $3 }'; }

members() {
  docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BROKER_IN_CONTAINER" \
    --describe --group "$GROUP" 2>/dev/null | awk -v topic="$TOPIC" '$2 == topic && $7 != "-" { print $7 }'
}

wait_for_lag_zero() {
  for _ in $(seq 1 "$((WAIT_TIMEOUT_S / WAIT_STEP_S))"); do
    [ "$(lag)" = "0" ] && return 0
    sleep "$WAIT_STEP_S"
  done
  return 1
}

wait_for_empty_group() {
  for _ in $(seq 1 "$((WAIT_TIMEOUT_S / WAIT_STEP_S))"); do
    [ -z "$(members)" ] && return 0
    sleep "$WAIT_STEP_S"
  done
  return 1
}

# The dead letter topic is shared and only grows: every run of this scenario appends to it
# and a dead letter cannot be deleted. It used to be read as "the oldest 100 records", so
# once the topic passed 100 records (it holds 133 now) the dump stopped containing the
# letter this run had just produced -- and the "newest of this stage" selection below read
# a stale one from a previous run, failing every header assertion against the wrong record.
# Read the tail of the topic instead: the letters of interest are always the recent ones.
DLQ_SCAN="${DLQ_SCAN:-200}"

dlq_end_offset() {
  docker exec "$KAFKA_CONTAINER" kafka-get-offsets \
    --bootstrap-server "$BROKER_IN_CONTAINER" --topic "$DLQ_TOPIC" 2>/dev/null \
    | awk -F: -v p="$PARTITION" '$2 == p { print $3; exit }'
}

# The dead letter topic into $DLQ_DUMP: one line per record, headers first, payload
# last, tab separated (the console consumer's own format).
dump_dlq() {
  local end start
  end="$(dlq_end_offset)"
  [ -n "$end" ] || end=0
  start=$(( end > DLQ_SCAN ? end - DLQ_SCAN : 0 ))
  docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server "$BROKER_IN_CONTAINER" \
    --topic "$DLQ_TOPIC" --partition "$PARTITION" --offset "$start" \
    --max-messages "$DLQ_SCAN" --timeout-ms 4000 \
    --property print.headers=true --property print.offset=true \
    > "$DLQ_DUMP" 2>/dev/null || true
}

dlq_has_stage() {
  dump_dlq
  # -a: the dump is binary (a payload starts with the wire format's 0x00 magic byte),
  # and BSD grep otherwise answers with "Binary file ... matches" instead of matching.
  grep -aq "dlq.stage:$1" "$DLQ_DUMP" 2>/dev/null
}

wait_for_dlq_stage() {
  for _ in $(seq 1 "$((WAIT_TIMEOUT_S / WAIT_STEP_S))"); do
    dlq_has_stage "$1" && return 0
    sleep "$WAIT_STEP_S"
  done
  return 1
}

# Offset of the latest dead letter of a given stage: after a rewind the same poison
# record can be dead-lettered more than once. Read in binary, so a payload that is not
# valid UTF-8 cannot derail the parsing.
dlq_offset_for_stage() {
  "$MM" kafka dlq-offset --path "$DLQ_DUMP" --stage "$1"
}

# Assertions on the $DLQ_DUMP contents. Parsed by the harness in bytes, because a
# payload is bytes and a header value can contain anything, including commas and tabs.
# Args: stage, original offset, original event id ("-" when unknown), original payload
# ("-" when it cannot be compared as text).
dlq_checks() {
  local output
  output="$("$MM" kafka dlq-checks --path "$DLQ_DUMP" --stage "$1" --offset "$2" \
    --event-id "$3" --payload "$4" --topic "$TOPIC" --group "$GROUP" --partition "$PARTITION")"
  while IFS= read -r line; do
    case "$line" in
      CHECK_PASS:*) ok "${line#CHECK_PASS:}" ;;
      CHECK_FAIL:*) bad "${line#CHECK_FAIL:}" ;;
      *) [ -n "$line" ] && note "$line" ;;
    esac
  done <<< "$output"
}

# ---------------------------------------------------------------------------
step "1. preconditions"
# ---------------------------------------------------------------------------
if pgrep -f "$MAIN_CLASS" > /dev/null; then
  ok "orchestrator is running (pid $(pgrep -f "$MAIN_CLASS" | tr '\n' ' '))"
else
  bad "orchestrator is not running - start it first:"
  note "mvn -pl services/match-orchestrator exec:java -Dexec.mainClass=$MAIN_CLASS &"
  note "(see the start order in requests/README.md)"
fi

allocations_before="$(psql_query "SELECT count(*) FROM match_allocations")"
processed_before="$(psql_query "SELECT count(*) FROM processed_events WHERE consumer_group = '$GROUP'")"

if [ -n "${allocations_before:-}" ] && [ -n "${processed_before:-}" ]; then
  ok "orchestrator tables readable: match_allocations=$allocations_before processed_events($GROUP)=$processed_before"
else
  bad "cannot read the orchestrator tables (run: docker compose up -d postgres)"
fi

if [ "${allocations_before:-0}" -gt 0 ] 2>/dev/null; then
  ok "there is a projected match for the conflicting effect of phase 2"
else
  bad "nothing projected yet - run the E2E first (requests/mm.sh e2e)"
fi

# The dead letter topic must exist BEFORE a record fails: an undeliverable dead letter
# leaves the offset uncommitted, which is the stall this sprint removes.
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BROKER_IN_CONTAINER" \
  --create --if-not-exists --topic "$DLQ_TOPIC" --partitions 1 --replication-factor 1 > /dev/null 2>&1
if docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BROKER_IN_CONTAINER" \
  --describe --topic "$DLQ_TOPIC" > /dev/null 2>&1; then
  ok "dead letter topic $DLQ_TOPIC exists"
else
  bad "dead letter topic $DLQ_TOPIC is missing and could not be created"
fi

[ "$FAIL" -eq 0 ] || { echo; echo "ABORTED: fix the preconditions above."; exit 1; }

# ---------------------------------------------------------------------------
step "2. phase 1: a payload that never decodes"
# ---------------------------------------------------------------------------
poison_offset="$(log_end)"
note "producing ${#POISON_PAYLOAD} byte(s) of non-event at offset $poison_offset"
printf '%s' "$POISON_PAYLOAD" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
  --bootstrap-server "$BROKER_IN_CONTAINER" --topic "$TOPIC" > /dev/null 2>&1

if [ "$(log_end)" -gt "$poison_offset" ] 2>/dev/null; then
  ok "the poisoned record is on the log at offset $poison_offset"
else
  bad "the poisoned record did not reach the log (end offset $(log_end))"
fi

if wait_for_dlq_stage DECODE; then
  ok "the consumer dead-lettered it instead of stalling on it"
else
  bad "no DECODE dead letter appeared within ${WAIT_TIMEOUT_S}s (see $CONSUMER_LOG)"
fi

# The property this sprint exists for: the group is past the record that could not be
# handled, so nothing behind it on the partition is stuck.
if wait_for_lag_zero; then
  if [ "$(committed)" -gt "$poison_offset" ] 2>/dev/null; then
    ok "the offset was committed past it (committed=$(committed) > offset=$poison_offset, lag 0)"
  else
    bad "lag is 0 but the committed offset $(committed) did not pass $poison_offset"
  fi
else
  bad "the partition is stuck: lag $(lag) after ${WAIT_TIMEOUT_S}s - the record was never committed"
fi

dump_dlq
dlq_checks DECODE "$poison_offset" "-" "$POISON_PAYLOAD"

if grep -q 'stage=DECODE' "$CONSUMER_LOG" 2>/dev/null && grep -q 'attempt=3/3' "$CONSUMER_LOG" 2>/dev/null; then
  ok "the log shows the attempts ($(grep -c 'stage=DECODE' "$CONSUMER_LOG") line(s)) before giving up"
else
  bad "the log does not show three attempts with stage=DECODE (see $CONSUMER_LOG)"
fi

allocations_after="$(psql_query "SELECT count(*) FROM match_allocations")"
processed_after="$(psql_query "SELECT count(*) FROM processed_events WHERE consumer_group = '$GROUP'")"
if [ "$allocations_after" = "$allocations_before" ] && [ "$processed_after" = "$processed_before" ]; then
  ok "it left no state behind ($allocations_after row(s) projected, inbox unchanged)"
else
  bad "the poison record left state behind: allocations $allocations_before->$allocations_after, inbox $processed_before->$processed_after"
fi

# ---------------------------------------------------------------------------
step "3. phase 2: a payload that decodes but whose effect cannot be applied"
# ---------------------------------------------------------------------------
# Take the newest event this group has applied, drop only its inbox row (the projection
# row stays, as it would after a partial restore or a manual fix) and rewind to it. The
# same event is delivered again: it decodes, it is not a duplicate any more (no inbox
# row), and its effect conflicts on match_id.
event_id="$(psql_query "SELECT event_id FROM processed_events WHERE consumer_group = '$GROUP' ORDER BY record_offset DESC LIMIT 1")"
event_offset="$(psql_query "SELECT record_offset FROM processed_events WHERE consumer_group = '$GROUP' ORDER BY record_offset DESC LIMIT 1")"
if [ -n "${event_id:-}" ] && [ -n "${event_offset:-}" ]; then
  ok "rewinding to the newest projected event (event $event_id at offset $event_offset)"
else
  bad "no inbox row to rewind to - phase 2 needs a group that has projected something"
  printf '\n%s passed, %s failed\n' "$PASS" "$FAIL"
  exit 1
fi

deleted="$(psql_write "DELETE FROM processed_events WHERE event_id = '$event_id'")"
if [ "$deleted" = "DELETE1" ]; then
  ok "its inbox row was removed (the effect stays, the record applies again)"
else
  bad "could not remove the inbox row ($deleted)"
fi

pkill -f "$MAIN_CLASS" && ok "stopped the orchestrator to rewind its group" || bad "could not stop the orchestrator"
if wait_for_empty_group; then
  ok "consumer group $GROUP has no members"
else
  bad "consumer group $GROUP still has members after ${WAIT_TIMEOUT_S}s"
fi

docker exec "$KAFKA_CONTAINER" kafka-consumer-groups --bootstrap-server "$BROKER_IN_CONTAINER" \
  --reset-offsets --to-offset "$event_offset" --topic "$TOPIC" --group "$GROUP" --execute > /dev/null 2>&1
rewound=0
for _ in $(seq 1 5); do
  [ "$(committed)" = "$event_offset" ] && { rewound=1; break; }
  sleep "$WAIT_STEP_S"
done
if [ "$rewound" = "1" ]; then
  ok "group rewound to offset $event_offset"
else
  bad "group was not rewound to $event_offset (committed=$(committed))"
fi

: > "$CONSUMER_LOG"
nohup mvn -pl services/match-orchestrator exec:java -Dexec.mainClass="$MAIN_CLASS" >> "$CONSUMER_LOG" 2>&1 &
note "consumer log: $CONSUMER_LOG"
for _ in $(seq 1 "$((WAIT_TIMEOUT_S / WAIT_STEP_S))"); do
  grep -q "consumer.subscribed" "$CONSUMER_LOG" && break
  sleep "$WAIT_STEP_S"
done
if grep -q "consumer.subscribed" "$CONSUMER_LOG"; then
  ok "consumer restarted and subscribed"
else
  bad "consumer did not start (see $CONSUMER_LOG; did you run: mvn -B install -DskipTests ?)"
fi

if wait_for_dlq_stage APPLY; then
  ok "the conflicting effect was dead-lettered after its attempts"
else
  bad "no APPLY dead letter appeared within ${WAIT_TIMEOUT_S}s (see $CONSUMER_LOG)"
fi

if wait_for_lag_zero; then
  ok "the partition moved on again (lag 0, committed=$(committed))"
else
  bad "the partition is stuck at lag $(lag)"
fi

dump_dlq
dlq_checks APPLY "$event_offset" "$event_id" "-"

if grep -q 'stage=APPLY' "$CONSUMER_LOG" 2>/dev/null; then
  ok "the log shows the effect failure (stage=APPLY), not a decoding one"
else
  bad "the log does not show an APPLY failure (see $CONSUMER_LOG)"
fi

allocations_final="$(psql_query "SELECT count(*) FROM match_allocations")"
if [ "$allocations_final" = "$allocations_before" ]; then
  ok "the effect was not duplicated by the replay ($allocations_final row(s))"
else
  bad "the projection changed: $allocations_before -> $allocations_final"
fi

# The dead letter is only worth keeping if it can be replayed, so decode that record
# again through the registry: if the bytes had been re-serialized or truncated, this is
# where it shows.
dlq_offset="$(dlq_offset_for_stage APPLY)"
decoded="$(docker exec "$REGISTRY_CONTAINER" kafka-protobuf-console-consumer \
  --bootstrap-server "$BROKER_FOR_REGISTRY" --topic "$DLQ_TOPIC" --partition "$PARTITION" \
  --offset "${dlq_offset:-0}" --max-messages 1 --timeout-ms 4000 \
  --property schema.registry.url="$REGISTRY_IN_CONTAINER" 2>/dev/null | grep '^{' | head -1)"
decoded_event_id="$(printf '%s' "$decoded" | "$MM" json - meta.eventId 2>/dev/null || true)"
if [ -n "$decoded_event_id" ] && [ "$decoded_event_id" = "$event_id" ]; then
  ok "the dead-lettered bytes still decode into the same event (eventId $event_id)"
else
  note "decoded: ${decoded:0:200}"
  bad "the dead-lettered payload no longer decodes into event $event_id (got '${decoded_event_id:-nothing}')"
fi

# ---------------------------------------------------------------------------
step "4. put the state back"
# ---------------------------------------------------------------------------
restored="$(psql_write "INSERT INTO processed_events (event_id, consumer_group, topic, partition_id, record_offset, processed_at)
  VALUES ('$event_id', '$GROUP', '$TOPIC', $PARTITION, $event_offset, now()) ON CONFLICT (event_id) DO NOTHING")"
if [ "$restored" = "INSERT01" ]; then
  ok "the inbox row was restored (the fixture leaves the group's state as it found it)"
else
  bad "could not restore the inbox row ($restored)"
fi

if pgrep -f "$MAIN_CLASS" > /dev/null; then
  ok "orchestrator running (stop it with: pkill -f $MAIN_CLASS)"
else
  bad "orchestrator is not running"
fi

printf '\n%s passed, %s failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
