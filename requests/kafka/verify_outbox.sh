#!/usr/bin/env bash
#
# W14 - the transactional outbox (ADR 0018).
#
# What it proves:
#   1. an event whose publisher never ran is still delivered. A row left PENDING - which is
#      exactly what a crash between "the match is committed" and "the broker has the event"
#      leaves behind, and what the old confirmation path (persist, then publish) lost - stays
#      in the table while no engine is running, and the relay publishes it after a restart.
#      The event id on the topic is the id stored with the match, so this is the same event,
#      not a new one;
#   2. the relay does not stall on a row it cannot handle: an unknown topic is parked with
#      the reason, and the good row behind it still goes out;
#   3. the numbers: the delay the relay adds (occurred_at -> published_at) and the attempts
#      a row needed.
#
# Preconditions: docker compose stack up (postgres, kafka, schema-registry) and at least one
# confirmed match already in outbox_events (E2E S11, or any confirmation by hand). The script
# runs `mvn install` itself (the engine must not be started against stale sibling artifacts -
# runbook item 1), stops the engine to create the "no publisher" window and starts it again.
#
# Usage:
#   bash requests/kafka/verify_outbox.sh
#
# Exit code 0 = every check held. The engine is left running.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
ENGINE_LOG="${ENGINE_LOG:-/tmp/matchmaking-engine-outbox.log}"
ENGINE_MAIN="com.victor.matchmaking.engine.MatchmakingEngineApplication"
# The relay polls every 250 ms and the backoff of a retried row is 1 s; this is generous
# enough to survive a slow CI box and short enough to fail a broken relay.
PUBLISH_TIMEOUT_S="${PUBLISH_TIMEOUT_S:-60}"
# How long the row must stay PENDING with no engine running before we believe it.
NO_PUBLISHER_WINDOW_S=3

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }

psqlq() { docker exec "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -At -c "$1"; }

# One column of one row, or the empty string.
scalar() { psqlq "$1" | head -1 | tr -d '\r'; }

# Waits until a SELECT returns the expected value, polling every second.
wait_for() { # <label> <sql> <expected> <timeout-seconds>
  local label="$1" sql="$2" expected="$3" timeout="$4" waited=0 value=""
  while [ "$waited" -lt "$timeout" ]; do
    value="$(scalar "$sql")"
    if [ "$value" = "$expected" ]; then
      note "$label after ${waited}s"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  note "$label: timed out after ${timeout}s, last value '${value}'"
  return 1
}

stop_engine() {
  pkill -f "$ENGINE_MAIN" 2>/dev/null || true
  local waited=0
  while pgrep -f "$ENGINE_MAIN" >/dev/null 2>&1 && [ "$waited" -lt 30 ]; do
    sleep 1
    waited=$((waited + 1))
  done
  ! pgrep -f "$ENGINE_MAIN" >/dev/null 2>&1
}

start_engine() {
  # </dev/null and the redirects matter: the engine outlives this script, and it must not
  # keep the caller's stdout open (a `bash verify_outbox.sh | tail` would never see EOF).
  (cd "$REPO_ROOT" && nohup mvn -B -q -pl services/matchmaking-engine exec:java \
      -Dexec.mainClass="$ENGINE_MAIN" </dev/null >"$ENGINE_LOG" 2>&1 &)
}

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
  bad "the Postgres container ($POSTGRES_CONTAINER) is not running: docker compose up -d postgres"
  printf '\n== result: %d passed, %d failed (nothing ran)\n' "$PASS" "$FAIL"
  exit 1
fi
ok "Postgres is up"

if ! mvn -B -q -f "$REPO_ROOT/pom.xml" install -DskipTests >/tmp/outbox-install.log 2>&1; then
  bad "the repository does not build (see /tmp/outbox-install.log)"
  printf '\n== result: %d passed, %d failed (nothing ran)\n' "$PASS" "$FAIL"
  exit 1
fi
ok "every module is installed (the engine will not run against stale artifacts)"

# The newest event a match already published: the script rewinds it to PENDING rather than
# inventing a payload, so what travels is a real MATCH_CONFIRMED, byte for byte.
EVENT_ID="$(scalar "SELECT event_id FROM outbox_events WHERE status = 'PUBLISHED' AND partition_key LIKE 'match-%' ORDER BY published_at DESC NULLS LAST LIMIT 1")"
if [ -z "$EVENT_ID" ]; then
  bad "no published event in outbox_events: confirm a match first (E2E S11 or the Postman flow)"
  printf '\n== result: %d passed, %d failed (nothing ran)\n' "$PASS" "$FAIL"
  exit 1
fi
MATCH_ID="$(scalar "SELECT partition_key FROM outbox_events WHERE event_id = '${EVENT_ID}'")"
note "rewinding event ${EVENT_ID} (match ${MATCH_ID})"
ok "there is a published event to work with"

# ---------------------------------------------------------------------------
step "1. an event with nobody to publish it is not lost"
# ---------------------------------------------------------------------------
if stop_engine; then
  ok "the engine is stopped: no relay is running"
else
  bad "the engine would not stop (is it running under a different command line?)"
fi

psqlq "UPDATE outbox_events SET status = 'PENDING', published_at = NULL, claimed_at = NULL, claimed_by = NULL, attempts = 0, next_attempt_at = NULL WHERE event_id = '${EVENT_ID}'" >/dev/null
if [ "$(scalar "SELECT status FROM outbox_events WHERE event_id = '${EVENT_ID}'")" = "PENDING" ]; then
  ok "the event is back in the queue, as a crash between the commit and the publish would leave it"
else
  bad "could not put the event back to PENDING"
fi

sleep "$NO_PUBLISHER_WINDOW_S"
if [ "$(scalar "SELECT status FROM outbox_events WHERE event_id = '${EVENT_ID}'")" = "PENDING" ]; then
  ok "it stays there with no engine running (${NO_PUBLISHER_WINDOW_S}s): this is the lost-event window the outbox closes"
else
  bad "the row changed while no engine was running"
fi

note "starting the engine ($ENGINE_LOG)"
REWIND_AT="$(date +%s)"
start_engine
if wait_for "the relay published the row again" \
    "SELECT status FROM outbox_events WHERE event_id = '${EVENT_ID}'" "PUBLISHED" "$PUBLISH_TIMEOUT_S"; then
  ok "the relay picked up the event nobody had published"
else
  bad "the relay did not publish the event within ${PUBLISH_TIMEOUT_S}s (see $ENGINE_LOG)"
fi

# How long the event waited with nobody to publish it is not the relay's latency - the row
# was rewound when the script started. What matters here is how long after the relay came
# back the event was out again; the steady-state lag the relay adds is in ADR 0018.
PUBLISHED_AT="$(scalar "SELECT extract(epoch FROM published_at)::bigint FROM outbox_events WHERE event_id = '${EVENT_ID}'")"
RECOVERY_S=$((PUBLISHED_AT - REWIND_AT))
note "the event was out again ${RECOVERY_S}s after the engine came back (engine boot included)"
if [ "$RECOVERY_S" -lt 15 ]; then
  ok "a queue that outlives its relay drains again in ${RECOVERY_S}s"
else
  bad "the relay took ${RECOVERY_S}s to drain a row that was already waiting"
fi
note "row age when it went out (occurred_at -> published_at): $(scalar "SELECT round(extract(epoch FROM (published_at - occurred_at)) * 1000) FROM outbox_events WHERE event_id = '${EVENT_ID}'") ms, attempts: $(scalar "SELECT attempts FROM outbox_events WHERE event_id = '${EVENT_ID}'")"

# The bytes on the topic, decoded through the Schema Registry: the event that was published
# is the event the row carried, not a fresh one built by the publisher.
if bash "$REPO_ROOT/requests/kafka/verify_match_confirmed.sh" "$MATCH_ID" >/tmp/outbox-topic.log 2>&1; then
  ok "the event decodes on the topic for ${MATCH_ID} (same event id, see $EVENT_ID)"
else
  bad "the event for ${MATCH_ID} is not readable on the topic (see /tmp/outbox-topic.log)"
  tail -5 /tmp/outbox-topic.log | sed 's/^/        /'
fi

# ---------------------------------------------------------------------------
step "2. a row the relay cannot handle is parked, and the queue keeps moving"
# ---------------------------------------------------------------------------
POISON_ID="$(psqlq "INSERT INTO outbox_events (event_id, topic, partition_key, event_type, payload, occurred_at, status, created_at) VALUES (gen_random_uuid(), 'matchmaking.events.not-a-topic', 'poison-w14', 'NOT_AN_EVENT', '\\x01'::bytea, now(), 'PENDING', now() - INTERVAL '10 seconds') RETURNING event_id" | head -1 | tr -d '\r')"
if [ -n "$POISON_ID" ]; then
  ok "a row with a topic no code knows was inserted ($POISON_ID)"
else
  bad "could not insert the poison row"
fi

psqlq "UPDATE outbox_events SET status = 'PENDING', published_at = NULL, claimed_at = NULL, claimed_by = NULL, attempts = 0, next_attempt_at = NULL WHERE event_id = '${EVENT_ID}'" >/dev/null

if wait_for "the undecodable row was parked" \
    "SELECT status FROM outbox_events WHERE event_id = '${POISON_ID}'" "PARKED" "$PUBLISH_TIMEOUT_S"; then
  ok "the relay gave up on it with a reason, instead of retrying bytes no code can read"
else
  bad "the unknown-topic row was not parked within ${PUBLISH_TIMEOUT_S}s"
fi

REASON="$(scalar "SELECT last_error FROM outbox_events WHERE event_id = '${POISON_ID}'")"
case "$REASON" in
  *"no contract is registered"*) ok "the reason is on the row: ${REASON}";;
  *) bad "the parked row does not say why: '${REASON}'";;
esac

if wait_for "the good row behind it was still published" \
    "SELECT status FROM outbox_events WHERE event_id = '${EVENT_ID}'" "PUBLISHED" "$PUBLISH_TIMEOUT_S"; then
  ok "a parked row does not block the queue"
else
  bad "the row behind the parked one never went out"
fi

psqlq "DELETE FROM outbox_events WHERE event_id = '${POISON_ID}'" >/dev/null
ok "the poison row was removed (it exists only for this run)"

# ---------------------------------------------------------------------------
step "3. the queue as it stands"
# ---------------------------------------------------------------------------
psqlq "SELECT status, count(*) FROM outbox_events GROUP BY status ORDER BY status" \
  | sed 's/^/        /'
note "PENDING is the backlog, PUBLISHED is the log of what the topic already has, PARKED needs a human"

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
