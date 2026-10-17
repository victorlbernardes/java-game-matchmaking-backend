#!/usr/bin/env bash
# Verifies MATCH_CONFIRMED on the Kafka log: the bytes must decode through the
# Schema Registry into the typed event, with the envelope and the roster intact.
#
# This is the strongest evidence short of a Java consumer: the decoder reads the
# schemaId from the record, fetches the schema from the registry (resolving the
# registered imports) and renders the payload as JSON.
#
# Preconditions: docker-compose stack up (kafka + schema-registry) and at least one
# confirmed match. The E2E scenario S10 drives one; the Postman collection (e2e-w5)
# confirms via WebSocket when running by hand.
#
# Usage:
#   requests/kafka/verify_match_confirmed.sh                 # any confirmed match
#   requests/kafka/verify_match_confirmed.sh match-551dc626  # one specific match
#
# Exit code: 0 when the subject is registered and every expected record decodes.
set -uo pipefail

REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8082}"
REGISTRY_CONTAINER="${REGISTRY_CONTAINER:-game-matchmaking-backend-schema-registry-1}"
BROKER_IN_CONTAINER="${BROKER_IN_CONTAINER:-kafka:29092}"
REGISTRY_IN_CONTAINER="${REGISTRY_IN_CONTAINER:-http://localhost:8081}"
TOPIC="matchmaking.events.match-confirmed"
SUBJECT="${TOPIC}-value"
# Console consumer exit conditions: read the whole (small) test log, then stop
# quickly. --timeout-ms acts as a quiet window, so this never hangs.
MAX_MESSAGES="${MAX_MESSAGES:-500}"
QUIET_WINDOW_MS="${QUIET_WINDOW_MS:-4000}"
# The topic is created by the broker with its default single partition; reading it
# with an explicit offset assignment skips the consumer-group join (several seconds
# per call) that a subscribe would pay.
PARTITION="${PARTITION:-0}"
EXPECTED_MATCH_ID="${1:-}"

echo "MATCH_CONFIRMED on ${TOPIC}"

# 1) The contract must be versioned: producers run with auto.register.schemas=false,
#    so without a registered schema no record could ever have been written.
versions="$(curl -fsS --max-time 5 "${REGISTRY_URL}/subjects/${SUBJECT}/versions" 2>/dev/null || true)"

# 2) Decode the log through the registry (log lines are filtered out: the decoded
#    records are the lines carrying the broker's CreateTime).
decoded="$(docker exec "${REGISTRY_CONTAINER}" kafka-protobuf-console-consumer \
    --bootstrap-server "${BROKER_IN_CONTAINER}" \
    --topic "${TOPIC}" \
    --partition "${PARTITION}" \
    --offset earliest \
    --max-messages "${MAX_MESSAGES}" \
    --timeout-ms "${QUIET_WINDOW_MS}" \
    --property schema.registry.url="${REGISTRY_IN_CONTAINER}" \
    --property print.timestamp=true 2>/dev/null | grep '^CreateTime:' || true)"

# The check below runs as a program fed on stdin, so the records travel by file
# (stdin is taken by the interpreter).
records_file="$(mktemp)"
trap 'rm -f "${records_file}"' EXIT
printf '%s\n' "${decoded}" > "${records_file}"

python3 - "${EXPECTED_MATCH_ID}" "${SUBJECT}" "${versions}" "${records_file}" <<'PY'
import json
import sys

expected_match_id, subject, versions = sys.argv[1], sys.argv[2], sys.argv[3]
failures = []


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  -> {detail}" if detail else ""))
    if not ok:
        failures.append(name)


def skill_at_match(player):
    # proto3 JSON omits fields left at their default, and skill 0 is a valid value.
    return player.get("skillAtMatch", 0)


records = []
for line in open(sys.argv[4]):
    line = line.strip()
    if not line:
        continue
    create_time, _, payload = line.partition("\t")
    try:
        records.append((int(create_time.split(":")[1]), json.loads(payload)))
    except (IndexError, ValueError):
        failures.append("undecodable record")

check(f"subject {subject} is registered", bool(versions.strip()), versions.strip() or "(missing)")
check("records decode as MatchConfirmed", bool(records), f"{len(records)} record(s)")
print(f"DECODED_RECORDS={len(records)}")

selected = records
if expected_match_id:
    selected = [r for r in records if r[1].get("matchId") == expected_match_id]
    check(f"record for matchId={expected_match_id} is on the log", bool(selected),
          "found=" + str([r[1].get("matchId") for r in records]))

for create_time_ms, event in selected:
    match_id = event.get("matchId", "?")
    meta = event.get("meta") or {}
    players = event.get("players") or []
    check(f"{match_id}: envelope carries type, contract version and event id",
          meta.get("eventType") == "MATCH_CONFIRMED" and meta.get("schemaVersion") == 1
          and bool(meta.get("eventId")) and bool(meta.get("occurredAt")), json.dumps(meta))
    check(f"{match_id}: roster carries player, team (1|2) and skill snapshot",
          bool(players) and all(p.get("playerId") and p.get("team") in (1, 2)
                                and 0 <= skill_at_match(p) <= 100 for p in players),
          json.dumps([{"playerId": p.get("playerId"), "team": p.get("team", 0),
                       "skillAtMatch": skill_at_match(p)} for p in players]))
    print(f"EVENT_JSON={json.dumps(event, separators=(',', ':'))}")
    print(f"EVENT_CREATE_TIME_MS={create_time_ms}")

print(f"RESULT: {len(failures)} failed")
sys.exit(1 if failures else 0)
PY
