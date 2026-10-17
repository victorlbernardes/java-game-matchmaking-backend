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
# The topic may also hold records that do NOT decode: a producer bug, an event written
# before its schema was registered, or a poison record placed on purpose by
# requests/kafka/verify_dlq.sh. The CLI consumer *stops* on the first of them, unlike
# the orchestrator, which dead-letters it and carries on (ADR 0019). So this script does
# the same thing at the tool level: it steps over the records it cannot read, counts
# them, and still requires that the record it was asked about is there and decodes.
#
# Usage:
#   requests/kafka/verify_match_confirmed.sh                 # any confirmed match
#   requests/kafka/verify_match_confirmed.sh match-551dc626  # one specific match
#
# Exit code: 0 when the subject is registered, at least one record decodes, and the
# requested match (when one was named) is on the log.
set -uo pipefail

REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8082}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-game-matchmaking-backend-kafka-1}"
REGISTRY_CONTAINER="${REGISTRY_CONTAINER:-game-matchmaking-backend-schema-registry-1}"
# The kafka CLI runs inside the broker container, the protobuf consumer inside the
# registry one: same stack, two different views of the compose network.
BROKER_IN_CONTAINER="${BROKER_IN_CONTAINER:-localhost:29092}"
BROKER_FOR_REGISTRY="${BROKER_FOR_REGISTRY:-kafka:29092}"
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

# 2) Read the log through the registry, one chunk at a time (the reads and the parsing
#    are driven from python, because a record the CLI cannot decode ends the read and
#    the next one has to start after it).
python3 - "${EXPECTED_MATCH_ID}" "${SUBJECT}" "${versions}" \
    "${KAFKA_CONTAINER}" "${REGISTRY_CONTAINER}" "${TOPIC}" "${PARTITION}" \
    "${BROKER_IN_CONTAINER}" "${BROKER_FOR_REGISTRY}" "${REGISTRY_IN_CONTAINER}" \
    "${MAX_MESSAGES}" "${QUIET_WINDOW_MS}" <<'PY'
import json
import re
import subprocess
import sys

(expected_match_id, subject, versions, kafka, registry, topic, partition, broker,
 registry_broker, registry_url, max_messages, quiet_window_ms) = sys.argv[1:13]

failures = []
MAX_READS = 200


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  -> {detail}" if detail else ""))
    if not ok:
        failures.append(name)


def docker(*args):
    return subprocess.run(["docker", "exec", *args], capture_output=True,
                          text=True, check=False).stdout


def log_end_offset():
    """The partition's end offset, from the broker: the record count to account for."""
    out = docker(kafka, "kafka-get-offsets", "--bootstrap-server", broker, "--topic", topic)
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[1] == partition:
            return int(parts[2])
    return 0


def read_from(offset):
    """The decoded records from `offset` on: [(createTimeMs, offset, event)].

    The CLI consumer stops at the first record it cannot decode, so an empty list means
    the record at `offset` is one of those - the caller steps over it.
    """
    out = docker(registry, "kafka-protobuf-console-consumer",
                 "--bootstrap-server", registry_broker, "--topic", topic,
                 "--partition", partition, "--offset", str(offset),
                 "--max-messages", str(max_messages), "--timeout-ms", str(quiet_window_ms),
                 "--property", f"schema.registry.url={registry_url}",
                 "--property", "print.offset=true", "--property", "print.timestamp=true")
    found = []
    for line in out.splitlines():
        # Decoded records start with the broker's CreateTime; log lines do not.
        if not line.startswith("CreateTime:"):
            continue
        create_time = re.search(r"CreateTime:(\d+)", line)
        record_offset = re.search(r"Offset:(\d+)", line)
        brace = line.find("{")
        if not create_time or not record_offset or brace < 0:
            continue
        try:
            found.append((int(create_time.group(1)), int(record_offset.group(1)),
                          json.loads(line[brace:])))
        except ValueError:
            continue
    return found


def skill_at_match(player):
    # proto3 JSON omits fields left at their default, and skill 0 is a valid value.
    return player.get("skillAtMatch", 0)


end = log_end_offset()
records = []
undecodable = 0
offset = 0
reads = 0
while offset < end and reads < MAX_READS:
    reads += 1
    chunk = read_from(offset)
    if chunk:
        records.extend(chunk)
        offset = chunk[-1][1] + 1
    else:
        undecodable += 1
        offset += 1

check(f"subject {subject} is registered", bool(versions.strip()), versions.strip() or "(missing)")
check("records decode as MatchConfirmed", bool(records), f"{len(records)} record(s)")
if undecodable:
    print(f"NOTE: {undecodable} record(s) on the topic do not decode - stepped over "
          f"(the dead letter queue covers them: docs/adr/0019-dlq-retry.md)")
check("every record on the partition was decoded or accounted for",
      len(records) + undecodable == end,
      f"decoded={len(records)} undecodable={undecodable} records={end}")
print(f"DECODED_RECORDS={len(records)}")
print(f"UNDECODABLE_RECORDS={undecodable}")

selected = records
if expected_match_id:
    selected = [record for record in records if record[2].get("matchId") == expected_match_id]
    check(f"record for matchId={expected_match_id} is on the log", bool(selected),
          "found=" + str([record[2].get("matchId") for record in records]))

for create_time_ms, _, event in selected:
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
