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
# The topic is shared and only grows (every load run appends; W16-W17 left ~18k records),
# and one chunk read is a `docker exec` that starts a JVM, so the scan starts near the end
# of the log: the last SCAN_RECORDS (default 2000). SCAN_RECORDS=0 examines the whole log,
# which is what the poison-record accounting of the W13 scenario wants. The script prints
# the window it scanned and the log's end offset, so a check never claims to have read
# more than it did.
#
# Usage:
#   requests/kafka/verify_match_confirmed.sh                 # any confirmed match
#   requests/kafka/verify_match_confirmed.sh match-551dc626  # one specific match
#   SCAN_RECORDS=0 requests/kafka/verify_match_confirmed.sh   # the whole log
#
# Exit code: 0 when the subject is registered, at least one record decodes, and the
# requested match (when one was named) is on the log.
set -uo pipefail

# The harness: one jar, one language, no Python (see tools/scenarios).
MM="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/requests/mm.sh"

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
# How much of the log to examine, counted back from the end. A shared test topic grows with
# every load run - the W16/W17 load scenarios left 18k records on it - and each chunk is a
# `docker exec` that starts a JVM (~8 s), so scanning all of it turns a 30 s check into
# minutes. The records of interest are always the recent ones; `SCAN_RECORDS=0` examines the
# whole log, which is what the dead-letter accounting wants after poison records are placed.
SCAN_RECORDS="${SCAN_RECORDS:-2000}"
# The topic is created by the broker with its default single partition; reading it
# with an explicit offset assignment skips the consumer-group join (several seconds
# per call) that a subscribe would pay.
PARTITION="${PARTITION:-0}"
EXPECTED_MATCH_ID="${1:-}"

echo "MATCH_CONFIRMED on ${TOPIC}"

# 1) The contract must be versioned: producers run with auto.register.schemas=false,
#    so without a registered schema no record could ever have been written.
versions="$(curl -fsS --max-time 5 "${REGISTRY_URL}/subjects/${SUBJECT}/versions" 2>/dev/null || true)"

# 2) Read the log through the registry, one chunk at a time: the reader steps over a record
#    it cannot decode and resumes after it, so one poison record cannot hide the rest.
"$MM" kafka verify-log \
  --match-id "${EXPECTED_MATCH_ID}" \
  --subject "${SUBJECT}" \
  --versions "${versions}" \
  --kafka-container "${KAFKA_CONTAINER}" \
  --registry-container "${REGISTRY_CONTAINER}" \
  --topic "${TOPIC}" \
  --partition "${PARTITION}" \
  --broker "${BROKER_IN_CONTAINER}" \
  --registry-broker "${BROKER_FOR_REGISTRY}" \
  --registry-url "${REGISTRY_IN_CONTAINER}" \
  --max-messages "${MAX_MESSAGES}" \
  --quiet-window-ms "${QUIET_WINDOW_MS}" \
  --scan-records "${SCAN_RECORDS}"
