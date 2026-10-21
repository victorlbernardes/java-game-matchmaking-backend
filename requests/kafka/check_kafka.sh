#!/usr/bin/env bash
#
# W12 task 1 — Kafka + Schema Registry smoke check.
#
# Proves, against the real containers from docker-compose.yml:
#   1. zookeeper / kafka / schema-registry are healthy;
#   2. the Schema Registry answers on the host (http://localhost:8082) and its
#      global compatibility level is FULL;
#   3. the registry reached the broker through the INTERNAL listener (the
#      _schemas topic it owns exists);
#   4. every message produced on the EXTERNAL listener (localhost:9092) comes
#      back out of the other end, in order and with none lost, and reports the
#      broker accept time (stdin -> record CreateTime);
#   5. produce-ack latency at p50/p99 (kafka-producer-perf-test).
#
# Usage:
#   bash requests/kafka/check_kafka.sh
#   MESSAGES=1000 bash requests/kafka/check_kafka.sh
#
# Exit code 0 = all checks passed.
set -uo pipefail

TOPIC="${TOPIC:-w12-roundtrip}"
PERF_TOPIC="${PERF_TOPIC:-w12-perf}"
MESSAGES="${MESSAGES:-200}"
SPACING_MS="${SPACING_MS:-5}"
RECORDS="${RECORDS:-2000}"
RECORD_SIZE="${RECORD_SIZE:-128}"
REGISTRY_URL="${REGISTRY_URL:-http://localhost:8082}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-game-matchmaking-backend-kafka-1}"
ZK_CONTAINER="${ZK_CONTAINER:-game-matchmaking-backend-zookeeper-1}"
SR_CONTAINER="${SR_CONTAINER:-game-matchmaking-backend-schema-registry-1}"

TMP_DIR="$(mktemp -d)"
# The harness: one jar, one language, no Python (see tools/scenarios).
MM="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/requests/mm.sh"
trap 'rm -rf "$TMP_DIR"' EXIT

PASS=0
FAIL=0

ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }
step() { printf '\n== %s\n' "$1"; }

# ---------------------------------------------------------------------------
step "1. containers healthy"
# ---------------------------------------------------------------------------
for pair in "$ZK_CONTAINER:zookeeper" "$KAFKA_CONTAINER:kafka" "$SR_CONTAINER:schema-registry"; do
  container="${pair%%:*}"
  name="${pair##*:}"
  status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$container" 2>/dev/null)"
  if [ "$status" = "healthy" ]; then ok "$name is healthy"; else bad "$name is '$status' (run: docker compose up -d zookeeper kafka schema-registry)"; fi
done

# ---------------------------------------------------------------------------
step "2. Schema Registry REST API (host port 8082)"
# ---------------------------------------------------------------------------
subjects="$(curl -fsS "$REGISTRY_URL/subjects" 2>/dev/null)"
if [ -n "$subjects" ]; then ok "GET /subjects -> $subjects"; else bad "GET $REGISTRY_URL/subjects unreachable"; fi

config="$(curl -fsS "$REGISTRY_URL/config" 2>/dev/null)"
case "$config" in
  *'"compatibilityLevel":"FULL"'*) ok "GET /config -> $config";;
  *) bad "GET /config -> ${config:-<empty>} (expected compatibilityLevel FULL)";;
esac

# ---------------------------------------------------------------------------
step "3. registry reached the broker over the INTERNAL listener"
# ---------------------------------------------------------------------------
topics="$(docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)"
if printf '%s' "$topics" | grep -qx '_schemas'; then
  ok "_schemas topic exists (registry is a working Kafka client)"
else
  bad "_schemas topic missing - registry cannot talk to the broker"
fi

# ---------------------------------------------------------------------------
step "4. produce -> consume round trip on the EXTERNAL listener"
# ---------------------------------------------------------------------------
# Arrival stamp: the consumer's stdout is piped through a tiny reader that
# writes the local receive time in front of every value, so the measured
# latency is "produced -> visible to a consumer", not "produced -> end of run".
# Sender: writes into the producer's stdin (a FIFO) keeping the JVM warm, so
# per-message latency is not polluted by a fresh 1.5s JVM boot per batch.
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic "$TOPIC" >/dev/null 2>&1 || true
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1

consumer_out="$TMP_DIR/consumed.txt"
: >"$consumer_out"
( docker exec "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic "$TOPIC" --from-beginning --max-messages "$((MESSAGES + 1))" --timeout-ms 60000 \
    --property print.timestamp=true 2>/dev/null \
  | "$MM" kafka stamp >>"$consumer_out" ) &
consumer_pid=$!

fifo="$TMP_DIR/in.fifo"
mkfifo "$fifo"
docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --bootstrap-server localhost:9092 \
  --topic "$TOPIC" <"$fifo" >/dev/null 2>&1 &
producer_pid=$!
exec 3>"$fifo"   # keeps the producer's stdin open until the end of the run

# Warm-up: proves the consumer is subscribed before the clock starts.
printf 'warmup:%s\n' "$("$MM" kafka epoch)" >&3
for _ in $(seq 1 30); do grep -q 'warmup' "$consumer_out" && break; sleep 1; done
if grep -q 'warmup' "$consumer_out"; then
  ok "consumer subscribed before measuring (warm-up round trip)"
else
  bad "consumer never received the warm-up message"
fi

"$MM" kafka send --count "$MESSAGES" --spacing-ms "$SPACING_MS" --fifo "$fifo"
for _ in $(seq 1 30); do [ "$(wc -l <"$consumer_out")" -ge "$((MESSAGES + 1))" ] && break; sleep 1; done
exec 3>&-
wait "$producer_pid" 2>/dev/null
wait "$consumer_pid" 2>/dev/null
if "$MM" kafka report --path "$consumer_out" --expected "$MESSAGES"; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); fi

# ---------------------------------------------------------------------------
step "5. produce-ack latency (kafka-producer-perf-test)"
# ---------------------------------------------------------------------------
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic "$PERF_TOPIC" >/dev/null 2>&1 || true
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 \
  --create --topic "$PERF_TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1

perf="$(docker exec "$KAFKA_CONTAINER" kafka-producer-perf-test \
  --topic "$PERF_TOPIC" --num-records "$RECORDS" --record-size "$RECORD_SIZE" \
  --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=all 2>&1 | tail -1)"
if [ -n "$perf" ]; then ok "$perf"; else bad "kafka-producer-perf-test produced no output"; fi

# ---------------------------------------------------------------------------
printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
