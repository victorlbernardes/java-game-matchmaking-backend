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
cat >"$TMP_DIR/stamp.py" <<'PY'
import sys, time
for line in sys.stdin:
    print(f"{int(time.time() * 1000)}\t{line.strip()}", flush=True)
PY

# Sender: writes into the producer's stdin (a FIFO) keeping the JVM warm, so
# per-message latency is not polluted by a fresh 1.5s JVM boot per batch.
cat >"$TMP_DIR/send.py" <<'PY'
import sys, time
count, spacing_ms, fifo = int(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
out = open(fifo, 'w')
for i in range(count):
    out.write(f"{i}:{int(time.time() * 1000)}\n")
    out.flush()
    time.sleep(spacing_ms / 1000)
PY

cat >"$TMP_DIR/report.py" <<'PY'
import sys
path, expected = sys.argv[1], int(sys.argv[2])
rows = []
for line in open(path):
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 3 or not parts[0].isdigit():
        continue
    received_at = int(parts[0])
    created_at = int(parts[1].split(':')[1])   # record timestamp set by the producer
    value = parts[2]
    if value.startswith('warmup:') or ':' not in value:
        continue
    index, sent_at = (int(x) for x in value.split(':', 1))
    rows.append((index, sent_at, created_at, received_at))

def stats(values):
    values = sorted(values)
    return values[0], values[len(values) // 2], values[-1]

if len(rows) != expected:
    print(f"  FAIL  consumed {len(rows)}/{expected} messages")
    sys.exit(1)
if [row[0] for row in rows] != sorted(row[0] for row in rows):
    print("  FAIL  messages arrived out of order on a single partition")
    sys.exit(1)

broker = stats([created_at - sent_at for _, sent_at, created_at, _ in rows])
cli = stats([received_at - sent_at for _, sent_at, _, received_at in rows])
print(f"  PASS  consumed {len(rows)}/{expected} messages, in order, none lost")
print(f"        broker accept (write -> record CreateTime): min={broker[0]}ms p50={broker[1]}ms max={broker[2]}ms")
print(f"        consumer CLI floor (write -> CLI stdout):    min={cli[0]}ms p50={cli[1]}ms max={cli[2]}ms"
      f"  [kafka-console-consumer output granularity, not a broker property]")
PY

docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic "$TOPIC" >/dev/null 2>&1 || true
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1

consumer_out="$TMP_DIR/consumed.txt"
: >"$consumer_out"
( docker exec "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic "$TOPIC" --from-beginning --max-messages "$((MESSAGES + 1))" --timeout-ms 60000 \
    --property print.timestamp=true 2>/dev/null \
  | python3 -u "$TMP_DIR/stamp.py" >>"$consumer_out" ) &
consumer_pid=$!

fifo="$TMP_DIR/in.fifo"
mkfifo "$fifo"
docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --bootstrap-server localhost:9092 \
  --topic "$TOPIC" <"$fifo" >/dev/null 2>&1 &
producer_pid=$!
exec 3>"$fifo"   # keeps the producer's stdin open until the end of the run

# Warm-up: proves the consumer is subscribed before the clock starts.
printf 'warmup:%s\n' "$(python3 -c 'import time; print(int(time.time() * 1000))')" >&3
for _ in $(seq 1 30); do grep -q 'warmup' "$consumer_out" && break; sleep 1; done
if grep -q 'warmup' "$consumer_out"; then
  ok "consumer subscribed before measuring (warm-up round trip)"
else
  bad "consumer never received the warm-up message"
fi

python3 "$TMP_DIR/send.py" "$MESSAGES" "$SPACING_MS" "$fifo"
for _ in $(seq 1 30); do [ "$(wc -l <"$consumer_out")" -ge "$((MESSAGES + 1))" ] && break; sleep 1; done
exec 3>&-
wait "$producer_pid" 2>/dev/null
wait "$consumer_pid" 2>/dev/null
if python3 "$TMP_DIR/report.py" "$consumer_out" "$MESSAGES"; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); fi

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
