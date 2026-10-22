#!/usr/bin/env bash
#
# W20 - one queue is a single point of contention: partition it and measure the hotspot
# (ADR 0027/0028).
#
# What it proves:
#   1. before: with one partition, 2000 arrivals are 2000 members of one key - 100% of the load
#      on one shard (the hotspot is the layout itself);
#   2. after: with 8 partitions, the same 2000 arrivals are spread over 8 keys, the deepest
#      within 15% of the mean, and the sum is still exactly 2000 (nothing was lost on the way);
#   3. the scrape agrees with Redis: the per-partition gauges, the queue depth and the hotspot
#      ratio are the numbers the queue actually holds, not a derived guess;
#   4. two players that hash to **different** partitions still pair - the engine merges the heads
#      of every partition, so a partition never becomes a matching boundary;
#   5. the admission cap is not multiplied by the partition count: the sum over partitions never
#      exceeds QUEUE_MAX_DEPTH.
#
# Preconditions: docker compose up -d redis postgres, auth-service on 8081, the working tree
# built (`mvn -o -B install -DskipTests`, runbook item 1). The script stops any engine it finds
# (it needs to own the pairing loop) and starts its own API on 8094 (metrics 9195) and engine
# (metrics 9194); it does not touch an API on 8080. No engine is left running at the end.
#
# Usage:
#   bash requests/partitioning/verify_partitioning.sh
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AUTH="http://localhost:8081"
API="http://localhost:${API_PORT:-8094}"
API_METRICS="http://localhost:${API_METRICS_PORT:-9195}/metrics"
ENGINE_METRICS="http://localhost:${ENGINE_METRICS_PORT:-9194}/metrics"
REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
MM="$REPO_ROOT/requests/mm.sh"
PLAYERS="${PLAYERS:-2000}"
PARTITIONS="${PARTITIONS:-8}"
CAP="${CAP:-100}"
OUT="/tmp/w20-probe.json"
API_LOG="${API_LOG:-/tmp/w20-api.log}"
ENGINE_LOG="${ENGINE_LOG:-/tmp/w20-engine.log}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

redis() { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }

field() { # <key> -> the value from $OUT
  "$MM" json "$OUT" "$1"
}

stop_services() {
  for pid_file in /tmp/w20-api.pid /tmp/w20-engine.pid; do
    if [ -f "$pid_file" ]; then
      kill -9 "$(cat "$pid_file")" 2>/dev/null || true
      rm -f "$pid_file"
    fi
  done
  # The engine owns the pairing loop and the scenario needs a quiet queue: an engine left by a
  # previous run would pair the arrivals while they are being counted.
  pkill -f MatchmakingEngineApplication 2>/dev/null || true
}
trap stop_services EXIT

free_port() { # <port>
  local holders
  holders="$(lsof -ti "tcp:$1" 2>/dev/null || true)"
  if [ -n "$holders" ]; then
    echo "$holders" | xargs kill -9 2>/dev/null || true
    sleep 1
  fi
}

wait_http() { # <url> <timeout>
  local deadline=$((SECONDS + $2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS "$1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_api() { # <partitions> <max-depth>
  stop_services
  free_port "$(echo "$API" | sed 's/.*://')"
  free_port 9195
  (
    cd "$REPO_ROOT" || exit 1
    PORT=8094 METRICS_PORT=9195 INSTANCE_ID=api-w20 \
      QUEUE_PARTITIONS="$1" QUEUE_MAX_DEPTH="$2" \
      mvn -o -q -pl services/matchmaking-api exec:java \
        -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication \
        </dev/null >"$API_LOG" 2>&1 &
    echo $! >/tmp/w20-api.pid
  )
  if wait_http "$API/health" 90; then
    return 0
  fi
  return 1
}

start_engine() { # <partitions>
  free_port 9194
  (
    cd "$REPO_ROOT" || exit 1
    METRICS_PORT=9194 QUEUE_PARTITIONS="$1" \
      mvn -o -q -pl services/matchmaking-engine exec:java \
        -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication \
        </dev/null >"$ENGINE_LOG" 2>&1 &
    echo $! >/tmp/w20-engine.pid
  )
  if wait_http "$ENGINE_METRICS" 120; then
    return 0
  fi
  return 1
}

probe() { # <partitions-subcommand and flags...>
  "$MM" partitions "$@" >"$OUT" 2>/tmp/w20-probe.err
  local code=$?
  if [ $code -ne 0 ]; then
    printf '        probe error: %s\n' "$(cat /tmp/w20-probe.err | tail -3)"
  fi
  return $code
}

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
if docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
  ok "$REDIS_CONTAINER is running"
else
  fail "$REDIS_CONTAINER is not running (docker compose up -d redis postgres)"
fi
if docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
  ok "postgres is running (the engine migrates on boot)"
else
  fail "postgres is not running (docker compose up -d postgres)"
fi
if curl -fsS "$AUTH/.well-known/jwks.json" >/dev/null 2>&1; then
  ok "auth-service answers on $AUTH"
else
  fail "auth-service is not answering on $AUTH"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. before: one queue, one key, the whole load on one shard"
# ---------------------------------------------------------------------------
if start_api 1 0; then
  ok "an API with QUEUE_PARTITIONS=1 is listening on $API"
else
  fail "the API did not start (log $API_LOG)"
fi
redis flushall >/dev/null
if probe dist --api "$API" --partitions 1 --players "$PLAYERS" --prefix w20-dist-; then
  before_sum="$(field sum)"
  before_keys="$(field keys_with_members)"
  before_share="$(field max_share_pct)"
  note "admitted=$(field admitted) keys=$(field counts) sum=$before_sum"
  if [ "$before_sum" = "$PLAYERS" ]; then
    ok "all $PLAYERS arrivals are in the queue"
  else
    fail "expected $PLAYERS members, found $before_sum"
  fi
  if [ "$before_keys" = "1" ]; then
    ok "one key holds every member - the single point of contention the sprint is about"
  else
    fail "expected one key, found $before_keys"
  fi
  before_ratio="$("$MM" ratio "$("$MM" mul "$before_share" "$PARTITIONS")" 100 2)"
  note "max share of the load: ${before_share}% of the queue on one key; against a $PARTITIONS-way target that is a hotspot ratio of $before_ratio"
  if "$MM" eq "$before_share" 100; then
    ok "the deepest key holds 100% of the queue (ratio $before_ratio against $PARTITIONS partitions)"
  else
    fail "expected 100% on one key, got ${before_share}%"
  fi
else
  fail "the distribution probe failed"
fi

# ---------------------------------------------------------------------------
step "2. after: the same arrivals spread over $PARTITIONS keys"
# ---------------------------------------------------------------------------
if start_api "$PARTITIONS" 0; then
  ok "an API with QUEUE_PARTITIONS=$PARTITIONS is listening on $API"
else
  fail "the API did not start with $PARTITIONS partitions (log $API_LOG)"
fi
redis flushall >/dev/null
if probe dist --api "$API" --partitions "$PARTITIONS" --players "$PLAYERS" --prefix w20-dist-; then
  after_sum="$(field sum)"
  after_keys="$(field keys_with_members)"
  after_hotspot="$(field hotspot)"
  after_share="$(field max_share_pct)"
  note "counts=$(field counts)"
  note "sum=$after_sum keys=$after_keys hotspot=${after_hotspot} max-share=${after_share}%"
  if [ "$after_sum" = "$PLAYERS" ]; then
    ok "every arrival is in exactly one partition (sum $after_sum)"
  else
    fail "expected $PLAYERS members across partitions, found $after_sum"
  fi
  if [ "$after_keys" = "$PARTITIONS" ]; then
    ok "all $PARTITIONS partitions are used (no empty key, no key left out)"
  else
    fail "expected $PARTITIONS partitions in use, found $after_keys"
  fi
  if "$MM" le "$after_hotspot" 1.15; then
    ok "the deepest partition is within 15% of the mean (hotspot $after_hotspot)"
  else
    fail "hotspot $after_hotspot is above 1.15: the hash is not spreading the queue"
  fi
  if "$MM" le "$after_share" 14; then
    ok "no partition holds more than 14% of the queue (${after_share}%)"
  else
    fail "one partition holds ${after_share}% of the queue"
  fi
  if "$MM" gt "$before_ratio" "$after_hotspot"; then
    ok "the hotspot fell from $before_ratio (one key) to $after_hotspot ($PARTITIONS keys)"
  else
    fail "the partitioned layout ($after_hotspot) is not better than the single key ($before_ratio)"
  fi
else
  fail "the distribution probe failed"
fi

# ---------------------------------------------------------------------------
step "3. the scrape agrees with Redis"
# ---------------------------------------------------------------------------
redis flushall >/dev/null
if start_engine "$PARTITIONS"; then
  ok "an engine with QUEUE_PARTITIONS=$PARTITIONS exposes its scrape on $ENGINE_METRICS"
else
  fail "the engine did not start (log $ENGINE_LOG)"
fi
# 5v5 with three players never pairs (it needs ten): the queue stays put while the poller reads
# it, so the gauge and Redis are answering the same question.
if probe dist --api "$API" --mode 5v5 --partitions "$PARTITIONS" --players 3 --prefix w20-gauge-; then
  note "queued $(field sum) players in 5v5/NA across $(field keys_with_members) partitions"
fi
sleep 4
if probe metrics --metrics "$ENGINE_METRICS" --mode 5v5 --partitions "$PARTITIONS"; then
  series="$(field partition_depth)"
  gauge_depth="$(field depth)"
  gauge_hotspot="$(field hotspot)"
  gauge_expected="$(field expected_hotspot)"
  note "partition_depth=$series depth=$gauge_depth hotspot=$gauge_hotspot expected=$gauge_expected"
  if "$MM" eq "$("$MM" json-length "$OUT" partition_depth)" "$PARTITIONS"; then
    ok "the scrape publishes one series per partition ($PARTITIONS)"
  else
    fail "expected $PARTITIONS partition series, got $series"
  fi
  if "$MM" eq "$gauge_depth" 3; then
    ok "queue_depth is the sum over partitions (3 players waiting)"
  else
    fail "expected queue_depth 3, got $gauge_depth"
  fi
  if "$MM" approx "$gauge_hotspot" "$gauge_expected" 0.01; then
    ok "the hotspot ratio is max/mean of the partition depths ($gauge_hotspot)"
  else
    fail "the hotspot ($gauge_hotspot) does not match the partition depths ($gauge_expected)"
  fi
  if "$MM" approx "$("$MM" json-sum "$OUT" partition_depth)" "$gauge_depth" 0.001; then
    ok "the per-partition series add up to the queue depth"
  else
    fail "the partition series do not add up to the queue depth"
  fi
else
  fail "the metrics probe failed"
fi

# ---------------------------------------------------------------------------
step "4. two players in different partitions still pair"
# ---------------------------------------------------------------------------
redis flushall >/dev/null
if probe cross --api "$API" --partitions "$PARTITIONS" --mode 1v1 --timeout 10; then
  match_id="$(field match_id)"
  part_a="$(field partition_a)"
  part_b="$(field partition_b)"
  key_a="$(field key_a_depth)"
  key_b="$(field key_b_depth)"
  waited="$(field waited_s)"
  states="$(field ticket_states)"
  note "players $(field player_a) (partition $part_a) and $(field player_b) (partition $part_b), skill $(field skill)"
  note "match=$match_id waited=${waited}s ticket states=$states"
  if [ -n "$match_id" ] && [ "$part_a" != "$part_b" ]; then
    ok "a roster spanning partitions $part_a and $part_b paired (the heads are merged)"
  else
    fail "no match formed across partitions (match='$match_id')"
  fi
  if [ "$key_a" = "0" ] && [ "$key_b" = "0" ]; then
    ok "both members left their own partition atomically"
  else
    fail "a member stayed behind (depths $key_a/$key_b)"
  fi
  if printf '%s' "$states" | grep -q CONFIRMING && printf '%s' "$states" | grep -q CONFIRMING; then
    ok "both tickets are CONFIRMING: the match is real, not a leftover member"
  else
    fail "unexpected ticket states: $states"
  fi
  if "$MM" le "$waited" 5; then
    ok "the match formed in ${waited}s (the engine does not wait for a partition to fill)"
  else
    fail "the match took ${waited}s: the partitions are acting as a boundary"
  fi
else
  fail "the cross-partition probe failed"
fi

# ---------------------------------------------------------------------------
step "5. the cap is not multiplied by the partition count"
# ---------------------------------------------------------------------------
if start_api "$PARTITIONS" "$CAP"; then
  ok "an API with QUEUE_MAX_DEPTH=$CAP and QUEUE_PARTITIONS=$PARTITIONS is listening"
else
  fail "the API did not start with the cap (log $API_LOG)"
fi
redis flushall >/dev/null
expected_cap=$(( (CAP / PARTITIONS) * PARTITIONS ))
note "per-partition cap = floor($CAP/$PARTITIONS) = $((CAP / PARTITIONS)); the sum is $expected_cap"
if probe cap --api "$API" --players 200 --mode 1v1; then
  cap_admitted="$(field admitted)"
  cap_refused="$(field refused)"
  cap_depth="$(field depth)"
  note "admitted=$cap_admitted refused=$cap_refused depth=$cap_depth"
  if [ "$cap_depth" -le "$CAP" ]; then
    ok "the queue never holds more than QUEUE_MAX_DEPTH ($cap_depth <= $CAP)"
  else
    fail "the partitioned queue holds $cap_depth, above the cap of $CAP"
  fi
  if [ "$cap_admitted" = "$expected_cap" ]; then
    ok "exactly $expected_cap arrivals were admitted and the rest refused ($cap_refused)"
  else
    fail "expected $expected_cap admitted, got $cap_admitted (refused $cap_refused)"
  fi
  if [ "$cap_refused" -gt 0 ]; then
    ok "the boundary refuses instead of growing ($cap_refused refusals counted)"
  else
    fail "no refusal: the cap did not bind"
  fi
else
  fail "the cap probe failed"
fi

# ---------------------------------------------------------------------------
step "6. cleanup"
# ---------------------------------------------------------------------------
stop_services
redis flushall >/dev/null
ok "the API and the engine started by this scenario are stopped"

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
