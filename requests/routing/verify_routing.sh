#!/usr/bin/env bash
#
# W19 - the connection is not pinned to one instance (ADR 0026).
#
# What it proves:
#   1. two API instances are distinguishable (name, presence, metrics) and share one Redis;
#   2. each notification is delivered by the instance that holds the player's socket, and the
#      instance that does not hold it counts the miss instead of pretending it delivered;
#   3. when the instance holding a player is killed **hard**, the player reconnects on the
#      survivor and the heartbeat re-presents the pending confirmation the lost socket never
#      delivered;
#   4. the confirmation is accepted by any instance, the match is confirmed (not dissolved),
#      and the reverse index is cleared;
#   5. a heartbeat with nothing owed re-presents nothing.
#
# Before W19, step 3 had no answer: the notification was published once to an instance that no
# longer existed, the reconnect got a PONG, and the match dissolved at its deadline with every
# process still alive looking healthy (W16 measured 24 of 1024 matches that way).
#
# Preconditions: docker compose up -d (redis, postgres), auth-service running on 8081, the
# engine running with METRICS_PORT=9091, and the working tree built (`mvn -B install
# -DskipTests`, runbook item 1). This script starts its own two APIs on 8082/8083 (metrics
# 9092/9093) and does not touch an API already running on 8080.
#
# Usage:
#   bash requests/routing/verify_routing.sh
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AUTH="http://localhost:8081"
API_A="http://localhost:${API_A_PORT:-8092}"
API_B="http://localhost:${API_B_PORT:-8093}"
METRICS_A="http://localhost:${METRICS_A_PORT:-9192}/metrics"
METRICS_B="http://localhost:${METRICS_B_PORT:-9193}/metrics"
ENGINE_METRICS="http://localhost:${ENGINE_METRICS_PORT:-9091}/metrics"
REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
PROBE_LOG="${PROBE_LOG:-/tmp/w19-routing-probe.log}"
API_A_LOG="${API_A_LOG:-/tmp/w19-api-a.log}"
API_B_LOG="${API_B_LOG:-/tmp/w19-api-b.log}"
WAIT_TIMEOUT_S="${WAIT_TIMEOUT_S:-90}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

redis() { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }

cleanup() {
  for pid_file in /tmp/w19-api-a.pid /tmp/w19-api-b.pid; do
    if [ -f "$pid_file" ]; then
      kill -9 "$(cat "$pid_file")" 2>/dev/null || true
      rm -f "$pid_file"
    fi
  done
}
trap cleanup EXIT

free_port() { # <port>
  local holders
  holders="$(lsof -ti "tcp:$1" 2>/dev/null || true)"
  if [ -n "$holders" ]; then
    echo "$holders" | xargs kill -9 2>/dev/null || true
    sleep 1
  fi
}

start_instance() { # <port> <metrics-port> <instance-id> <log>
  free_port "$1"
  free_port "$2"
  (
    cd "$REPO_ROOT" || exit 1
    PORT="$1" METRICS_PORT="$2" INSTANCE_ID="$3" \
      mvn -o -q -pl services/matchmaking-api exec:java \
        -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication \
        </dev/null >"$4" 2>&1 &
    echo $! >"/tmp/w19-$3.pid"
  )
}

wait_health() { # <base-url> <instance-id> <timeout>
  local base="$1" wanted="$2" deadline=$((SECONDS + $3))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS "$base/health" 2>/dev/null | grep -q "\"instance\":\"$wanted\""; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
if docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
  ok "$REDIS_CONTAINER is running"
else
  fail "$REDIS_CONTAINER is not running (docker compose up -d redis postgres)"
fi
if docker ps --format '{{.Names}}' | grep -q "^game-matchmaking-backend-postgres-1$"; then
  ok "postgres is running (both instances migrate on boot)"
else
  fail "postgres is not running (docker compose up -d postgres)"
fi
if curl -fsS "$AUTH/.well-known/jwks.json" >/dev/null 2>&1; then
  ok "auth-service answers on $AUTH (a token is needed to enqueue)"
else
  fail "auth-service is not answering on $AUTH"
fi
if curl -fsS "$ENGINE_METRICS" >/dev/null 2>&1; then
  ok "the engine's scrape answers on $ENGINE_METRICS"
else
  fail "no scrape on $ENGINE_METRICS: is the engine running with METRICS_PORT=9091?"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. two instances, one Redis"
# ---------------------------------------------------------------------------
# A known starting point: the queue and the presence keys of earlier runs are cleared (the
# other scenarios do the same). The engine's script cache survives FLUSHALL - only a restart
# or SCRIPT FLUSH clears it - so the pairing keeps working.
redis flushall >/dev/null
note "redis flushed: the queue and presence start empty"

start_instance "${API_A_PORT:-8092}" "${METRICS_A_PORT:-9192}" "api-a" "$API_A_LOG"
start_instance "${API_B_PORT:-8093}" "${METRICS_B_PORT:-9193}" "api-b" "$API_B_LOG"
note "starting two API instances (mvn exec:java); this is the slow part"

if wait_health "$API_A" "api-a" "$WAIT_TIMEOUT_S"; then
  ok "instance A answers /health as api-a"
else
  fail "instance A did not come up in ${WAIT_TIMEOUT_S}s (log: $API_A_LOG)"
fi
if wait_health "$API_B" "api-b" "$WAIT_TIMEOUT_S"; then
  ok "instance B answers /health as api-b"
else
  fail "instance B did not come up in ${WAIT_TIMEOUT_S}s (log: $API_B_LOG)"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (the instances did not start)\n' "$PASS" "$FAIL"
  exit 1
fi
if curl -fsS "$METRICS_A" >/dev/null 2>&1 && curl -fsS "$METRICS_B" >/dev/null 2>&1; then
  ok "each instance scrapes on its own port (9192/9193)"
else
  fail "a metrics scrape is missing: the socket counters are read per instance"
fi

# ---------------------------------------------------------------------------
step "2. routing, delivery and the re-presentation (probe)"
# ---------------------------------------------------------------------------
API_A_PID="$(cat /tmp/w19-api-a.pid 2>/dev/null || echo 0)"
note "api-a pid=$API_A_PID; the probe kills it hard once the match is pending"

API_A="$API_A" API_B="$API_B" AUTH="$AUTH" \
  METRICS_A="$METRICS_A" METRICS_B="$METRICS_B" ENGINE_METRICS="$ENGINE_METRICS" \
  REDIS_CONTAINER="$REDIS_CONTAINER" API_A_PID="$API_A_PID" \
  python3 "$REPO_ROOT/requests/routing/routing_probe.py" 2>&1 | tee "$PROBE_LOG"
probe_rc=${PIPESTATUS[0]}

probe_pass="$(grep -c '^  \[PASS\]' "$PROBE_LOG" || true)"
probe_fail="$(grep -c '^  \[FAIL\]' "$PROBE_LOG" || true)"
PASS=$((PASS + ${probe_pass:-0}))
FAIL=$((FAIL + ${probe_fail:-0}))

if [ "$probe_rc" -ne 0 ] && [ "${probe_fail:-0}" -eq 0 ]; then
  fail "the probe exited $probe_rc without reporting a failed check (see $PROBE_LOG)"
fi

# ---------------------------------------------------------------------------
step "3. the killed instance left the player on the survivor"
# ---------------------------------------------------------------------------
if curl -fsS "$API_A" >/dev/null 2>&1; then
  fail "instance A still answers: the probe did not kill it"
else
  ok "instance A is down (SIGKILL, so no close handler ran)"
fi
if curl -fsS "$METRICS_B" >/dev/null 2>&1; then
  ok "instance B is still serving"
else
  fail "instance B did not survive"
fi
note "logs: $API_A_LOG / $API_B_LOG"

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
