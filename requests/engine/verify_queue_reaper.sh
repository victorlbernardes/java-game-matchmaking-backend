#!/usr/bin/env bash
#
# W16 - the reaper: an expired ticket must not block the queue (ADR 0025).
#
# What it proves:
#   1. a ticket that expires on its TTL leaves its queue member behind (a ZSET member cannot
#      expire) - the state the W15 KPIs made visible;
#   2. the engine takes the ghost out of the queue within a tick or two, so the depth and the
#      head's wait go back to zero instead of staying at a value nobody can explain;
#   3. with a ghost at the head, two compatible players still get paired - which is the bug
#      W15 measured: before the fix the ghost was read as the head, loaded as a missing
#      ticket, and every compatible player behind it waited forever.
#
# Preconditions: docker compose up -d (redis, postgres), auth-service and matchmaking-api
# running, and the engine running with METRICS_PORT=9091 (see requests/README.md). The engine
# must be built after this change: `mvn -B install -DskipTests` (runbook item 1).
#
# Usage:
#   bash requests/engine/verify_queue_reaper.sh
#   ENGINE_LOG=/tmp/matchmaking-engine-w15.log bash requests/engine/verify_queue_reaper.sh
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
API="http://localhost:8080"
AUTH="http://localhost:8081"
ENGINE_METRICS="http://localhost:${ENGINE_METRICS_PORT:-9091}/metrics"
REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
ENGINE_LOG="${ENGINE_LOG:-/tmp/matchmaking-engine.log}"
QUEUE_KEY="queue:1v1:NA"
WAIT_TIMEOUT_S="${WAIT_TIMEOUT_S:-30}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

redis() { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }

metric_sum() { # <url> <metric> <label-fragment>
  curl -fsS "$1" 2>/dev/null | python3 -c '
import sys
name, label_filter = sys.argv[1], sys.argv[2]
total, seen = 0.0, False
for line in sys.stdin:
    if line.startswith("#") or not line.startswith(name):
        continue
    head, _, value = line.rpartition(" ")
    if label_filter and label_filter not in head:
        continue
    try:
        total += float(value)
        seen = True
    except ValueError:
        pass
print(total if seen else "ABSENT")
' "$2" "$3"
}

metric_contains() { curl -fsS "$1" 2>/dev/null | grep -q "^$2"; }

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
if docker ps --format '{{.Names}}' | grep -q "^${REDIS_CONTAINER}$"; then
  ok "$REDIS_CONTAINER is running"
else
  fail "$REDIS_CONTAINER is not running (docker compose up -d redis)"
fi
if curl -fsS "$AUTH/.well-known/jwks.json" >/dev/null 2>&1; then
  ok "auth-service is up (a token is needed to enqueue)"
else
  fail "auth-service is not answering on $AUTH"
fi
if curl -fsS "$ENGINE_METRICS" >/dev/null 2>&1; then
  ok "the engine's scrape answers on $ENGINE_METRICS"
else
  fail "no scrape on $ENGINE_METRICS: is the engine running with METRICS_PORT=9091 and the W16 build?"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. an expired ticket leaves a ghost behind"
# ---------------------------------------------------------------------------
# Player A waits alone (a skill nobody in the queue shares), so the queue cannot move.
players="$(python3 -c "
import sys, time
sys.path.insert(0, '$REPO_ROOT/requests/e2e')
import run_e2e
base = 'reaper%d' % time.time()
groups, pair, i = {}, None, 0
while pair is None:
    pid = '%s-%d' % (base, i)
    s = run_e2e.skill(pid)
    groups.setdefault(s, []).append(pid)
    if len(groups[s]) >= 2:
        pair = groups[s][:2]
    i += 1
while True:
    ghost = '%s-%d' % (base, i)
    if run_e2e.skill(ghost) != run_e2e.skill(pair[0]):
        break
    i += 1
print(' '.join(pair + [ghost]))
")"
player_a="$(echo "$players" | cut -d' ' -f1)"
player_b="$(echo "$players" | cut -d' ' -f2)"
player_ghost="$(echo "$players" | cut -d' ' -f3)"

token() {
  curl -fsS -X POST "$AUTH/v1/auth/login" -H 'Content-Type: application/json' \
    -d "{\"playerId\":\"$1\",\"displayName\":\"$1\"}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])' 2>/dev/null
}
enqueue() { # <token> -> ticket id
  curl -fsS -X POST "$API/v1/matchmaking/tickets" -H "Authorization: Bearer $1" \
    -H 'Content-Type: application/json' -d '{"gameMode":"1v1","region":"NA","maxLatencyMs":80}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("ticketId", ""))' 2>/dev/null
}

# A known starting point: the queue of the local stack is cleared (the E2E scenarios do the same).
redis flushall >/dev/null
note "redis flushed: the queue starts empty"

token_ghost="$(token "$player_ghost")"
ghost_ticket="$(enqueue "$token_ghost")"
if [ -n "$ghost_ticket" ]; then
  ok "a lone player is waiting: $ghost_ticket"
else
  fail "the enqueue did not return a ticket id"
  printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
  exit 1
fi

# What the 900 s TTL does: the ticket key is gone, the member stays in the ZSET.
redis del "ticket:$ghost_ticket" >/dev/null
queued="$(redis zcard "$QUEUE_KEY")"
if [ "$queued" = "1" ]; then
  ok "the ghost is real: the ticket is gone and the queue still holds the member"
else
  fail "the queue holds $queued members after deleting the ticket (expected 1)"
fi

# ---------------------------------------------------------------------------
step "2. the engine reaps it instead of getting stuck on it"
# ---------------------------------------------------------------------------
# The depth is derived from the queue itself (W15), so it is the honest way to watch the reap.
ghost_gone=0
waited=0
while [ "$waited" -lt "$WAIT_TIMEOUT_S" ]; do
  if [ "$(redis zcard "$QUEUE_KEY")" = "0" ]; then
    ghost_gone=1
    break
  fi
  sleep 1
  waited=$((waited + 1))
done
if [ "$ghost_gone" = "1" ]; then
  ok "the ghost left the queue after ${waited}s, without a player ever being paired"
else
  fail "the ghost is still in $QUEUE_KEY after ${WAIT_TIMEOUT_S}s (the reaper did not run)"
fi

reaped_log="$(grep -c "engine.reapedStaleMembers" "$ENGINE_LOG" 2>/dev/null || true)"
if [ -f "$ENGINE_LOG" ]; then
  if [ "${reaped_log:-0}" -ge 1 ]; then
    ok "the engine said it: $(grep "engine.reapedStaleMembers" "$ENGINE_LOG" | tail -1)"
  else
    fail "no engine.reapedStaleMembers line in $ENGINE_LOG"
  fi
else
  note "no engine log at $ENGINE_LOG (ENGINE_LOG=... to point at it); the queue itself already proved the reap"
fi

depth="$(metric_sum "$ENGINE_METRICS" matchmaking_queue_depth 'mode="1v1",region="NA"')"
if python3 -c "import sys; sys.exit(0 if '$depth' != 'ABSENT' and float('$depth') == 0 else 1)"; then
  ok "the queue depth KPI is back to 0: the number no longer counts a player who left"
else
  note "the KPI reads $depth until the next poll (2 s)"
fi

# ---------------------------------------------------------------------------
step "3. a ghost at the head no longer blocks the pairing"
# ---------------------------------------------------------------------------
# This is the regression W15 measured: ghost at the head, two compatible players behind it,
# and no match. The ghost is put back, the two live players go behind it, and the queue has
# to pair them anyway.
token_a="$(token "$player_a")"
token_b="$(token "$player_b")"
ghost_again="ticket-ghost-$(date +%s)"
redis zadd "$QUEUE_KEY" "$(python3 -c 'import time; print(int((time.time() - 30) * 1000))')" "$ghost_again" >/dev/null
ticket_a="$(enqueue "$token_a")"
ticket_b="$(enqueue "$token_b")"
if [ -n "$ticket_a" ] && [ -n "$ticket_b" ]; then
  ok "two compatible players are behind a ghost at the head of the queue"
else
  fail "the enqueue of the two compatible players failed"
fi

match_id=""
waited=0
while [ -z "$match_id" ] && [ "$waited" -lt "$WAIT_TIMEOUT_S" ]; do
  match_id="$(python3 "$REPO_ROOT/requests/observability/pending_match.py" \
      "$player_a" "$player_b" "$REDIS_CONTAINER" 2>/dev/null)"
  [ -z "$match_id" ] && sleep 1 && waited=$((waited + 1))
done
if [ -n "$match_id" ]; then
  ok "the queue paired them into $match_id in ${waited}s, with the ghost at the head"
else
  fail "no pending match in ${WAIT_TIMEOUT_S}s: the ghost at the head is blocking the queue again"
fi

ghost_score="$(redis zscore "$QUEUE_KEY" "$ghost_again")"
if [ -z "$ghost_score" ] || [ "$ghost_score" = "(nil)" ]; then
  ok "the ghost is off the queue after the pairing"
else
  fail "the ghost is still in the queue (score $ghost_score)"
fi

# Leave nothing behind: the two tickets are CONFIRMING in a pending match the engine will
# sweep in 30 s anyway, and the ghost member is already gone.
redis del "match-pending:$match_id" >/dev/null 2>&1
note "cleanup: the pending match was deleted, the queue is empty ($(redis zcard "$QUEUE_KEY") members)"

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
