#!/usr/bin/env bash
#
# W16 - the objectives are a contract: the SLOs and their error budget (ADR 0021).
#
# What it proves:
#   1. the rules are loaded, healthy and the ones in the repository - a rule file that never
#      loaded looks exactly like a satisfied objective, which is why this is checked first and
#      by path (a relative `rule_files` entry that resolves somewhere else loads zero groups);
#   2. the objectives are answered by the KPIs: driving one real match by hand, the SL-1 latency
#      ratio and the SL-2 confirm ratio of that window are both 1.0, the recorded rules agree
#      with the hand count, and the budget spent is inside 1%;
#   3. the recorded queue rules follow the queue itself (a waiting player raises them, cancelling
#      brings them back), so an objective cannot drift away from the thing it measures;
#   4. the alerts are quiet and alive: none firing, and each expression answers at a threshold
#      that must trip it and stays empty at the real one - an expression that is always empty
#      (a typo, a series that no longer exists) never fires either, and that is the failure this
#      check exists to catch.
#
# Preconditions: docker compose up -d (redis, postgres, kafka, schema-registry, prometheus,
# grafana) and auth-service + matchmaking-api + matchmaking-engine running (mvn install first -
# runbook item 1). The engine's metrics port is 9091, so it is started with METRICS_PORT=9091.
#
# Usage:
#   bash requests/observability/check_slos.sh
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MM="$REPO_ROOT/requests/mm.sh"
REDIS_PORT="${REDIS_PORT:-6379}"
API="http://localhost:8080"
AUTH="http://localhost:8081"
ENGINE_METRICS="http://localhost:${ENGINE_METRICS_PORT:-9091}/metrics"
PROMETHEUS="http://localhost:${PROMETHEUS_PORT:-9095}"
REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
RULE_FILE="ops/prometheus/rules/matchmaking-slos.yml"
# A recording rule evaluates every 15 s (ops/prometheus/rules/matchmaking-slos.yml), so a check
# that reads a recorded series waits for one evaluation - and a little more than one.
RULE_WAIT_S="${RULE_WAIT_S:-35}"
WAIT_TIMEOUT_S="${WAIT_TIMEOUT_S:-30}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

redis() { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }

# The numeric value of an expression, or NO SERIES when it has no result (both "no data" and
# "condition false" look like that here, so a caller that needs to tell them apart counts series).
promql() { # <expr> -> the value, or NO SERIES when the expression answers nothing
  "$MM" observability prometheus-query "$PROMETHEUS" "$1" 2>/dev/null
}

# How many series an expression answers with. 0 means the expression is empty at this threshold.
promql_series() { # <expr> -> how many series it answers with (0 = the expression is empty)
  "$MM" observability prometheus-series "$PROMETHEUS" "$1" 2>/dev/null
}

# A ratio in [0, 1] and not NaN: 0/0 is NaN, and an objective read off an empty window is not a
# satisfied objective.
is_ratio() { # <value> -> 0 when it is a number in [0, 1] (0/0 is NaN, and NaN is not a verdict)
  "$MM" within "$1" 0 1 2>/dev/null
}

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
if [ -f "$REPO_ROOT/$RULE_FILE" ]; then
  ok "the rule file is in the repository ($RULE_FILE)"
else
  fail "no $RULE_FILE in the repository (nothing to load)"
fi

if curl -fsS "$PROMETHEUS/-/ready" >/dev/null 2>&1; then
  ok "Prometheus answers on $PROMETHEUS (ready)"
else
  fail "no Prometheus on $PROMETHEUS (docker compose up -d prometheus grafana)"
fi
if curl -fsS "$ENGINE_METRICS" >/dev/null 2>&1; then
  ok "the engine exposes a scrape on $ENGINE_METRICS"
else
  fail "no scrape on $ENGINE_METRICS: is matchmaking-engine running with METRICS_PORT=9091?"
fi

# Every objective is computed from a scrape of a running product, so a product that is down puts
# every ratio at NaN - and a supporting alert fires, which section 4 would report as a failure
# without saying why.
up_values="$("$MM" observability prometheus-up "$PROMETHEUS" 2>/dev/null)"
note "prometheus up: ${up_values:-<no answer>}"
case "$up_values" in
  *matchmaking-api=1*matchmaking-engine=1*) ok "Prometheus scrapes both jobs (up == 1)" ;;
  *) fail "a job is not being scraped: ${up_values:-<no answer>}" ;;
esac

if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. the objectives are loaded, healthy, and the ones in the repository"
# ---------------------------------------------------------------------------
curl -fsS "$PROMETHEUS/api/v1/rules" > "$TMP/rules.json" 2>/dev/null
rules_summary="$("$MM" observability rules "$TMP/rules.json" 2>/dev/null)"
field() { printf '%s\n' "$1" | sed -n "s/^$2=//p"; }

groups="$(field "$rules_summary" groups)"
note "rules loaded: $(field "$rules_summary" recording) recording, $(field "$rules_summary" alerting) alerting, in 2 groups"
if [ "$groups" = "2" ]; then
  ok "both groups are loaded (recording + alerts)"
else
  fail "expected 2 rule groups, Prometheus loaded $groups - a rule_files path that resolves elsewhere loads none"
fi

file_path="$(field "$rules_summary" files)"
if [ "$file_path" = "$RULE_FILE" ]; then
  ok "the loaded rules are this repository's file ($file_path)"
else
  fail "the loaded rules come from '$file_path', not $RULE_FILE"
fi

names="$(field "$rules_summary" groupnames)"
if [ "$names" = "matchmaking-slo-alerts,matchmaking-slo-recording" ]; then
  ok "the group names are the documented ones"
else
  fail "unexpected group names: $names"
fi

# Four objectives: 10 recording rules (4 latency + 4 confirm + 2 queue) and 8 alerts.
if [ "$(field "$rules_summary" recording)" = "10" ] && [ "$(field "$rules_summary" alerting)" = "8" ]; then
  ok "the four objectives are expressed as 10 recorded series and 8 alerts"
else
  fail "expected 10 recording + 8 alerting, got $(field "$rules_summary" recording) + $(field "$rules_summary" alerting)"
fi

if [ "$(field "$rules_summary" unhealthy)" = "none" ] \
   && [ "$(field "$rules_summary" unevaluated)" = "none" ]; then
  ok "every rule evaluated without an error (health=ok, lastEvaluation set)"
else
  fail "rules with a problem: health=$(field "$rules_summary" unhealthy) never-evaluated=$(field "$rules_summary" unevaluated)"
fi

if [ "$(field "$rules_summary" withoutlabels)" = "none" ]; then
  ok "every alert carries the routing labels (severity, slo)"
else
  fail "alerts without severity/slo: $(field "$rules_summary" withoutlabels)"
fi

# A burn-rate alert without `for` pages on a single evaluation - the reason the durations exist.
if [ "$(field "$rules_summary" fastburnfor)" = "MatchmakingLatencyFastBurn:120s,MatchmakingConfirmFastBurn:120s" ]; then
  ok "the fast-burn alerts hold their condition for 2 minutes before firing"
else
  fail "unexpected for-durations: $(field "$rules_summary" fastburnfor)"
fi

# ---------------------------------------------------------------------------
step "2. one real match, driven by hand: the ratios are 1.0 and the rules agree"
# ---------------------------------------------------------------------------
# Two players with the same deterministic skill (the pairing policy refuses a wider spread than
# a ticket's search range, so the ids are chosen, not invented); the skill helper is the E2E's.
# Two players the policy can pair: the ids come from the harness, which uses the product's own
# DeterministicSkill instead of importing the E2E's copy of it.
players="$("$MM" pairing pair --prefix "slo$(date +%s)-")"
player_a="$(echo "$players" | cut -d' ' -f1)"
player_b="$(echo "$players" | cut -d' ' -f2)"

token() {
  curl -fsS -X POST "$AUTH/v1/auth/login" -H 'Content-Type: application/json' \
    -d "{\"playerId\":\"$1\",\"displayName\":\"$1\"}" \
    | "$MM" json - token 2>/dev/null
}
enqueue() {
  curl -fsS -X POST "$API/v1/matchmaking/tickets" -H "Authorization: Bearer $1" \
    -H 'Content-Type: application/json' -d '{"gameMode":"1v1","region":"NA","maxLatencyMs":80}' \
    | "$MM" json - ticketId 2>/dev/null
}

token_a="$(token "$player_a")"
token_b="$(token "$player_b")"
if [ -z "$token_a" ] || [ -z "$token_b" ]; then
  fail "could not log players in: is auth-service running?"
  printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
  exit 1
fi

# A known start: the queue and the pending matches of the local stack are cleared, exactly as
# the E2E scenarios do. The window is then measured on this run's traffic alone.
redis flushall >/dev/null
"$MM" observability slo read "$ENGINE_METRICS" > "$TMP/before.json"
note "redis flushed; before snapshot taken"

enqueue "$token_a" >/dev/null
enqueue "$token_b" >/dev/null
match_id=""
waited=0
while [ -z "$match_id" ] && [ "$waited" -lt "$WAIT_TIMEOUT_S" ]; do
  match_id="$("$MM" observability pending-match "$player_a" "$player_b" \
      --redis-port "$REDIS_PORT" 2>/dev/null)"
  [ -z "$match_id" ] && sleep 1 && waited=$((waited + 1))
done
if [ -n "$match_id" ]; then
  ok "the engine paired the two players into a real match ($match_id)"
else
  fail "no pending match appeared within ${WAIT_TIMEOUT_S}s"
  printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
  exit 1
fi

for player in "$player_a" "$player_b"; do
  redis publish match_confirmations \
    "{\"matchId\":\"$match_id\",\"playerId\":\"$player\",\"action\":\"CONFIRM\"}" >/dev/null
done
waited=0
while [ "$(redis exists "match-pending:$match_id")" != "0" ] && [ "$waited" -lt "$WAIT_TIMEOUT_S" ]; do
  sleep 1
  waited=$((waited + 1))
done
if [ "$(redis exists "match-pending:$match_id")" = "0" ]; then
  ok "both confirmations landed and the match was confirmed"
else
  fail "the pending match is still in Redis after the confirmations"
fi

"$MM" observability slo read "$ENGINE_METRICS" > "$TMP/after.json"
traffic="$("$MM" observability slo window "$TMP/before.json" "$TMP/after.json")"
note "this window: $(field "$traffic" formed) formed, $(field "$traffic" confirmed) confirmed, $(field "$traffic" matched_tickets) matched tickets"
latency_hand="$(field "$traffic" latency_within_10_ratio)"
confirm_hand="$(field "$traffic" confirm_ratio)"

if [ "$latency_hand" = "1.0" ]; then
  ok "SL-1 hand-computed: every matched ticket of this window waited at most 10 s ($latency_hand)"
else
  fail "SL-1 hand-computed ratio was $latency_hand, expected 1.0"
fi
if [ "$confirm_hand" = "1.0" ]; then
  ok "SL-2 hand-computed: every formed match of this window was confirmed ($confirm_hand)"
else
  fail "SL-2 hand-computed ratio was $confirm_hand, expected 1.0"
fi
if "$MM" eq "$(field "$traffic" dissolved_timeout)" 0; then
  ok "no match of this window dissolved on its confirmation deadline"
else
  fail "$(field "$traffic" dissolved_timeout) matches dissolved on the deadline"
fi

# The recorded rules must answer with the same number this scenario just counted by hand. The
# rules evaluate every 15 s, so the value is read after the window they describe has passed.
note "waiting up to ${RULE_WAIT_S}s for the recording rules to evaluate the window..."
rule_latency="NO SERIES"
rule_confirm="NO SERIES"
waited=0
while [ "$waited" -lt "$RULE_WAIT_S" ]; do
  rule_latency="$(promql 'matchmaking:slo_latency_within_10s:ratio_5m')"
  rule_confirm="$(promql 'matchmaking:slo_confirm:ratio_5m')"
  if is_ratio "$rule_latency" && is_ratio "$rule_confirm"; then
    break
  fi
  sleep 2
  waited=$((waited + 2))
done
note "recorded rules: latency=$rule_latency confirm=$rule_confirm"

if is_ratio "$rule_latency" && is_ratio "$rule_confirm"; then
  ok "both recorded ratios are numbers in [0,1], not NaN"
else
  fail "a recorded ratio is not a number: latency=$rule_latency confirm=$rule_confirm"
fi
if "$MM" ge "${rule_latency:-0}" 0.99; then
  ok "SL-1 holds: $rule_latency of matched tickets are inside 10 s (target 0.99)"
else
  fail "SL-1 is at $rule_latency, below the 0.99 target"
fi
if "$MM" approx "$latency_hand" "$rule_latency" 0.05; then
  ok "the recorded rule agrees with the hand-computed ratio (|$latency_hand - $rule_latency| <= 0.05)"
else
  fail "the recorded rule ($rule_latency) disagrees with the hand count ($latency_hand)"
fi
if "$MM" ge "${rule_confirm:-0}" 0.99; then
  ok "SL-2 holds: $rule_confirm of formed matches are confirmed (target 0.99)"
else
  # This Prometheus holds everything the repository runs, and a chaos/load run spends this budget
  # on purpose. The check stays falsifiable: a shortfall is acceptable only while the recorded
  # burn agrees with it, so a rule that stopped measuring fails right here.
  note "SL-2 is at $rule_confirm over the 5m window: this Prometheus contains a run that leaves matches unconfirmed on purpose"
  if [ "$(promql_series '1 - matchmaking:slo_confirm:ratio_5m >= (14.4 * 0.01)')" != "0" ]; then
    ok "SL-2 is below target and the recorded burn agrees: the spend is real, not a broken rule"
  else
    fail "SL-2 is at $rule_confirm but the recorded burn is empty (the ratio is not measuring the window)"
  fi
fi

budget="$("$MM" sub 1 "${rule_latency:-0}" 6)"
note "SL-1 budget spent in the 5m window: $budget of 0.01"
if "$MM" le "${budget:-1}" 0.01; then
  ok "the error budget is inside 1% - no spend beyond the objective"
elif [ "$(promql_series '1 - matchmaking:slo_latency_within_10s:ratio_5m >= (14.4 * 0.01)')" != "0" ]; then
  note "the budget is at $budget: this Prometheus contains a load run whose wait p99 really crossed 10 s"
  ok "the spend beyond the budget is real and the recorded burn agrees (see requests/load/)"
else
  fail "the error budget is at $budget, past the 1% budget, and the recorded burn is empty"
fi

# ---------------------------------------------------------------------------
step "3. the recorded queue rules follow the queue itself"
# ---------------------------------------------------------------------------
# SL-4 is measured on the queue, not on the matches, so it is checked against a player who is
# actually waiting: the recorded series must be the scrape's own sum, not a constant.
solo_token="$(token "solo-$player_a")"
ticket_solo="$(enqueue "$solo_token")"
if [ -n "$ticket_solo" ]; then
  ok "a third player is waiting alone in 1v1/NA (queue cannot move under the observation)"
else
  fail "could not enqueue the lonely player"
fi

note "waiting up to ${RULE_WAIT_S}s for the queue rules to see the waiting player..."
waiting="NO SERIES"
oldest="NO SERIES"
waited=0
while [ "$waited" -lt "$RULE_WAIT_S" ]; do
  waiting="$(promql 'matchmaking:queue:waiting_players')"
  oldest="$(promql 'matchmaking:queue:oldest_wait_seconds')"
  # Both must be there: the head's wait is whole seconds, so the first poll after an enqueue can
  # honestly report 0 while the player has been waiting less than a second.
  if [ "$waiting" = "1" ] && [ -n "$oldest" ] && [ "$oldest" != "NO SERIES" ] \
     && "$MM" gt "$oldest" 0; then
    break
  fi
  sleep 2
  waited=$((waited + 2))
done
depth_now="$("$MM" observability slo read "$ENGINE_METRICS" 2>/dev/null \
  | "$MM" json-int - queue_depth)"

if [ "$waiting" != "NO SERIES" ] && "$MM" eq "$waiting" "$depth_now"; then
  ok "matchmaking:queue:waiting_players ($waiting) equals the scrape's own sum of matchmaking_queue_depth ($depth_now)"
else
  fail "the recorded waiting_players is $waiting, the scrape sums to $depth_now"
fi
if [ -n "$oldest" ] && [ "$oldest" != "NO SERIES" ] && "$MM" gt "$oldest" 0; then
  ok "matchmaking:queue:oldest_wait_seconds follows the head of the queue (${oldest}s)"
else
  fail "the recorded oldest wait is $oldest while a player is waiting"
fi

# The recorded series must be derived, not cached: cancelling empties the queue and both go back.
# The cancel happens at the end of the alert section, because the SL-4 expression needs a player
# who is still waiting in order to have anything to answer with.

# ---------------------------------------------------------------------------
step "4. the alerts are quiet, and alive"
# ---------------------------------------------------------------------------
curl -fsS "$PROMETHEUS/api/v1/rules?type=alert" > "$TMP/alerts.json" 2>/dev/null
alert_states() { # -> name=state,... for every loaded alert
  "$MM" observability alert-states "$TMP/alerts.json" 2>/dev/null
}
# The query of one loaded alert, as Prometheus itself holds it.
alert_query() { # <name> -> the expression the loaded alert holds
  "$MM" observability alert-query "$TMP/alerts.json" "$1" 2>/dev/null
}
state_of() { alert_states | tr ',' '\n' | sed -n "s/^$1=//p"; }

alerts="$(alert_states)"
note "alert states: $alerts"

# The alerts that describe *now* and do not depend on the window's budget must be quiet. This is
# the assertion the scenario controls: it just drove a healthy match, and these four describe the
# last minutes (a stuck queue head, a stale outbox, a parked row, an unscraped service).
firing_now=""
for name in MatchmakingHeadWaitTooLong MatchmakingOutboxBacklogOld MatchmakingOutboxParkedRows \
            MatchmakingServiceDown; do
  [ "$(state_of "$name")" = "firing" ] && firing_now="$firing_now $name"
done
if [ -z "$firing_now" ]; then
  ok "no alert that describes the last minutes is firing"
else
  fail "firing now:$firing_now (states: $alerts)"
fi

# The four burn alerts read windows this Prometheus fills with everything the repository runs:
# the W16 saturation measurement (requests/load/results-ceiling-after.json: 4000 players arriving
# faster than the node pairs) spends the latency budget, and the W18 chaos scenario spends the
# confirmation budget on purpose (a match formed while Redis or Postgres is down is never
# confirmed). So the scenario does not pretend they must be quiet - it verifies a firing one
# instead of assuming, so a phantom cannot pass as history.
for name in MatchmakingLatencyFastBurn MatchmakingConfirmFastBurn \
            MatchmakingLatencySlowBurn MatchmakingConfirmSlowBurn; do
  state="$(state_of "$name")"
  if [ "$state" != "firing" ]; then
    ok "$name is not firing (state=$state)"
    continue
  fi
  condition="$(alert_query "$name")"
  if [ "$(promql_series "$condition")" != "0" ]; then
    ok "$name fires with its condition satisfied: recorded budget spend from the local saturation runs"
  else
    fail "$name is firing while its condition holds nothing (a phantom alert)"
  fi
done

# Alive: an expression that never answers anything never fires either. Evaluating the recorded
# burn at a threshold of 0 must return a series (it would fire), and at the real threshold it
# must stay empty. The same trick on SL-4, the rule that would have caught W15's ghost member.
if [ "$(promql_series '(1 - matchmaking:slo_latency_within_10s:ratio_5m) >= 0')" != "0" ]; then
  ok "the SL-1 burn expression answers at a threshold of 0 (it is data, not an empty result)"
else
  fail "the SL-1 burn expression returns nothing even at a threshold of 0"
fi
if [ "$(promql_series '(1 - matchmaking:slo_latency_within_10s:ratio_5m) > (14.4 * 0.01)')" = "0" ]; then
  ok "the SL-1 burn expression is empty at its real threshold: nothing is burning"
else
  fail "the SL-1 burn expression would fire at its real threshold"
fi

# SL-4 with a player waiting: truthy at -1, and empty just *above* the head's real wait. The
# second threshold is derived from the registry (`head + 10`) rather than being the literal 30 s,
# because a player the scenario deliberately leaves waiting crosses 30 s while the other checks
# run - asserting on 30 s here would have measured the scenario's own timing, not the rule. That
# the real 30 s line is not crossed is the `MatchmakingHeadWaitTooLong` state asserted above.
if [ "$(promql_series '(matchmaking_queue_oldest_wait_seconds > -1) and on (mode, region) (matchmaking_queue_depth > 0)')" != "0" ]; then
  ok "the SL-4 head-wait expression answers with a waiting player (it is data, not an empty result)"
else
  fail "the SL-4 head-wait expression returns nothing with a player waiting"
fi
head_wait="$(promql 'max(matchmaking_queue_oldest_wait_seconds)')"
above_head="$("$MM" sum "${head_wait:-0}" 10)"
if [ -n "$above_head" ] \
   && [ "$(promql_series "(matchmaking_queue_oldest_wait_seconds > $above_head) and on (mode, region) (matchmaking_queue_depth > 0)")" = "0" ]; then
  ok "the SL-4 expression selects on the threshold (truthy at -1, empty at ${above_head}s, above the head's ${head_wait}s)"
else
  fail "the SL-4 expression does not discriminate at ${above_head}s (head wait ${head_wait}s)"
fi

parked="$("$MM" observability slo read "$ENGINE_METRICS" 2>/dev/null \
  | "$MM" json-int - outbox_parked)"
oldest_pending="$(promql 'matchmaking_outbox_oldest_pending_seconds')"
note "SL-3: parked rows=$parked, oldest pending event=${oldest_pending}s (line: 0 parked, leader under 5 s)"
if [ "$parked" = "0" ]; then
  ok "SL-3 holds: no outbox row has been given up on (PARKED == 0)"
else
  fail "SL-3: $parked parked rows need a human (SELECT last_error FROM outbox_events WHERE status='PARKED')"
fi
if [ -z "$oldest_pending" ] || [ "$oldest_pending" = "NO SERIES" ] || "$MM" le "$oldest_pending" 5; then
  ok "SL-3 holds: the oldest pending event is within the 5 s line"
else
  fail "SL-3: the oldest pending event is ${oldest_pending}s old, past the 5 s line"
fi

# The recording must be derived, not cached: with the queue empty, the head's wait rule goes back
# to zero instead of holding the last thing it saw.
if [ -n "$ticket_solo" ]; then
  curl -fsS -X DELETE "$API/v1/matchmaking/tickets/$ticket_solo" \
    -H "Authorization: Bearer $solo_token" >/dev/null 2>&1
  waited=0
  waiting_after="NO SERIES"
  while [ "$waited" -lt "$RULE_WAIT_S" ]; do
    waiting_after="$(promql 'matchmaking:queue:waiting_players')"
    [ "$waiting_after" = "0" ] && break
    sleep 2
    waited=$((waited + 2))
  done
  if [ "$waiting_after" = "0" ]; then
    ok "cancelling the ticket brought the recorded rule back to 0: no stale objective survives the queue"
  else
    fail "the recorded waiting_players is $waiting_after with an empty queue"
  fi
fi

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
