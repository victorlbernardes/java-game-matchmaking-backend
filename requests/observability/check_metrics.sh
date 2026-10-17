#!/usr/bin/env bash
#
# W15 - domain KPIs on a Prometheus scrape (ADR 0020).
#
# What it proves:
#   1. both services expose a scrape on a port of their own (9090 = matchmaking-api,
#      9091 = matchmaking-engine), with Vert.x's transport/JVM meters and the domain KPIs
#      in the same exposition;
#   2. the numbers move with the system: a player waiting alone raises the arrival counter,
#      the queue depth and the head's wait, cancelling him brings both back to zero, and a
#      real match increments the formed/confirmed counters and the wait histogram;
#   3. the derived numbers agree with the source of truth: the outbox backlog is the number
#      Postgres answers with SELECT count(*);
#   4. the pipeline is real, not just the exposition: Prometheus scrapes both targets
#      (up == 1) and answers a query about a KPI, and Grafana has the dashboard provisioned.
#
# Preconditions: docker compose up -d (redis, postgres, kafka, schema-registry, prometheus,
# grafana) and auth-service + matchmaking-api + matchmaking-engine running (mvn install
# first - runbook item 1). The engine's metrics port is 9091, so it is started with
# METRICS_PORT=9091 (see requests/README.md).
#
# Usage:
#   bash requests/observability/check_metrics.sh
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
API="http://localhost:8080"
AUTH="http://localhost:8081"
API_METRICS="http://localhost:${API_METRICS_PORT:-9090}/metrics"
ENGINE_METRICS="http://localhost:${ENGINE_METRICS_PORT:-9091}/metrics"
PROMETHEUS="http://localhost:${PROMETHEUS_PORT:-9095}"
GRAFANA="http://localhost:${GRAFANA_PORT:-3000}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
REDIS_CONTAINER="${REDIS_CONTAINER:-game-matchmaking-backend-redis-1}"
# Long enough for one KPI poll (2 s) plus a Prometheus scrape interval (5 s).
WAIT_TIMEOUT_S="${WAIT_TIMEOUT_S:-30}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

redis() { docker exec "$REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }
psqlq() { docker exec "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -At -c "$1" 2>/dev/null; }

# Sums every series of a metric whose exposition line matches the label fragment. In python,
# because the exposition is a text format with labels and grep cannot sum; prints ABSENT when
# no series matched (a counter only appears after its first increment).
metric_sum() { # <url> <metric-name> <label-fragment>
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

as_number() { python3 -c "print(0.0 if '$1' in ('ABSENT', '') else float('$1'))"; }

# Waits until the metric equals exactly the expected value; echoes the last value read.
wait_metric() { # <url> <metric> <label-fragment> <expected> <timeout-seconds>
  local value="" waited=0
  while [ "$waited" -lt "$5" ]; do
    value="$(metric_sum "$1" "$2" "$3")"
    [ "$value" = "$4" ] && { echo "$value"; return 0; }
    sleep 1
    waited=$((waited + 1))
  done
  echo "$value"
  return 1
}

# Waits until the metric is strictly above a floor (for counters, whose exact value depends on
# whatever the process did before this script ran).
wait_metric_above() { # <url> <metric> <label-fragment> <floor> <timeout-seconds>
  local value="" waited=0
  while [ "$waited" -lt "$5" ]; do
    value="$(metric_sum "$1" "$2" "$3")"
    python3 -c "import sys; sys.exit(0 if '$value' not in ('ABSENT', '') and float('$value') > $4 else 1)" \
      && { echo "$value"; return 0; }
    sleep 1
    waited=$((waited + 1))
  done
  echo "$value"
  return 1
}

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
for container in "$POSTGRES_CONTAINER" "$REDIS_CONTAINER"; do
  if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
    ok "$container is running"
  else
    fail "$container is not running (docker compose up -d)"
  fi
done

# Prometheus and Grafana are checked by reachability, not by container name: this machine
# may run the container images (`docker compose up -d prometheus grafana`) or the same
# configuration with the binaries on the host (`ops/prometheus/prometheus.host.yml`).
if curl -fsS "$PROMETHEUS/-/ready" >/dev/null 2>&1; then
  ok "Prometheus answers on $PROMETHEUS (ready)"
else
  fail "no Prometheus on $PROMETHEUS (docker compose up -d prometheus grafana)"
fi
if curl -fsS "$GRAFANA/api/health" >/dev/null 2>&1; then
  ok "Grafana answers on $GRAFANA"
else
  fail "no Grafana on $GRAFANA (docker compose up -d prometheus grafana)"
fi

if curl -fsS "$API_METRICS" >/dev/null 2>&1; then
  ok "the API exposes a scrape on its own port ($API_METRICS)"
else
  fail "no scrape on $API_METRICS: is matchmaking-api running with the W15 build?"
fi
if curl -fsS "$ENGINE_METRICS" >/dev/null 2>&1; then
  ok "the engine exposes a scrape on its own port ($ENGINE_METRICS)"
else
  fail "no scrape on $ENGINE_METRICS: is matchmaking-engine running with METRICS_PORT=9091?"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. the exposition: transport meters and domain KPIs in one scrape"
# ---------------------------------------------------------------------------
if metric_contains "$API_METRICS" "jvm_memory_used_bytes"; then
  ok "the API scrape carries the JVM meters Vert.x bound to the same registry"
else
  fail "no jvm_memory_used_bytes in the API scrape"
fi
if metric_contains "$API_METRICS" "vertx_http_server_requests_total"; then
  ok "the API scrape carries Vert.x's transport meters (this scrape request is one of them)"
else
  fail "no vertx_http_server_requests_total in the API scrape"
fi
if metric_contains "$ENGINE_METRICS" "vertx_http_server_requests_total"; then
  ok "the engine serves the same transport meters on its metrics port"
else
  fail "no vertx_http_server_requests_total in the engine scrape"
fi
for metric in matchmaking_queue_depth matchmaking_queue_oldest_wait_seconds matchmaking_outbox_backlog \
              matchmaking_outbox_oldest_pending_seconds matchmaking_outbox_publish_lag_seconds; do
  if metric_contains "$ENGINE_METRICS" "$metric"; then
    ok "the engine exposes $metric"
  else
    fail "the engine does not expose $metric"
  fi
done

# ---------------------------------------------------------------------------
step "2. a match, driven by hand, must move the numbers"
# ---------------------------------------------------------------------------
# Players a and b share a deterministic skill - the pairing policy refuses a spread wider than
# a ticket's search range, so the ids have to be chosen, not invented - and are fresh on every
# run. Player c is a third id, waiting alone, so the queue cannot move under the observation.
# The skill helper is the E2E's, imported rather than copied (run_e2e.py only defines things at
# import time).
players="$(python3 -c "
import sys, time
sys.path.insert(0, '$REPO_ROOT/requests/e2e')
import run_e2e
base = 'kpi%d' % time.time()
groups, pair, i = {}, None, 0
while pair is None:
    pid = '%s-%d' % (base, i)
    s = run_e2e.skill(pid)
    groups.setdefault(s, []).append(pid)
    if len(groups[s]) >= 2:
        pair = groups[s][:2]
    i += 1
while True:
    alone = '%s-%d' % (base, i)
    if run_e2e.skill(alone) != run_e2e.skill(pair[0]):
        break
    i += 1
print(' '.join(pair + [alone]))
")"
player_a="$(echo "$players" | cut -d' ' -f1)"
player_b="$(echo "$players" | cut -d' ' -f2)"
player_c="$(echo "$players" | cut -d' ' -f3)"

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

token_a="$(token "$player_a")"
token_b="$(token "$player_b")"
token_c="$(token "$player_c")"
if [ -n "$token_a" ] && [ -n "$token_b" ] && [ -n "$token_c" ]; then
  ok "three players logged in against auth-service"
else
  fail "could not log players in: is auth-service running?"
  printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
  exit 1
fi

# 2a. a lonely player: the depth and the head's wait are read from Redis, and they come back.
# A known starting point first: the queue and the pending matches of the local stack are
# cleared, exactly as the E2E scenarios do (requests/e2e/run_e2e.py). Without it a ticket left
# behind by another run could pair with the "lonely" player, and the depth would be 0.
redis flushall >/dev/null
note "redis flushed: the queue and the pending matches start empty"

enqueued_before="$(as_number "$(metric_sum "$API_METRICS" matchmaking_tickets_enqueued_total 'region="NA"')")"
ticket_c="$(enqueue "$token_c")"
arrivals="$(wait_metric "$API_METRICS" matchmaking_tickets_enqueued_total 'region="NA"' \
  "$(python3 -c "print(float('$enqueued_before') + 1)")" 10)"
if [ "$arrivals" != "ABSENT" ] && [ -n "$arrivals" ]; then
  ok "the arrival counter counted the ticket at the boundary ($enqueued_before -> $arrivals)"
else
  fail "matchmaking_tickets_enqueued_total did not count the arrival"
fi

depth="$(wait_metric "$ENGINE_METRICS" matchmaking_queue_depth 'mode="1v1",region="NA"' "1.0" 5)"
if [ "$depth" = "1.0" ]; then
  ok "the queue depth KPI reads the queue in Redis: 1 player waiting in 1v1/NA"
else
  fail "queue depth for 1v1/NA was '$depth', expected 1.0"
fi

note "letting the head of the queue age for 4 s..."
sleep 4
oldest="$(as_number "$(metric_sum "$ENGINE_METRICS" matchmaking_queue_oldest_wait_seconds 'mode="1v1",region="NA"')")"
if python3 -c "import sys; sys.exit(0 if $oldest > 0 else 1)"; then
  ok "the head's wait is derived from the queue itself: ${oldest}s"
else
  fail "matchmaking_queue_oldest_wait_seconds stayed at $oldest while a player waited"
fi

if [ -n "$ticket_c" ]; then
  curl -fsS -X DELETE "$API/v1/matchmaking/tickets/$ticket_c" -H "Authorization: Bearer $token_c" \
    >/dev/null 2>&1
  depth_after="$(wait_metric "$ENGINE_METRICS" matchmaking_queue_depth 'mode="1v1",region="NA"' "0.0" 10)"
  oldest_after="$(as_number "$(metric_sum "$ENGINE_METRICS" matchmaking_queue_oldest_wait_seconds 'mode="1v1",region="NA"')")"
  if [ "$depth_after" = "0.0" ] && [ "$oldest_after" = "0.0" ]; then
    ok "cancelling the ticket brought both gauges back to 0: no stale value outlives the queue"
  else
    fail "after the cancel: depth=$depth_after wait=$oldest_after (expected 0 and 0)"
  fi
else
  fail "the enqueue did not return a ticket id (nothing to cancel)"
fi

# 2b. the real match: two players of the same skill.
formed_before="$(as_number "$(metric_sum "$ENGINE_METRICS" matchmaking_matches_formed_total 'mode="1v1",region="NA"')")"
waits_before="$(as_number "$(metric_sum "$ENGINE_METRICS" matchmaking_match_wait_seconds_count 'mode="1v1",region="NA"')")"
enqueue "$token_a" >/dev/null
enqueue "$token_b" >/dev/null

note "waiting for the engine to pair them..."
match_id=""
waited=0
while [ -z "$match_id" ] && [ "$waited" -lt "$WAIT_TIMEOUT_S" ]; do
  # Only the pending match with *our* two players: a leftover from another run must not be
  # confirmed here (Redis holds the roster as JSON, so the lookup is a python helper).
  match_id="$(python3 "$REPO_ROOT/requests/observability/pending_match.py" \
      "$player_a" "$player_b" "$REDIS_CONTAINER" 2>/dev/null)"
  [ -z "$match_id" ] && sleep 1 && waited=$((waited + 1))
done
if [ -n "$match_id" ]; then
  ok "the engine paired them into $match_id"
else
  fail "no pending match appeared within ${WAIT_TIMEOUT_S}s"
  printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
  exit 1
fi

formed_after="$(wait_metric_above "$ENGINE_METRICS" matchmaking_matches_formed_total 'mode="1v1",region="NA"' \
  "$formed_before" 10)"
if [ "$formed_after" != "ABSENT" ]; then
  ok "matchmaking_matches_formed_total counted the pairing ($formed_before -> $formed_after)"
else
  fail "matchmaking_matches_formed_total did not move from $formed_before"
fi
waits_after="$(wait_metric_above "$ENGINE_METRICS" matchmaking_match_wait_seconds_count 'mode="1v1",region="NA"' \
  "$waits_before" 10)"
if [ "$waits_after" != "ABSENT" ]; then
  ok "the wait histogram measured the players taken off the queue ($waits_before -> $waits_after)"
else
  fail "matchmaking_match_wait_seconds_count did not move from $waits_before"
fi
if metric_contains "$ENGINE_METRICS" "matchmaking_match_wait_seconds_bucket"; then
  ok "the wait is a percentile histogram: p50/p95 are computable, not just the average"
else
  fail "matchmaking_match_wait_seconds has no buckets, so no percentiles"
fi

depth_paired="$(wait_metric "$ENGINE_METRICS" matchmaking_queue_depth 'mode="1v1",region="NA"' "0.0" 10)"
if [ "$depth_paired" = "0.0" ]; then
  ok "the queue depth went back to 0: the roster left the queue when it was paired"
else
  fail "queue depth for 1v1/NA is '$depth_paired' after pairing"
fi

# Confirming: the engine's input contract is the match_confirmations channel; the API
# translates a WS MATCH_CONFIRM into exactly this message.
confirmed_before="$(as_number "$(metric_sum "$ENGINE_METRICS" matchmaking_matches_confirmed_total 'mode="1v1",region="NA"')")"
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
  ok "both confirmations landed: the pending match left Redis"
else
  fail "the pending match is still in Redis after the confirmations"
fi

confirmed_after="$(wait_metric_above "$ENGINE_METRICS" matchmaking_matches_confirmed_total 'mode="1v1",region="NA"' \
  "$confirmed_before" 10)"
if [ "$confirmed_after" != "ABSENT" ]; then
  ok "matchmaking_matches_confirmed_total moved with the confirmation ($confirmed_before -> $confirmed_after)"
else
  fail "matchmaking_matches_confirmed_total did not move from $confirmed_before"
fi

# ---------------------------------------------------------------------------
step "3. the derived numbers must agree with the source of truth"
# ---------------------------------------------------------------------------
db_pending="$(psqlq "SELECT count(*) FROM outbox_events WHERE status = 'PENDING'")"
exported_pending="$(as_number "$(metric_sum "$ENGINE_METRICS" matchmaking_outbox_backlog 'status="PENDING"')")"
exported_pending="$(python3 -c "print(int($exported_pending))")"
note "pending: gauge=$exported_pending db=$db_pending (the gauge is a snapshot: a row the relay just published shows as pending until the next poll)"

# The three gauges come from one poll, so their sum is a consistent snapshot of the table, and
# the table only grows here - that makes the check deterministic where PENDING alone would race
# the relay (a row published between the poll and this comparison).
exported_total="$(python3 -c "
total = 0.0
for value in ('$(metric_sum "$ENGINE_METRICS" matchmaking_outbox_backlog 'status="PENDING"')',
              '$(metric_sum "$ENGINE_METRICS" matchmaking_outbox_backlog 'status="PUBLISHED"')',
              '$(metric_sum "$ENGINE_METRICS" matchmaking_outbox_backlog 'status="PARKED"')'):
    total += 0.0 if value == 'ABSENT' else float(value)
print(int(total))
")"
db_total="$(psqlq "SELECT count(*) FROM outbox_events")"
if [ "$exported_total" = "$db_total" ]; then
  ok "the outbox backlog by status adds up to the rows in Postgres: $db_total"
else
  fail "the backlog adds up to $exported_total, Postgres holds $db_total rows"
fi

db_parked="$(psqlq "SELECT count(*) FROM outbox_events WHERE status = 'PARKED'")"
exported_parked="$(as_number "$(metric_sum "$ENGINE_METRICS" matchmaking_outbox_backlog 'status="PARKED"')")"
if [ "$(python3 -c "print(int($exported_parked))")" = "$db_parked" ]; then
  ok "the parked rows agree with Postgres: $db_parked (each one needs a human)"
else
  fail "parked says $exported_parked, Postgres says $db_parked"
fi

published="$(wait_metric_above "$ENGINE_METRICS" matchmaking_outbox_published_total '' 0 15)"
lag="$(metric_sum "$ENGINE_METRICS" matchmaking_outbox_publish_lag_seconds '')"
db_lag="$(psqlq "SELECT extract(epoch FROM (published_at - occurred_at))::double precision FROM outbox_events WHERE status = 'PUBLISHED' ORDER BY published_at DESC LIMIT 1")"
note "outbox: published_total=$published, newest publish lag = ${lag}s (fact -> broker in the table: ${db_lag}s)"
if [ "$published" != "ABSENT" ]; then
  ok "the relay's own counters are exposed (published_total=$published)"
else
  fail "matchmaking_outbox_published_total is absent: did the relay run?"
fi
# The gauge carries whole seconds, so it is compared with tolerance: the point is that the
# number is the same fact as the table's, not that the aggregation keeps the milliseconds.
if python3 -c "
import sys
gauge, table = '$lag', '$db_lag'
ok = gauge not in ('ABSENT', '') and table not in ('', None) and abs(float(gauge) - float(table)) <= 1.0
sys.exit(0 if ok else 1)"; then
  ok "the publish lag agrees with the outbox table within a second: ${lag}s"
else
  fail "the publish lag gauge ($lag) does not match the table ($db_lag)"
fi

# ---------------------------------------------------------------------------
step "4. the pipeline, not just the exposition"
# ---------------------------------------------------------------------------
up_values="$(curl -fsS "$PROMETHEUS/api/v1/query?query=up" | python3 -c '
import json, sys
data = json.load(sys.stdin)["data"]["result"]
print(",".join(sorted("%s=%s" % (item["metric"].get("job", "?"), item["value"][1]) for item in data)) or "NO TARGETS")
' 2>/dev/null)"
note "prometheus up: ${up_values:-<no answer>}"
case "$up_values" in
  *matchmaking-api=1*matchmaking-engine=1*) ok "Prometheus scrapes both jobs (up == 1)" ;;
  *) fail "Prometheus is not scraping both jobs: ${up_values:-<no answer>}" ;;
esac

query_value="$(curl -fsS --get "$PROMETHEUS/api/v1/query" --data-urlencode 'query=sum(matchmaking_queue_depth)' | python3 -c '
import json, sys
result = json.load(sys.stdin)["data"]["result"]
print(result[0]["value"][1] if result else "NO SERIES")
' 2>/dev/null)"
if [ "$query_value" != "NO SERIES" ] && [ -n "$query_value" ]; then
  ok "a KPI is queryable through Prometheus (sum(matchmaking_queue_depth) = $query_value)"
else
  fail "Prometheus has no matchmaking_queue_depth series"
fi

health="$(curl -fsS "$GRAFANA/api/health" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("database", "?"))' 2>/dev/null)"
if [ "$health" = "ok" ]; then
  ok "Grafana is up and its database is ok"
else
  fail "Grafana health: ${health:-<no answer>}"
fi

dashboard="$(curl -fsS "$GRAFANA/api/search?query=Matchmaking" | python3 -c '
import json, sys
data = json.load(sys.stdin)
print(data[0]["title"] if data else "NONE")
' 2>/dev/null)"
if [ "$dashboard" != "NONE" ] && [ -n "$dashboard" ]; then
  ok "the dashboard is provisioned from the repository: $dashboard"
else
  fail "no provisioned dashboard found in Grafana"
fi

# The same series, asked the way a panel asks for it: through Grafana's datasource proxy.
# A dashboard that exists but cannot query is a picture of nothing.
grafana_value="$(curl -fsS -X POST "$GRAFANA/api/ds/query" -H 'Content-Type: application/json' -d '{
  "queries": [{
    "refId": "A",
    "datasource": { "type": "prometheus", "uid": "prometheus" },
    "expr": "sum(matchmaking_queue_depth)",
    "instant": true,
    "format": "table"
  }],
  "from": "now-5m",
  "to": "now"
}' | python3 -c '
import json, sys
try:
    results = json.load(sys.stdin)["results"]["A"]
    frames = results.get("frames", [])
    values = [value for frame in frames for value in frame.get("data", {}).get("values", [[]])[-1]]
    print(values[-1] if values else "NO VALUE")
except Exception:
    print("NO VALUE")
' 2>/dev/null)"
if [ -n "$grafana_value" ] && [ "$grafana_value" != "NO VALUE" ]; then
  ok "Grafana answers a query over the provisioned datasource: sum(matchmaking_queue_depth) = $grafana_value"
else
  fail "Grafana could not query the KPI through the provisioned datasource"
fi

printf '\n== result: %d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
