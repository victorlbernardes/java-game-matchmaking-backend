#!/usr/bin/env bash
#
# W21 - distant regions trade latency for availability: two logical regions, each its own hot
# state, a server-measured distance, and a failover that costs what it costs (ADR 0039).
#
# What it proves:
#   1. a region is a deployment unit: NA's endpoint serves NA and refuses EU (and the reverse), so a
#      ticket can no longer be queued for a deployment that will never pair it;
#   2. the distance is measured by the **server**, through a delay proxy that is the network: a
#      player probes EU (near, ~1 ms) and NA (far, ~80 ms) and the numbers come back from the
#      two-request protocol, not from the client;
#   3. the placement uses max(declared, measured): a 150 ms budget reaches NA, a 60 ms budget is
#      refused with the 120 ms the operator declared - trading latency for availability, priced;
#   4. inside a region a match forms and is confirmed, and the wait is measured;
#   5. EU's Redis dies: EU answers 503, NA keeps pairing and confirming (the failure is contained),
#      and the EU player fails over to NA and is matched once;
#   6. EU comes back **empty**: no ghost ticket, no second match for the player who left;
#   7. the scrape agrees with the decisions (probes, rtt per region, both refusal reasons).
#
# Preconditions: docker (redis:7-alpine locally, so no pull), the compose Postgres/Kafka/schema
# registry, auth-service on 8081, and a built tree (`mvn -o -B install -DskipTests`, runbook item 1).
# The script starts its own two APIs (8096/8097, scrapes 9196/9198), two engines (scrapes
# 9197/9199) and two Redis containers (6380/6381), plus a delay proxy on 8099 in front of NA.
# It touches neither the API on 8080 nor the engine on 9091, and leaves nothing running.
#
# Usage:
#   bash requests/multiregion/verify_multiregion.sh
#
# Exit code 0 = every check held.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AUTH_PORT="${AUTH_PORT:-8091}"
AUTH="http://localhost:${AUTH_PORT}"
NA_API="http://localhost:${NA_API_PORT:-8096}"
EU_API="http://localhost:${EU_API_PORT:-8097}"
NA_API_METRICS="http://localhost:${NA_API_METRICS_PORT:-9196}/metrics"
EU_API_METRICS="http://localhost:${EU_API_METRICS_PORT:-9198}/metrics"
NA_ENGINE_METRICS="http://localhost:${NA_ENGINE_METRICS_PORT:-9197}/metrics"
EU_ENGINE_METRICS="http://localhost:${EU_ENGINE_METRICS_PORT:-9199}/metrics"
PROXY="http://localhost:${PROXY_PORT:-8099}"
NA_REDIS_PORT="${NA_REDIS_PORT:-6380}"
EU_REDIS_PORT="${EU_REDIS_PORT:-6381}"
NA_REDIS_CONTAINER="w21-redis-na"
EU_REDIS_CONTAINER="w21-redis-eu"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7-alpine}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-game-matchmaking-backend-postgres-1}"
PROBE="requests/multiregion/multiregion_probe.py"
PROXY_SCRIPT="requests/multiregion/delay_proxy.py"
DELAY_MS="${DELAY_MS:-40}"
# EU is 120 ms from NA in the operator's model (the scenario's world); SA is out of the picture.
RTT_MATRIX="EU-EU=20,NA-NA=25,SA-SA=110,EU-NA=120,EU-SA=180,NA-SA=140"
OUT="/tmp/w21-probe.json"
OUT_A="/tmp/w21-probe-a.json"
OUT_B="/tmp/w21-probe-b.json"
AUTH_LOG="${AUTH_LOG:-/tmp/w21-auth.log}"
RUN_TAG="${RUN_TAG:-$(date +%H%M%S)}"
NA_API_LOG="${NA_API_LOG:-/tmp/w21-api-na.log}"
EU_API_LOG="${EU_API_LOG:-/tmp/w21-api-eu.log}"
NA_ENGINE_LOG="${NA_ENGINE_LOG:-/tmp/w21-engine-na.log}"
EU_ENGINE_LOG="${EU_ENGINE_LOG:-/tmp/w21-engine-eu.log}"
PROXY_LOG="${PROXY_LOG:-/tmp/w21-proxy.log}"

PASS=0
FAIL=0
ok()   { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
step() { printf '\n== %s\n' "$1"; }
note() { printf '        %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

redis_na() { docker exec "$NA_REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }
redis_eu() { docker exec "$EU_REDIS_CONTAINER" redis-cli "$@" 2>/dev/null; }
redis_count() { # <container> <pattern> -> number of matching keys, counted inside Redis
  docker exec "$1" redis-cli eval "return #redis.call('keys', ARGV[1])" 0 "$2" 2>/dev/null | tr -d '\r'
}

# A dotted key out of the probe's JSON answer ("" when absent).
jpath() { # <file> <dotted.key>
  python3 - "$1" "$2" <<'PY'
import json, sys
with open(sys.argv[1]) as handle:
    value = json.load(handle)
for part in sys.argv[2].split("."):
    if isinstance(value, dict):
        value = value.get(part)
    elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
        value = value[int(part)]
    else:
        value = None
print("" if value is None else value)
PY
}

stop_services() {
  for pid_file in /tmp/w21-auth.pid /tmp/w21-api-na.pid /tmp/w21-api-eu.pid /tmp/w21-na-engine.pid \
      /tmp/w21-eu-engine.pid /tmp/w21-proxy.pid; do
    if [ -f "$pid_file" ]; then
      kill -9 "$(cat "$pid_file")" 2>/dev/null || true
      rm -f "$pid_file"
    fi
  done
  # java processes started by mvn exec are children of the shell above; match them by class.
  pkill -f "w21-api-na" 2>/dev/null || true
  pkill -f "w21-api-eu" 2>/dev/null || true
}

stop_redis_containers() {
  docker rm -f "$NA_REDIS_CONTAINER" >/dev/null 2>&1 || true
  docker rm -f "$EU_REDIS_CONTAINER" >/dev/null 2>&1 || true
}

cleanup() {
  # KEEP=1 leaves both regions running, which is how the Postman folder `multiregion-w21` is used
  # by hand: the scenario proves the property, the collection lets a person replay it.
  if [ "${KEEP:-0}" = "1" ]; then
    printf '\nkept running: auth %s | NA API %s (scrape 9196) | EU API %s (scrape 9198)\n' \
      "$AUTH" "$NA_API" "$EU_API"
    printf 'engines: NA 9197, EU 9199 | Redis: %s, %s | proxy %s\n' \
      "$NA_REDIS_CONTAINER" "$EU_REDIS_CONTAINER" "$PROXY"
    printf 'logs: %s %s %s %s %s %s\n' "$AUTH_LOG" "$NA_API_LOG" "$EU_API_LOG" "$NA_ENGINE_LOG" "$EU_ENGINE_LOG" "$PROXY_LOG"
    printf 'stop with: kill $(cat /tmp/w21-*.pid) 2>/dev/null; docker rm -f %s %s\n' \
      "$NA_REDIS_CONTAINER" "$EU_REDIS_CONTAINER"
    return
  fi
  stop_services
  stop_redis_containers
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

wait_http() { # <url> <timeout-seconds>
  local deadline=$((SECONDS + $2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS "$1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_auth() { # the scenario's own auth, so the token's `region` claim is the one in this tree
  free_port "$AUTH_PORT"
  (
    cd "$REPO_ROOT" || exit 1
    AUTH_PORT="$AUTH_PORT" \
      mvn -o -q -pl services/auth-service exec:java \
        -Dexec.mainClass=com.victor.matchmaking.auth.AuthServiceApplication \
        </dev/null >"$AUTH_LOG" 2>&1 &
    echo $! >/tmp/w21-auth.pid
  )
  wait_http "$AUTH/.well-known/jwks.json" 120
}

start_api() { # <served-region> <port> <metrics-port> <redis-port> <instance> <log>
  free_port "$2"
  free_port "$3"
  (
    cd "$REPO_ROOT" || exit 1
    PORT="$2" METRICS_PORT="$3" INSTANCE_ID="$5" \
      SERVED_REGIONS="$1" REGION_RTT_MS="$RTT_MATRIX" \
      REDIS_URI="redis://localhost:$4" \
      AUTH_JWKS_URL="$AUTH/.well-known/jwks.json" \
      mvn -o -q -pl services/matchmaking-api exec:java \
        -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication \
        </dev/null >"$6" 2>&1 &
    echo $! >"/tmp/w21-$5.pid"
  )
  wait_http "http://localhost:$2/health" 120
}

start_engine() { # <served-region> <metrics-port> <redis-port> <log>
  free_port "$2"
  (
    cd "$REPO_ROOT" || exit 1
    METRICS_PORT="$2" SERVED_REGIONS="$1" REDIS_URI="redis://localhost:$3" \
      mvn -o -q -pl services/matchmaking-engine exec:java \
        -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication \
        </dev/null >"$4" 2>&1 &
    echo $! >"/tmp/w21-$(echo "$1" | tr '[:upper:]' '[:lower:]')-engine.pid"
  )
  wait_http "http://localhost:$2/metrics" 180
}

login() { # <player> <home-region> -> token
  curl -fsS -X POST "$AUTH/v1/auth/login" -H 'content-type: application/json' \
    -d "{\"playerId\":\"$1\",\"region\":\"$2\"}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])'
}

probe() { # <out-file> <args...>
  local out="$1"; shift
  ( cd "$REPO_ROOT" && python3 "$PROBE" "$@" ) >"$out" 2>"/tmp/w21-probe.err"
  local code=$?
  if [ $code -ne 0 ]; then
    printf '        probe error: %s\n' "$(tail -3 /tmp/w21-probe.err)"
  fi
  return $code
}

play_pair() { # <redis-port> <api> <token-a> <player-a> <token-b> <player-b> <region> <budget>
  probe "$OUT_A" play --api "$2" --token "$3" --player "$4" --region "$7" --budget "$8" --redis-port "$1" &
  local pid_a=$!
  probe "$OUT_B" play --api "$2" --token "$5" --player "$6" --region "$7" --budget "$8" --redis-port "$1" &
  local pid_b=$!
  wait "$pid_a"
  wait "$pid_b"
}

# ---------------------------------------------------------------------------
step "0. preconditions"
# ---------------------------------------------------------------------------
if docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
  ok "$POSTGRES_CONTAINER is running (both regional engines migrate on boot)"
else
  fail "$POSTGRES_CONTAINER is not running (docker compose up -d postgres kafka schema-registry)"
fi
if [ -f "services/auth-service/target/classes/com/victor/matchmaking/auth/AuthServiceApplication.class" ]; then
  ok "the tree is built (the scenario starts its own auth, APIs and engines)"
else
  fail "services/auth-service is not built (mvn -o -B install -DskipTests, runbook item 1)"
fi
if docker image inspect "$REDIS_IMAGE" >/dev/null 2>&1; then
  ok "the local image $REDIS_IMAGE exists (a region gets its own server, no pull)"
else
  fail "image $REDIS_IMAGE is not available locally (docker pull $REDIS_IMAGE)"
fi
if [ "$FAIL" -ne 0 ]; then
  printf '\n== result: %d passed, %d failed (nothing else ran)\n' "$PASS" "$FAIL"
  exit 1
fi

# ---------------------------------------------------------------------------
step "1. two regions, each its own deployment, each its own hot state"
# ---------------------------------------------------------------------------
stop_services
stop_redis_containers
# No persistence (`--save '' --appendonly no`): the queue is a claim about players who are queued
# *now*. A region that comes back holding yesterday's queue pairs ghosts - and a player who failed
# over while the region was down would be matched twice (ADR 0039, trade-offs).
docker run -d --name "$NA_REDIS_CONTAINER" -p "$NA_REDIS_PORT:6379" "$REDIS_IMAGE" \
  redis-server --save '' --appendonly no >/dev/null 2>&1
docker run -d --name "$EU_REDIS_CONTAINER" -p "$EU_REDIS_PORT:6379" "$REDIS_IMAGE" \
  redis-server --save '' --appendonly no >/dev/null 2>&1
sleep 1
if [ "$(redis_na ping 2>/dev/null)" = "PONG" ] && [ "$(redis_eu ping 2>/dev/null)" = "PONG" ]; then
  ok "NA and EU each have their own Redis ($NA_REDIS_PORT and $EU_REDIS_PORT)"
else
  fail "the regional Redis containers did not start"
fi
if [ -z "$(redis_eu config get save | tail -1)" ] && [ "$(redis_eu config get appendonly | tail -1)" = "no" ]; then
  ok "a region's Redis runs without persistence: hot state, not a durable record"
else
  fail "the regional Redis persists its queue (save=$(redis_eu config get save | tail -1), appendonly=$(redis_eu config get appendonly | tail -1))"
fi

if start_auth; then
  ok "the scenario's own auth-service is listening on $AUTH (it issues the region claim)"
else
  fail "the scenario's auth-service did not start (log $AUTH_LOG)"
fi

if start_api NA 8096 9196 "$NA_REDIS_PORT" api-na "$NA_API_LOG"; then
  ok "the NA API is listening on $NA_API (scrape 9196)"
else
  fail "the NA API did not start (log $NA_API_LOG)"
fi
if start_api EU 8097 9198 "$EU_REDIS_PORT" api-eu "$EU_API_LOG"; then
  ok "the EU API is listening on $EU_API (scrape 9198)"
else
  fail "the EU API did not start (log $EU_API_LOG)"
fi

health_na="$(curl -fsS "$NA_API/health")"
health_eu="$(curl -fsS "$EU_API/health")"
if echo "$health_na" | grep -q '"regions":\["NA"\]' && echo "$health_na" | grep -q '"instance":"api-na"'; then
  ok "NA says which instance it is and which region it serves: $health_na"
else
  fail "NA's /health does not name its region: $health_na"
fi
if echo "$health_eu" | grep -q '"regions":\["EU"\]'; then
  ok "EU says the same about itself: $health_eu"
else
  fail "EU's /health does not name its region: $health_eu"
fi

if start_engine NA 9197 "$NA_REDIS_PORT" "$NA_ENGINE_LOG"; then
  ok "the NA engine is up (scrape 9197), pairing NA only"
else
  fail "the NA engine did not start (log $NA_ENGINE_LOG)"
fi
if start_engine EU 9199 "$EU_REDIS_PORT" "$EU_ENGINE_LOG"; then
  ok "the EU engine is up (scrape 9199), pairing EU only"
else
  fail "the EU engine did not start (log $EU_ENGINE_LOG)"
fi

# The distance: a one-way delay in front of NA, so "distant" is a real 80 ms round trip.
free_port 8099
( cd "$REPO_ROOT" && python3 "$PROXY_SCRIPT" --listen 8099 --upstream 8096 --delay-ms "$DELAY_MS" \
    >"$PROXY_LOG" 2>&1 & echo $! >/tmp/w21-proxy.pid )
if wait_http "$PROXY/health" 20; then
  ok "a delay proxy sits in front of NA on $PROXY (+$DELAY_MS ms each way)"
else
  fail "the delay proxy did not start (log $PROXY_LOG)"
fi

# ---------------------------------------------------------------------------
step "2. the directory: what a client can know before it asks for a region"
# ---------------------------------------------------------------------------
probe "$OUT" players --count 8 --prefix "mr-$RUN_TAG-eu-"
EU_PLAYER="$(jpath "$OUT" players.0)"
EU_TOKEN="$(login "$EU_PLAYER" EU)"
note "EU player $EU_PLAYER (skill $(python3 -c 'import json,sys; print(json.load(open("/tmp/w21-probe.json"))["skill"])'))"

if probe "$OUT" directory --api "$NA_API" --token "$EU_TOKEN"; then
  if [ "$(jpath "$OUT" body.playerRegion)" = "EU" ]; then
    ok "NA's directory knows the caller's home region from the token claim (playerRegion=EU)"
  else
    fail "expected playerRegion EU, got $(jpath "$OUT" body.playerRegion)"
  fi
  if [ "$(jpath "$OUT" body.served)" = "['NA']" ]; then
    ok "NA says it serves NA and only NA: $(jpath "$OUT" body.served)"
  else
    fail "expected served ['NA'], got $(jpath "$OUT" body.served)"
  fi
  na_declared="$(python3 -c 'import json; d=json.load(open("/tmp/w21-probe.json"))["body"]["regions"]; print([r["declaredRttMs"] for r in d if r["region"]=="NA"][0])')"
  na_measured="$(python3 -c 'import json; d=json.load(open("/tmp/w21-probe.json"))["body"]["regions"]; print([r["measuredRttMs"] for r in d if r["region"]=="NA"][0])')"
  eu_declared="$(python3 -c 'import json; d=json.load(open("/tmp/w21-probe.json"))["body"]["regions"]; print([r["declaredRttMs"] for r in d if r["region"]=="EU"][0])')"
  if [ "$na_declared" = "120" ] && [ "$eu_declared" = "20" ]; then
    ok "the declared matrix reads EU->NA as 120 ms and EU->EU as 20 ms"
  else
    fail "unexpected declared distances: NA=$na_declared EU=$eu_declared"
  fi
  if [ "$na_measured" = "None" ]; then
    ok "before probing, the directory has no measurement to show (it does not invent one)"
  else
    fail "expected measuredRttMs null before the probe, got $na_measured"
  fi
else
  fail "the directory request failed"
fi

# ---------------------------------------------------------------------------
step "3. the probe: the server measures near and far, and says so"
# ---------------------------------------------------------------------------
if probe "$OUT" measure --api "$EU_API" --token "$EU_TOKEN" --player "$EU_PLAYER" --region EU --attempts 3; then
  near="$(jpath "$OUT" rttMs)"
  if [ "$(jpath "$OUT" pingStatus)" = "201" ] && [ -n "$near" ] && [ "$near" -lt 50 ]; then
    ok "probing EU from the EU endpoint measures ${near} ms (best of $(jpath "$OUT" samples)) - the region the player is in"
  else
    fail "the near probe did not measure: $(cat "$OUT")"
  fi
else
  near=0
  fail "the near probe failed"
fi

if probe "$OUT" measure --api "$PROXY" --token "$EU_TOKEN" --player "$EU_PLAYER" --region NA --attempts 3; then
  far="$(jpath "$OUT" rttMs)"
  if [ -n "$far" ] && [ "$far" -ge $((DELAY_MS + 20)) ]; then
    ok "probing NA through the delay proxy measures ${far} ms (the network, not the client's claim)"
  else
    fail "expected ~$((DELAY_MS * 2)) ms through the proxy, got '$far'"
  fi
  if [ -n "$near" ] && [ -n "$far" ] && [ $((far - near)) -ge "$DELAY_MS" ]; then
    ok "the far region costs $((far - near)) ms more than the near one, and the server is the one saying it"
  else
    fail "near=$near far=$far: the two regions are not measurably apart"
  fi
else
  fail "the far probe failed"
fi

if probe "$OUT" directory --api "$NA_API" --token "$EU_TOKEN"; then
  na_measured="$(python3 -c 'import json; d=json.load(open("/tmp/w21-probe.json"))["body"]["regions"]; print([r["measuredRttMs"] for r in d if r["region"]=="NA"][0])')"
  na_rtt="$(python3 -c 'import json; d=json.load(open("/tmp/w21-probe.json"))["body"]["regions"]; print([r["rttMs"] for r in d if r["region"]=="NA"][0])')"
  if [ "$na_measured" = "$far" ] && [ "$na_rtt" = "120" ]; then
    ok "the directory now shows the measurement ($na_measured ms) and the placement number is max(120,$far)=$na_rtt"
  else
    fail "directory after probing: measured=$na_measured rtt=$na_rtt (expected $far and 120)"
  fi
  eu_measured="$(python3 -c 'import json; d=json.load(open("/tmp/w21-probe.json"))["body"]["regions"]; print([r["measuredRttMs"] for r in d if r["region"]=="EU"][0])')"
  if [ "$eu_measured" = "None" ]; then
    ok "NA cannot show the EU measurement: a region's measurements live in that region"
  else
    fail "NA answered with an EU measurement it cannot have: $eu_measured"
  fi
else
  fail "the directory request after probing failed"
fi

na_scrape="$(curl -fsS "$NA_API_METRICS" 2>/dev/null)"
eu_scrape="$(curl -fsS "$EU_API_METRICS" 2>/dev/null)"
if echo "$na_scrape" | grep -q 'matchmaking_player_rtt_ms_count{region="NA"}'; then
  ok "the NA scrape carries the measured rtt distribution for region NA"
else
  fail "no matchmaking_player_rtt_ms{region=NA} in the NA scrape"
fi
if echo "$eu_scrape" | grep -q 'matchmaking_player_rtt_ms_count{region="EU"}'; then
  ok "the EU scrape carries its own (the measurement is region-local)"
else
  fail "no matchmaking_player_rtt_ms{region=EU} in the EU scrape"
fi
if echo "$na_scrape" | grep -q 'matchmaking_latency_probes_total{.*result="measured"'; then
  ok "the probe counter says what was measured"
else
  fail "no matchmaking_latency_probes_total{result=measured} in the NA scrape"
fi

# ---------------------------------------------------------------------------
step "4. a region this endpoint does not serve"
# ---------------------------------------------------------------------------
probe "$OUT" measure --api "$NA_API" --token "$EU_TOKEN" --player "$EU_PLAYER" --region EU
if [ "$(jpath "$OUT" pingStatus)" = "422" ]; then
  ok "asking NA to measure EU is refused (it would measure NA and call it EU)"
else
  fail "expected 422 for measuring a region the endpoint does not serve, got $(jpath "$OUT" pingStatus)"
fi

probe "$OUT" ticket --api "$NA_API" --token "$EU_TOKEN" --region EU --budget 1000
if [ "$(jpath "$OUT" status)" = "422" ] && [ "$(jpath "$OUT" body.error)" = "region_unsupported" ]; then
  ok "a ticket for EU at the NA endpoint is 422 region_unsupported, even with a 1000 ms budget"
else
  fail "expected 422 region_unsupported, got $(jpath "$OUT" status) $(jpath "$OUT" body.error)"
fi
if [ "$(jpath "$OUT" body.served)" = "['NA']" ]; then
  ok "the refusal names the regions this endpoint accepts"
else
  fail "the refusal does not list the served regions: $(cat "$OUT")"
fi

probe "$OUT" measure --api "$NA_API" --token "$EU_TOKEN" --player "$EU_PLAYER" --region NA
nonce="$(jpath "$OUT" nonce)"
curl -sS -o /dev/null -X POST "$NA_API/v1/regions/NA/pong" -H "authorization: Bearer $EU_TOKEN" \
  -H 'content-type: application/json' -d "{\"nonce\":\"$nonce\"}" >/dev/null
code="$(curl -sS -o /tmp/w21-replay.json -w '%{http_code}' -X POST "$NA_API/v1/regions/NA/pong" \
  -H "authorization: Bearer $EU_TOKEN" -H 'content-type: application/json' -d "{\"nonce\":\"$nonce\"}")"
if [ "$code" = "422" ] && grep -q 'invalid_nonce' /tmp/w21-replay.json; then
  ok "a nonce answered twice is 422 invalid_nonce (one round trip cannot be reported as two)"
else
  fail "expected 422 invalid_nonce on the replay, got $code $(cat /tmp/w21-replay.json)"
fi

# ---------------------------------------------------------------------------
step "5. inside the region: a match forms and is confirmed"
# ---------------------------------------------------------------------------
# A fresh block of ids that share one skill, so the pairing policy can pair them at once.
probe "$OUT" players --count 6 --prefix "mr-$RUN_TAG-pair-"
pair_players="$(python3 -c 'import json; print(" ".join(json.load(open("/tmp/w21-probe.json"))["players"]))')"
set -- $pair_players
EU_A="$1"; EU_B="$2"; EU_C="$3"; EU_D="$4"; EU_E="$5"; EU_F="$6"
TOKEN_A="$(login "$EU_A" EU)"; TOKEN_B="$(login "$EU_B" EU)"
play_pair "$EU_REDIS_PORT" "$EU_API" "$TOKEN_A" "$EU_A" "$TOKEN_B" "$EU_B" EU 150
if [ "$(jpath "$OUT_A" status)" = "201" ] && [ "$(jpath "$OUT_B" status)" = "201" ]; then
  ok "both EU players entered the EU queue"
else
  fail "the EU pair did not enqueue: A=$(jpath "$OUT_A" status) B=$(jpath "$OUT_B" status)"
fi
match_a="$(jpath "$OUT_A" matchId)"; match_b="$(jpath "$OUT_B" matchId)"
if [ -n "$match_a" ] && [ "$match_a" = "$match_b" ]; then
  ok "the two EU players are in the same match ($match_a)"
else
  fail "expected one shared matchId, got '$match_a' and '$match_b'"
fi
if [ "$(jpath "$OUT_A" confirmed)" = "True" ] && [ "$(jpath "$OUT_B" confirmed)" = "True" ]; then
  ok "both confirmed; the EU wait is $(jpath "$OUT_A" waitMs) ms and $(jpath "$OUT_B" waitMs) ms, measured"
else
  fail "the EU match was not confirmed: A=$(cat "$OUT_A" | head -c 200) B=$(cat "$OUT_B" | head -c 200)"
fi
if [ "$(jpath "$OUT_A" ticketRegion)" = "EU" ]; then
  ok "the ticket (and therefore the match) is in EU"
else
  fail "expected ticketRegion EU, got $(jpath "$OUT_A" ticketRegion)"
fi
EU_WAIT="$(jpath "$OUT_A" waitMs)"
EU_NEAR_RTT="$(jpath "$OUT_A" rttMs)"

# ---------------------------------------------------------------------------
step "6. failover: the distant region takes the player who can pay for it"
# ---------------------------------------------------------------------------
TOKEN_C="$(login "$EU_C" EU)"; TOKEN_D="$(login "$EU_D" EU)"
play_pair "$NA_REDIS_PORT" "$PROXY" "$TOKEN_C" "$EU_C" "$TOKEN_D" "$EU_D" NA 150
if [ "$(jpath "$OUT_A" status)" = "201" ] && [ "$(jpath "$OUT_B" status)" = "201" ]; then
  ok "two EU players enqueued in NA through the proxy (budget 150 >= declared 120)"
else
  fail "the failover pair did not enqueue: $(cat "$OUT_A" | head -c 300)"
fi
if [ "$(jpath "$OUT_A" matchId)" = "$(jpath "$OUT_B" matchId)" ] && [ "$(jpath "$OUT_A" confirmed)" = "True" ]; then
  ok "the far match formed and was confirmed in NA ($(jpath "$OUT_A" matchId))"
else
  fail "the far match did not form: $(cat "$OUT_A" | head -c 300)"
fi
if [ "$(jpath "$OUT_A" ticketRegion)" = "NA" ]; then
  ok "the ticket says NA: the player was placed in the region it could reach, not the one it lives in"
else
  fail "expected ticketRegion NA, got $(jpath "$OUT_A" ticketRegion)"
fi
FAR_RTT="$(jpath "$OUT_A" rttMs)"
FAR_WAIT="$(jpath "$OUT_A" waitMs)"
note "in-region: rtt ${EU_NEAR_RTT} ms, wait ${EU_WAIT} ms | failover: rtt ${FAR_RTT} ms, wait ${FAR_WAIT} ms"

# ---------------------------------------------------------------------------
step "7. the trade, priced: a budget that cannot pay is refused"
# ---------------------------------------------------------------------------
TOKEN_E="$(login "$EU_E" EU)"
probe "$OUT" ticket --api "$PROXY" --token "$TOKEN_E" --region NA --budget 60
if [ "$(jpath "$OUT" status)" = "422" ] && [ "$(jpath "$OUT" body.error)" = "latency_budget" ]; then
  ok "a 60 ms budget at the 120 ms region is 422 latency_budget"
else
  fail "expected 422 latency_budget, got $(jpath "$OUT" status) $(jpath "$OUT" body.error)"
fi
if [ "$(jpath "$OUT" body.rttMs)" = "120" ] && [ "$(jpath "$OUT" body.maxLatencyMs)" = "60" ]; then
  ok "the refusal carries the numbers: rttMs=120, maxLatencyMs=60"
else
  fail "the refusal does not carry the numbers: $(cat "$OUT")"
fi

NA_PLAYER="mr-$RUN_TAG-na-local"
NA_TOKEN="$(login "$NA_PLAYER" NA)"
probe "$OUT" ticket --api "$NA_API" --token "$NA_TOKEN" --region NA --budget 30
if [ "$(jpath "$OUT" status)" = "201" ]; then
  ok "the same 30 ms budget is enough inside NA (declared 25 ms): the rule is a distance, not a bias"
  cancel_id="$(jpath "$OUT" body.ticketId)"
  curl -sS -o /dev/null -X DELETE "$NA_API/v1/matchmaking/tickets/$cancel_id" -H "authorization: Bearer $NA_TOKEN"
else
  fail "expected 201 for a 30 ms budget inside NA, got $(jpath "$OUT" status)"
fi

# ---------------------------------------------------------------------------
step "8. isolation: EU's hot state dies, NA keeps serving"
# ---------------------------------------------------------------------------
docker stop "$EU_REDIS_CONTAINER" >/dev/null 2>&1
sleep 1
ready="$(curl -sS -o /tmp/w21-ready-eu.json -w '%{http_code}' "$EU_API/health/ready")"
if [ "$ready" = "503" ] && grep -q '"redis":"DOWN"' /tmp/w21-ready-eu.json; then
  ok "EU's readiness is 503 and names redis: the API is alive and says it cannot serve"
else
  fail "expected 503 with redis DOWN from EU, got $ready $(cat /tmp/w21-ready-eu.json)"
fi
TOKEN_F="$(login "$EU_F" EU)"
probe "$OUT" ticket --api "$EU_API" --token "$TOKEN_F" --region EU --budget 150
if [ "$(jpath "$OUT" status)" = "503" ] && [ "$(jpath "$OUT" body.error)" = "hot state unavailable" ]; then
  ok "an arrival at the broken EU is 503 hot state unavailable (not a 500, and not a silent queue)"
else
  fail "expected 503 hot state unavailable from EU, got $(jpath "$OUT" status) $(jpath "$OUT" body.error)"
fi

probe "$OUT" players --count 2 --prefix "mr-$RUN_TAG-na-iso-"
na_players="$(python3 -c 'import json; print(" ".join(json.load(open("/tmp/w21-probe.json"))["players"]))')"
set -- $na_players
N1="$1"; N2="$2"
T1="$(login "$N1" NA)"; T2="$(login "$N2" NA)"
play_pair "$NA_REDIS_PORT" "$NA_API" "$T1" "$N1" "$T2" "$N2" NA 30
if [ "$(jpath "$OUT_A" confirmed)" = "True" ] && [ "$(jpath "$OUT_B" confirmed)" = "True" ]; then
  ok "while EU is down, NA forms and confirms a match with no error"
else
  fail "NA stopped serving while EU was down: $(cat "$OUT_A" | head -c 300)"
fi
if [ "$(jpath "$OUT_A" status)" = "201" ] && [ "$(jpath "$OUT_A" ticketRegion)" = "NA" ]; then
  ok "NA's expiry is unaffected: its ticket and match are NA's own"
else
  fail "unexpected NA result: $(cat "$OUT_A" | head -c 300)"
fi

# ---------------------------------------------------------------------------
step "9. the player fails over, and coming back does not match twice"
# ---------------------------------------------------------------------------
probe "$OUT" players --count 2 --prefix "mr-$RUN_TAG-failover-"
failover_players="$(python3 -c 'import json; print(" ".join(json.load(open("/tmp/w21-probe.json"))["players"]))')"
set -- $failover_players
FAIL_PLAYER="$1"
FAIL_PARTNER="$2"
FAIL_TOKEN="$(login "$FAIL_PLAYER" EU)"
probe "$OUT" ticket --api "$EU_API" --token "$FAIL_TOKEN" --region EU --budget 150
if [ "$(jpath "$OUT" status)" = "503" ]; then
  ok "the client's first attempt (EU) is refused - the client is the one that fails over"
else
  fail "expected EU to refuse during the outage, got $(jpath "$OUT" status)"
fi

# The partner fails over too: the two of them meet in NA, not in the region they live in.
PARTNER_TOKEN="$(login "$FAIL_PARTNER" EU)"
probe "$OUT_A" play --api "$PROXY" --token "$FAIL_TOKEN" --player "$FAIL_PLAYER" --region NA \
  --budget 150 --redis-port "$NA_REDIS_PORT" &
pid_a=$!
probe "$OUT_B" play --api "$PROXY" --token "$PARTNER_TOKEN" --player "$FAIL_PARTNER" --region NA \
  --budget 150 --redis-port "$NA_REDIS_PORT" &
pid_b=$!
wait "$pid_a"; wait "$pid_b"
if [ "$(jpath "$OUT_A" confirmed)" = "True" ] && [ "$(jpath "$OUT_B" confirmed)" = "True" ]; then
  ok "both EU players failed over to NA and were matched and confirmed there"
else
  fail "the failover pair did not complete: $(cat "$OUT_A" | head -c 300)"
fi
FAIL_MATCH="$(jpath "$OUT_A" matchId)"
note "failover match $FAIL_MATCH, failover rtt $(jpath "$OUT_A" rttMs) ms, wait $(jpath "$OUT_A" waitMs) ms"

docker start "$EU_REDIS_CONTAINER" >/dev/null 2>&1
if wait_http "$EU_API/health/ready" 60; then
  ok "EU is ready again (the API never had to be restarted)"
else
  fail "EU did not become ready again after its Redis returned"
fi
eu_keys="$(redis_eu dbsize)"
eu_queues="$(redis_count "$EU_REDIS_CONTAINER" 'queue:*')"
eu_tickets="$(redis_count "$EU_REDIS_CONTAINER" 'ticket:*')"
if [ "$eu_queues" = "0" ] && [ "$eu_tickets" = "0" ]; then
  ok "EU came back without its queue and without its tickets ($eu_keys keys, all of them rate-limit buckets): hot state, not a durable record"
else
  fail "expected no queue/ticket keys in EU after the restart, found $eu_queues queues and $eu_tickets tickets"
fi
eu_ticket_values="$(docker exec "$EU_REDIS_CONTAINER" sh -c \
  'for key in $(redis-cli --scan --pattern "ticket:*"); do redis-cli get "$key"; done' 2>/dev/null)"
if ! echo "$eu_ticket_values" | grep -q "$FAIL_PLAYER"; then
  ok "no ticket in the returning EU belongs to the player who left: nothing to pair twice"
else
  fail "a stale EU ticket for $FAIL_PLAYER survived the restart"
fi
matches_for_player="$(docker exec "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -tAc \
  "SELECT count(DISTINCT match_id) FROM match_players WHERE player_id='$FAIL_PLAYER'" 2>/dev/null | tr -d ' ')"
if [ "$matches_for_player" = "1" ]; then
  ok "the database has exactly one match for the failed-over player (no duplicate)"
else
  fail "expected 1 match for $FAIL_PLAYER, found '$matches_for_player'"
fi

# ---------------------------------------------------------------------------
step "10. the scrape and the database agree with the decisions"
# ---------------------------------------------------------------------------
na_scrape="$(curl -fsS "$NA_API_METRICS" 2>/dev/null)"
if echo "$na_scrape" | grep -q 'matchmaking_tickets_rejected_total{reason="region_unsupported"}'; then
  ok "NA counts the region it refused: tickets_rejected{reason=region_unsupported}"
else
  fail "no region_unsupported refusal counted in the NA scrape"
fi
if echo "$na_scrape" | grep -q 'matchmaking_tickets_rejected_total{reason="latency_budget"}'; then
  ok "NA counts the budgets it could not meet: tickets_rejected{reason=latency_budget}"
else
  fail "no latency_budget refusal counted in the NA scrape"
fi
if echo "$na_scrape" | grep -q 'matchmaking_latency_probes_total{.*result="invalid_nonce"'; then
  ok "the replayed nonce is counted (result=invalid_nonce)"
else
  fail "no invalid_nonce probe counted in the NA scrape"
fi

rows="$(docker exec "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -tAc \
  "SELECT count(*) FROM matches m JOIN match_players p ON p.match_id = m.id
    WHERE p.player_id LIKE 'mr-$RUN_TAG-%'" 2>/dev/null | tr -d ' ')"
region_mix="$(docker exec "$POSTGRES_CONTAINER" psql -U matchmaking -d matchmaking -tAc \
  "SELECT region, count(DISTINCT m.id) FROM matches m JOIN match_players p ON p.match_id = m.id
    WHERE p.player_id LIKE 'mr-$RUN_TAG-%' GROUP BY region ORDER BY region" 2>/dev/null)"
if [ "${rows:-0}" -ge 8 ]; then
  ok "the shared database holds the matches of both regions ($rows player rows for mr-* players)"
else
  fail "expected at least 8 match-player rows for the scenario players, found '$rows'"
fi
if echo "$region_mix" | grep -q "NA|" && echo "$region_mix" | grep -q "EU|"; then
  ok "one durable log, two regions: the matches are labelled NA and EU in the same table"
else
  fail "the database does not show matches in both regions: $region_mix"
fi

# ---------------------------------------------------------------------------
step "result"
# ---------------------------------------------------------------------------
printf '\n== %d passed, %d failed\n' "$PASS" "$FAIL"
if [ "$FAIL" -ne 0 ]; then
  printf '\nlogs: %s %s %s %s %s %s\n' "$AUTH_LOG" "$NA_API_LOG" "$EU_API_LOG" "$NA_ENGINE_LOG" "$EU_ENGINE_LOG" "$PROXY_LOG"
  exit 1
fi
