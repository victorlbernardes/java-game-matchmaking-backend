#!/usr/bin/env python3
"""W19 (ADR 0026): the WebSocket connection is not pinned to an instance.

Two API instances share one Redis.

Phase 1 (routing): player A connects to A, player B to B; the engine forms a match; each
notification is delivered by the instance that holds the player's socket, and the instance
that does not hold it counts the miss. Both confirm through their own instance.

Phase 2 (recovery): player C connects to A, player D never connects at all. They enqueue, the
match forms, and only C is notified - D's notification is published to nobody. Then instance
A is killed **hard** (no close handlers) and C reconnects on B. C and D each send a heartbeat:
both must be re-presented the pending confirmation, D for the first time, and the match must
still be confirmed.

Before W19, phase 2 had no answer: D's notification was published once to an instance that
held no socket for D, the heartbeat answered {@code PONG}, and the match dissolved at its 30 s
deadline with every process that was still alive looking healthy (W16 measured 24 of 1024
matches that way).
"""
import asyncio
import json
import os
import signal
import subprocess
import time
import urllib.error
import urllib.request

import websockets

API_A = os.environ.get("API_A", "http://localhost:8092")
API_B = os.environ.get("API_B", "http://localhost:8093")
AUTH = os.environ.get("AUTH", "http://localhost:8081")
METRICS_A = os.environ.get("METRICS_A", "http://localhost:9192/metrics")
METRICS_B = os.environ.get("METRICS_B", "http://localhost:9193/metrics")
ENGINE_METRICS = os.environ.get("ENGINE_METRICS", "http://localhost:9091/metrics")
REDIS_CONTAINER = os.environ.get("REDIS_CONTAINER", "game-matchmaking-backend-redis-1")
API_A_PID = int(os.environ.get("API_A_PID", "0") or 0)
NOTIFY_TIMEOUT_S = 20
REPRESENT_TIMEOUT_S = 5

PASS, FAIL = [], []
NUMBERS = {}


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  -> {detail}" if detail else ""))
    return cond


def api(method, url, token=None, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def redis(*args):
    out = subprocess.run(["docker", "exec", REDIS_CONTAINER, "redis-cli", *args],
                         capture_output=True, text=True, check=False)
    return out.stdout.strip()


def token(player_id):
    status, body = api("POST", AUTH + "/v1/auth/login", body={"playerId": player_id})
    assert status == 200, f"login failed for {player_id}: {status} {body}"
    return body["token"]


def enqueue(base_url, player_id):
    status, body = api("POST", base_url + "/v1/matchmaking/tickets", token(player_id),
                       {"gameMode": "1v1", "region": "NA", "maxLatencyMs": 80})
    return status, body


def java_hash(text):
    h = 0
    for ch in text:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h >= 2 ** 31:
        h -= 2 ** 32
    return h


def skill(player_id):
    return abs(java_hash(player_id)) % 101


def pick_pairs(count):
    """`count` pairs of ids where each pair shares a skill, or the engine never pairs them."""
    groups = {}
    pairs = []
    for i in range(1, 20000):
        pid = f"routing-{i}"
        group = groups.setdefault(skill(pid), [])
        if len(group) == 1:
            pairs.append([group[0], pid])
            group.append(pid)
            if len(pairs) == count:
                return pairs
            continue
        group.append(pid)
    raise RuntimeError(f"could not find {count} pairs with a shared skill")


def metric(text, name, **labels):
    """Value of one Prometheus sample, summing every series that matches the labels."""
    total = 0.0
    found = False
    for line in text.splitlines():
        if not line.startswith(name):
            continue
        if labels and not all(f'{k}="{v}"' in line for k, v in labels.items()):
            continue
        value = line.rsplit(" ", 1)[-1]
        try:
            total += float(value)
            found = True
        except ValueError:
            continue
    return total if found else None


def scrape(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode()
    except Exception:
        return ""


def wait_until(predicate, timeout=10, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def pending_matches():
    """[(matchId, [playerIds])] from Redis - the scenario's own view, not the client's."""
    result = []
    for key in redis("keys", "match-pending:*").splitlines():
        key = key.strip()
        if not key:
            continue
        raw = redis("get", key)
        if raw and raw != "(nil)":
            pending = json.loads(raw)
            result.append((pending["matchId"], [p["playerId"] for p in pending["players"]]))
    return result


def match_for(players):
    wanted = set(players)
    for match_id, roster in pending_matches():
        if set(roster) == wanted:
            return match_id
    return None


class Client:
    """One WebSocket client that keeps every frame it receives."""

    def __init__(self, url):
        self.url = url
        self.frames = []
        self.ws = None
        self.reader = None

    async def connect(self):
        self.ws = await websockets.connect(self.url)
        self.reader = asyncio.create_task(self._read())

    async def _read(self):
        try:
            async for message in self.ws:
                try:
                    self.frames.append(json.loads(message))
                except Exception:
                    self.frames.append({"type": "unparseable", "raw": message})
        except Exception:
            return

    async def send(self, frame):
        await self.ws.send(json.dumps(frame))

    def of_type(self, frame_type):
        return [f for f in self.frames if f.get("type") == frame_type]

    async def heartbeat(self):
        self.frames.clear()
        await self.send({"type": "PING", "payload": {}})

    async def close(self):
        if self.reader:
            self.reader.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass


async def await_frame(client, frame_type, timeout):
    """Waits for a frame of that type and returns (frame, seconds waited)."""
    start = time.time()
    while time.time() - start < timeout:
        found = client.of_type(frame_type)
        if found:
            return found[-1], time.time() - start
        await asyncio.sleep(0.005)
    return None, time.time() - start


def ws_url(base_url, player_id):
    return f"{base_url.replace('http', 'ws')}/v1/ws?token={token(player_id)}"


async def phase_routing(a_name, b_name, player_a, player_b):
    print("\n--- phase 1: each notification lands on the instance holding the socket ---")
    client_a = Client(ws_url(API_A, player_a))
    client_b = Client(ws_url(API_B, player_b))
    await client_a.connect()
    await client_b.connect()
    try:
        presence_a = wait_until(lambda: redis("get", f"presence:{player_a}") == a_name, 3)
        presence_b = wait_until(lambda: redis("get", f"presence:{player_b}") == b_name, 3)
        check("presence names the instance holding the socket", bool(presence_a and presence_b),
              f"{player_a}@{redis('get', f'presence:{player_a}')} "
              f"{player_b}@{redis('get', f'presence:{player_b}')}")

        status_a, body_a = enqueue(API_A, player_a)
        status_b, body_b = enqueue(API_B, player_b)
        check("both players enqueue", status_a == 201 and status_b == 201, f"{status_a}/{status_b}")

        notified_a, _ = await await_frame(client_a, "MATCH_PENDING_CONFIRMATION", NOTIFY_TIMEOUT_S)
        notified_b, _ = await await_frame(client_b, "MATCH_PENDING_CONFIRMATION", NOTIFY_TIMEOUT_S)
        match_id = (notified_a or {}).get("payload", {}).get("matchId")
        check("both players were notified", bool(notified_a and notified_b), f"matchId={match_id}")
        check("the notification names the same match", bool(match_id)
              and match_id == (notified_b or {}).get("payload", {}).get("matchId"))

        delivered_a = metric(scrape(METRICS_A), "matchmaking_ws_notifications_total", result="delivered")
        missed_a = metric(scrape(METRICS_A), "matchmaking_ws_notifications_total",
                          result="no_local_connection")
        delivered_b = metric(scrape(METRICS_B), "matchmaking_ws_notifications_total", result="delivered")
        missed_b = metric(scrape(METRICS_B), "matchmaking_ws_notifications_total",
                          result="no_local_connection")
        check("each instance delivered the notification for its own socket",
              bool(delivered_a) and bool(delivered_b), f"A={delivered_a} B={delivered_b}")
        check("each instance counted the notification it had no socket for",
              bool(missed_a) and bool(missed_b), f"A={missed_a} B={missed_b}")
        NUMBERS["delivered_a"] = delivered_a
        NUMBERS["missed_a"] = missed_a
        NUMBERS["delivered_b"] = delivered_b
        NUMBERS["missed_b"] = missed_b

        # End the match cleanly, through each player's own instance: routing works both ways.
        await client_a.send({"type": "MATCH_CONFIRM", "payload": {"matchId": match_id}})
        await client_b.send({"type": "MATCH_CONFIRM", "payload": {"matchId": match_id}})
        confirmed = wait_until(lambda: redis("exists", f"match-confirmed-claim:{match_id}") == "1", 10)
        check("both confirmations are accepted and the match resolves", bool(confirmed),
              f"match-pending exists={redis('exists', f'match-pending:{match_id}')}")
    finally:
        await client_a.close()
        await client_b.close()


async def phase_recovery(a_name, player_c, player_d):
    print("\n--- phase 2: the instance dies and the notification was never delivered ---")
    # Player C has a socket on A; player D has none at all. Both enqueue, so the engine forms
    # the match and publishes D's notification to an instance that holds no socket for D.
    client_c = Client(ws_url(API_A, player_c))
    await client_c.connect()
    try:
        status_c, _ = enqueue(API_B, player_c)
        status_d, _ = enqueue(API_B, player_d)
        check("both players enqueue (D without ever connecting)",
              status_c == 201 and status_d == 201, f"{status_c}/{status_d}")

        match_id = wait_until(lambda: match_for([player_c, player_d]), NOTIFY_TIMEOUT_S, 0.1)
        check("the engine formed the match while D had no socket", bool(match_id), f"{match_id}")
        if not match_id:
            return
        notification_c, _ = await await_frame(client_c, "MATCH_PENDING_CONFIRMATION", 3)
        check("C, connected on A, was notified", notification_c is not None)

        # The instance holding C's socket dies. SIGKILL: no close handler, no last goodbye.
        check("the scenario knows which process to kill", API_A_PID > 0, f"pid={API_A_PID}")
        if API_A_PID > 0:
            os.kill(API_A_PID, signal.SIGKILL)
        check("instance A is gone", bool(wait_until(lambda: not answers(API_A + "/health"), 15)))

        # C reconnects on B and asks with a heartbeat.
        resumed_c = Client(ws_url(API_B, player_c))
        await resumed_c.connect()
        await resumed_c.heartbeat()
        pong_c, _ = await await_frame(resumed_c, "PONG", REPRESENT_TIMEOUT_S)
        represented_c, _ = await await_frame(resumed_c, "MATCH_PENDING_CONFIRMATION", REPRESENT_TIMEOUT_S)
        check("C's heartbeat is answered", pong_c is not None)
        check("C's heartbeat re-presents the pending confirmation", represented_c is not None)

        # D connects for the first time, on B, and asks the same question.
        client_d = Client(ws_url(API_B, player_d))
        await client_d.connect()
        await client_d.heartbeat()
        pong_d, _ = await await_frame(client_d, "PONG", REPRESENT_TIMEOUT_S)
        represented_d, latency = await await_frame(client_d, "MATCH_PENDING_CONFIRMATION",
                                             REPRESENT_TIMEOUT_S)
        check("D's heartbeat is answered", pong_d is not None)
        check("D, who never received the notification, is told what it missed",
              represented_d is not None,
              f"matchId={(represented_d or {}).get('payload', {}).get('matchId')}")
        delivered_match = (represented_d or {}).get("payload", {}).get("matchId")
        check("the re-presented match is the pending one", delivered_match == match_id)
        NUMBERS["first_heartbeat_to_representation_s"] = round(latency, 3)

        presence_c = wait_until(lambda: redis("get", f"presence:{player_c}") == "api-b", 3)
        check("presence follows C to the surviving instance", bool(presence_c),
              f"{player_c}@{redis('get', f'presence:{player_c}')}")

        # Both answer with the id the heartbeat gave them - D had no other way to know it.
        await resumed_c.send({"type": "MATCH_CONFIRM", "payload": {"matchId": delivered_match}})
        await client_d.send({"type": "MATCH_CONFIRM", "payload": {"matchId": delivered_match}})
        confirmed = wait_until(lambda: redis("exists", f"match-confirmed-claim:{match_id}") == "1", 10)
        check("the match is confirmed by the two reconnected clients", bool(confirmed))
        pending_gone = wait_until(lambda: redis("exists", f"match-pending:{match_id}") == "0", 10)
        check("the pending match key is gone", bool(pending_gone))
        index_gone = wait_until(
            lambda: redis("exists", f"player-pending:{player_c}") == "0"
            and redis("exists", f"player-pending:{player_d}") == "0", 10)
        check("the reverse index is cleared once the match resolves", bool(index_gone))

        # Nothing owed any more: the same heartbeat must not re-present a resolved match.
        await client_d.heartbeat()
        pong_after, _ = await await_frame(client_d, "PONG", REPRESENT_TIMEOUT_S)
        stale, _ = await await_frame(client_d, "MATCH_PENDING_CONFIRMATION", 1.0)
        check("a resolved match is not re-presented", pong_after is not None and stale is None,
              f"frames={[f.get('type') for f in client_d.frames]}")

        await client_d.close()
        await resumed_c.close()
    finally:
        await client_c.close()


def answers(url):
    try:
        with urllib.request.urlopen(url, timeout=1) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


async def main():
    print("=== W19: routing a notification to whichever instance holds the socket ===")
    status_a, health_a = api("GET", API_A + "/health")
    status_b, health_b = api("GET", API_B + "/health")
    a_name, b_name = health_a.get("instance"), health_b.get("instance")
    check("both instances answer /health", status_a == 200 and status_b == 200, f"{status_a}/{status_b}")
    check("the instances are distinguishable", a_name and b_name and a_name != b_name,
          f"{a_name} vs {b_name}")
    if not a_name or not b_name or a_name == b_name:
        return

    (player_a, player_b), (player_c, player_d) = pick_pairs(2)
    print(f"  phase 1 players: {player_a} / {player_b} (skill {skill(player_a)})")
    print(f"  phase 2 players: {player_c} / {player_d} (skill {skill(player_c)})")

    before_confirmed = metric(scrape(ENGINE_METRICS), "matchmaking_matches_confirmed_total") or 0
    before_dissolved = metric(scrape(ENGINE_METRICS), "matchmaking_matches_dissolved_total") or 0

    await phase_routing(a_name, b_name, player_a, player_b)
    await phase_recovery(a_name, player_c, player_d)

    confirmed_after = metric(scrape(ENGINE_METRICS), "matchmaking_matches_confirmed_total") or 0
    dissolved_after = metric(scrape(ENGINE_METRICS), "matchmaking_matches_dissolved_total") or 0
    check("the engine confirmed both matches", confirmed_after - before_confirmed >= 2,
          f"confirmed {before_confirmed} -> {confirmed_after}")
    check("nothing dissolved: no match outlived its notification",
          dissolved_after == before_dissolved, f"dissolved {before_dissolved} -> {dissolved_after}")


if __name__ == "__main__":
    asyncio.run(main())
    print()
    print(f"W19 routing: {len(PASS)} passed, {len(FAIL)} failed")
    for name, value in NUMBERS.items():
        print(f"  {name}: {value}")
    for name in FAIL:
        print(f"  FAILED: {name}")
    raise SystemExit(1 if FAIL else 0)
