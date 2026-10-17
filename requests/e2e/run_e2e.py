#!/usr/bin/env python3
"""End-to-end validation of the matchmaking backend (W9/W10/W11) against the
running services and the real Redis/Postgres from docker-compose."""
import json
import subprocess
import time
import urllib.error
import urllib.request

API = "http://localhost:8080"
AUTH = "http://localhost:8081"
REDIS_CONTAINER = "game-matchmaking-backend-redis-1"

PASS, FAIL = [], []


def skill(pid):
    h = 0
    for c in pid:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    if h >= 2 ** 31:
        h -= 2 ** 32
    return abs(h) % 101


def redis(*args):
    out = subprocess.run(["docker", "exec", REDIS_CONTAINER, "redis-cli", *args],
                         capture_output=True, text=True, check=False)
    return out.stdout.strip()


def flush():
    redis("flushall")


def keys(pattern):
    raw = redis("keys", pattern)
    return [k for k in raw.splitlines() if k.strip()]


def call(method, url, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, (json.loads(e.read() or b"{}"))


_tokens = {}


def token(pid):
    if pid not in _tokens:
        st, body = call("POST", AUTH + "/v1/auth/login", body={"playerId": pid})
        assert st == 200, f"login failed for {pid}: {st} {body}"
        _tokens[pid] = body["token"]
    return _tokens[pid]


def enqueue(pid, mode="1v1", region="NA", latency=80, party_id=None, members=None):
    body = {"gameMode": mode, "region": region, "maxLatencyMs": latency}
    if party_id is not None:
        body["partyId"] = party_id
        body["members"] = members
    st, resp = call("POST", API + "/v1/matchmaking/tickets", token(pid), body)
    assert st == 201, f"enqueue failed for {pid}: {st} {resp}"
    return resp["ticketId"]


def pending_matches():
    """Returns [(matchId, [playerIds])] from Redis."""
    out = []
    for k in keys("match-pending:*"):
        raw = redis("get", k)
        pm = json.loads(raw)
        out.append((pm["matchId"], [p["playerId"] for p in pm["players"]]))
    return out


def wait_until(pred, timeout=10, interval=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(interval)
    return None


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  -> {detail}" if detail else ""))


def skill_group(n):
    """n player ids that all share the same deterministic skill."""
    groups = {}
    i = 1
    while True:
        pid = f"p{i}"
        s = skill(pid)
        groups.setdefault(s, []).append(pid)
        if len(groups[s]) >= n:
            return s, groups[s][:n]
        i += 1
        if i > 20000:
            raise RuntimeError("group not found")


def scenario(title):
    print(f"\n=== {title} ===")
    flush()


# ---------------------------------------------------------------- scenarios
def s_health():
    scenario("S0 health + auth")
    st, body = call("GET", API + "/health")
    check("GET /health -> 200 UP", st == 200 and body.get("status") == "UP", f"{st} {body}")
    check("auth login returns a token", bool(token("p2")))


def s_1v1():
    scenario("S1 1v1 pairing (queue head, skill window 2)")
    s, pair = skill_group(2)
    a, b = pair
    ta, tb = enqueue(a), enqueue(b)
    pm = wait_until(lambda: pending_matches() or None, 8)
    check("a match-pending exists", bool(pm), str(pm))
    check("the pending match has exactly 2 players", pm and len(pm[0][1]) == 2, str(pm))
    check("the pair is exactly the two queued players", pm and set(pm[0][1]) == {a, b}, str(pm))
    check("ticket a is CONFIRMING", json.loads(redis("get", f"ticket:{ta}"))["status"] == "CONFIRMING")
    check("queue is drained (zcard 0)", redis("zcard", "queue:1v1:NA") == "0", redis("zcard", "queue:1v1:NA"))


def s_5v5():
    scenario("S2 5v5 pairing requires exactly 10 (W9)")
    s, players = skill_group(10)
    ids = [enqueue(p, mode="5v5") for p in players]
    pm = wait_until(lambda: (pending_matches() or None), 8)
    check("one pending match formed", pm and len(pm) == 1, str(pm))
    check("the pending match has 10 players", pm and len(pm[0][1]) == 10, str(pm))
    check("all ten queued players are in it", pm and set(pm[0][1]) == set(players))
    check("all tickets CONFIRMING", all(json.loads(redis("get", f"ticket:{i}"))["status"] == "CONFIRMING" for i in ids))


def s_party_fill():
    scenario("S3 party fill: 3-member party + 7 solos -> one 10-player match (W11)")
    s, pool = skill_group(13)
    party3, solos = pool[:3], pool[3:10]
    leader = party3[0]
    st, created = call("POST", API + "/v1/parties", token(leader))
    check("POST /v1/parties -> 201", st == 201, str(created))
    party_id = created["partyId"]
    for m in party3[1:]:
        st, body = call("POST", API + f"/v1/parties/{party_id}/members", token(m))
        check(f"add member {m} -> 200", st == 200 and m in body.get("members", []), str(body))

    st, party = call("GET", API + f"/v1/parties/{party_id}", token(leader))
    check("GET party shows 3 members + aggregatedSkill", st == 200 and len(party["members"]) == 3
          and party["aggregatedSkill"] == s, str(party))

    first = enqueue(leader, mode="5v5", region="NA", party_id=party_id, members=party3)
    tickets = json.loads(redis("get", f"ticket:{first}"))
    check("party ticket carries the partyId", tickets["partyId"] == party_id, str(tickets.get("partyId")))

    for solo in solos:
        enqueue(solo, mode="5v5", region="NA")

    pm = wait_until(lambda: (pending_matches() or None), 8)
    check("exactly one pending match", pm and len(pm) == 1, str(pm))
    check("it has 10 players", pm and len(pm[0][1]) == 10, str(pm))
    check("the 3 party members are together (party never split)",
          pm and set(party3).issubset(set(pm[0][1])), str(pm))
    check("the queue is drained", redis("zcard", "queue:5v5:NA") == "0", redis("zcard", "queue:5v5:NA"))


def s_party_not_fillable():
    scenario("S4 party not fillable: 2 parties of 3 + 4 solos -> no match (party stays queued)")
    s, pool = skill_group(10)
    pa, pb, solos = pool[:3], pool[3:6], pool[6:10]
    pa_id = call("POST", API + "/v1/parties", token(pa[0]))[1]["partyId"]
    for m in pa[1:]:
        call("POST", API + f"/v1/parties/{pa_id}/members", token(m))
    pb_id = call("POST", API + "/v1/parties", token(pb[0]))[1]["partyId"]
    for m in pb[1:]:
        call("POST", API + f"/v1/parties/{pb_id}/members", token(m))
    enqueue(pa[0], mode="5v5", region="SA", party_id=pa_id, members=pa)
    enqueue(pb[0], mode="5v5", region="SA", party_id=pb_id, members=pb)
    for solo in solos:
        enqueue(solo, mode="5v5", region="SA")
    time.sleep(3)
    check("no pending match was created", not pending_matches(), str(pending_matches()))
    check("all 10 tickets stay SEARCHING",
          redis("zcard", "queue:5v5:SA") == "10", redis("zcard", "queue:5v5:SA"))


def s_cancel_member():
    scenario("S5 atomic cancel: cancelling one member cancels the whole party (W11)")
    s, pool = skill_group(3)
    pid = call("POST", API + "/v1/parties", token(pool[0]))[1]["partyId"]
    for m in pool[1:]:
        call("POST", API + f"/v1/parties/{pid}/members", token(m))
    enqueue(pool[0], mode="5v5", region="EU", party_id=pid, members=pool)
    # collect the three party tickets from Redis
    ptick = [t for t in keys("ticket:*") if json.loads(redis("get", t))["partyId"] == pid]
    check("party enqueue created 3 ticketed members", len(ptick) == 3, str(len(ptick)))
    victim = json.loads(redis("get", ptick[0]))
    st, body = call("DELETE", API + f"/v1/matchmaking/tickets/{victim['ticketId']}", token(victim["playerId"]))
    check("DELETE member ticket -> 200", st == 200, f"{st} {body}")
    statuses = [json.loads(redis("get", t))["status"] for t in ptick]
    check("all 3 party tickets are CANCELLED", statuses == ["CANCELLED"] * 3, str(statuses))
    check("the party is out of the queue", redis("zcard", "queue:5v5:EU") == "0", redis("zcard", "queue:5v5:EU"))


def s_remove_member():
    scenario("S6 removing a member via the party API cancels the whole party")
    s, pool = skill_group(3)
    pid = call("POST", API + "/v1/parties", token(pool[0]))[1]["partyId"]
    for m in pool[1:]:
        call("POST", API + f"/v1/parties/{pid}/members", token(m))
    enqueue(pool[0], mode="5v5", region="EU", party_id=pid, members=pool)
    ptick = [t for t in keys("ticket:*") if json.loads(redis("get", t))["partyId"] == pid]
    st, body = call("DELETE", API + f"/v1/parties/{pid}/members/{pool[1]}", token(pool[0]))
    check("DELETE member -> 200", st == 200, f"{st} {body}")
    statuses = [json.loads(redis("get", t))["status"] for t in ptick]
    check("all 3 party tickets are CANCELLED", statuses == ["CANCELLED"] * 3, str(statuses))
    check("the party stays active with the remaining members", body.get("active") is True and pool[1] not in body.get("members", []))


def s_dissolve():
    scenario("S7 dissolving a party cancels every member ticket")
    s, pool = skill_group(3)
    pid = call("POST", API + "/v1/parties", token(pool[0]))[1]["partyId"]
    for m in pool[1:]:
        call("POST", API + f"/v1/parties/{pid}/members", token(m))
    enqueue(pool[0], mode="5v5", region="EU", party_id=pid, members=pool)
    ptick = [t for t in keys("ticket:*") if json.loads(redis("get", t))["partyId"] == pid]
    st, body = call("DELETE", API + f"/v1/parties/{pid}", token(pool[0]))
    check("DELETE party -> 200", st == 200, f"{st} {body}")
    check("party dissolved (active=false)", body.get("active") is False, str(body.get("active")))
    statuses = [json.loads(redis("get", t))["status"] for t in ptick]
    check("all 3 party tickets are CANCELLED", statuses == ["CANCELLED"] * 3, str(statuses))


def s_widening():
    scenario("S8 widening (W10): skill gap 3 not paired at t=0, paired after waiting")
    # p1=87, p4=90 -> gap 3
    a, b = "p1", "p4"
    ta, tb = enqueue(a, region="NA"), enqueue(b, region="NA")
    time.sleep(4)
    check("not paired immediately (half-range 2)", not pending_matches(), str(pending_matches()))
    pm = wait_until(lambda: (pending_matches() or None), 45, 1.0)
    check("paired after the head waited (half-range widened)", bool(pm), str(pm))
    if pm:
        check("the widened match contains both players", set(pm[0][1]) == {a, b}, str(pm))


def s_timeout():
    scenario("S9 confirmation timeout: pending match is swept and tickets requeue")
    s, pair = skill_group(2)
    a, b = pair
    ta, tb = enqueue(a, region="NA"), enqueue(b, region="NA")
    pm = wait_until(lambda: (pending_matches() or None), 8)
    check("pending match created", bool(pm), str(pm))
    time.sleep(36)
    versions = [json.loads(redis("get", f"ticket:{t}"))["version"] for t in (ta, tb)]
    check("tickets were swept and re-queued/re-paired (version >= 3)", all(v >= 3 for v in versions), str(versions))


def main():
    print("E2E validation of the matchmaking backend")
    s_health()
    s_1v1()
    s_5v5()
    s_party_fill()
    s_party_not_fillable()
    s_cancel_member()
    s_remove_member()
    s_dissolve()
    s_widening()
    s_timeout()
    print(f"\n===== RESULT: {len(PASS)} passed, {len(FAIL)} failed =====")
    for f in FAIL:
        print("  FAILED:", f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
