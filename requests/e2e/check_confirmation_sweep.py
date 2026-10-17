#!/usr/bin/env python3
import json
import subprocess
import time
import urllib.request

R = ["docker", "exec", "game-matchmaking-backend-redis-1", "redis-cli"]


def r(*a):
    return subprocess.run(R + list(a), capture_output=True, text=True).stdout.strip()


def post(url, body, token=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


r("flushall")
t1 = post("http://localhost:8081/v1/auth/login", {"playerId": "p1"})["token"]
t2 = post("http://localhost:8081/v1/auth/login", {"playerId": "p37"})["token"]
ta = post("http://localhost:8080/v1/matchmaking/tickets",
          {"gameMode": "1v1", "region": "NA", "maxLatencyMs": 80}, t1)["ticketId"]
tb = post("http://localhost:8080/v1/matchmaking/tickets",
          {"gameMode": "1v1", "region": "NA", "maxLatencyMs": 80}, t2)["ticketId"]
print("tickets:", ta, tb)

start = time.time()
for i in range(11):
    time.sleep(4)
    pend = r("keys", "match-pending:*")
    pend = pend.splitlines() if pend else []
    exp = r("get", pend[0]) if pend else ""
    expires = json.loads(exp)["expiresAt"] if exp else "-"
    va = json.loads(r("get", f"ticket:{ta}"))
    vb = json.loads(r("get", f"ticket:{tb}"))
    print(f"t={time.time()-start:5.1f}s pending={len(pend)} expiresAt={expires} "
          f"a=(v{va['version']},{va['status']}) b=(v{vb['version']},{vb['status']})")
