#!/usr/bin/env python3
"""W20 probe: the queue's partition distribution and the pairing across partitions.

One process, four questions, each answered from what Redis and the services actually hold:

  dist     enqueue N players and report how many landed in each partition key
  metrics  read the engine's scrape and report the per-partition gauges and the hotspot
  cross    enqueue two compatible players that hash to different partitions and wait for the
           match (the merge of the partition heads, end to end)
  cap      enqueue more than the configured depth and report what was admitted and refused

The bash scenario (verify_partitioning.sh) starts the services with the partition counts each
phase needs and calls this probe; every number in the ADR comes from here.
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

REDIS_CONTAINER = "game-matchmaking-backend-redis-1"


# ── the same hash the Java side uses (QueuePartitions: FNV-1a 64 + jump consistent hash) ──
def fnv1a64(value):
    h = 0xCBF29CE484222325
    for ch in value:
        h ^= ord(ch)
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def partition_of(key, partitions):
    if partitions <= 1:
        return 0
    h = fnv1a64(key)
    bucket, nxt = -1, 0
    while nxt < partitions:
        bucket = nxt
        h = (h * 2862933555777941757 + 1) & 0xFFFFFFFFFFFFFFFF
        nxt = int(((bucket + 1) * (1 << 31)) / ((h >> 33) + 1))
    return bucket


def skill(pid):
    """The server's deterministic skill for a player id (Java String.hashCode % 101)."""
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


def keys(pattern):
    return [k for k in redis("keys", pattern).splitlines() if k.strip()]


def call(method, url, token=None, body=None, timeout=15):
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
    except Exception as e:  # noqa: BLE001 - the probe reports, the scenario asserts
        return 0, {"error": str(e)}


_tokens = {}


def token(auth, pid):
    if pid not in _tokens:
        st, body = call("POST", auth + "/v1/auth/login", body={"playerId": pid})
        if st != 200:
            raise SystemExit(f"login failed for {pid}: {st} {body}")
        _tokens[pid] = body["token"]
    return _tokens[pid]


def enqueue(api, auth, pid, mode="1v1", region="NA", latency=80):
    st, body = call("POST", api + "/v1/matchmaking/tickets", token(auth, pid),
                    {"gameMode": mode, "region": region, "maxLatencyMs": latency})
    return st, body


def scrape(url):
    text = urllib.request.urlopen(url, timeout=10).read().decode()
    samples = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name = line.split("{")[0].split(" ")[0]
        labels = {}
        if "{" in line:
            raw = line[line.index("{") + 1:line.index("}")]
            for pair in raw.split(","):
                k, _, v = pair.partition("=")
                labels[k.strip()] = v.strip().strip('"')
        value = float(line.rsplit(" ", 1)[1])
        samples.setdefault(name, []).append((labels, value))
    return samples


def command_dist(args):
    pending = [f"{args.prefix}{i}" for i in range(args.players)]

    def one(pid):
        return enqueue(args.api, args.auth, pid, args.mode, args.region)

    admitted = 0
    refused = 0
    with ThreadPoolExecutor(max_workers=16) as pool:
        for st, _ in pool.map(one, pending):
            if st == 201:
                admitted += 1
            else:
                refused += 1

    counts = {}
    for queue_key in keys(f"queue:{args.mode}:{args.region}*"):
        counts[queue_key] = int(redis("zcard", queue_key) or 0)
    total = sum(counts.values())
    seen = [v for v in counts.values() if v > 0]
    mean = total / args.partitions if args.partitions else 0
    deepest = max(seen) if seen else 0
    print(json.dumps({
        "admitted": admitted,
        "refused": refused,
        "counts": counts,
        "sum": total,
        "keys_with_members": len(seen),
        "partitions": args.partitions,
        "mean": mean,
        "deepest": deepest,
        "hotspot": (deepest / mean) if mean else 0,
        "max_share_pct": (100.0 * deepest / total) if total else 0,
    }))


def command_metrics(args):
    samples = scrape(args.metrics)
    depth = 0.0
    partitions = {}
    for labels, value in samples.get("matchmaking_queue_partition_depth", []):
        if labels.get("mode") == args.mode and labels.get("region") == args.region:
            partitions[int(labels.get("partition", -1))] = value
    for labels, value in samples.get("matchmaking_queue_depth", []):
        if labels.get("mode") == args.mode and labels.get("region") == args.region:
            depth = value
    hotspot = 0.0
    for labels, value in samples.get("matchmaking_queue_hotspot_ratio", []):
        if labels.get("mode") == args.mode and labels.get("region") == args.region:
            hotspot = value
    mean = sum(partitions.values()) / len(partitions) if partitions else 0
    deepest = max(partitions.values()) if partitions else 0
    print(json.dumps({
        "partition_depth": partitions,
        "depth": depth,
        "hotspot": hotspot,
        "mean": mean,
        "deepest": deepest,
        "expected_hotspot": (deepest / mean) if mean else 0,
    }))


def command_cross(args):
    # Two players that share a skill (compatible) and hash to different partitions (so the
    # match can only form if the engine merges the heads). The ids are chosen, and the chosen
    # keys are read back from Redis afterwards - a check that the hash and the server agree.
    first_by_skill = {}
    pair = None
    for i in range(50000):
        player = f"w20-cross-{i}"
        partition = partition_of(player, args.partitions)
        player_skill = skill(player)
        if player_skill in first_by_skill and first_by_skill[player_skill][1] != partition:
            pair = (first_by_skill[player_skill], (player, partition))
            break
        first_by_skill.setdefault(player_skill, (player, partition))
    if pair is None:
        raise SystemExit("could not find two compatible players in different partitions")
    (player_a, pa), (player_b, pb) = pair
    skill_a = skill(player_a)
    skill_b = skill(player_b)

    st_a, body_a = enqueue(args.api, args.auth, player_a, args.mode, args.region)
    st_b, body_b = enqueue(args.api, args.auth, player_b, args.mode, args.region)
    if st_a != 201 or st_b != 201:
        raise SystemExit(f"enqueue failed: {st_a} {body_a} / {st_b} {body_b}")

    match_id, players, waited = "", [], 0.0
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        for key in keys("match-pending:*"):
            pm = json.loads(redis("get", key) or "{}")
            ids = [p["playerId"] for p in pm.get("players", [])]
            if player_a in ids and player_b in ids:
                match_id, players = pm.get("matchId"), ids
                break
        if match_id:
            break
        time.sleep(0.2)
        waited += 0.2

    key_a = f"queue:{args.mode}:{args.region}:{pa}" if args.partitions > 1 else f"queue:{args.mode}:{args.region}"
    key_b = f"queue:{args.mode}:{args.region}:{pb}" if args.partitions > 1 else f"queue:{args.mode}:{args.region}"
    states = {}
    for ticket in (body_a.get("ticketId"), body_b.get("ticketId")):
        raw = redis("get", f"ticket:{ticket}") if ticket else ""
        states[ticket] = json.loads(raw).get("status") if raw else ""
    print(json.dumps({
        "player_a": player_a, "player_b": player_b,
        "partition_a": pa, "partition_b": pb,
        "skill": skill_a,
        "match_id": match_id, "players": players,
        "waited_s": round(waited, 2),
        "key_a_depth": int(redis("zcard", key_a) or 0),
        "key_b_depth": int(redis("zcard", key_b) or 0),
        "ticket_states": states,
    }))


def command_cap(args):
    admitted = 0
    refused = 0
    for i in range(args.players):
        st, _ = enqueue(args.api, args.auth, f"w20-cap-{i}", args.mode, args.region)
        if st == 201:
            admitted += 1
        else:
            refused += 1
    total = 0
    for queue_key in keys(f"queue:{args.mode}:{args.region}*"):
        total += int(redis("zcard", queue_key) or 0)
    print(json.dumps({"admitted": admitted, "refused": refused, "depth": total}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["dist", "metrics", "cross", "cap"])
    parser.add_argument("--api", default="http://localhost:8094")
    parser.add_argument("--auth", default="http://localhost:8081")
    parser.add_argument("--metrics", default="http://localhost:9194/metrics")
    parser.add_argument("--mode", default="1v1")
    parser.add_argument("--region", default="NA")
    parser.add_argument("--players", type=int, default=2000)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--prefix", default="w20-dist-")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    {"dist": command_dist, "metrics": command_metrics,
     "cross": command_cross, "cap": command_cap}[args.command](args)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)
