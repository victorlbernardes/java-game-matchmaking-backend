#!/usr/bin/env python3
"""W17 - fills one queue until the API refuses, and reports what the API said (ADR 0022).

The queue cap only exists while there is something to cap, so this helper drives real arrivals at
the stack until the API starts answering `503` with the capacity in the body - the honest way to
see the cap is to reach it. It logs in and enqueues `--players` solo players, concurrently (the
cap is atomic, so concurrency is exactly what it must survive), and prints one JSON summary on
stdout for the scenario to assert on. Human-readable progress goes to stderr.

Usage:
  python3 requests/backpressure/fill_queue.py --players 1100 --concurrency 32

Exit code 1 when nothing was ever shed: a scenario asserting on a cap that was never reached would
pass for the wrong reason.
"""
import argparse
import concurrent.futures
import json
import os
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API", "http://localhost:8080")
AUTH = os.environ.get("AUTH", "http://localhost:8081")


def call(method, url, token=None, body=None, timeout=20):
    """Returns (status, headers, body-dict); a 4xx/5xx is answered, not raised."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, dict(response.headers), _json(response.read())
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), _json(error.read())


def _json(raw):
    try:
        return json.loads(raw or b"{}")
    except ValueError:
        return {}


def login(player_id):
    status, _, body = call("POST", AUTH + "/v1/auth/login",
                           body={"playerId": player_id, "displayName": player_id})
    if status not in (200, 201) or "token" not in body:
        raise RuntimeError(f"login of {player_id} answered {status}: {body}")
    return body["token"]


def enqueue(player_id, token, idempotency_key):
    """Sends one arrival. The idempotency key makes the replay check possible: the same call,
    sent again, must return the ticket rather than a refusal."""
    request_body = {"gameMode": "1v1", "region": "NA", "maxLatencyMs": 80}
    data = json.dumps(request_body).encode()
    request = urllib.request.Request(API + "/v1/matchmaking/tickets", data=data, method="POST")
    request.add_header("Content-Type", "application/json")
    request.add_header("Authorization", "Bearer " + token)
    request.add_header("Idempotency-Key", idempotency_key)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, dict(response.headers), _json(response.read())
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), _json(error.read())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--players", type=int, default=1100,
                        help="arrivals to attempt (must exceed the expected cap)")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--label", default="cap")
    args = parser.parse_args()

    base = f"{args.label}-{int(time.time())}"
    ids = [f"{base}-{i}" for i in range(args.players)]

    print(f"  {args.players} players: logging in", file=sys.stderr)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        tokens = dict(zip(ids, pool.map(login, ids)))

    print(f"  {args.players} players: enqueueing", file=sys.stderr)
    started = time.time()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(enqueue, player_id, tokens[player_id], f"{base}-idem-{player_id}")
                   for player_id in ids]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    elapsed = time.time() - started

    accepted = [r for r in results if r[0] in (200, 201)]
    shed = [r for r in results if r[0] == 503]
    other = [r for r in results if r[0] not in (200, 201, 503)]

    first_shed = shed[0] if shed else (None, {}, {})
    payload = first_shed[2] or {}
    summary = {
        "label": base,
        "players": args.players,
        "elapsed_seconds": round(elapsed, 2),
        "accepted": len(accepted),
        "shed": len(shed),
        "other": len(other),
        "statuses": _histogram([r[0] for r in results]),
        "other_examples": [{"status": r[0], "body": str(r[2])[:200]} for r in other[:3]],
        "capacity": payload.get("capacity"),
        "queue": payload.get("queue"),
        "depth": payload.get("depth"),
        "retry_after": first_shed[1].get("Retry-After"),
        "shed_body": {k: payload.get(k) for k in ("error", "queue", "depth", "capacity", "retryAfterSeconds")},
    }

    # An accepted ticket's replay must still answer with the ticket, even with the queue full: the
    # player did enqueue, and refusing the retry would lose a ticket that exists.
    if accepted:
        player_id = ids[results.index(accepted[0])]
        status, _, body = enqueue(player_id, tokens[player_id], f"{base}-idem-{player_id}")
        summary["replay_status"] = status
        summary["replay_ticket"] = body.get("ticketId")

    print(json.dumps(summary))
    if not shed:
        print("  nothing was shed: the queue cap was never reached", file=sys.stderr)
        return 1
    return 0


def _histogram(values):
    counts = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return counts


if __name__ == "__main__":
    sys.exit(main())
