#!/usr/bin/env python3
"""Helpers for requests/resilience/verify_degradation.sh (W18, ADR 0024).

The scenario drives the stack through each dependency failure and then asks the state machine
what state it left behind. Every question the shell needs to ask lives here, so the shell script
stays a list of assertions instead of a list of JSON parsers.

Subcommands (each prints one line of JSON, or a bare value the shell can compare):

  enqueue --count N --prefix p      POST /v1/matchmaking/tickets N times, printing the statuses,
                                    their latencies and the bodies (for the refusal shape)
  drive [--confirm]                 log in two compatible players, enqueue them, wait for the
                                    pending match, publish both confirmations, print the match id,
                                    the ticket ids and their states
  ticket-status TICKET_ID           the ticket's state, or MISSING
  pending-status MATCH_ID           the pending match's state (PRESENT/MISSING)
  outbox-status MATCH_ID            {"PENDING": n, "PUBLISHED": n, "PARKED": n} for the match
  row-count MATCH_ID                rows in `matches` for the match id
  history-status PLAYER_ID          the HTTP status of GET /v1/players/{id}/matches
  readiness                         the HTTP status and body of GET /health/ready
  dependents REDIS|POSTGRES         stop/start/restart a dependency (used by the shell)
"""

import argparse
import json
import statistics
import sys
import time

sys.path.insert(0, "requests/load")
sys.path.insert(0, "requests/backpressure")

import run_load  # noqa: E402
from db_backpressure import pending_matches  # noqa: E402


def login(player_id):
    _, body = run_load.call("POST", run_load.AUTH + "/v1/auth/login",
                            body={"playerId": player_id, "displayName": player_id})
    return body["token"]


def enqueue(token, game_mode="1v1", region="NA", max_latency_ms=80, idempotency_key=None):
    headers = {}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    started = time.time()
    status, body = run_load.call("POST", run_load.API + "/v1/matchmaking/tickets", token=token,
                                 body={"gameMode": game_mode, "region": region,
                                       "maxLatencyMs": max_latency_ms}, headers=headers)
    return status, body, (time.time() - started) * 1000.0


def cmd_enqueue(args):
    token = login(args.player)
    statuses, latencies, bodies = {}, [], {}
    for i in range(args.count):
        status, body, latency = enqueue(token, idempotency_key=f"{args.prefix}-{i}")
        statuses[str(status)] = statuses.get(str(status), 0) + 1
        latencies.append(latency)
        bodies.setdefault(str(status), body)
    print(json.dumps({
        "statuses": statuses,
        "p50Ms": round(statistics.median(latencies), 1),
        "p99Ms": round(sorted(latencies)[max(0, int(len(latencies) * 0.99) - 1)], 1),
        "maxMs": round(max(latencies), 1),
        "bodies": {status: bodies[status] for status in sorted(bodies)},
    }))


def cmd_drive(args):
    _, ids = run_load.players_with_one_skill(2)
    tickets = {}
    for player_id in ids:
        token = login(player_id)
        status, body, _ = enqueue(token, idempotency_key=f"w18-drive-{player_id}-{time.time_ns()}")
        if status != 201:
            print(json.dumps({"error": f"enqueue {player_id} -> {status}", "body": body}))
            sys.exit(3)
        tickets[player_id] = body["ticketId"]

    deadline = time.time() + args.wait
    matches = {}
    while time.time() < deadline and not matches:
        matches = pending_matches(set(ids))
        if not matches:
            time.sleep(0.3)
    if not matches:
        print(json.dumps({"error": "no pending match formed", "tickets": tickets}))
        sys.exit(4)
    match_id = next(iter(matches))

    if args.confirm:
        publisher = run_load.ConfirmationPublisher("match_confirmations", retries=1)
        for player_id in ids:
            publisher.confirm(match_id, player_id)
        publisher.close()

    states = {player_id: ticket_status(ticket_id) for player_id, ticket_id in tickets.items()}
    print(json.dumps({"matchId": match_id, "players": ids, "tickets": tickets, "states": states,
                      "confirmed": bool(args.confirm)}))


def ticket_status(ticket_id):
    raw = run_load.redis("get", f"ticket:{ticket_id}")
    if not raw:
        return "MISSING"
    try:
        return json.loads(raw).get("status", "UNKNOWN")
    except ValueError:
        return "UNPARSEABLE"


def pending_present(match_id):
    return bool(run_load.redis("exists", f"match-pending:{match_id}") == "1")


def cmd_ticket_status(args):
    print(ticket_status(args.ticket_id))


def cmd_pending_status(args):
    print("PRESENT" if pending_present(args.match_id) else "MISSING")


def cmd_outbox_status(args):
    rows = run_load.postgres(
        "SELECT status, count(*) FROM outbox_events WHERE partition_key = '%s' GROUP BY status;"
        % args.match_id)
    counts = {}
    for line in rows.splitlines():
        if "|" in line:
            status, count = line.split("|")
            counts[status] = int(count)
    print(json.dumps(counts))


def cmd_row_count(args):
    rows = run_load.postgres("SELECT count(*) FROM matches WHERE id = '%s';" % args.match_id)
    print(rows or "0")


def cmd_confirm(args):
    publisher = run_load.ConfirmationPublisher("match_confirmations", retries=1)
    for player_id in args.players:
        publisher.confirm(args.match_id, player_id)
    publisher.close()
    print(json.dumps({"matchId": args.match_id, "players": list(args.players),
                      "published": publisher.sent}))


def cmd_history_status(args):
    token = login(args.player)
    status, body = run_load.call("GET", run_load.API + f"/v1/players/{args.player}/matches?limit=5",
                                 token=token)
    print(json.dumps({"status": status, "body": body}))


def cmd_ticket_http_status(args):
    token = login(args.player)
    status, _ = run_load.call("GET", run_load.API + f"/v1/matchmaking/tickets/{args.ticket_id}",
                              token=token)
    print(status)


def cmd_readiness(args):
    status, body = run_load.call("GET", run_load.API + "/health/ready")
    print(json.dumps({"status": status, "body": body}))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("enqueue")
    p.add_argument("--count", type=int, default=1)
    p.add_argument("--prefix", default="w18")
    p.add_argument("--player", default="w18-degraded-enqueue")
    p.set_defaults(func=cmd_enqueue)

    p = sub.add_parser("drive")
    p.add_argument("--confirm", action="store_true")
    p.add_argument("--wait", type=float, default=30)
    p.set_defaults(func=cmd_drive)

    p = sub.add_parser("ticket-status")
    p.add_argument("ticket_id")
    p.set_defaults(func=cmd_ticket_status)

    p = sub.add_parser("pending-status")
    p.add_argument("match_id")
    p.set_defaults(func=cmd_pending_status)

    p = sub.add_parser("outbox-status")
    p.add_argument("match_id")
    p.set_defaults(func=cmd_outbox_status)

    p = sub.add_parser("row-count")
    p.add_argument("match_id")
    p.set_defaults(func=cmd_row_count)

    p = sub.add_parser("history-status")
    p.add_argument("player")
    p.set_defaults(func=cmd_history_status)

    p = sub.add_parser("confirm")
    p.add_argument("match_id")
    p.add_argument("players", nargs="+")
    p.set_defaults(func=cmd_confirm)

    p = sub.add_parser("ticket-http-status")
    p.add_argument("ticket_id")
    p.add_argument("--player", default="w18-probe")
    p.set_defaults(func=cmd_ticket_http_status)

    p = sub.add_parser("readiness")
    p.set_defaults(func=cmd_readiness)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
