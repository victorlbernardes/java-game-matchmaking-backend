#!/usr/bin/env python3
"""The W22 client: put players in the queue, answer the confirmation, report what happened.

The scenario needs to separate two moments that every earlier client ran together: the match being
**formed** and the match being **confirmed**. Capacity sits between them - the engine decides whether
forming is worth doing, and the orchestrator decides where the confirmed match runs - so the probe
has to be able to stop at each one.

It also talks to Redis over a plain socket (the same way a game client does): the push comes from
`player_notifications` and the answer goes to `match_confirmations`. A client whose own plumbing
drops frames cannot measure a delivery, which is the lesson the W16 load harness paid for.

Commands (each prints one JSON object):
  players  --count N                        ids that share one skill, so they can pair
  enqueue  --api URL --token T --player P --region R --redis-port N [--wait S]
  confirm  --redis-port N --match-id M --player P
  play     --api URL --token T --player P --region R --redis-port N [--wait S]
  ticket   --api URL --token T --ticket-id I
"""

import argparse
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request

NOTIFICATIONS_CHANNEL = "player_notifications"
CONFIRMATION_CHANNEL = "match_confirmations"
DEFAULT_WAIT_SECONDS = 20


# --------------------------------------------------------------------------- pairing helpers
def skill(player_id):
    """The engine's deterministic skill for a player id (the same function the E2E uses)."""
    h = 0
    for c in player_id:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    if h >= 2 ** 31:
        h -= 2 ** 32
    return abs(h) % 101


def players_with_one_skill(count, prefix):
    """`count` ids that share one skill, so the widening policy can pair them immediately."""
    scan = max(20000, 300 * count)
    buckets = {}
    for i in range(1, scan + 1):
        pid = f"{prefix}{i}"
        buckets.setdefault(skill(pid), []).append(pid)
    skill_value, ids = max(buckets.items(), key=lambda item: len(item[1]))
    if len(ids) < count:
        raise RuntimeError(f"only {len(ids)} ids share skill {skill_value} out of {scan} scanned")
    return skill_value, ids[:count]


# --------------------------------------------------------------------------- HTTP
def http_json(method, url, token=None, body=None, timeout=15):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    if token:
        request.add_header("authorization", f"Bearer {token}")
    if data is not None:
        request.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as failure:
        raw = failure.read() or b"{}"
        try:
            return failure.code, json.loads(raw)
        except json.JSONDecodeError:
            return failure.code, {"raw": raw.decode("utf-8", "replace")}


# --------------------------------------------------------------------------- RESP (plain socket)
def encode_command(*args):
    out = bytearray(f"*{len(args)}\r\n".encode())
    for arg in args:
        payload = arg.encode() if isinstance(arg, str) else arg
        out += f"${len(payload)}\r\n".encode() + payload + b"\r\n"
    return bytes(out)


def read_resp(stream):
    prefix = stream.read(1)
    if not prefix:
        raise EOFError("redis closed the connection")
    line = stream.readline().rstrip(b"\r\n")
    if prefix == b"*":
        return [read_resp(stream) for _ in range(int(line))]
    if prefix == b"$":
        size = int(line)
        if size < 0:
            return None
        return stream.read(size + 2)[:-2].decode("utf-8", "replace")
    return line.decode("utf-8", "replace")


class Subscriber:
    """The client's push side: one connection subscribed to the player notifications channel."""

    def __init__(self, port, channel=NOTIFICATIONS_CHANNEL):
        self.port = port
        self.channel = channel
        self.matches = {}
        self.stop = threading.Event()
        self.sock = socket.create_connection(("localhost", port), timeout=10)
        self.stream = self.sock.makefile("rb")
        self.sock.sendall(encode_command("SUBSCRIBE", channel))

    def run(self):
        try:
            while not self.stop.is_set():
                message = read_resp(self.stream)
                if not message or message[0] != "message":
                    continue
                self._on_frame(message[2])
        except (OSError, EOFError, ValueError):
            pass

    def _on_frame(self, text):
        try:
            routed = json.loads(text)
        except (TypeError, json.JSONDecodeError):
            return
        envelope = routed.get("envelope") or {}
        if envelope.get("type") != "MATCH_PENDING_CONFIRMATION":
            return
        payload = envelope.get("payload") or {}
        player, match_id = routed.get("targetPlayerId"), payload.get("matchId")
        if player and match_id:
            self.matches[player] = match_id

    def match_for(self, player):
        return self.matches.get(player)

    def close(self):
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


class Publisher:
    """The client's answer side: one connection, one PUBLISH at a time."""

    def __init__(self, port, channel=CONFIRMATION_CHANNEL):
        self.port = port
        self.channel = channel
        self.sock = socket.create_connection(("localhost", port), timeout=10)
        self.stream = self.sock.makefile("rb")

    def confirm(self, match_id, player_id):
        payload = json.dumps({"matchId": match_id, "playerId": player_id, "action": "CONFIRM"},
                             separators=(",", ":"))
        self.sock.sendall(encode_command("PUBLISH", self.channel, payload))
        read_resp(self.stream)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


# --------------------------------------------------------------------------- the flows
def enqueue(api, token, player, region, redis_port, wait_seconds):
    """Enqueue and wait for the match announcement, without answering it."""
    subscriber = Subscriber(redis_port)
    thread = threading.Thread(target=subscriber.run, daemon=True)
    thread.start()
    start = time.monotonic()
    try:
        status, body = http_json("POST", f"{api}/v1/matchmaking/tickets", token,
                                 {"gameMode": "1v1", "region": region, "maxLatencyMs": 150})
        result = {"player": player, "status": status, "body": body}
        if status != 201:
            return result
        result["ticketId"] = body.get("ticketId")
        result["queuedStatus"] = body.get("status")
        match_id = None
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline:
            match_id = subscriber.match_for(player)
            if match_id:
                break
            time.sleep(0.01)
        result["matchId"] = match_id
        result["matched"] = match_id is not None
        result["waitedMs"] = round((time.monotonic() - start) * 1000, 1)
        return result
    finally:
        subscriber.close()


def ticket(api, token, ticket_id):
    status, body = http_json("GET", f"{api}/v1/matchmaking/tickets/{ticket_id}", token)
    return {"status": status, "ticketStatus": body.get("status"), "body": body}


def confirm(redis_port, match_id, player):
    publisher = Publisher(redis_port)
    try:
        publisher.confirm(match_id, player)
        return {"matchId": match_id, "player": player, "published": True}
    finally:
        publisher.close()


def play(api, token, player, region, redis_port, wait_seconds):
    """Enqueue, answer the confirmation the moment it arrives, and report the wait."""
    start = time.monotonic()
    queued = enqueue(api, token, player, region, redis_port, wait_seconds)
    if not queued.get("matchId"):
        return queued
    ticket_id = queued["ticketId"]
    publisher = Publisher(redis_port)
    try:
        publisher.confirm(queued["matchId"], player)
    finally:
        publisher.close()

    confirmed_at = None
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        code, current = http_json("GET", f"{api}/v1/matchmaking/tickets/{ticket_id}", token)
        if code == 200 and current.get("status") == "CONFIRMED":
            confirmed_at = time.monotonic()
            break
        time.sleep(0.02)
    queued["confirmed"] = confirmed_at is not None
    queued["waitMs"] = round((confirmed_at - start) * 1000, 1) if confirmed_at else None
    return queued


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    players = sub.add_parser("players")
    players.add_argument("--count", type=int, default=2)
    players.add_argument("--prefix", default="gs-")

    for name in ("enqueue", "play"):
        command = sub.add_parser(name)
        command.add_argument("--api", required=True)
        command.add_argument("--token", required=True)
        command.add_argument("--player", required=True)
        command.add_argument("--region", default="NA")
        command.add_argument("--redis-port", type=int, required=True)
        command.add_argument("--wait", type=int, default=DEFAULT_WAIT_SECONDS)

    confirm_command = sub.add_parser("confirm")
    confirm_command.add_argument("--redis-port", type=int, required=True)
    confirm_command.add_argument("--match-id", required=True)
    confirm_command.add_argument("--player", required=True)

    ticket_command = sub.add_parser("ticket")
    ticket_command.add_argument("--api", required=True)
    ticket_command.add_argument("--token", required=True)
    ticket_command.add_argument("--ticket-id", required=True)

    args = parser.parse_args()
    if args.command == "players":
        value, ids = players_with_one_skill(args.count, args.prefix)
        print(json.dumps({"skill": value, "players": ids}))
    elif args.command == "enqueue":
        print(json.dumps(enqueue(args.api, args.token, args.player, args.region, args.redis_port, args.wait)))
    elif args.command == "play":
        print(json.dumps(play(args.api, args.token, args.player, args.region, args.redis_port, args.wait)))
    elif args.command == "confirm":
        print(json.dumps(confirm(args.redis_port, args.match_id, args.player)))
    else:
        print(json.dumps(ticket(args.api, args.token, args.ticket_id)))


if __name__ == "__main__":
    main()
