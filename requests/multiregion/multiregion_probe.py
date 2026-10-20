#!/usr/bin/env python3
"""The W21 client: probe a region, enqueue, answer the confirmation, report what it measured.

Every number the sprint claims is produced here, from the client's side, against the real services:

  * the rtt the **server** measured for this player in that region (`GET ping` + `POST pong`);
  * the ticket's wait, from enqueue to the ticket being `CONFIRMED`;
  * the region the match was formed in, as the ticket reports it.

It talks to a region's Redis over a plain socket for the push (`player_notifications`) and the
answer (`match_confirmations`), the same way a game client does - and for the same reason the W16
load harness stopped using `docker exec redis-cli`: a client whose own plumbing drops frames cannot
measure a delivery objective.

Commands (each prints one JSON object):
  players   --count N                      player ids that share a skill, so they can pair
  measure   --api URL --token T --player P --region R
  directory --api URL --token T
  ticket    --api URL --token T --region R --budget B
  play      --api URL --token T --player P --region R --budget B --redis-port N

Usage (the shell scenario drives this, e.g.):
  python3 multiregion_probe.py play --api http://localhost:8097 --token "$T" \\
      --player mr-42 --region EU --budget 150 --redis-port 6381
"""

import argparse
import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request

NOTIFICATIONS_CHANNEL = "player_notifications"
CONFIRMATION_CHANNEL = "match_confirmations"
WAIT_FOR_MATCH_SECONDS = 25
WAIT_FOR_CONFIRM_SECONDS = 25


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
    """The client's push side: one connection subscribed to the region's notifications channel."""

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
def measure(api, token, player, region, attempts=1):
    """Two requests, timed by the server: the probe the placement is later decided on.

    `attempts` repeats the probe and reports the best sample, because a single round trip on a
    machine that is also running four JVMs measures the machine as much as the path. The server
    stores the minimum too, so the client and the profile agree on which sample counts.
    """
    samples = []
    last = {}
    for _ in range(max(1, attempts)):
        ping_status, ping = http_json("GET", f"{api}/v1/regions/{region}/ping", token)
        if ping_status != 201:
            return {"pingStatus": ping_status, "pongStatus": None, "rttMs": None, "samples": samples,
                    "body": ping}
        pong_status, pong = http_json("POST", f"{api}/v1/regions/{region}/pong", token,
                                      {"nonce": ping.get("nonce")})
        last = {"pingStatus": ping_status, "pongStatus": pong_status, "body": pong,
                "nonce": ping.get("nonce")}
        if pong.get("rttMs") is not None:
            samples.append(pong["rttMs"])
    last["samples"] = samples
    last["rttMs"] = min(samples) if samples else None
    return last


def ticket(api, token, region, budget):
    status, body = http_json("POST", f"{api}/v1/matchmaking/tickets", token,
                             {"gameMode": "1v1", "region": region, "maxLatencyMs": budget})
    return {"status": status, "body": body}


def play(api, token, player, region, budget, redis_port, do_probe=True):
    """Probe (optionally), enqueue, answer the confirmation, and report the timings."""
    start = time.monotonic()
    rtt_ms = None
    if do_probe:
        probed = measure(api, token, player, region)
        if probed["rttMs"] is None:
            return {"player": player, "probeStatus": probed.get("pingStatus"),
                    "probe": probed.get("body"), "status": probed.get("pingStatus"),
                    "body": probed.get("body")}
        rtt_ms = probed["rttMs"]

    # Subscribe before enqueuing: a match can be announced before this process gets to wait.
    subscriber = Subscriber(redis_port)
    thread = threading.Thread(target=subscriber.run, daemon=True)
    thread.start()
    try:
        status, body = http_json("POST", f"{api}/v1/matchmaking/tickets", token,
                                 {"gameMode": "1v1", "region": region, "maxLatencyMs": budget})
        result = {"player": player, "status": status, "body": body, "rttMs": rtt_ms}
        if status != 201:
            return result
        ticket_id = body.get("ticketId")
        result["ticketId"] = ticket_id
        result["ticketRegion"] = body.get("region")

        match_id = None
        deadline = time.monotonic() + WAIT_FOR_MATCH_SECONDS
        while time.monotonic() < deadline:
            match_id = subscriber.match_for(player)
            if match_id:
                break
            time.sleep(0.01)
        result["matchId"] = match_id
        result["enqueuedMs"] = round((time.monotonic() - start) * 1000, 1)
        if not match_id:
            return result

        publisher = Publisher(redis_port)
        try:
            publisher.confirm(match_id, player)
        finally:
            publisher.close()

        confirmed_at = None
        deadline = time.monotonic() + WAIT_FOR_CONFIRM_SECONDS
        while time.monotonic() < deadline:
            code, current = http_json("GET", f"{api}/v1/matchmaking/tickets/{ticket_id}", token)
            if code == 200 and current.get("status") == "CONFIRMED":
                confirmed_at = time.monotonic()
                break
            time.sleep(0.02)
        result["confirmed"] = confirmed_at is not None
        result["waitMs"] = round((confirmed_at - start) * 1000, 1) if confirmed_at else None
        return result
    finally:
        subscriber.close()


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    players = sub.add_parser("players")
    players.add_argument("--count", type=int, default=2)
    players.add_argument("--prefix", default="mr-")

    for name in ("measure", "directory", "ticket", "play"):
        command = sub.add_parser(name)
        command.add_argument("--api", required=True)
        command.add_argument("--token", required=True)
        if name == "measure":
            command.add_argument("--attempts", type=int, default=1)
        if name in ("measure", "play"):
            command.add_argument("--player", required=True)
            command.add_argument("--redis-port", type=int, required=(name == "play"))
            command.add_argument("--no-probe", action="store_true")
        if name in ("measure", "ticket", "play"):
            command.add_argument("--region", required=True)
            command.add_argument("--budget", type=int, default=150)

    args = parser.parse_args()
    if args.command == "players":
        value, ids = players_with_one_skill(args.count, args.prefix)
        print(json.dumps({"skill": value, "players": ids}))
    elif args.command == "measure":
        print(json.dumps(measure(args.api, args.token, args.player, args.region, args.attempts)))
    elif args.command == "directory":
        status, body = http_json("GET", f"{args.api}/v1/regions", args.token)
        print(json.dumps({"status": status, "body": body}))
    elif args.command == "ticket":
        print(json.dumps(ticket(args.api, args.token, args.region, args.budget)))
    else:
        print(json.dumps(play(args.api, args.token, args.player, args.region, args.budget,
                              args.redis_port, do_probe=not args.no_probe)))


if __name__ == "__main__":
    main()
