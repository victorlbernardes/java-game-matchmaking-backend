#!/usr/bin/env python3
"""W16 load test: where is the ceiling, and what does the system do at it.

Drives real players through the running stack (auth -> API -> engine -> Redis/Postgres/Kafka),
with an auto-confirmer standing in for the players' clients (the engine's input contract for a
confirmation is a `match_confirmations` message, the same one the W12 E2E publishes), and reports
the numbers the sprint is about: enqueue throughput and latency percentiles, the pairing rate
(players out of the queue per second - the ceiling), the wait percentiles the players actually
felt, the confirmation rate, and the cost in CPU, Redis operations and Postgres writes.

It also checks the invariant the load is most likely to break: one match, one event. The rows of
this run are counted in Postgres (matches vs distinct partition_key in outbox_events) and a
duplicate exits non-zero, because a second MATCH_CONFIRMED for the same match is a second fact
for every consumer downstream.

Usage:
    python3 requests/load/run_load.py --players 400 --concurrency 64 --label before
    python3 requests/load/run_load.py --players 400 --concurrency 64 --label after

Writes requests/load/results-<label>.json and prints a table. Preconditions: the stack is up
(see requests/README.md), the engine runs with METRICS_PORT=9091, and its build matches the code
under test (`mvn -B install -DskipTests` before starting it).
"""
import argparse
import json
import math
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
API = os.environ.get("API", "http://localhost:8080")
AUTH = os.environ.get("AUTH", "http://localhost:8081")
ENGINE_METRICS = os.environ.get("ENGINE_METRICS", "http://localhost:9091/metrics")
REDIS_CONTAINER = os.environ.get("REDIS_CONTAINER", "game-matchmaking-backend-redis-1")
POSTGRES_CONTAINER = os.environ.get("POSTGRES_CONTAINER", "game-matchmaking-backend-postgres-1")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
QUEUE_ZCARD = "queue:1v1:NA"
NOTIFICATIONS_CHANNEL = "player_notifications"
CONFIRMATION_CHANNEL = "match_confirmations"


# --------------------------------------------------------------------------- helpers
def skill(player_id):
    """The engine's deterministic skill for a player id (same function the E2E uses)."""
    h = 0
    for c in player_id:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    if h >= 2 ** 31:
        h -= 2 ** 32
    return abs(h) % 101


def players_with_one_skill(count):
    """`count` player ids that all share one skill, so the pairing policy can always pair them.

    The skill is the engine's deterministic function of the player id, so ids are scanned and
    bucketed: the biggest bucket wins, which keeps the scan bounded whatever the spread.
    """
    scan = max(20000, 300 * count)
    buckets = {}
    for i in range(1, scan + 1):
        pid = f"load-{i}"
        buckets.setdefault(skill(pid), []).append(pid)
    skill_value, ids = max(buckets.items(), key=lambda item: len(item[1]))
    if len(ids) < count:
        raise RuntimeError(f"only {len(ids)} ids share skill {skill_value} out of {scan} scanned")
    return skill_value, ids[:count]


def redis(*args):
    out = subprocess.run(["docker", "exec", REDIS_CONTAINER, "redis-cli", *args],
                         capture_output=True, text=True, check=False)
    return out.stdout.strip()


def postgres(sql):
    out = subprocess.run(["docker", "exec", POSTGRES_CONTAINER, "psql", "-U", "matchmaking",
                          "-d", "matchmaking", "-At", "-c", sql],
                         capture_output=True, text=True, check=False)
    return out.stdout.strip()


def count_sql(sql):
    raw = postgres(sql)
    return int(raw) if raw.isdigit() else 0


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


def scrape(url):
    """Parses an exposition into {metric: [({labels}, value)]}."""
    metrics = {}
    try:
        body = urllib.request.urlopen(url, timeout=5).read().decode()
    except Exception as e:  # noqa: BLE001 - a missing scrape must not kill the run
        print(f"  ! could not scrape {url}: {e}")
        return metrics
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        head, _, value = line.rpartition(" ")
        name, _, labels_raw = head.partition("{")
        labels = {}
        if labels_raw:
            for pair in labels_raw.rstrip("}").split(","):
                k, _, v = pair.partition("=")
                labels[k.strip()] = v.strip().strip('"')
        try:
            metrics.setdefault(name, []).append((labels, float(value)))
        except ValueError:
            continue
    return metrics


def total(metrics, name, **label_filter):
    """Sums every series of a metric that matches the label filter; missing = 0."""
    value = 0.0
    for labels, v in metrics.get(name, []):
        if all(labels.get(k) == val for k, val in label_filter.items()):
            value += v
    return value


def bucket_counts(metrics, name):
    """[(le, cumulative count)] of one histogram, with +Inf last."""
    buckets = []
    for labels, count in metrics.get(name + "_bucket", []):
        le = labels.get("le")
        if le is None:
            continue
        buckets.append((math.inf if le == "+Inf" else float(le), count))
    buckets.sort()
    return buckets


def histogram_quantile_delta(before, after, name, q):
    """The quantile of what happened *during* a run, not of everything since boot.

    The scrape is cumulative, so the plain quantile mixes this run with every earlier one - and
    the tail of `matchmaking_match_wait_seconds` is exactly where a previous run's dissolved
    matches sit (a requeued ticket waits its 30 s window again). Subtracting the buckets first
    makes the number describe the run, which is what an objective is calibrated on.
    """
    after_buckets = dict(bucket_counts(after, name))
    before_buckets = dict(bucket_counts(before, name))
    delta = [(le, count - before_buckets.get(le, 0.0)) for le, count in after_buckets.items()]
    return quantile_from_buckets(sorted(delta), q)


def quantile_from_buckets(buckets, q):
    """The Prometheus algorithm: interpolate inside the bucket the rank falls in."""
    if not buckets:
        return None
    total_count = buckets[-1][1]
    if total_count <= 0:
        return None
    rank = q * total_count
    prev_le, prev_count = 0.0, 0.0
    for le, count in buckets:
        if count >= rank:
            if math.isinf(le) or count == prev_count:
                return le if not math.isinf(le) else prev_le
            return prev_le + (le - prev_le) * (rank - prev_count) / (count - prev_count)
        prev_le, prev_count = le, count
    return prev_le


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))
    return ordered[idx]


def observed_rate(samples, field, decreasing=True):
    """How fast `field` moved across the sampling window (works on a partial drain too).

    Depth goes down as the queue drains; a counter goes up as the work happens. The direction is
    the caller's to state, or a partial window reports a negative rate.
    """
    if len(samples) < 2:
        return None
    first, last = samples[0], samples[-1]
    elapsed = last["t"] - first["t"]
    if elapsed <= 0 or first[field] is None or last[field] is None:
        return None
    moved = (first[field] - last[field]) if decreasing else (last[field] - first[field])
    return moved / elapsed


def parse_notification(line):
    """(matchId, playerId) from a `player_notifications` frame, or None for anything else.

    The engine announces the match to each player on the shared channel, wrapped in a
    RoutedEnvelope: {"targetPlayerId": ..., "envelope": {"type": ..., "payload": {...}}}.
    """
    line = line.strip()
    if not line.startswith("{"):
        return None
    try:
        routed = json.loads(line)
    except json.JSONDecodeError:
        return None
    envelope = routed.get("envelope") or {}
    if envelope.get("type") != "MATCH_PENDING_CONFIRMATION":
        return None
    payload = envelope.get("payload") or {}
    match_id, player_id = payload.get("matchId"), routed.get("targetPlayerId")
    if not match_id or not player_id:
        return None
    return match_id, player_id


# ------------------------------------------------------------------- raw Redis clients
#
# The generator talks to Redis over a plain socket, not through `docker exec redis-cli`.
# Measured why (W16, 2000 players): with the exec-based subscriber, 24 of 1024 matches were
# announced on `player_notifications` but never reached the client, each one dissolving at the
# 30 s confirmation deadline. The engine had published them; the hop in front of the client was
# the lossy part - a `docker exec -i` attach stream per direction, plus the round trip through
# the Docker daemon for every frame. A load generator whose own plumbing drops 2% of the input
# cannot measure a 99% confirm objective, so the client here is a socket and a RESP parser.
def encode_command(*args):
    """One command as a RESP array of bulk strings - length-prefixed, so no quoting rules."""
    out = bytearray(f"*{len(args)}\r\n".encode())
    for arg in args:
        payload = arg.encode() if isinstance(arg, str) else arg
        out += f"${len(payload)}\r\n".encode() + payload + b"\r\n"
    return bytes(out)


def read_resp(stream):
    """Reads one RESP value: simple string, bulk string, integer, error or array."""
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


class NotificationSubscriber:
    """The client's push side: one long-lived connection subscribed to `player_notifications`."""

    def __init__(self, channel, on_message):
        self.channel = channel
        self.on_message = on_message
        self.stop = threading.Event()
        self.received = 0
        self.closed = False
        self.sock = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=10)
        self.stream = self.sock.makefile("rb")
        self.sock.sendall(encode_command("SUBSCRIBE", channel))

    def run(self):
        try:
            while not self.stop.is_set():
                message = read_resp(self.stream)
                # The subscribe ack arrives first; only `message` arrays are frames.
                if not message or message[0] != "message":
                    continue
                self.received += 1
                self.on_message(message[2])
        except (OSError, EOFError, ValueError):
            pass
        finally:
            self.closed = True

    def close(self):
        self.stop.set()
        try:
            self.sock.close()
        except OSError:
            pass


class ConfirmationPublisher:
    """The client's answer side: one connection, one synchronous PUBLISH at a time."""

    def __init__(self, channel, retries):
        self.channel = channel
        self.retries = retries
        self.lock = threading.Lock()
        self.sent = 0
        self.sock = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=10)
        self.stream = self.sock.makefile("rb")

    def confirm(self, match_id, player_id):
        payload = json.dumps({"matchId": match_id, "playerId": player_id, "action": "CONFIRM"},
                             separators=(",", ":"))
        with self.lock:
            for _ in range(self.retries):
                self.sock.sendall(encode_command("PUBLISH", self.channel, payload))
                read_resp(self.stream)
                self.sent += 1

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass



# --------------------------------------------------------------------------- the run
class Run:
    def __init__(self, args, players):
        self.args = args
        self.players = players
        self.player_ids = set(players)
        self.tokens = {}
        self.tickets = {}
        self.enqueue_ms = []
        self.login_ms = []
        self.samples = []
        self.peak_players_per_second = 0.0
        self.confirmed_matches = set()
        self.confirm_messages = 0
        self.pending_received = 0
        self.confirm_stop = threading.Event()
        self.confirm_thread = None
        self.subscriber = None
        self.publisher = None
        self.errors = []

    # -- phase 1: tokens (the auth service is part of the path under test)
    def login_all(self):
        with ThreadPoolExecutor(max_workers=self.args.concurrency) as pool:
            list(pool.map(self._login, self.players))

    def _login(self, pid):
        started = time.perf_counter()
        status, body = call("POST", AUTH + "/v1/auth/login", body={"playerId": pid})
        elapsed = (time.perf_counter() - started) * 1000
        if status != 200:
            self.errors.append(f"login {pid}: {status} {body}")
            return
        self.tokens[pid] = body["token"]
        self.login_ms.append(elapsed)

    # -- phase 2: enqueue
    def enqueue_all(self):
        with ThreadPoolExecutor(max_workers=self.args.concurrency) as pool:
            list(pool.map(self._enqueue, self.players))

    def _enqueue(self, pid):
        started = time.perf_counter()
        status, body = call("POST", API + "/v1/matchmaking/tickets", self.tokens[pid],
                            {"gameMode": self.args.mode, "region": self.args.region,
                             "maxLatencyMs": 80})
        elapsed = (time.perf_counter() - started) * 1000
        if status != 201:
            self.errors.append(f"enqueue {pid}: {status} {body}")
            return
        self.tickets[pid] = body["ticketId"]
        self.enqueue_ms.append(elapsed)

    # -- the standing-in client: confirm a match as soon as the engine announces it
    #
    # Event-driven on purpose. The engine publishes MATCH_PENDING_CONFIRMATION on
    # `player_notifications` the moment the match exists, so the client reacts to that. Polling
    # the pending keys instead cost a `docker exec` spawn (and a keyspace scan) per cycle and
    # could not keep up with a drain of tens of matches per second: matches hit their 30 s window,
    # were dissolved and their players requeued, and the churn hid the number the run existed to
    # measure. The generator was the ceiling, not the engine.
    def start_confirmer(self):
        self.confirm_stop.clear()
        self.publisher = ConfirmationPublisher(CONFIRMATION_CHANNEL, self.args.confirm_retries)
        self.subscriber = NotificationSubscriber(NOTIFICATIONS_CHANNEL, self._on_notification)
        self.confirm_thread = threading.Thread(target=self.subscriber.run, daemon=True)
        self.confirm_thread.start()

    def _on_notification(self, frame):
        routed = parse_notification(frame)
        if routed is None:
            return
        self.pending_received += 1
        match_id, player_id = routed
        if player_id not in self.player_ids:
            return
        self.confirmed_matches.add(match_id)
        # A confirmation can be repeated on purpose (--confirm-retries): a client that did not
        # get an answer retries, and the engine must still announce the match once.
        self.publisher.confirm(match_id, player_id)
        self.confirm_messages = self.publisher.sent

    def stop_confirmer(self):
        self.confirm_stop.set()
        for client in (self.subscriber, self.publisher):
            if client is not None:
                client.close()
        if self.confirm_thread:
            self.confirm_thread.join(timeout=5)

    # -- phase 3: watch the queue drain, sampling what it costs
    def watch_drain(self, deadline_s):
        started = time.perf_counter()
        peak_depth = 0
        drain_s = None
        previous = None
        # The peak rate, not the window average: the drain slows as the queue empties (a tick
        # pairs up to a batch, and a shallow queue gives it nothing to pair), so the average over
        # the whole window describes the tail, not the ceiling.
        self.peak_players_per_second = 0.0
        while time.perf_counter() - started < deadline_s:
            depth = int(redis("zcard", QUEUE_ZCARD) or 0)
            now = time.perf_counter() - started
            peak_depth = max(peak_depth, depth)
            if previous is not None and depth < previous[0]:
                elapsed = now - previous[1]
                if elapsed > 0:
                    self.peak_players_per_second = max(self.peak_players_per_second,
                                                       (previous[0] - depth) / elapsed)
            previous = (depth, now)
            engine = scrape(ENGINE_METRICS)
            self.samples.append({
                "t": round(now, 2),
                "depth": depth,
                "oldest_wait_s": self.head_wait_s(),
                "engine_cpu": total(engine, "process_cpu_usage"),
                "system_cpu": total(engine, "system_cpu_usage"),
                "heap_mb": total(engine, "jvm_memory_used_bytes", area="heap") / 1024 / 1024,
                "redis_ops": self.redis_ops(),
                "formed": total(engine, "matchmaking_matches_formed_total"),
                "confirmed": total(engine, "matchmaking_matches_confirmed_total"),
                "dissolved_timeout": total(engine, "matchmaking_matches_dissolved_total", reason="timeout"),
                "outbox_pending": total(engine, "matchmaking_outbox_backlog", status="PENDING"),
            })
            if depth == 0 and total(engine, "matchmaking_matches_formed_total") > 0:
                drain_s = time.perf_counter() - started
                break
            time.sleep(1)
        return peak_depth, drain_s

    def head_wait_s(self):
        """How long the oldest member of the queue has been waiting, read from the ZSET score."""
        parts = redis("zrange", QUEUE_ZCARD, "0", "0", "WITHSCORES").split()
        if len(parts) < 2:
            return 0.0
        try:
            return max(0.0, round(time.time() - float(parts[1]) / 1000.0, 1))
        except ValueError:
            return 0.0

    def redis_ops(self):
        info = redis("info", "stats")
        for line in info.splitlines():
            if line.startswith("instantaneous_ops_per_sec:"):
                return int(line.split(":")[1])
        return None

    def redis_memory_mb(self):
        info = redis("info", "memory")
        for line in info.splitlines():
            if line.startswith("used_memory_human:"):
                raw = line.split(":")[1].strip().replace("M", "").replace("K", "e-3").replace("B", "")
                try:
                    return round(float(raw), 2)
                except ValueError:
                    return None
        return None

    # -- phase 4: let the confirmations settle, then collect the final numbers
    def settle(self, deadline_s):
        started = time.perf_counter()
        while time.perf_counter() - started < deadline_s:
            pending = redis("--scan", "--pattern", "match-pending:*")
            if not pending.strip():
                return
            time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--players", type=int, default=400)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--label", default="run")
    parser.add_argument("--mode", default="1v1")
    parser.add_argument("--region", default="NA")
    parser.add_argument("--drain-timeout", type=float, default=400)
    parser.add_argument("--settle-timeout", type=float, default=60)
    parser.add_argument("--confirm-retries", type=int, default=1,
                        help="confirmations per player (a retrying client sends more than one)")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    run_started = time.perf_counter()

    print(f"== load test: {args.players} players, concurrency {args.concurrency}, "
          f"{args.mode}/{args.region}, label '{args.label}'")

    # A known start: queue empty. The counters are read as deltas, so the past does not mix in.
    redis("flushall")
    before_engine = scrape(ENGINE_METRICS)
    before_pg = postgres("SELECT xact_commit FROM pg_stat_database WHERE datname = 'matchmaking';")
    # The database keeps everything previous runs wrote, so the rows of THIS run are the ones
    # written after this instant.
    run_started_at = postgres("SELECT now();")

    target_skill, player_ids = players_with_one_skill(args.players)
    run = Run(args, player_ids)
    print(f"   players share skill {target_skill} (all pairable, none waiting on the policy)")

    started = time.perf_counter()
    run.login_all()
    login_s = time.perf_counter() - started
    print(f"1. logins: {len(run.tokens)}/{args.players} in {login_s:.1f}s "
          f"({len(run.tokens) / login_s:.0f}/s)")

    run.start_confirmer()
    started = time.perf_counter()
    run.enqueue_all()
    enqueue_s = time.perf_counter() - started
    print(f"2. enqueues: {len(run.tickets)}/{args.players} in {enqueue_s:.1f}s "
          f"({len(run.tickets) / enqueue_s:.0f}/s), client p50 "
          f"{percentile(run.enqueue_ms, 0.5):.0f}ms p95 {percentile(run.enqueue_ms, 0.95):.0f}ms "
          f"p99 {percentile(run.enqueue_ms, 0.99):.0f}ms")

    peak_depth, drain_s = run.watch_drain(args.drain_timeout)
    window_players_per_s = observed_rate(run.samples, "depth")
    window_matches_per_s = observed_rate(run.samples, "formed", decreasing=False)
    if drain_s is None:
        print(f"3. drain: DID NOT FINISH in {args.drain_timeout:.0f}s "
              f"(peak depth {peak_depth}, still {redis('zcard', QUEUE_ZCARD)} queued, "
              f"observed {window_players_per_s} players/s over the window)")
    else:
        print(f"3. drain: {peak_depth} players out of the queue in {drain_s:.1f}s "
              f"({peak_depth / drain_s:.0f} players/s, {peak_depth / 2 / drain_s:.1f} matches/s)")
    run.settle(args.settle_timeout)
    notifications_received = run.subscriber.received
    run.stop_confirmer()

    after_engine = scrape(ENGINE_METRICS)
    after_pg = postgres("SELECT xact_commit FROM pg_stat_database WHERE datname = 'matchmaking';")
    run_seconds = time.perf_counter() - run_started

    # One match, one event. A burst of confirmations used to write one event per confirmation
    # (W16, measured: 312 events for 204 matches), and Postgres is where that survives, so the
    # scenario counts its own rows instead of trusting the counters.
    matches_written = count_sql(
        f"SELECT count(*) FROM matches WHERE created_at >= '{run_started_at}';")
    events_written = count_sql(
        f"SELECT count(*) FROM outbox_events WHERE created_at >= '{run_started_at}';")
    events_distinct = count_sql(f"SELECT count(DISTINCT partition_key) FROM outbox_events "
                                f"WHERE created_at >= '{run_started_at}';")
    duplicate_events = max(0, events_written - events_distinct)

    def delta(metric, **labels):
        return total(after_engine, metric, **labels) - total(before_engine, metric, **labels)

    formed = delta("matchmaking_matches_formed_total")
    confirmed = delta("matchmaking_matches_confirmed_total")
    dissolved_timeout = delta("matchmaking_matches_dissolved_total", reason="timeout")
    enqueued_metric = delta("matchmaking_tickets_enqueued_total")
    wait_p50 = histogram_quantile_delta(before_engine, after_engine, "matchmaking_match_wait_seconds", 0.50)
    wait_p95 = histogram_quantile_delta(before_engine, after_engine, "matchmaking_match_wait_seconds", 0.95)
    wait_p99 = histogram_quantile_delta(before_engine, after_engine, "matchmaking_match_wait_seconds", 0.99)
    pg_tx = (int(after_pg) if after_pg.isdigit() else 0) - (int(before_pg) if before_pg.isdigit() else 0)
    drain_s = drain_s or 0.0

    result = {
        "label": args.label,
        "players": args.players,
        "concurrency": args.concurrency,
        "mode": args.mode,
        "region": args.region,
        "skill": target_skill,
        "logins": {"count": len(run.tokens), "seconds": round(login_s, 2),
                   "per_second": round(len(run.tokens) / login_s, 1) if login_s else None},
        "enqueue": {"count": len(run.tickets), "seconds": round(enqueue_s, 2),
                    "per_second": round(len(run.tickets) / enqueue_s, 1) if enqueue_s else None,
                    "p50_ms": round(percentile(run.enqueue_ms, 0.5), 1),
                    "p95_ms": round(percentile(run.enqueue_ms, 0.95), 1),
                    "p99_ms": round(percentile(run.enqueue_ms, 0.99), 1),
                    "max_ms": round(max(run.enqueue_ms), 1) if run.enqueue_ms else None},
        "drain": {"peak_depth": peak_depth, "seconds": round(drain_s, 2),
                  "completed": drain_s is not None,
                  "players_per_second": round(peak_depth / drain_s, 1) if drain_s else None,
                  "matches_per_second": round(peak_depth / 2 / drain_s, 2) if drain_s else None,
                  "peak_players_per_second": round(run.peak_players_per_second, 1),
                  "observed_players_per_second": round(window_players_per_s, 1)
                  if window_players_per_s is not None else None,
                  "observed_matches_per_second": round(window_matches_per_s, 2)
                  if window_matches_per_s is not None else None},
        "wait": {"p50_s": wait_p50, "p95_s": wait_p95, "p99_s": wait_p99,
                 "queued_players_per_second_effective": round(peak_depth / drain_s, 1) if drain_s else None},
        "matches": {"enqueued_metric": enqueued_metric, "formed": formed, "confirmed": confirmed,
                    "dissolved_timeout": dissolved_timeout,
                    "confirm_rate": round(confirmed / formed, 4) if formed else None,
                    "confirmed_by_client": len(run.confirmed_matches),
                    "notifications_received": notifications_received,
                    "pending_notifications_received": run.pending_received,
                    "confirmations_sent": run.confirm_messages},
        "resources": {
            "engine_cpu_peak": max((s["engine_cpu"] for s in run.samples), default=None),
            "system_cpu_peak": max((s["system_cpu"] for s in run.samples), default=None),
            "heap_used_peak_mb": round(max((s["heap_mb"] for s in run.samples), default=0), 1),
            "redis_ops_peak": max((s["redis_ops"] for s in run.samples if s["redis_ops"] is not None),
                                  default=None),
            "redis_memory_mb": run.redis_memory_mb(),
            "postgres_tx_in_run": pg_tx,
            "postgres_tx_per_second": round(pg_tx / run_seconds, 1) if run_seconds else None,
        },
        "outbox": {"publish_lag_s": total(after_engine, "matchmaking_outbox_publish_lag_seconds"),
                   "oldest_pending_s": total(after_engine, "matchmaking_outbox_oldest_pending_seconds")},
        "events": {"matches_rows": matches_written, "events": events_written,
                   "distinct_matches": events_distinct, "duplicate_events": duplicate_events},
        "errors": run.errors[:10],
        "samples": run.samples,
    }
    out_path = args.out or os.path.join(REPO_ROOT, "requests", "load", f"results-{args.label}.json")
    with open(out_path, "w") as handle:
        json.dump(result, handle, indent=2)

    print("\n== result")
    print(f"   enqueue        {result['enqueue']['per_second']} req/s  "
          f"p50 {result['enqueue']['p50_ms']}ms p95 {result['enqueue']['p95_ms']}ms "
          f"p99 {result['enqueue']['p99_ms']}ms")
    print(f"   pairing (ceiling) {result['drain']['peak_players_per_second']} players/s peak, "
          f"{result['drain']['players_per_second'] or result['drain']['observed_players_per_second']}"
          f" players/s over the window "
          f"({(result['drain']['matches_per_second'] or result['drain']['observed_matches_per_second'])}"
          f" matches/s), peak depth {peak_depth}, "
          f"{'drained in ' + str(result['drain']['seconds']) + 's' if result['drain']['completed'] else 'partial window'}")
    print(f"   wait felt      p50 {wait_p50}s  p95 {wait_p95}s  p99 {wait_p99}s")
    print(f"   matches        formed {formed:.0f}  confirmed {confirmed:.0f}  "
          f"dissolved(timeout) {dissolved_timeout:.0f}  confirm rate {result['matches']['confirm_rate']}")
    print(f"   client         {len(run.confirmed_matches)} matches seen, "
          f"{run.pending_received} pending frames of {notifications_received} received, "
          f"{run.confirm_messages} confirmations sent (retries {args.confirm_retries})")
    print(f"   resources      engine cpu peak {result['resources']['engine_cpu_peak']}  "
          f"system cpu peak {result['resources']['system_cpu_peak']}  "
          f"heap peak {result['resources']['heap_used_peak_mb']}MB  "
          f"redis {result['resources']['redis_ops_peak']} ops/s  "
          f"postgres {result['resources']['postgres_tx_per_second']} tx/s")
    print(f"   events         {matches_written} match rows, {events_written} events "
          f"({events_distinct} matches), duplicates {duplicate_events}")
    print(f"   (written to {out_path})")
    if run.errors:
        print(f"   !! {len(run.errors)} errors, first: {run.errors[0]}")
        return 1
    if duplicate_events:
        print(f"   !! {duplicate_events} duplicate events: one match must be announced once")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
