#!/usr/bin/env python3
"""W17 - a burst of confirmations against a deliberately slow Postgres (ADR 0022).

The property: the number of transactions the engine has in flight against Postgres is bounded, so
a database that is behind is not asked harder. To see it, the database has to actually be slow -
the shell scenario installs a `BEFORE INSERT ON matches` trigger that sleeps, and this helper
drives a real burst:

  1. logs in `--players` players that share one skill, so the pairing policy can pair them;
  2. enqueues them and waits for the engine to form `--min-matches` pending matches;
  3. publishes both confirmations of every match at once, over one RESP connection;
  4. while they are being written, samples `pg_stat_activity` for the sessions actually running
     the insert, and the engine's `matchmaking_db_writes_in_flight` gauge;
  5. waits for every match to land in Postgres.

Prints one JSON summary on stdout and progress on stderr. Exit code 1 if the matches never formed
or a confirmed match never landed - the two ways the run would be lying.

Usage:
  python3 requests/backpressure/db_backpressure.py --players 24 --min-matches 12
"""
import argparse
import concurrent.futures
import json
import os
import sys
import threading
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(REPO_ROOT, "requests", "load"))
import run_load  # noqa: E402 - path set above; the load harness owns the RESP client

REDIS_CONTAINER = os.environ.get("REDIS_CONTAINER", "game-matchmaking-backend-redis-1")
POSTGRES_CONTAINER = os.environ.get("POSTGRES_CONTAINER", "game-matchmaking-backend-postgres-1")
CONFIRMATION_CHANNEL = "match_confirmations"


def postgres(sql):
    return run_load.postgres(sql)


def active_inserts():
    """Sessions Postgres is running right now whose statement is the match insert.

    The ground truth of "how many transactions is the database being asked to hold": the gate
    claims at most N, and this counts what the database actually saw. The probe excludes itself -
    its own statement contains the text it searches for, and counting itself would make the
    measurement read one higher than reality.
    """
    raw = postgres("SELECT count(*) FROM pg_stat_activity "
                   "WHERE pid <> pg_backend_pid() AND state = 'active' "
                   "AND query ILIKE '%insert into matches%'")
    return int(raw) if raw.isdigit() else 0


def rows_present(match_ids):
    if not match_ids:
        return 0
    listed = ",".join("'" + match_id.replace("'", "") + "'" for match_id in match_ids)
    raw = postgres(f"SELECT count(*) FROM matches WHERE id IN ({listed})")
    return int(raw) if raw.isdigit() else 0


def clean_up(match_ids):
    """Removes this run's rows: the test data must not outlive the test.

    Order matters: `match_players` references `matches`, and the orchestrator's projection and the
    outbox are keyed by the match id. Deleting the matches first would be refused.
    """
    if not match_ids:
        return 0
    listed = ",".join("'" + match_id.replace("'", "") + "'" for match_id in match_ids)
    postgres(f"DELETE FROM match_players WHERE match_id IN ({listed})")
    postgres(f"DELETE FROM match_allocations WHERE match_id IN ({listed})")
    postgres(f"DELETE FROM outbox_events WHERE partition_key IN ({listed})")
    postgres(f"DELETE FROM matches WHERE id IN ({listed})")
    return rows_present(match_ids)


def pending_matches(ours):
    """The pending matches whose whole roster belongs to this run."""
    found = {}
    for key in run_load.redis("keys", "match-pending:*").splitlines():
        raw = run_load.redis("get", key)
        if not raw:
            continue
        try:
            pending = json.loads(raw)
        except ValueError:
            continue
        roster = [player["playerId"] for player in pending.get("players", [])]
        if roster and all(player_id in ours for player_id in roster):
            found[pending["matchId"]] = roster
    return found


class Sampler(threading.Thread):
    """Watches the database and the engine's gauge while the burst is being written."""

    def __init__(self, interval=0.2):
        super().__init__(daemon=True)
        self.interval = interval
        self.stop = threading.Event()
        self.max_active_inserts = 0
        self.max_in_flight = 0
        self.samples = 0

    def run(self):
        while not self.stop.is_set():
            try:
                self.max_active_inserts = max(self.max_active_inserts, active_inserts())
                metrics = run_load.scrape(os.environ.get("ENGINE_METRICS",
                                                         "http://localhost:9091/metrics"))
                in_flight = run_load.total(metrics, "matchmaking_db_writes_in_flight")
                self.max_in_flight = max(self.max_in_flight, int(in_flight))
                self.samples += 1
            except Exception:  # noqa: BLE001 - sampling must never break the run
                pass
            self.stop.wait(self.interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--players", type=int, default=24)
    parser.add_argument("--min-matches", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--label", default="w17db")
    parser.add_argument("--keep", action="store_true",
                        help="leave the rows in Postgres (they are deleted by default)")
    args = parser.parse_args()

    skill_value, ids = run_load.players_with_one_skill(args.players)
    ours = set(ids)
    base = f"{args.label}-{int(time.time())}"
    print(f"  {args.players} players share skill {skill_value}", file=sys.stderr)

    tokens = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        tokens = dict(zip(ids, pool.map(lambda pid: run_load.call(
            "POST", run_load.AUTH + "/v1/auth/login",
            body={"playerId": pid, "displayName": pid})[1].get("token"), ids)))

    def enqueue(player_id):
        status, body = run_load.call("POST", run_load.API + "/v1/matchmaking/tickets",
                                     token=tokens[player_id],
                                     body={"gameMode": "1v1", "region": "NA", "maxLatencyMs": 80})
        if status not in (200, 201):
            raise RuntimeError(f"enqueue of {player_id} answered {status}: {body}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(enqueue, ids))
    print(f"  {args.players} players enqueued", file=sys.stderr)

    deadline = time.time() + args.timeout
    matches = {}
    while time.time() < deadline and len(matches) < args.min_matches:
        matches = pending_matches(ours)
        if len(matches) < args.min_matches:
            time.sleep(0.2)
    print(f"  the engine formed {len(matches)} matches of the {args.players // 2} possible",
          file=sys.stderr)
    if len(matches) < args.min_matches:
        print(json.dumps({"error": "not enough matches", "matches": len(matches),
                          "wanted": args.min_matches}))
        return 1

    deferred_before = run_load.total(run_load.scrape(run_load.ENGINE_METRICS),
                                     "matchmaking_db_write_deferred_total")

    sampler = Sampler()
    sampler.start()

    publisher = run_load.ConfirmationPublisher(CONFIRMATION_CHANNEL, retries=1)
    started = time.time()
    try:
        for match_id, roster in matches.items():
            for player_id in roster:
                publisher.confirm(match_id, player_id)
    finally:
        publisher.close()
    sent = publisher.sent
    print(f"  {sent} confirmations published in {time.time() - started:.2f}s", file=sys.stderr)

    written = 0
    while time.time() < deadline and written < len(matches):
        written = rows_present(list(matches))
        if written < len(matches):
            time.sleep(0.2)
    elapsed = time.time() - started
    time.sleep(0.5)
    sampler.stop.set()
    sampler.join(timeout=5)

    deferred_after = run_load.total(run_load.scrape(run_load.ENGINE_METRICS),
                                    "matchmaking_db_write_deferred_total")

    summary = {
        "label": base,
        "players": args.players,
        "matches": len(matches),
        "confirmations": sent,
        "rows_written": written,
        "max_active_inserts": sampler.max_active_inserts,
        "max_db_writes_in_flight": sampler.max_in_flight,
        "samples": sampler.samples,
        "deferred_before": int(deferred_before),
        "deferred_after": int(deferred_after),
        "deferred_delta": int(deferred_after - deferred_before),
        "burst_seconds": round(elapsed, 2),
    }
    if args.keep:
        summary["rows_after_cleanup"] = written
    else:
        summary["rows_after_cleanup"] = clean_up(list(matches))
    print(json.dumps(summary))
    return 0 if written == len(matches) else 1


if __name__ == "__main__":
    sys.exit(main())
