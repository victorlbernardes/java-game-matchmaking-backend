#!/usr/bin/env python3
"""Prints the matchId of the pending match whose roster holds both given players.

The KPI scenario needs the match the engine just formed by *its* two players: another
pending match left over from a previous run must not be picked up. Redis holds the pending
match as JSON, and bash cannot parse it - hence this file.

Usage: pending_match.py <playerA> <playerB> [redis-container]
Exit code 1 when nothing matches.
"""
import json
import subprocess
import sys

REDIS_DEFAULT = "game-matchmaking-backend-redis-1"


def redis(*args, container):
    return subprocess.run(["docker", "exec", container, "redis-cli", *args],
                          capture_output=True, text=True, check=False).stdout.strip()


def main():
    player_a, player_b = sys.argv[1], sys.argv[2]
    container = sys.argv[3] if len(sys.argv) > 3 else REDIS_DEFAULT
    for key in redis("keys", "match-pending:*", container=container).splitlines():
        raw = redis("get", key, container=container)
        if not raw:
            continue
        pending = json.loads(raw)
        players = [player["playerId"] for player in pending.get("players", [])]
        if player_a in players and player_b in players:
            print(pending["matchId"])
            return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
