# ADR-0034 — A no-show is a marked player, not a dissolved match (W23)

## Status
Accepted (2026-10-21)

## Context

The last hole in the lifecycle is the one with no server in it: the match was allocated, the game
server started it, and then **a player does not arrive**. Before this ADR the system had two bad
answers and no third one — dissolve the whole match (one absent player costs the other nine their
game, and the wait, and their confirmation), or say nothing (the match runs a player short and the
absence is invisible to the statistics, so nobody can act on it).

There is also the opposite case, which is not the player's fault: **nobody** arrives. A server that
started a match into a room that stays empty is holding a slot for nothing, and the players' tickets
must come back — the same requeue as any other rejection (ADR 0032).

The sprint's property is: **an absence does not kill the match; a completely absent match is not a
match.** That needs a per-player fact (`no_show_at`), a window, and a rule about who the absence
belongs to.

## Options

### Where the absence is decided

**A. The game server decides and reports "player X is absent".** The server is the only one that knows
the room, and it would be the right answer if the server were trusted to be honest about it: a
mis-reporting server marks a player absent, and the penalty (W24's skill) is applied to a player who
was there. It is also a second protocol to define, for a fact the matchmaker can derive from what it
already has (the connect reports it asked for).

**B. The matchmaker derives it from `connected_at` (chosen).** The game server reports `connected` per
player (the same internal API that reports `started`), the watchdog compares with the connect window
and writes `no_show_at`. The fact is the *absence of a report*, which the server cannot fake without
also refusing to serve the player.

### When the window closes

**C. A fixed timeout from `started_at`.** Simple and wrong for a slow start: the server's own start is
the clock's origin only if every player is expected to join immediately.

**D. `CONNECT_WINDOW_SECONDS` from `started_at` (chosen, default 90).** The origin is the instant the
server said the match began — the one moment all players and the server agree on — and the window is
long enough for a client to reconnect once (the W19 re-presentation) and short enough that a slot is
not held for a room nobody joined.

### What happens to the match

**E. Reject as soon as one player is missing.** A 5v5 with one absent player is not a 4v5 the
matchmaker should silently allow, and it is not a match it should dissolve either: the game server is
the one that can decide to continue short-handed, and the system's job is to record who was there.

**F. Mark the absent players and keep the match running; reject only when nobody connected (chosen).**
The match's outcome belongs to the game (W24). The matchmaker's two facts are "this player never
arrived" (`no_show_at`) and "this match has nobody in it" (`all_no_show`, which is a rejection and a
requeue).

## Decision

`match_roster` carries `connected_at` and `no_show_at` per player. The game server reports
`POST /v1/game-servers/:serverId/allocations/:matchId/players/:playerId/connected` (idempotent: the
first instant is the one that counts, `ALREADY` on a repeat, `404 player_not_in_match` for a player
outside the roster, `409 invalid_transition` before the match started).

Every second, past `CONNECT_WINDOW_SECONDS` from `started_at`, the `AllocationWatchdog`:

1. marks every silent player of a `STARTED` match (`no_show_at = now()`,
   `matchmaking.no.shows` counted) and leaves the match `STARTED`;
2. rejects the matches where **nobody** connected — `REJECTED` with `all_no_show`, the slot back to the
   pool, the roster requeued by the engine (ADR 0032).

`connected_at` and `no_show_at` are exclusive by construction: the marking is `WHERE connected_at IS
NULL`, so a player who reconnects after being marked is still marked (the window is a deadline, not a
poll), and a player who connected is never marked.

## How it works (files)

- `common/common-db/src/main/resources/db/migration/V5__allocation_lifecycle.sql` — `connected_at`,
  `no_show_at` on `match_roster`; `all_no_show` in the rejection vocabulary's `CHECK`.
- `common/common-db/.../AllocationRepository.java` — `connect(serverId, matchId, playerId)` (the
  `Transition` enum), `markNoShows(windowSeconds)`, `allNoShow(windowSeconds, batch)`.
- `services/match-orchestrator/.../application/AllocationWatchdog.java` — the last two questions of
  the tick, after the retry and the three deadlines.
- `services/match-orchestrator/.../presentation/GameServerRoutes.java` — `POST …/players/:playerId/
  connected`.
- `common/common-metrics/.../MetricNames.java` — `matchmaking.no.shows` and
  `matchmaking.match.rejections{reason="all_no_show"}`.

## Measured numbers (local stack, 2026-10-21)

`bash requests/lifecycle/verify_lifecycle.sh` with `CONNECT_WINDOW=10` (75 checks, 0 failures), and
`NoShowTest` (6 tests) against a real Postgres.

**One absent player is marked, and the match keeps running.** A match was started, one of the two
players reported `connected`, and nobody else did: after the **10 s** window the silent player had
`no_show_at` set, the connected one had none, the allocation was still `STARTED`, and
`matchmaking_no_shows_total` had moved. The match was not dissolved — the property the sprint exists
for.

**A match nobody joined is rejected and its slot comes back.** A second match was allocated on the
same server, started, and nobody connected: at the window's end the watchdog wrote `REJECTED` with
`all_no_show` (distinct from `start_timeout` and `server_failed` in the same table), the pool read
`allocated 1` (only the first match still holds a slot) and the roster was requeued.

**The reports are idempotent and guarded.** A connection before `started` is `409 invalid_transition`;
a repeated `started` keeps the first instant (`ALREADY`); a repeated `connected` keeps the first
instant; a player outside the roster is `404 player_not_in_match`.

**The invariant, read from the database at the end of the run:** no player is both connected and
absent — the two states are exclusive on every row.

`NoShowTest` drives the same three properties with a fake clock: inside the window nothing is touched;
one absent player is marked and the match stays `STARTED`; nobody connected → `REJECTED` with
`all_no_show` and the slot back.

## How to test

```bash
# the properties, without a running stack (Postgres)
mvn -o -B -pl services/match-orchestrator test -Dtest=NoShowTest

# by hand, against the scenario's stack (CONNECT_WINDOW=10 makes the wait short)
bash requests/lifecycle/verify_lifecycle.sh
```

By hand: after starting a match, `SELECT player_id, connected_at, no_show_at FROM match_roster WHERE
match_id = '<id>';` before and after the window, and
`curl -s localhost:9096/metrics | grep no_shows`. The `lifecycle-w23` folder's requests 10-12 are the
reports; the collection has no request for the passage of time, which is why the script exists.

The guard was checked by **removing** it: with `markNoShows` returning zero (the marking skipped), the
scenario's "the player who never connected is marked" fails while every other check still passes —
the absence is invisible, which is exactly the state this ADR ends.

## Trade-offs

- **The window is measured from `started_at`, not from each player's own join.** A player who arrives
  at the window's edge is marked even if he is in the room a second later: the mark is a deadline, not
  a poll, and the game server still has him connected. The alternative (per-player windows) needs a
  per-player clock the server would have to report.
- **A marked player is not removed from the match.** The match keeps running short-handed; whether
  that is a win, a loss or a forfeit is the game's rule (W24). The matchmaker records the fact and
  stops there.
- **`all_no_show` reuses the rejection path, so the players go back to the queue** — with their
  original score (ADR 0032). A player who never arrived is therefore queued again, which is right for
  a client crash and wrong for a player who abandoned on purpose; the distinction is a penalty, and
  penalties are W24's skill rule.
- **The no-show count is per player, not per match.** `matchmaking.no.shows` counts marked players;
  the rejection counter counts the matches nobody joined. Two numbers because they answer two
  questions (how often does a player fail to arrive, how often does a match die of it).
- **Nothing punishes the absence yet.** The counter is the input a skill penalty would use; applying
  it before the game's outcome exists would punish a player for a disconnect the game might still
  recover from.
- **The reports are the game server's word.** A server that never reports `connected` marks its whole
  match absent and gets it rejected — the incentive is to report, which is the behaviour the protocol
  wants. A lying server is out of scope (the internal API is a shared token, ADR 0031).

## Consequences

- An absence is a fact about a player, visible in the database and counted, instead of a match that
  silently disappears.
- A match nobody joined releases its slot and requeues its players through the same path as every
  other rejection — one lifecycle, four reasons.
- W24 can apply a skill penalty from `no_show_at` without a new protocol: the fact is already stored
  with its instant.
- The game server has two reports (`started`, `connected`) and both are idempotent, so a server that
  retries its own calls is safe.
