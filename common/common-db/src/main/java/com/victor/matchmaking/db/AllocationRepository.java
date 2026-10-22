package com.victor.matchmaking.db;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.domain.match.RejectionReason;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

/**
 * The life of an allocation after it exists (W23, ADR 0032): retry, deadlines, start, connection
 * and rejection.
 *
 * <p>{@link GameServerRepository} answers "where does a new match run"; this class answers "what
 * happens to a match that is already known" - the half that keeps a confirmed match from becoming
 * an orphan. It is the only writer of {@code match_allocations.status} past {@code ALLOCATED}, and
 * the only writer of {@code match_roster}.
 *
 * <p>The watchdog asks it four questions every second, in this order: <em>can a pending match get
 * a slot now</em> (retry), <em>did a pending match run out of time</em>, <em>did an allocated match
 * start in time</em>, and <em>is the server that holds a match still alive</em>. Each answer is a
 * single SQL statement where it can be, because a check-then-write is a race the watchdog would
 * lose once per busy second.
 */
public final class AllocationRepository {

    /** One pending match the retry loop can try to place. */
    public record PendingMatch(String matchId, GameMode gameMode, Region region) {
    }

    /** A rejected allocation whose slot must go back to the pool. */
    public record RejectedAllocation(String matchId, String serverId) {
    }

    /** What the orchestrator knows about one allocation. */
    public record Allocation(String matchId, AllocationStatus status, String serverId, Instant startedAt,
            RejectionReason rejectionReason) {
    }

    /** What a game server's report did to the row (idempotent by design). */
    public enum Transition {
        APPLIED,
        ALREADY,
        /** No allocation for that (match, server) — the server is reporting about somebody else's match. */
        NOT_FOUND,
        /** The allocation exists but is not in a state that accepts the report. */
        INVALID_TRANSITION,
        /** The match is started but the player is not in its roster. */
        PLAYER_NOT_IN_MATCH
    }

    /** The outcome of a start/connect report, with the instant the row holds afterwards. */
    public record ReportOutcome(Transition transition, Instant at) {
    }

    private static final String PENDING_TO_RETRY =
            "SELECT match_id, game_mode, region FROM match_allocations "
                    + "WHERE status = ? ORDER BY confirmed_at LIMIT ?";

    /**
     * The conditional flip: a pending match can only become allocated if it is still pending. The
     * row count is what tells the caller that the watchdog and a rejection raced and the slot must
     * go back — a leak here is a slot nobody can use until the next release.
     */
    private static final String MARK_ALLOCATED =
            "UPDATE match_allocations SET status = ?, server_id = ?, allocated_at = now() "
                    + "WHERE match_id = ? AND status = ?";

    /**
     * The deadline, as a single statement: every pending match older than the window is rejected in
     * one write, and the caller learns how many without a scan. {@code confirmed_at} (the event's
     * clock) is the right one to measure from — it is when the players stopped waiting for a match
     * and started waiting for a server.
     */
    private static final String REJECT_EXPIRED_PENDING =
            "UPDATE match_allocations SET status = ?, rejected_at = now(), rejection_reason = ? "
                    + "WHERE status = ? AND confirmed_at < now() - make_interval(secs => ?)";

    private static final String EXPIRED_STARTS =
            "SELECT match_id, server_id FROM match_allocations "
                    + "WHERE status = ? AND allocated_at < now() - make_interval(secs => ?) LIMIT ?";

    /**
     * A server that stopped heartbeating cannot be running the match. {@code draining} is
     * deliberately not part of this predicate: draining means "no new matches", not "stop the ones
     * you have".
     */
    private static final String STALE_SERVERS =
            "SELECT a.match_id, a.server_id FROM match_allocations a "
                    + "JOIN game_servers g ON g.server_id = a.server_id "
                    + "WHERE a.status IN (?, ?) AND g.last_seen < now() - make_interval(secs => ?) LIMIT ?";

    /**
     * The rejection and the slot it gives back, in one transaction. The status predicate is the
     * whole idempotence: a match already rejected or released is not rejected twice, and a slot is
     * never returned twice.
     */
    private static final String REJECT =
            "UPDATE match_allocations SET status = ?, rejected_at = now(), rejection_reason = ? "
                    + "WHERE match_id = ? AND server_id = ? AND status IN (?, ?)";

    private static final String START =
            "UPDATE match_allocations SET status = ?, started_at = now() "
                    + "WHERE match_id = ? AND server_id = ? AND status IN (?, ?) AND started_at IS NULL";

    private static final String STARTED_AT =
            "SELECT started_at FROM match_allocations WHERE match_id = ? AND server_id = ?";

    /**
     * The server reports the match result. Only a STARTED match accepts a result (a match that
     * never started cannot finish), and only the server that owns the allocation. Idempotent:
     * a repeated call returns the stored winner (the row count is 1 on the first call, 0 on
     * subsequent calls because winner_team is already set — we don't overwrite).
     */
    private static final String FINISH =
            "UPDATE match_allocations SET winner_team = ?, finished_at = now() "
                    + "WHERE match_id = ? AND server_id = ? AND status = ? AND winner_team IS NULL "
                    + "RETURNING winner_team";

    /** A player connecting is a report from the server, not a state machine of its own. */
    private static final String CONNECT =
            "UPDATE match_roster r SET connected_at = now() "
                    + "FROM match_allocations a "
                    + "WHERE r.match_id = ? AND r.player_id = ? AND a.match_id = r.match_id "
                    + "AND a.server_id = ? AND a.status = ? AND r.connected_at IS NULL";

    private static final String CONNECTED_AT =
            "SELECT connected_at FROM match_roster WHERE match_id = ? AND player_id = ?";

    private static final String NO_SHOW_WINDOW =
            "UPDATE match_roster r SET no_show_at = now() FROM match_allocations a "
                    + "WHERE a.match_id = r.match_id AND a.status = ? "
                    + "AND a.started_at < now() - make_interval(secs => ?) "
                    + "AND r.connected_at IS NULL AND r.no_show_at IS NULL";

    private static final String ALL_NO_SHOW =
            "SELECT a.match_id, a.server_id FROM match_allocations a "
                    + "WHERE a.status = ? AND a.started_at < now() - make_interval(secs => ?) "
                    + "AND NOT EXISTS (SELECT 1 FROM match_roster r "
                    + "                WHERE r.match_id = a.match_id AND r.connected_at IS NOT NULL) "
                    + "LIMIT ?";

    private static final String FIND =
            "SELECT match_id, status, server_id, started_at, rejection_reason "
                    + "FROM match_allocations WHERE match_id = ?";

    private static final String STATUS_OF =
            "SELECT status FROM match_allocations WHERE match_id = ? AND server_id = ?";

    private final Pool pool;
    private final GameServerRepository servers;

    public AllocationRepository(Pool pool, GameServerRepository servers) {
        this.pool = pool;
        this.servers = servers;
    }

    public static AllocationRepository connect(Vertx vertx, DatabaseConfig config) {
        Pool pool = DatabasePool.connect(vertx, config);
        return new AllocationRepository(pool, new GameServerRepository(pool));
    }

    /**
     * One retry: take a slot and flip the row, in one transaction. Empty means the pool had no room
     * (or the row stopped being pending while the slot was being taken, in which case the slot is
     * given back before the commit — the retry must not leak capacity).
     */
    public Future<Boolean> tryAllocate(PendingMatch pending) {
        return pool.withTransaction(connection -> servers.reserve(connection, pending.gameMode(), pending.region())
                .compose(reserved -> {
                    if (reserved.isEmpty()) {
                        return Future.succeededFuture(false);
                    }
                    return connection.preparedQuery(MARK_ALLOCATED)
                            .execute(Tuple.of(AllocationStatus.ALLOCATED.name(), reserved.get(),
                                    pending.matchId(), AllocationStatus.PENDING_ALLOCATION.name()))
                            .compose(flipped -> flipped.rowCount() == 1
                                    ? Future.succeededFuture(true)
                                    : servers.giveSlotBack(connection, reserved.get())
                                            .map(ignored -> false));
                }));
    }

    /** Pending matches worth a retry, oldest first. */
    public Future<List<PendingMatch>> pendingForRetry(int limit) {
        return pool.preparedQuery(PENDING_TO_RETRY)
                .execute(Tuple.of(AllocationStatus.PENDING_ALLOCATION.name(), limit))
                .map(rows -> {
                    List<PendingMatch> pending = new ArrayList<>();
                    rows.forEach(row -> pending.add(new PendingMatch(row.getString("match_id"),
                            GameMode.fromLabel(row.getString("game_mode")),
                            Region.valueOf(row.getString("region")))));
                    return pending;
                });
    }

    /** Rejects every pending match past its deadline; returns how many. */
    public Future<Integer> rejectExpiredPending(int allocationTimeoutSeconds) {
        return pool.preparedQuery(REJECT_EXPIRED_PENDING)
                .execute(Tuple.of(AllocationStatus.REJECTED.name(), RejectionReason.NO_CAPACITY.wireName(),
                        AllocationStatus.PENDING_ALLOCATION.name(), allocationTimeoutSeconds))
                .map(rows -> rows.rowCount());
    }

    /** Allocated matches whose server never said "started", with the slot still held. */
    public Future<List<RejectedAllocation>> expiredStarts(int startTimeoutSeconds, int limit) {
        return pool.preparedQuery(EXPIRED_STARTS)
                .execute(Tuple.of(AllocationStatus.ALLOCATED.name(), startTimeoutSeconds, limit))
                .map(AllocationRepository::rejectedRows);
    }

    /** Allocated/started matches whose server left the pool. */
    public Future<List<RejectedAllocation>> staleServers(int ttlSeconds, int limit) {
        return pool.preparedQuery(STALE_SERVERS)
                .execute(Tuple.of(AllocationStatus.ALLOCATED.name(), AllocationStatus.STARTED.name(), ttlSeconds,
                        limit))
                .map(AllocationRepository::rejectedRows);
    }

    /**
     * Rejects one allocation and gives its slot back, in one transaction. Empty when there was
     * nothing to reject (already rejected, released, or never allocated) — the caller does not
     * have to know which.
     */
    public Future<Boolean> reject(RejectedAllocation allocation, RejectionReason reason) {
        return pool.withTransaction(connection -> connection.preparedQuery(REJECT)
                .execute(Tuple.of(AllocationStatus.REJECTED.name(), reason.wireName(), allocation.matchId(),
                        allocation.serverId(), AllocationStatus.ALLOCATED.name(),
                        AllocationStatus.STARTED.name()))
                .compose(rejected -> rejected.rowCount() == 1
                        ? servers.giveSlotBack(connection, allocation.serverId()).map(ignored -> true)
                        : Future.succeededFuture(false)));
    }

    /** Marks every silent player of a started match past the connect window; returns how many. */
    public Future<Integer> markNoShows(int connectWindowSeconds) {
        return pool.preparedQuery(NO_SHOW_WINDOW)
                .execute(Tuple.of(AllocationStatus.STARTED.name(), connectWindowSeconds))
                .map(rows -> rows.rowCount());
    }

    /** Started matches past the connect window with no connected player at all. */
    public Future<List<RejectedAllocation>> allNoShow(int connectWindowSeconds, int limit) {
        return pool.preparedQuery(ALL_NO_SHOW)
                .execute(Tuple.of(AllocationStatus.STARTED.name(), connectWindowSeconds, limit))
                .map(AllocationRepository::rejectedRows);
    }

    /**
     * The server says the match began. Idempotent: a second report keeps the first instant and
     * answers {@link Transition#ALREADY} — a server that retries its own report is not an error.
     */
    public Future<ReportOutcome> start(String serverId, String matchId) {
        return pool.preparedQuery(START)
                .execute(Tuple.of(AllocationStatus.STARTED.name(), matchId, serverId,
                        AllocationStatus.ALLOCATED.name(), AllocationStatus.STARTED.name()))
                .compose(started -> {
                    if (started.rowCount() == 1) {
                        return startedAt(serverId, matchId).map(at -> new ReportOutcome(Transition.APPLIED, at));
                    }
                    return statusOf(serverId, matchId).compose(status -> {
                        if (status.isEmpty()) {
                            return Future.succeededFuture(new ReportOutcome(Transition.NOT_FOUND, null));
                        }
                        if (status.get() == AllocationStatus.STARTED) {
                            return startedAt(serverId, matchId)
                                    .map(at -> new ReportOutcome(Transition.ALREADY, at));
                        }
                        return Future.succeededFuture(new ReportOutcome(Transition.INVALID_TRANSITION, null));
                    });
                });
    }

    /**
     * The server reports a player in. Only a started match accepts connections (a connection before
     * the start is not a connection), and only a player of that allocation's roster — the two
     * answers a game server can act on.
     */
    public Future<ReportOutcome> connect(String serverId, String matchId, String playerId) {
        return pool.preparedQuery(CONNECT)
                .execute(Tuple.of(matchId, playerId, serverId, AllocationStatus.STARTED.name()))
                .compose(connected -> {
                    if (connected.rowCount() == 1) {
                        return connectedAt(matchId, playerId)
                                .map(at -> new ReportOutcome(Transition.APPLIED, at.orElse(null)));
                    }
                    return connectedAt(matchId, playerId).compose(at -> {
                        if (at.isPresent()) {
                            return Future.succeededFuture(new ReportOutcome(Transition.ALREADY, at.get()));
                        }
                        return statusOf(serverId, matchId).map(status -> {
                            if (status.isEmpty()) {
                                return new ReportOutcome(Transition.NOT_FOUND, null);
                            }
                            return status.get() == AllocationStatus.STARTED
                                    ? new ReportOutcome(Transition.PLAYER_NOT_IN_MATCH, null)
                                    : new ReportOutcome(Transition.INVALID_TRANSITION, null);
                        });
                    });
                });
    }

    /**
     * The server reports the match result. Only a STARTED match accepts a result, and only
     * the server that owns the allocation. Idempotent: a repeated call returns the stored
     * winner.
     */
    public Future<ReportOutcome> finish(String serverId, String matchId,
            com.victor.matchmaking.domain.match.MatchWinner winner) {
        return pool.preparedQuery(FINISH)
                .execute(Tuple.of(winner.team(), matchId, serverId, AllocationStatus.STARTED.name()))
                .compose(finished -> {
                    if (finished.rowCount() == 1) {
                        // First call: winner_team was NULL, now set. Read it back to confirm.
                        return Future.succeededFuture(new ReportOutcome(Transition.APPLIED, java.time.Instant.now()));
                    }
                    // Either already finished or invalid state - check current status.
                    return statusOf(serverId, matchId).compose(status -> {
                        if (status.isEmpty()) {
                            return Future.succeededFuture(new ReportOutcome(Transition.NOT_FOUND, null));
                        }
                        if (status.get() == AllocationStatus.STARTED) {
                            // Check if winner_team is already set (idempotent repeat).
                            return pool.preparedQuery("SELECT winner_team FROM match_allocations WHERE match_id = ? AND server_id = ?")
                                    .execute(Tuple.of(matchId, serverId))
                                    .map(rows -> {
                                        var iterator = rows.iterator();
                                        if (iterator.hasNext() && iterator.next().getInteger("winner_team") != null) {
                                            return new ReportOutcome(Transition.ALREADY, java.time.Instant.now());
                                        }
                                        return new ReportOutcome(Transition.INVALID_TRANSITION, null);
                                    });
                        }
                        return Future.succeededFuture(new ReportOutcome(Transition.INVALID_TRANSITION, null));
                    });
                });
    }

    public Future<Optional<Allocation>> find(String matchId) {
        return pool.preparedQuery(FIND).execute(Tuple.of(matchId)).map(rows -> {
            var iterator = rows.iterator();
            if (!iterator.hasNext()) {
                return Optional.<Allocation>empty();
            }
            Row row = iterator.next();
            String reason = row.getString("rejection_reason");
            return Optional.of(new Allocation(row.getString("match_id"),
                    AllocationStatus.valueOf(row.getString("status")), row.getString("server_id"),
                    MatchRepository.instantOf(row.getValue("started_at")),
                    reason == null ? null : RejectionReason.fromWireName(reason)));
        });
    }

    private Future<Optional<AllocationStatus>> statusOf(String serverId, String matchId) {
        return pool.preparedQuery(STATUS_OF).execute(Tuple.of(matchId, serverId)).map(rows -> {
            var iterator = rows.iterator();
            return iterator.hasNext()
                    ? Optional.of(AllocationStatus.valueOf(iterator.next().getString("status")))
                    : Optional.<AllocationStatus>empty();
        });
    }

    private Future<Instant> startedAt(String serverId, String matchId) {
        return pool.preparedQuery(STARTED_AT).execute(Tuple.of(matchId, serverId))
                .map(rows -> {
                    var iterator = rows.iterator();
                    return iterator.hasNext() ? MatchRepository.instantOf(iterator.next().getValue("started_at")) : null;
                });
    }

    private Future<Optional<Instant>> connectedAt(String matchId, String playerId) {
        return pool.preparedQuery(CONNECTED_AT).execute(Tuple.of(matchId, playerId)).map(rows -> {
            var iterator = rows.iterator();
            return iterator.hasNext()
                    ? Optional.ofNullable(MatchRepository.instantOf(iterator.next().getValue("connected_at")))
                    : Optional.<Instant>empty();
        });
    }

    private static List<RejectedAllocation> rejectedRows(io.vertx.sqlclient.RowSet<Row> rows) {
        List<RejectedAllocation> rejected = new ArrayList<>();
        rows.forEach(row -> rejected.add(new RejectedAllocation(row.getString("match_id"),
                row.getString("server_id"))));
        return rejected;
    }
}
