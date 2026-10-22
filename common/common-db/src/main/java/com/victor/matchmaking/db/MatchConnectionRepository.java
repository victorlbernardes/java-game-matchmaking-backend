package com.victor.matchmaking.db;

import java.time.Instant;
import java.util.Optional;

import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.domain.match.RejectionReason;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

/**
 * The read side of "where do I play?" (W23, ADR 0033): one query that answers, for one player in one
 * match, whether there is a server to join and why not when there is not.
 *
 * <p>The API reads the orchestrator's projection directly. That is a deliberate choice over a call
 * to the orchestrator: the projection already exists, is written by a durable consumer and is
 * correct within a second, while a hop would add a service token, a timeout and a failure mode to
 * answer a question one indexed query answers. The same shape the engine already uses to read the
 * pool (W22) - the shared schema is the contract between the two halves of the allocation.
 *
 * <p>The player is part of the join on purpose: a player who is not in the roster gets "not found",
 * the same answer a stranger gets, because "this match exists and you are not in it" is information
 * about somebody else's match.
 */
public final class MatchConnectionRepository {

    /**
     * One player's view of an allocation.
     *
     * @param serverAddress the game server's advertised endpoint, or null when the row that named it
     *                      is gone (a pool row deleted under a live allocation, which the watchdog
     *                      rejects on its next tick)
     */
    public record MatchConnection(String matchId, AllocationStatus status, String serverId, String serverAddress,
            int team, RejectionReason rejectionReason, Instant startedAt) {
    }

    private static final String FIND =
            "SELECT a.match_id, a.status, a.server_id, g.endpoint, r.team, a.rejection_reason, a.started_at "
                    + "FROM match_allocations a "
                    + "JOIN match_roster r ON r.match_id = a.match_id AND r.player_id = ? "
                    + "LEFT JOIN game_servers g ON g.server_id = a.server_id "
                    + "WHERE a.match_id = ?";

    private final Pool pool;

    public MatchConnectionRepository(Pool pool) {
        this.pool = pool;
    }

    public static MatchConnectionRepository connect(Vertx vertx, DatabaseConfig config) {
        return new MatchConnectionRepository(DatabasePool.connect(vertx, config));
    }

    /** Empty means \"no such match for this player\"; the caller must not distinguish the two. */
    public Future<Optional<MatchConnection>> find(String matchId, String playerId) {
        return pool.preparedQuery(FIND).execute(Tuple.of(playerId, matchId)).map(rows -> {
            var iterator = rows.iterator();
            if (!iterator.hasNext()) {
                return Optional.<MatchConnection>empty();
            }
            Row row = iterator.next();
            String reason = row.getString("rejection_reason");
            return Optional.of(new MatchConnection(
                    row.getString("match_id"),
                    AllocationStatus.valueOf(row.getString("status")),
                    row.getString("server_id"),
                    row.getString("endpoint"),
                    row.getInteger("team"),
                    reason == null ? null : RejectionReason.fromWireName(reason),
                    MatchRepository.instantOf(row.getValue("started_at"))));
        });
    }
}
