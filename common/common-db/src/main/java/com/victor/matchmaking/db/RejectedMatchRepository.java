package com.victor.matchmaking.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.victor.matchmaking.domain.ticket.Region;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Tuple;

/**
 * The engine's half of a rejection (W23, ADR 0032): which matches the orchestrator decided cannot
 * run, and who has to go back to the queue.
 *
 * <p>It reads the orchestrator's projection and writes exactly one column ({@code requeued_at}).
 * That split is deliberate: the decision is the orchestrator's (it owns the pool and the deadlines)
 * and the act is the engine's (it owns the queue). The table in between is the channel, so a
 * rejection that happens while the engine is down is not lost — it is a row nobody has marked yet.
 *
 * <p>The query is scoped by region, the same boundary every other queue write has (W21): an engine
 * never requeues a ticket into a queue it does not pair from.
 */
public final class RejectedMatchRepository {

    /** One rejected match, when it was rejected, and the queue entries that must come back. */
    public record RejectedMatch(String matchId, java.time.Instant rejectedAt, List<Entry> entries) {

        /** The tickets that can actually be requeued; an event from before W23 carries none. */
        public List<String> ticketIds() {
            return entries.stream().map(Entry::ticketId).filter(id -> id != null && !id.isBlank()).toList();
        }

        public int playersWithoutTicket() {
            return (int) entries.stream().filter(e -> e.ticketId() == null || e.ticketId().isBlank()).count();
        }
    }

    /** One player of the rejected match, with the queue entry he was paired with. */
    public record Entry(String playerId, String ticketId) {
    }

    /**
     * The limit bounds <em>matches</em>, not joined rows: a subquery first picks the matches, then
     * the roster is joined. A plain {@code LIMIT} on the join would cut a match's roster in half and
     * requeue half a team.
     *
     * <p>The join is a {@code LEFT JOIN} because of what an <em>inner</em> one does to the sweep: a
     * rejected match with no roster (a projection that predates W23, or an event that carried no
     * players) produces no rows, so it is never marked — and since the batch is the oldest
     * {@code LIMIT} rows, such a row stays at the head of every pass and starves every rejection
     * behind it. Measured on the local stack: 18 930 of them, and the sweep marked none of the new
     * ones. A row with nothing to give back must still be <em>reachable</em>, so the marker says
     * "this rejection was acted on", not "players came back".
     */
    private static final String PENDING_REQUEUE =
            "SELECT a.match_id, a.rejected_at, r.player_id, r.ticket_id FROM "
                    + "(SELECT match_id, rejected_at FROM match_allocations WHERE status = ? AND requeued_at IS NULL "
                    + " AND region = ? ORDER BY rejected_at LIMIT ?) a "
                    + "LEFT JOIN match_roster r ON r.match_id = a.match_id "
                    + "ORDER BY a.match_id, r.player_id";

    /**
     * The marker is the last write, and it is conditional: a second sweep after a crash finds
     * nothing to mark, and a second requeue of the same tickets is a no-op in the queue itself.
     */
    private static final String MARK_REQUEUED =
            "UPDATE match_allocations SET requeued_at = now() WHERE match_id = ? AND requeued_at IS NULL";

    private final Pool pool;

    public RejectedMatchRepository(Pool pool) {
        this.pool = pool;
    }

    public static RejectedMatchRepository connect(Vertx vertx, DatabaseConfig config) {
        return new RejectedMatchRepository(DatabasePool.connect(vertx, config));
    }

    /** Rejected matches of one region whose players have not gone back to the queue yet. */
    public Future<List<RejectedMatch>> pendingRequeue(Region region, int limit) {
        return pool.preparedQuery(PENDING_REQUEUE)
                .execute(Tuple.of(com.victor.matchmaking.domain.match.AllocationStatus.REJECTED.name(),
                        region.name(), limit))
                .map(rows -> {
                    Map<String, List<Entry>> byMatch = new LinkedHashMap<>();
                    Map<String, java.time.Instant> rejectedAt = new LinkedHashMap<>();
                    rows.forEach(row -> {
                        String matchId = row.getString("match_id");
                        rejectedAt.putIfAbsent(matchId,
                                MatchRepository.instantOf(row.getValue("rejected_at")));
                        // A rosterless rejection (the LEFT JOIN's null side) has no player and no
                        // ticket: it is carried by the match id alone, so the sweep can mark it.
                        String playerId = row.getString("player_id");
                        if (playerId != null) {
                            byMatch.computeIfAbsent(matchId, ignored -> new ArrayList<>())
                                    .add(new Entry(playerId, row.getString("ticket_id")));
                        } else {
                            byMatch.putIfAbsent(matchId, new ArrayList<>());
                        }
                    });
                    List<RejectedMatch> rejected = new ArrayList<>(byMatch.size());
                    byMatch.forEach((matchId, entries) -> rejected.add(
                            new RejectedMatch(matchId, rejectedAt.get(matchId), List.copyOf(entries))));
                    return rejected;
                });
    }

    /** @return true when this call was the one that marked it */
    public Future<Boolean> markRequeued(String matchId) {
        return pool.preparedQuery(MARK_REQUEUED).execute(Tuple.of(matchId)).map(rows -> rows.rowCount() == 1);
    }
}
