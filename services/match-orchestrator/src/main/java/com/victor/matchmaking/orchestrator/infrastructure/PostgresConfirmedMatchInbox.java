package com.victor.matchmaking.orchestrator.infrastructure;

import java.sql.Timestamp;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.db.GameServerRepository;
import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.metrics.MetricNames;
import com.victor.matchmaking.orchestrator.application.ConfirmedMatchInbox;
import com.victor.matchmaking.orchestrator.domain.AllocationOutcome;
import com.victor.matchmaking.orchestrator.domain.AllocationRequest;
import com.victor.matchmaking.orchestrator.domain.ProcessedEvent;
import com.victor.matchmaking.orchestrator.domain.ProcessingOutcome;

import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

/**
 * Inbox-backed idempotent projection: the event and its effect are written in one
 * transaction, so "have I already applied this?" is answered by the same write that
 * applies it.
 *
 * <p>The check is a unique index, not a read-then-write: two consumers racing on the
 * same event cannot both see "not processed". The loser's {@code ON CONFLICT} finds
 * the winner's uncommitted row, waits for it, and then does nothing - which is
 * exactly the behaviour a rebalance produces, and the reason this is a database
 * guarantee instead of application logic.
 *
 * <p><b>The allocation is part of that effect (W22, ADR 0031).</b> The projection row and
 * the slot it takes on a game server are written by the same transaction, for the same
 * reason the inbox exists: a redelivery must not take a second slot, and two confirmations
 * arriving together must not take the same last one. The row lock is what makes the second
 * half true ({@link GameServerRepository#reserve}); this class only decides <em>where</em> in
 * the effect it belongs, which is here — after the inbox insert has won, before the commit.
 *
 * <p>No capacity is not a failure: it is an answer, and the row stays
 * {@code PENDING_ALLOCATION} (W23 closes that loop). Counting it is the whole difference
 * between a pool that is full and a pool nobody is watching.
 */
public final class PostgresConfirmedMatchInbox implements ConfirmedMatchInbox, AutoCloseable {

    private static final String RECORD_EVENT =
            "INSERT INTO processed_events (event_id, consumer_group, topic, partition_id, record_offset, processed_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING RETURNING event_id";

    private static final String PROJECT_ALLOCATION =
            "INSERT INTO match_allocations (match_id, game_mode, region, player_count, status, confirmed_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?)";

    private static final String MARK_ALLOCATED =
            "UPDATE match_allocations SET status = ?, server_id = ?, allocated_at = now() WHERE match_id = ?";

    private final Pool pool;
    private final GameServerRepository servers;
    private final MeterRegistry registry;

    public PostgresConfirmedMatchInbox(Pool pool, GameServerRepository servers, MeterRegistry registry) {
        this.pool = pool;
        this.servers = servers;
        this.registry = registry;
    }

    public static PostgresConfirmedMatchInbox connect(Vertx vertx, DatabaseConfig config, MeterRegistry registry) {
        Pool pool = DatabasePool.connect(vertx, config);
        return new PostgresConfirmedMatchInbox(pool, new GameServerRepository(pool), registry);
    }

    @Override
    public Future<ProcessingOutcome> applyOnce(AllocationRequest allocation, ProcessedEvent event) {
        return pool.withTransaction(connection -> recordEvent(connection, event)
                .compose(outcome -> outcome == ProcessingOutcome.APPLIED
                        ? projectAndAllocate(connection, allocation)
                        : Future.succeededFuture(new Applied(ProcessingOutcome.DUPLICATE, null))))
                // Counted after the commit, never inside it: a rollback that undid the effect
                // while the counter kept the allocation would be an instrument lying about the
                // one question it answers.
                .map(applied -> {
                    if (applied.allocation() != null) {
                        registry.counter(MetricNames.ALLOCATIONS, MetricNames.TAG_RESULT,
                                applied.allocation() == AllocationOutcome.ALLOCATED
                                        ? MetricNames.RESULT_ALLOCATED
                                        : MetricNames.RESULT_NO_CAPACITY)
                                .increment();
                    }
                    return applied.status();
                });
    }

    @Override
    public void close() {
        pool.close();
    }

    /** @return APPLIED when this insert was the first one for the event id. */
    private Future<ProcessingOutcome> recordEvent(SqlConnection connection, ProcessedEvent event) {
        return connection.preparedQuery(RECORD_EVENT)
                .execute(Tuple.of(event.eventId(), event.consumerGroup(), event.topic(), event.partition(),
                        event.offset(), timestampOf(event.processedAt())))
                // rowCount(), not size(): the Vert.x JDBC client reports the number of
                // rows an INSERT affected in rowCount(), and size() is -1 for it - so
                // `size() == 0` would look like a duplicate on every insert.
                .map(inserted -> inserted.rowCount() == 0 ? ProcessingOutcome.DUPLICATE : ProcessingOutcome.APPLIED);
    }

    /**
     * A plain insert, without {@code ON CONFLICT}: two rows for one match is a data
     * conflict, not a redelivery, and it has to hurt. Swallowing it here would also
     * make the deduplication look correct for the wrong reason - the effect of a
     * duplicate would be absorbed by this statement instead of by the inbox.
     */
    private Future<ProcessingOutcome> projectAllocation(SqlConnection connection, AllocationRequest allocation) {
        return connection.preparedQuery(PROJECT_ALLOCATION)
                .execute(Tuple.of(allocation.matchId(), allocation.gameMode().label(), allocation.region().name(),
                        allocation.playerCount(), allocation.status().name(), timestampOf(allocation.confirmedAt())))
                .map(ProcessingOutcome.APPLIED);
    }

    private Future<Applied> projectAndAllocate(SqlConnection connection, AllocationRequest allocation) {
        return projectAllocation(connection, allocation)
                .compose(projected -> servers.reserve(connection, allocation.gameMode(), allocation.region()))
                .compose(reserved -> {
                    if (reserved.isEmpty()) {
                        // The pool is full, draining, stale or absent. The match is still
                        // confirmed and still projected; it simply has nowhere to run yet.
                        return Future.succeededFuture(
                                new Applied(ProcessingOutcome.APPLIED, AllocationOutcome.NO_CAPACITY));
                    }
                    return markAllocated(connection, allocation.matchId(), reserved.get())
                            .map(updated -> new Applied(ProcessingOutcome.APPLIED, AllocationOutcome.ALLOCATED));
                });
    }

    private Future<Integer> markAllocated(SqlConnection connection, String matchId, String serverId) {
        return connection.preparedQuery(MARK_ALLOCATED)
                .execute(Tuple.of(AllocationStatus.ALLOCATED.name(), serverId, matchId))
                .map(updated -> updated.rowCount());
    }

    private static Timestamp timestampOf(java.time.Instant instant) {
        return Timestamp.from(instant);
    }

    /** The effect and what it did about a server; the allocation is null for a redelivery. */
    private record Applied(ProcessingOutcome status, AllocationOutcome allocation) {
    }
}
