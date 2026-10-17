package com.victor.matchmaking.orchestrator.infrastructure;

import java.sql.Timestamp;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.orchestrator.application.ConfirmedMatchInbox;
import com.victor.matchmaking.orchestrator.domain.AllocationRequest;
import com.victor.matchmaking.orchestrator.domain.ProcessedEvent;
import com.victor.matchmaking.orchestrator.domain.ProcessingOutcome;

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
 */
public final class PostgresConfirmedMatchInbox implements ConfirmedMatchInbox, AutoCloseable {

    private static final String RECORD_EVENT =
            "INSERT INTO processed_events (event_id, consumer_group, topic, partition_id, record_offset, processed_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING RETURNING event_id";

    private static final String PROJECT_ALLOCATION =
            "INSERT INTO match_allocations (match_id, game_mode, region, player_count, status, confirmed_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?)";

    private final Pool pool;

    public PostgresConfirmedMatchInbox(Pool pool) {
        this.pool = pool;
    }

    public static PostgresConfirmedMatchInbox connect(Vertx vertx, DatabaseConfig config) {
        return new PostgresConfirmedMatchInbox(DatabasePool.connect(vertx, config));
    }

    @Override
    public Future<ProcessingOutcome> applyOnce(AllocationRequest allocation, ProcessedEvent event) {
        return pool.withTransaction(connection -> recordEvent(connection, event)
                .compose(outcome -> outcome == ProcessingOutcome.APPLIED
                        ? projectAllocation(connection, allocation)
                        : Future.succeededFuture(ProcessingOutcome.DUPLICATE)));
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

    private static Timestamp timestampOf(java.time.Instant instant) {
        return Timestamp.from(instant);
    }
}
