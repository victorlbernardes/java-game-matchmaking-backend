package com.victor.matchmaking.engine.infrastructure;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.db.MatchRepository;
import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;
import com.victor.matchmaking.engine.application.OutboxQueue;
import com.victor.matchmaking.engine.application.OutboxRecord;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

/**
 * The outbox table of ADR 0018, in PostgreSQL.
 *
 * <p>The write side is one transaction: the match, its roster and the event row. The
 * read side is a lease - {@code FOR UPDATE SKIP LOCKED} inside an {@code UPDATE} hands
 * each row to one relay, keeps it {@code PENDING} (an event that was never acknowledged
 * is not published data) and lets the lease expire so a relay that dies holding a row
 * does not take the event with it.
 *
 * <p>{@code SKIP LOCKED} is what makes two relays safe: the second one skips the rows the
 * first is working on instead of waiting for them, so a batch is never published twice
 * by the same pair of processes. It does not make publication exactly-once - a crash
 * between the broker's ack and {@code markPublished} publishes again - and that is by
 * design: at-least-once publication absorbed by an idempotent consumer (ADR 0038) is
 * cheaper than a distributed transaction across Postgres and Kafka.
 */
public final class PostgresOutboxQueue implements OutboxQueue, AutoCloseable {

    /** Waiting for the relay. */
    public static final String PENDING = "PENDING";
    /** Acknowledged by the broker. Kept: an event that was published is a fact. */
    public static final String PUBLISHED = "PUBLISHED";
    /** Given up on; needs a human, with `last_error` saying why. */
    public static final String PARKED = "PARKED";

    /** How long a claim holds a row before another relay may take it over. */
    private static final Duration LEASE = Duration.ofSeconds(30);

    private static final String INSERT_EVENT =
            "INSERT INTO outbox_events (event_id, topic, partition_key, event_type, payload, occurred_at, "
                    + "status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)";

    // Two statements, one transaction. The single `WITH claimable AS (…) UPDATE … RETURNING`
    // form is shorter, but the Vert.x JDBC client does not hand back the rows of a top-level
    // UPDATE's RETURNING clause (the same trap as INSERT … RETURNING, runbook item 12): the
    // claim silently looked empty. A SELECT … FOR UPDATE SKIP LOCKED followed by the UPDATE
    // that stamps the lease is the textbook shape, and the locks the SELECT takes are held
    // until the transaction commits.
    private static final String SELECT_CLAIMABLE = "SELECT event_id, topic, partition_key, event_type, payload,"
            + " occurred_at, created_at, attempts FROM outbox_events"
            + " WHERE status = 'PENDING'"
            + "   AND (next_attempt_at IS NULL OR next_attempt_at <= now())"
            + "   AND (claimed_at IS NULL OR claimed_at < now() - (? * INTERVAL '1 second'))"
            + " ORDER BY created_at, event_id"
            + " LIMIT ?"
            + " FOR UPDATE SKIP LOCKED";

    private static final String MARK_PUBLISHED =
            "UPDATE outbox_events SET status = 'PUBLISHED', published_at = ?, claimed_at = NULL, claimed_by = NULL,"
                    + " last_error = NULL WHERE event_id = ?";

    private static final String MARK_FAILED =
            "UPDATE outbox_events SET status = 'PENDING', attempts = attempts + 1, last_error = ?,"
                    + " next_attempt_at = ?, claimed_at = NULL, claimed_by = NULL WHERE event_id = ?";

    private static final String PARK =
            "UPDATE outbox_events SET status = 'PARKED', attempts = attempts + 1, last_error = ?,"
                    + " claimed_at = NULL, claimed_by = NULL WHERE event_id = ?";

    private final Pool pool;
    private final MatchRepository matches;

    public PostgresOutboxQueue(Pool pool, MatchRepository matches) {
        this.pool = pool;
        this.matches = matches;
    }

    public static PostgresOutboxQueue connect(Vertx vertx, DatabaseConfig config) {
        Pool pool = DatabasePool.connect(vertx, config);
        return new PostgresOutboxQueue(pool, new MatchRepository(pool));
    }

    @Override
    public Future<Void> writeMatchWithEvent(Match match, List<MatchPlayer> players, OutboxRecord event) {
        return pool.withTransaction(connection -> matches.saveOn(connection, match, players)
                .compose(ignored -> insertEvent(connection, event)));
    }

    @Override
    public Future<List<OutboxRecord>> claim(String relayId, int batchSize) {
        return pool.withTransaction(connection -> connection.preparedQuery(SELECT_CLAIMABLE)
                .execute(Tuple.of(LEASE.toSeconds(), batchSize))
                .compose(rows -> {
                    List<OutboxRecord> claimed = toRecords(rows);
                    if (claimed.isEmpty()) {
                        return Future.succeededFuture(List.<OutboxRecord>of());
                    }
                    return stampLease(connection, relayId, claimed).map(ignored -> claimed);
                }));
    }

    /** Stamps the lease of the rows the SELECT locked, in the same transaction. */
    private static Future<Void> stampLease(SqlConnection connection, String relayId, List<OutboxRecord> claimed) {
        Tuple parameters = Tuple.of(relayId);
        String placeholders = String.join(", ", Collections.nCopies(claimed.size(), "?"));
        for (OutboxRecord record : claimed) {
            parameters.addUUID(UUID.fromString(record.eventId()));
        }
        return connection.preparedQuery("UPDATE outbox_events SET claimed_at = now(), claimed_by = ?"
                        + " WHERE event_id IN (" + placeholders + ")")
                .execute(parameters)
                .mapEmpty();
    }

    @Override
    public Future<Void> markPublished(String eventId, Instant publishedAt) {
        return pool.preparedQuery(MARK_PUBLISHED)
                .execute(Tuple.of(timestampOf(publishedAt), UUID.fromString(eventId)))
                .mapEmpty();
    }

    @Override
    public Future<Void> markFailed(String eventId, String error, Instant nextAttemptAt) {
        return pool.preparedQuery(MARK_FAILED)
                .execute(Tuple.of(error, timestampOf(nextAttemptAt), UUID.fromString(eventId)))
                .mapEmpty();
    }

    @Override
    public Future<Void> park(String eventId, String error) {
        return pool.preparedQuery(PARK)
                .execute(Tuple.of(error, UUID.fromString(eventId)))
                .mapEmpty();
    }

    @Override
    public void close() {
        pool.close();
    }

    private Future<Void> insertEvent(SqlConnection connection, OutboxRecord event) {
        return connection.preparedQuery(INSERT_EVENT)
                .execute(Tuple.of(
                        UUID.fromString(event.eventId()),
                        event.topic(),
                        event.partitionKey(),
                        event.eventType(),
                        event.payload(),
                        timestampOf(event.occurredAt()),
                        timestampOf(event.createdAt())))
                .mapEmpty();
    }

    private static List<OutboxRecord> toRecords(RowSet<Row> rows) {
        List<OutboxRecord> records = new ArrayList<>();
        for (Row row : rows) {
            records.add(new OutboxRecord(
                    row.getUUID("event_id").toString(),
                    row.getString("topic"),
                    row.getString("partition_key"),
                    row.getString("event_type"),
                    row.getBuffer("payload").getBytes(),
                    instantOf(row.getValue("occurred_at")),
                    instantOf(row.getValue("created_at")),
                    row.getInteger("attempts")));
        }
        return records;
    }

    private static Timestamp timestampOf(Instant instant) {
        return Timestamp.from(instant);
    }

    private static Instant instantOf(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant();
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        throw new IllegalStateException("unsupported timestamp type: " + value.getClass());
    }
}
