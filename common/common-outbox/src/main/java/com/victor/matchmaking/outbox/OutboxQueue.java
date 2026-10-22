package com.victor.matchmaking.outbox;

import java.time.Instant;
import java.util.List;

import com.google.protobuf.Message;

import io.vertx.core.Future;

/**
 * The generic outbox (ADR 0018, generalized in W24): multiple services write events
 * next to their facts, and a relay publishes them. The write side is specific to what
 * the service is committing (match + roster + event, finished + winner + event,
 * recalculation + update + event); the read side is identical.
 */
public interface OutboxQueue {

    /**
     * Writes the fact and the event that announces it in ONE transaction.
     * Implementations decide what the fact is; the contract is "one transaction".
     *
     * @param factWriter writes the fact on the connection the caller already owns
     * @param event      the event to publish, already built
     */
    Future<Void> writeWithEvent(FactWriter factWriter, OutboxRecord event);

    /**
     * Claims up to {@code batchSize} pending rows for this relay, oldest first.
     *
     * <p>A claim is exclusive for its lease and leaves the rows pending: the relay is
     * allowed to die holding them, and they come back when the lease expires.
     *
     * @param relayId who is claiming, kept on the row for whoever has to understand a
     *                stuck queue later
     */
    Future<List<OutboxRecord>> claim(String relayId, int batchSize);

    /** Records that the broker acknowledged the record. */
    Future<Void> markPublished(String eventId, Instant publishedAt);

    /** Puts the row back in the queue for a later attempt, remembering why. */
    Future<Void> markFailed(String eventId, String error, Instant nextAttemptAt);

    /**
     * Gives up on the row and keeps it for a human, with the reason. The relay never
     * deletes anything: an event that could not be published is a fact too.
     */
    Future<Void> park(String eventId, String error);

    /** Counts rows by status, for KPIs. */
    Future<java.util.Map<String, Long>> countByStatus();

    /** Age of the oldest pending row (the relay's real lag). */
    Future<java.time.Duration> oldestPendingAge();

    /** Fact-to-broker delay of the newest published row. */
    Future<java.time.Duration> newestPublishLag();

    /**
     * Writes the fact on a connection the caller already owns.
     *
     * <p>This is the half the service knows: what "the fact" means. The outbox only
     * guarantees it is written in the same transaction as the event row.
     */
    @FunctionalInterface
    interface FactWriter {
        Future<Void> write(io.vertx.sqlclient.SqlConnection connection);
    }
}