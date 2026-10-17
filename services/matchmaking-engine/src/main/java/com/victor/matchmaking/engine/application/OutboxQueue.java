package com.victor.matchmaking.engine.application;

import java.time.Instant;
import java.util.List;

import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;

import io.vertx.core.Future;

/**
 * The outbox as the relay and the confirmation use case need it (ADR 0018).
 *
 * <p>Two halves of one idea: the write side puts the event in the same transaction as
 * the fact, the read side hands pending events to the relay under a lease.
 */
public interface OutboxQueue {

    /**
     * Writes the match, its roster and the event that announces it in ONE transaction:
     * either the match exists with its event, or neither does.
     */
    Future<Void> writeMatchWithEvent(Match match, List<MatchPlayer> players, OutboxRecord event);

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
}
