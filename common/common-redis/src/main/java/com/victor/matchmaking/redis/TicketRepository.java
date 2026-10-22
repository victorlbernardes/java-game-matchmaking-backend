package com.victor.matchmaking.redis;

import java.time.Instant;
import java.util.List;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;

import io.vertx.core.Future;

/** Port for ticket persistence. Async so the Redis client never blocks the event loop. */
public interface TicketRepository {

    Future<MatchmakingTicket> findById(String ticketId);

    Future<MatchmakingTicket> findByIdempotencyKey(String key);

    Future<Void> save(String idempotencyKey, MatchmakingTicket ticket);

    /**
     * Atomic idempotent create: GET idem:{key} + capacity check + SET ticket + ZADD queue + SET
     * idem in one Lua script.
     *
     * @throws QueueFullException when the queue is at its admission limit; the ticket was not
     *         created and nothing was written, so the caller can shed the arrival
     */
    Future<String> createIdempotent(String idempotencyKey, MatchmakingTicket ticket);

    /**
     * Saves every ticket of a party in one go and returns the created ticket ids
     * in creation order. The caller links them to the party afterwards.
     */
    Future<List<String>> createPartyTickets(List<MatchmakingTicket> tickets);

    /** Tickets waiting in one queue. The party path consults it for a bounded admission check. */
    Future<Long> queueDepth(String gameModeLabel, String regionName);

    /**
     * Puts a ticket back in the queue it came from, at its original position (W23, ADR 0032).
     *
     * <p>Atomic on purpose: the state check (only a {@code CONFIRMING}/{@code CONFIRMED} ticket can
     * come back) and the {@code ZADD} are one step, so a ticket cancelled between the caller's read
     * and this call does not come back to life. Repeating the call is safe — an already
     * {@code SEARCHING} ticket is {@link RequeueOutcome#UNCHANGED} — which is what lets the caller
     * mark the match requeued <em>after</em> this succeeds.
     */
    Future<RequeueOutcome> requeue(MatchmakingTicket ticket);

    /** What {@link #requeue} did; a word, never a boolean flag (the caller logs the difference). */
    enum RequeueOutcome {
        /** The ticket is searching again, in its own partition, with its original score. */
        REQUEUED,
        /** Nothing to do: the ticket was already searching, cancelled or expired by its owner. */
        UNCHANGED,
        /** The ticket key no longer exists (its TTL ran out while the match waited). */
        MISSING
    }

    String newTicketId();

    Instant now();
}
