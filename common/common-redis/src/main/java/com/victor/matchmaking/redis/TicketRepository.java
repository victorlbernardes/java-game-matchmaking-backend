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

    String newTicketId();

    Instant now();
}
