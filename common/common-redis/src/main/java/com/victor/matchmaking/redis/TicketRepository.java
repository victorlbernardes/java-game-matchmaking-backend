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

    /** Atomic idempotent create: GET idem:{key} + SET ticket + ZADD queue + SET idem in one Lua script. */
    Future<String> createIdempotent(String idempotencyKey, MatchmakingTicket ticket);

    /**
     * Saves every ticket of a party in one go and returns the created ticket ids
     * in creation order. The caller links them to the party afterwards.
     */
    Future<List<String>> createPartyTickets(List<MatchmakingTicket> tickets);

    String newTicketId();

    Instant now();
}
