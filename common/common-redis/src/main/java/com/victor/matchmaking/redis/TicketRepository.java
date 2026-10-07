package com.victor.matchmaking.redis;

import java.time.Instant;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;

import io.vertx.core.Future;

/** Port for ticket persistence. Async so the Redis client never blocks the event loop. */
public interface TicketRepository {

    Future<MatchmakingTicket> findById(String ticketId);

    Future<MatchmakingTicket> findByIdempotencyKey(String key);

    Future<Void> save(String idempotencyKey, MatchmakingTicket ticket);

    String newTicketId();

    Instant now();
}
