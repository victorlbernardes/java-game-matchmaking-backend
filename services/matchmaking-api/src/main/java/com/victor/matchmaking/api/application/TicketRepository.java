package com.victor.matchmaking.api.application;

import java.time.Instant;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;

/** Port for ticket persistence. Redis-backed implementation arrives in W3. */
public interface TicketRepository {

    MatchmakingTicket findById(String ticketId);

    MatchmakingTicket findByIdempotencyKey(String key);

    void save(String idempotencyKey, MatchmakingTicket ticket);

    String newTicketId();

    Instant now();
}
