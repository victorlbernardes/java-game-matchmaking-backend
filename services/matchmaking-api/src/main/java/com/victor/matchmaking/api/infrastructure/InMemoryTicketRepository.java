package com.victor.matchmaking.api.infrastructure;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.victor.matchmaking.redis.TicketRepository;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;

import io.vertx.core.Future;

/** In-memory ticket store. Temporary until Redis is wired in tests/dev. */
public final class InMemoryTicketRepository implements TicketRepository {

    private static final int TICKET_ID_LENGTH = 8;

    private final ConcurrentHashMap<String, MatchmakingTicket> ticketsById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> idempotencyIndex = new ConcurrentHashMap<>();

    @Override
    public Future<MatchmakingTicket> findById(String ticketId) {
        return Future.succeededFuture(ticketsById.get(ticketId));
    }

    @Override
    public Future<MatchmakingTicket> findByIdempotencyKey(String key) {
        String ticketId = idempotencyIndex.get(key);
        return Future.succeededFuture(ticketId != null ? ticketsById.get(ticketId) : null);
    }

    @Override
    public Future<Void> save(String idempotencyKey, MatchmakingTicket ticket) {
        ticketsById.put(ticket.id(), ticket);
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            idempotencyIndex.put(idempotencyKey, ticket.id());
        }
        return Future.succeededFuture();
    }

    @Override
    public String newTicketId() {
        return "ticket-" + UUID.randomUUID().toString().substring(0, TICKET_ID_LENGTH);
    }

    @Override
    public Instant now() {
        return Instant.now();
    }
}
