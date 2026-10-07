package com.victor.matchmaking.api.application;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.redis.TicketRepository;

import io.vertx.core.Future;

/** Fetches a ticket, enforcing ownership. */
public final class GetTicketUseCase {

    private final TicketRepository repository;

    public GetTicketUseCase(TicketRepository repository) {
        this.repository = repository;
    }

    public Future<MatchmakingTicket> execute(String ticketId, String playerId) {
        return repository.findById(ticketId).map(ticket -> {
            if (ticket == null) {
                throw new TicketNotFoundException("ticket not found");
            }
            if (!ticket.playerId().equals(playerId)) {
                throw new TicketForbiddenException("ticket belongs to another player");
            }
            return ticket;
        });
    }

    public static final class TicketNotFoundException extends RuntimeException {
        public TicketNotFoundException(String message) {
            super(message);
        }
    }

    public static final class TicketForbiddenException extends RuntimeException {
        public TicketForbiddenException(String message) {
            super(message);
        }
    }
}
