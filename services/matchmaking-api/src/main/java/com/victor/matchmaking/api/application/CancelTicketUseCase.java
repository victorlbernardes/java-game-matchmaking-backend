package com.victor.matchmaking.api.application;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.domain.ticket.TicketStateMachine;

import io.vertx.core.Future;

/** Cancels a search ticket. Only the owner, only while SEARCHING. */
public final class CancelTicketUseCase {

    private final TicketRepository repository;

    public CancelTicketUseCase(TicketRepository repository) {
        this.repository = repository;
    }

    public Future<MatchmakingTicket> execute(String ticketId, String playerId) {
        return repository.findById(ticketId).flatMap(ticket -> {
            if (ticket == null) {
                return Future.failedFuture(new GetTicketUseCase.TicketNotFoundException("ticket not found"));
            }
            if (!ticket.playerId().equals(playerId)) {
                return Future.failedFuture(new GetTicketUseCase.TicketForbiddenException("ticket belongs to another player"));
            }
            if (ticket.status() != TicketState.SEARCHING) {
                return Future.failedFuture(new TicketConflictException("only SEARCHING tickets can be cancelled"));
            }
            MatchmakingTicket cancelled = new MatchmakingTicket(
                    ticket.id(),
                    ticket.playerId(),
                    ticket.gameMode(),
                    ticket.region(),
                    ticket.maxLatencyMs(),
                    ticket.skill(),
                    ticket.queuedAt(),
                    TicketStateMachine.transition(ticket.status(), TicketState.CANCELLED),
                    ticket.version() + 1);
            return repository.save(null, cancelled).map(v -> cancelled);
        });
    }

    public static final class TicketConflictException extends RuntimeException {
        public TicketConflictException(String message) {
            super(message);
        }
    }
}
