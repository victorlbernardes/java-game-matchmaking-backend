package com.victor.matchmaking.api.application;

import java.util.ArrayList;
import java.util.List;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.domain.ticket.TicketStateMachine;
import com.victor.matchmaking.redis.PartyStore;
import com.victor.matchmaking.redis.TicketRepository;

import io.vertx.core.Future;

/** Cancels a search ticket. Only the owner, only while SEARCHING. */
public final class CancelTicketUseCase {

    private final TicketRepository repository;
    private final PartyStore partyStore;

    public CancelTicketUseCase(TicketRepository repository, PartyStore partyStore) {
        this.repository = repository;
        this.partyStore = partyStore;
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
                    ticket.version() + 1,
                    ticket.partyId());
            return repository.save(null, cancelled)
                    .onSuccess(saved -> leavePartyIfMember(cancelled.partyId()))
                    .map(v -> cancelled);
        });
    }

    /**
     * A ticket that belongs to a party leaves the whole party unit out of the
     * queue when it is cancelled: the remaining members keep the party but can
     * re-enqueue later.
     */
    private Future<Void> leavePartyIfMember(String partyId) {
        if (partyId == null) {
            return Future.succeededFuture();
        }
        return partyStore.findById(partyId)
                .flatMap(party -> cancelAllMemberTickets(party));
    }

    private Future<Void> cancelAllMemberTickets(Party party) {
        List<Future<Void>> cancels = new ArrayList<>();
        for (String ticketId : party.memberTicketIds()) {
            cancels.add(cancelTicket(ticketId));
        }
        return Future.all(cancels).mapEmpty();
    }

    private Future<Void> cancelTicket(String ticketId) {
        return repository.findById(ticketId)
                .flatMap(ticket -> {
                    if (ticket == null) {
                        return Future.succeededFuture();
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
                            ticket.version() + 1,
                            ticket.partyId());
                    return repository.save(null, cancelled).onSuccess(v -> Future.succeededFuture());
                });
    }

    public static final class TicketConflictException extends RuntimeException {
        TicketConflictException(String message) {
            super(message);
        }
    }
}
