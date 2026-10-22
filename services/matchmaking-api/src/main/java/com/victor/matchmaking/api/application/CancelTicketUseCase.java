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
            // The 200 is a promise about the whole party, so the party's cancellation is awaited
            // before it is made: `onSuccess` would hand the reply back as soon as the caller's own
            // ticket was written, and a client that read the other members in that window saw them
            // still SEARCHING and still in the queue. The product's contract is "cancelling one
            // member cancels the whole party" (W11); a reply that is earlier than the effect makes
            // the contract untestable, and every assertion about it a race.
            return repository.save(null, cancelled)
                    .flatMap(saved -> leavePartyIfMember(cancelled.partyId()))
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
                    // Already cancelled: a second cancellation is a no-op, not a state transition
                    // error. It is reachable by a normal client — a member cancels their ticket
                    // (which cancels the party) and the leader then dissolves the party — and
                    // CANCELLED -> CANCELLED used to throw out of a future nobody waited for.
                    if (ticket == null || ticket.status() == TicketState.CANCELLED) {
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
