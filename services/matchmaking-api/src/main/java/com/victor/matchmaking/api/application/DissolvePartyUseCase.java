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

/** Dissolves a party. Only the leader may dissolve; the search is cancelled. */
public final class DissolvePartyUseCase {

    private final PartyStore partyStore;
    private final TicketRepository ticketRepository;

    public DissolvePartyUseCase(PartyStore partyStore, TicketRepository ticketRepository) {
        this.partyStore = partyStore;
        this.ticketRepository = ticketRepository;
    }

    /** Dissolves the party and returns the dissolved party. */
    public Future<Party> execute(String partyId, String leaderId) {
        return partyStore.findById(partyId).flatMap(party -> {
            if (party == null) {
                return Future.failedFuture(new PartyNotFoundException("party not found"));
            }
            if (!party.leaderId().equals(leaderId)) {
                return Future.failedFuture(new ForbiddenException("only the leader may dissolve a party"));
            }
            Party dissolved = new Party(party.id(), party.leaderId(), party.members(),
                    party.createdAt(), false, List.of());
            // Dissolving cancels the whole party unit: every member ticket leaves the queue.
            return leaveParty(party)
                    .flatMap(v -> partyStore.save(dissolved))
                    .map(saved -> dissolved);
        });
    }

    /**
     * Dissolving cancels the whole party unit: every member ticket leaves the
     * queue, exactly like when a member leaves or a member cancels their ticket.
     */
    private Future<Void> leaveParty(Party party) {
        List<Future<Void>> cancels = new ArrayList<>();
        for (String ticketId : party.memberTicketIds()) {
            cancels.add(cancelTicket(ticketId));
        }
        return Future.all(cancels).mapEmpty();
    }

    private Future<Void> cancelTicket(String ticketId) {
        return ticketRepository.findById(ticketId)
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
                    return ticketRepository.save(null, cancelled).onSuccess(v -> Future.succeededFuture());
                });
    }

    public static final class PartyNotFoundException extends RuntimeException {
        PartyNotFoundException(String message) {
            super(message);
        }
    }

    public static final class ForbiddenException extends RuntimeException {
        ForbiddenException(String message) {
            super(message);
        }
    }
}
