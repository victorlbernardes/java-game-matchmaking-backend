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

/** Removes a member from a party. Removing the leader dissolves the whole party. */
public final class RemoveMemberUseCase {

    private final PartyStore partyStore;
    private final TicketRepository ticketRepository;

    public RemoveMemberUseCase(PartyStore partyStore, TicketRepository ticketRepository) {
        this.partyStore = partyStore;
        this.ticketRepository = ticketRepository;
    }

    /** Removes the authenticated player (the actor) and returns the updated party. */
    public Future<Party> execute(String partyId, String actorId, String playerId) {
        return partyStore.findById(partyId).flatMap(party -> {
            if (party == null) {
                return Future.failedFuture(new PartyNotFoundException("party not found"));
            }
            if (!party.members().contains(playerId)) {
                return Future.failedFuture(new ForbiddenException("player is not a member of this party"));
            }
            if (!playerId.equals(actorId) && !party.leaderId().equals(actorId)) {
                return Future.failedFuture(new ForbiddenException("only the party leader may remove another member"));
            }
            Party updated;
            if (playerId.equals(party.leaderId())) {
                updated = new Party(party.id(), party.leaderId(), party.members(),
                        party.createdAt(), false, party.memberTicketIds());
            } else {
                List<String> members = new ArrayList<>(party.members());
                members.remove(playerId);
                updated = new Party(party.id(), party.leaderId(), members,
                        party.createdAt(), true, party.memberTicketIds());
            }
            // The party unit is atomic: leaving cancels every member ticket.
            return leaveParty(party)
                    .flatMap(v -> partyStore.save(updated))
                    .map(saved -> updated);
        });
    }

    /**
     * The party unit is atomic: when a member leaves, every member ticket leaves
     * the queue together, so the party can be re-enqueued as a group later.
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
