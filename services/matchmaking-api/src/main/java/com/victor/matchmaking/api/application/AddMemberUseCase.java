package com.victor.matchmaking.api.application;

import java.util.ArrayList;
import java.util.List;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.redis.PartyStore;

import io.vertx.core.Future;

/** Adds a member to an active party. The member acts with their own token. */
public final class AddMemberUseCase {

    private final PartyStore partyStore;

    public AddMemberUseCase(PartyStore partyStore) {
        this.partyStore = partyStore;
    }

    /** Adds the authenticated player to the party and returns the updated party. */
    public Future<Party> execute(String partyId, String playerId) {
        return partyStore.findById(partyId).flatMap(party -> {
            if (party == null) {
                return Future.failedFuture(new PartyNotFoundException("party not found"));
            }
            if (!party.active()) {
                return Future.failedFuture(new PartyNotActiveException("party is dissolved"));
            }
            if (party.members().contains(playerId)) {
                return Future.failedFuture(new AlreadyMemberException("player is already in the party"));
            }
            if (party.members().size() >= Party.MAX_MEMBERS) {
                return Future.failedFuture(new PartyFullException("party is already full"));
            }
            List<String> members = new ArrayList<>(party.members());
            members.add(playerId);
            return partyStore.save(updated(party, members));
        });
    }

    private static Party updated(Party party, List<String> members) {
        return new Party(party.id(), party.leaderId(), members, party.createdAt(), true, party.memberTicketIds());
    }

    public static final class PartyFullException extends RuntimeException {
        PartyFullException(String message) {
            super(message);
        }
    }

    public static final class AlreadyMemberException extends RuntimeException {
        AlreadyMemberException(String message) {
            super(message);
        }
    }

    public static final class PartyNotActiveException extends RuntimeException {
        PartyNotActiveException(String message) {
            super(message);
        }
    }

    public static final class PartyNotFoundException extends RuntimeException {
        PartyNotFoundException(String message) {
            super(message);
        }
    }
}
