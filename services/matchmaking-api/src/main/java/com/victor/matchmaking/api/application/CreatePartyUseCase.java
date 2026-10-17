package com.victor.matchmaking.api.application;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.redis.PartyStore;

import io.vertx.core.Future;

/** Creates a party owned by the authenticated leader. */
public final class CreatePartyUseCase {

    private static final int PARTY_ID_LENGTH = 8;

    private final PartyStore partyStore;

    public CreatePartyUseCase(PartyStore partyStore) {
        this.partyStore = partyStore;
    }

    /** Generates a new active party with the leader as its only member. */
    public Future<String> execute(String leaderId) {
        String partyId = "party-" + UUID.randomUUID().toString().substring(0, PARTY_ID_LENGTH);
        Party party = new Party(
                partyId,
                leaderId,
                List.of(leaderId),
                Instant.now(),
                true,
                List.of());
        return partyStore.save(party)
                .map(saved -> partyId);
    }
}
