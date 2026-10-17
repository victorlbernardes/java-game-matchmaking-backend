package com.victor.matchmaking.api.infrastructure;

import java.util.concurrent.ConcurrentHashMap;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.redis.PartyStore;

import io.vertx.core.Future;

/** In-memory party store. Temporary until Redis is wired in tests/dev. */
public final class InMemoryPartyStore implements PartyStore {

    private final ConcurrentHashMap<String, Party> partiesById = new ConcurrentHashMap<>();

    @Override
    public Future<Party> findById(String partyId) {
        return Future.succeededFuture(partiesById.get(partyId));
    }

    @Override
    public Future<Party> save(Party party) {
        partiesById.put(party.id(), party);
        return Future.succeededFuture(party);
    }

    @Override
    public Future<Void> delete(String partyId) {
        partiesById.remove(partyId);
        return Future.succeededFuture();
    }
}
