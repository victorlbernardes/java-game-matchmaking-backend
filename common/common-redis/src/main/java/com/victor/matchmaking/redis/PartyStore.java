package com.victor.matchmaking.redis;

import com.victor.matchmaking.domain.player.Party;

import io.vertx.core.Future;

/** Port for party hot state. Async so the Redis client never blocks the event loop. */
public interface PartyStore {

    Future<Party> findById(String partyId);

    Future<Party> save(Party party);

    Future<Void> delete(String partyId);
}
