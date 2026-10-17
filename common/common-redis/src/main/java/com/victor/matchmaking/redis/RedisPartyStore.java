package com.victor.matchmaking.redis;

import com.victor.matchmaking.domain.player.Party;

import io.vertx.core.Future;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Redis;

/** Redis-backed hot state for parties: party JSON keyed by partyId. */
public final class RedisPartyStore implements PartyStore {

    /** Party records expire after seven days even when still active. */
    private static final long PARTY_TTL_SECONDS = 7 * 24 * 60 * 60;

    private final Redis client;

    public RedisPartyStore(Redis client) {
        this.client = client;
    }

    @Override
    public Future<Party> findById(String partyId) {
        return client.send(Request.cmd(Command.GET).arg(RedisKeys.partyKey(partyId)))
                .map(response -> response != null
                        ? PartyCodec.fromJson(new io.vertx.core.json.JsonObject(response.toString()))
                        : null);
    }

    @Override
    public Future<Party> save(Party party) {
        String json = PartyCodec.toJson(party).encode();
        return client.send(Request.cmd(Command.SET)
                        .arg(RedisKeys.partyKey(party.id()))
                        .arg(json)
                        .arg("EX").arg(String.valueOf(PARTY_TTL_SECONDS)))
                .map(v -> party);
    }

    @Override
    public Future<Void> delete(String partyId) {
        return client.send(Request.cmd(Command.DEL).arg(RedisKeys.partyKey(partyId)))
                .mapEmpty();
    }
}
