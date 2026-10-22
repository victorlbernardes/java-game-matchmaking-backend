package com.victor.matchmaking.api.infrastructure;

import java.util.List;

import com.victor.matchmaking.api.application.PendingConfirmationLookup;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.message.Payloads;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;

/**
 * Reads the pending confirmation of a player from Redis (W19, ADR 0026).
 *
 * <p>Two reads, both written by the engine: {@code player-pending:{playerId}} → {@code matchId}
 * (the reverse index) and {@code match-pending:{matchId}} → the roster. A third decides whether
 * the answer is still worth sending: a match that already has a confirmed claim is done, and
 * re-presenting it would ask the player to confirm a match that no longer exists.
 *
 * <p>The JSON is parsed here because this is the boundary that owns Redis — the rest of the
 * service speaks {@link Payloads.MatchPending}. A stale index pointing at a match whose pending
 * key expired is not an error: it answers "nothing to confirm", which is exactly what the
 * heartbeat should tell the client.
 */
public final class RedisPendingConfirmationLookup implements PendingConfirmationLookup {

    private final Redis client;

    public RedisPendingConfirmationLookup(Redis client) {
        this.client = client;
    }

    @Override
    public Future<Payloads.MatchPending> findForPlayer(String playerId) {
        return get(RedisKeys.playerPendingKey(playerId)).compose(matchId -> {
            if (matchId == null) {
                return Future.succeededFuture(null);
            }
            return get(RedisKeys.matchConfirmedClaimKey(matchId)).compose(claim -> {
                if (claim != null) {
                    return Future.succeededFuture(null);
                }
                return get(RedisKeys.matchPendingKey(matchId))
                        .map(json -> json != null ? toPending(matchId, json) : null);
            });
        });
    }

    private Future<String> get(String key) {
        return client.send(Request.cmd(Command.GET).arg(key))
                .map(response -> response != null ? response.toString() : null);
    }

    private static Payloads.MatchPending toPending(String matchId, String pendingJson) {
        JsonObject json = new JsonObject(pendingJson);
        List<String> playerIds = json.getJsonArray("players").stream()
                .map(player -> ((JsonObject) player).getString("playerId"))
                .toList();
        return new Payloads.MatchPending(matchId, playerIds);
    }
}
