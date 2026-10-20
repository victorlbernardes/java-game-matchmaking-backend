package com.victor.matchmaking.engine.infrastructure;

import java.util.ArrayList;
import java.util.List;

import com.victor.matchmaking.engine.application.PendingMatch;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.redis.RedisKeys;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;

/** Redis-backed pending-match store: match-pending:{id} with TTL. */
public final class RedisPendingMatchStore implements PendingMatchStore {

    private static final int PENDING_TTL_SECONDS = 120;

    private final Redis client;

    public RedisPendingMatchStore(Redis client) {
        this.client = client;
    }

    @Override
    public Future<Void> save(PendingMatch pendingMatch) {
        return client.send(Request.cmd(Command.SET)
                        .arg(RedisKeys.matchPendingKey(pendingMatch.matchId()))
                        .arg(pendingMatch.toJson().encode())
                        .arg("EX").arg(PENDING_TTL_SECONDS))
                .mapEmpty();
    }

    @Override
    public Future<PendingMatch> findById(String matchId) {
        return client.send(Request.cmd(Command.GET).arg(RedisKeys.matchPendingKey(matchId)))
                .map(response -> response != null ? PendingMatch.fromJson(new JsonObject(response.toString())) : null);
    }

    @Override
    public Future<Void> delete(String matchId) {
        return client.send(Request.cmd(Command.DEL).arg(RedisKeys.matchPendingKey(matchId))).mapEmpty();
    }

    /**
     * {@code player-pending:{playerId}} → {@code matchId}, one key per player (W19, ADR 0026). The
     * TTL is the pending match's: the day after the match resolves, the index can only be read by
     * a heartbeat that has nothing to re-present, so it must expire with the thing it points at.
     */
    @Override
    public Future<Void> indexPlayers(PendingMatch pendingMatch) {
        List<Future<Void>> writes = new ArrayList<>();
        for (PendingMatch.PendingPlayer player : pendingMatch.players()) {
            writes.add(client.send(Request.cmd(Command.SET)
                            .arg(RedisKeys.playerPendingKey(player.playerId()))
                            .arg(pendingMatch.matchId())
                            .arg("EX").arg(PENDING_TTL_SECONDS))
                    .mapEmpty());
        }
        return Future.all(writes).mapEmpty();
    }

    @Override
    public Future<Void> clearPlayerIndex(PendingMatch pendingMatch) {
        if (pendingMatch.players().isEmpty()) {
            return Future.succeededFuture();
        }
        Request del = Request.cmd(Command.DEL);
        for (PendingMatch.PendingPlayer player : pendingMatch.players()) {
            del.arg(RedisKeys.playerPendingKey(player.playerId()));
        }
        return client.send(del).mapEmpty();
    }
}
