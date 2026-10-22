package com.victor.matchmaking.api.infrastructure;

import com.victor.matchmaking.api.application.PlayerPresence;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.RedisScript;

import io.vertx.core.Future;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;

/**
 * Presence in Redis (W19, ADR 0026): {@code presence:{playerId}} → instance id, with a TTL.
 *
 * <p>The TTL is the honest part of presence: an instance that dies without running its close
 * handlers leaves no entry behind, because nobody refreshes it. It is roughly one and a half
 * heartbeats — long enough that a single missed heartbeat does not mark a live player offline,
 * short enough that a dead socket stops being "online" in the time it takes a client to notice.
 */
public final class RedisPlayerPresence implements PlayerPresence {

    /** The client heartbeats every 60 s; the entry must outlive a missed one, not a dead one. */
    static final int PRESENCE_TTL_SECONDS = 90;

    private final Redis client;
    private final RedisScript release;

    public RedisPlayerPresence(Redis client) {
        this.client = client;
        this.release = new RedisScript(client, "/presence-release.lua");
    }

    @Override
    public Future<Void> markOnline(String playerId, String instanceId) {
        return client.send(Request.cmd(Command.SET)
                        .arg(RedisKeys.presenceKey(playerId))
                        .arg(instanceId)
                        .arg("EX").arg(PRESENCE_TTL_SECONDS))
                .mapEmpty();
    }

    @Override
    public Future<Void> offline(String playerId, String instanceId) {
        return release.eval(sha -> Request.cmd(Command.EVALSHA).arg(sha)
                        .arg("1").arg(RedisKeys.presenceKey(playerId)).arg(instanceId))
                .mapEmpty();
    }
}
