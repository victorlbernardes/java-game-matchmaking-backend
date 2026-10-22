package com.victor.matchmaking.skill.infrastructure;

import java.time.Duration;
import java.util.Optional;

import com.victor.matchmaking.domain.skill.PlayerSkill;
import com.victor.matchmaking.redis.RedisClients;
import com.victor.matchmaking.redis.RedisKeys;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Response;

/**
 * Redis cache for player skills (W24).
 *
 * <p>Key: {@code skill:{playerId}} (hash with fields {@code rating}, {@code matchesPlayed},
 * {@code provisional}, {@code updatedAt}). TTL 1 hour: the cache is an acceleration, not the
 * source of truth. Losing it costs one Postgres read on the next event, never a wrong skill
 * for a match (the matchmaker falls back to the deterministic provider).
 */
public final class RedisSkillCache {

    private static final String TTL_SECONDS_ENV = "SKILL_CACHE_TTL_SECONDS";
    private static final int DEFAULT_TTL_SECONDS = 3600;

    private final Redis redis;
    private final int ttlSeconds;

    public RedisSkillCache(Redis redis) {
        this(redis, ttlFromEnv());
    }

    public RedisSkillCache(Redis redis, int ttlSeconds) {
        this.redis = redis;
        this.ttlSeconds = ttlSeconds;
    }

    public static int ttlFromEnv() {
        String raw = System.getenv(TTL_SECONDS_ENV);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_TTL_SECONDS;
        }
        int seconds = Integer.parseInt(raw.trim());
        if (seconds <= 0) {
            throw new IllegalArgumentException(TTL_SECONDS_ENV + " must be positive, got " + seconds);
        }
        return seconds;
    }

    /** Reads the cached skill. Empty means "not in cache" (not "not found"). */
    public Future<Optional<PlayerSkill>> get(String playerId) {
        String key = RedisKeys.playerSkillKey(playerId);
        return redis.send(Request.cmd(Command.HGETALL).arg(key))
                .map(response -> {
                    if (response == null || response.size() == 0) {
                        return Optional.<PlayerSkill>empty();
                    }
                    JsonObject obj = new JsonObject();
                    // Response is an array of alternating key/value strings; use get(i).toString()
                    for (int i = 0; i < response.size(); i += 2) {
                        String k = response.get(i).toString();
                        String v = response.get(i + 1).toString();
                        obj.put(k, v);
                    }
                    return Optional.of(new PlayerSkill(
                            playerId,
                            obj.getDouble("rating"),
                            obj.getInteger("matchesPlayed"),
                            obj.getBoolean("provisional"),
                            java.time.Instant.ofEpochMilli(obj.getLong("updatedAt"))));
                });
    }

    /** Writes the skill to the cache (after the DB commit). */
    public Future<Void> set(String playerId, PlayerSkill skill) {
        String key = RedisKeys.playerSkillKey(playerId);
        return redis.send(Request.cmd(Command.HSET)
                        .arg(key)
                        .arg("rating").arg(String.valueOf(skill.rating()))
                        .arg("matchesPlayed").arg(String.valueOf(skill.matchesPlayed()))
                        .arg("provisional").arg(String.valueOf(skill.provisional()))
                        .arg("updatedAt").arg(String.valueOf(skill.updatedAt().toEpochMilli())))
                .compose(ignored -> redis.send(Request.cmd(Command.EXPIRE).arg(key).arg(String.valueOf(ttlSeconds))))
                .mapEmpty();
    }

    /** Invalidates the cache (on manual admin action or debug). */
    public Future<Void> invalidate(String playerId) {
        return redis.send(Request.cmd(Command.DEL).arg(RedisKeys.playerSkillKey(playerId)))
                .mapEmpty();
    }
}