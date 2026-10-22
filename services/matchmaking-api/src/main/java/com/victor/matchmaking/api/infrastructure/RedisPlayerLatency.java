package com.victor.matchmaking.api.infrastructure;

import java.util.UUID;

import com.victor.matchmaking.api.application.PlayerLatency;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.RedisScript;

import io.vertx.core.Future;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;

/**
 * The player's measured distance, in the Redis of the region that measured it (W21, ADR 0039).
 *
 * <p>Two keys: {@code latency-probe:{nonce}} holds the instant the server issued the probe (30 s,
 * bound to the player and the region, consumed only by a pong that matches), and
 * {@code latency:{playerId}} holds the best recent sample per region (5 min, renewed by every
 * probe). Both write paths are scripts: the probe so the check-and-consume cannot be split, the
 * profile so a bad sample never replaces a good one.
 *
 * <p>The value stored is the minimum; the value returned by {@link #complete} is the sample that
 * was just taken, because that is the number a dashboard or a test is measuring.
 */
public final class RedisPlayerLatency implements PlayerLatency {

    private static final int PROBE_TTL_SECONDS = 30;
    private static final int PROFILE_TTL_SECONDS = 300;

    private final Redis client;
    private final RedisScript consumeProbe;
    private final RedisScript recordMeasurement;

    public RedisPlayerLatency(Redis client) {
        this.client = client;
        this.consumeProbe = new RedisScript(client, "/latency-probe.lua");
        this.recordMeasurement = new RedisScript(client, "/latency.lua");
    }

    @Override
    public Future<Probe> issue(String playerId, Region region) {
        String nonce = UUID.randomUUID().toString();
        String key = RedisKeys.latencyProbeKey(nonce);
        return client.send(Request.cmd(Command.HSET).arg(key)
                        .arg("player").arg(playerId)
                        .arg("region").arg(region.name())
                        .arg("at").arg(String.valueOf(System.currentTimeMillis())))
                .compose(ignored -> client.send(Request.cmd(Command.EXPIRE).arg(key).arg(PROBE_TTL_SECONDS)))
                .map(ignored -> new Probe(nonce, region));
    }

    @Override
    public Future<Integer> complete(String playerId, Region region, String nonce) {
        if (nonce == null || nonce.isBlank()) {
            return Future.succeededFuture(null);
        }
        return consumeProbe.eval(sha -> Request.cmd(Command.EVALSHA).arg(sha)
                        .arg("1").arg(RedisKeys.latencyProbeKey(nonce))
                        .arg(playerId).arg(region.name()))
                .compose(issued -> {
                    long issuedAt = issuedAtOf(issued);
                    if (issuedAt < 0) {
                        return Future.succeededFuture((Integer) null);
                    }
                    // A clock that went backwards answers 0 rather than a negative rtt, which would
                    // look like the best measurement ever taken.
                    int rttMs = (int) Math.max(0, System.currentTimeMillis() - issuedAt);
                    return recordMeasurement.eval(sha -> Request.cmd(Command.EVALSHA).arg(sha)
                                    .arg("1").arg(RedisKeys.playerLatencyKey(playerId))
                                    .arg(region.name())
                                    .arg(String.valueOf(rttMs))
                                    .arg(String.valueOf(PROFILE_TTL_SECONDS)))
                            .map(stored -> rttMs);
                });
    }

    @Override
    public Future<Integer> rtt(String playerId, Region region) {
        return client.send(Request.cmd(Command.HGET)
                        .arg(RedisKeys.playerLatencyKey(playerId))
                        .arg(region.name()))
                .map(reply -> {
                    if (reply == null) {
                        return null;
                    }
                    try {
                        return Integer.valueOf(reply.toString());
                    } catch (NumberFormatException notANumber) {
                        return null;
                    }
                });
    }

    private static long issuedAtOf(io.vertx.redis.client.Response issued) {
        if (issued == null) {
            return -1;
        }
        try {
            return Long.parseLong(issued.toString());
        } catch (NumberFormatException notANumber) {
            return -1;
        }
    }
}
