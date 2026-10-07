package com.victor.matchmaking.redis;

import io.vertx.core.Future;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;

/** Publishes JSON payloads to Redis Pub/Sub channels. */
public final class RedisPublisher {

    private final Redis client;

    public RedisPublisher(Redis client) {
        this.client = client;
    }

    /** Publishes an already-encoded JSON string on the channel. */
    public Future<Void> publish(String channel, String jsonPayload) {
        return client.send(Request.cmd(Command.PUBLISH).arg(channel).arg(jsonPayload)).mapEmpty();
    }
}
