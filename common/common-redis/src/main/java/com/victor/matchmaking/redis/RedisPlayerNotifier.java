package com.victor.matchmaking.redis;

import java.util.function.Function;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

/** Sends a notification envelope targeted at a player to the shared notifications channel. */
public final class RedisPlayerNotifier {

    private final RedisPublisher publisher;

    public RedisPlayerNotifier(RedisPublisher publisher) {
        this.publisher = publisher;
    }

    public <T> Future<Void> notifyPlayer(String playerId, Envelope<T> envelope, Function<T, JsonObject> payloadEncoder) {
        RoutedEnvelope routed = new RoutedEnvelope(playerId, envelope.toJson(payloadEncoder).encode());
        return publisher.publish(RedisKeys.CHANNEL_PLAYER_NOTIFICATIONS, routed.toJson().encode());
    }
}
