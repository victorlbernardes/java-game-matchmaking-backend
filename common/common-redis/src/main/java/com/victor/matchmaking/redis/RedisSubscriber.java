package com.victor.matchmaking.redis;

import java.util.function.Consumer;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;

/** Subscribes to a Redis channel; every JSON message is forwarded to the handler. */
public final class RedisSubscriber {

    private static final System.Logger LOG = System.getLogger("matchmaking.redis");

    private RedisSubscriber() {
    }

    public static void subscribe(Vertx vertx, String redisUri, String channel, Consumer<JsonObject> handler) {
        Redis client = Redis.createClient(vertx, new RedisOptions().setConnectionString(redisUri));
        client.connect().onSuccess(connection -> {
            connection.handler(response -> {
                if (response == null || response.size() < 3) {
                    return; // subscription confirmation frame
                }
                if ("message".equals(response.get(0).toString())) {
                    handler.accept(new JsonObject(response.get(2).toString()));
                }
            });
            connection.exceptionHandler(err ->
                    LOG.log(System.Logger.Level.ERROR, "redis.connection.error channel=" + channel, err));
            connection.send(Request.cmd(Command.SUBSCRIBE).arg(channel))
                    .onSuccess(v -> LOG.log(System.Logger.Level.INFO,
                            "redis.subscribed channel={0}", channel))
                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                            "redis.subscribe.failed channel=" + channel, err));
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "redis.connect.failed channel=" + channel, err));
    }
}
