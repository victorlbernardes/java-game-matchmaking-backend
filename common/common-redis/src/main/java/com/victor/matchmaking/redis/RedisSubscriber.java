package com.victor.matchmaking.redis;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;

/**
 * Subscribes to a Redis channel; every JSON message is forwarded to the handler.
 *
 * <p>The subscription is **repaired, not owned once** (W18, ADR 0024). Pub/Sub lives on the
 * connection, so a Redis restart, a failover or an idle timeout takes the subscription with it.
 * Before this, the subscriber connected once and never looked again: a client whose connection died
 * stayed subscribed to nothing for the life of the process, and the only evidence was the messages
 * that never arrived. Measured on the local stack: after {@code docker restart redis}, the engine
 * received no confirmation at all, so every match it formed dissolved at its 30 s deadline **while
 * both processes looked healthy** — the same failure the ADR calls out for the script cache, one
 * layer down.
 *
 * <p>Recovery is: reconnect after {@link #RECONNECT_DELAY_MS}, re-subscribe, keep trying. The delay
 * is flat rather than a backoff sequence because an attempt costs one TCP connect to a dependency
 * that is either up (the subscription is back in a second) or down (and nothing else the service does
 * is working either). The flag is what keeps a single death from scheduling two reconnects: a dead
 * connection ends the response stream, fails the pending subscribe and may raise an exception — three
 * signals for one death, and three reconnects would become three subscriptions.
 *
 * <p>What it does not promise: a message published while the subscription is down is lost. Neither
 * Redis nor this class buffers. That is the deliberate half of W16's open deviation (re-presenting a
 * match to a player whose notification was dropped); for confirmations the cost is bounded — the
 * match dissolves at its deadline and the players are requeued.
 */
public final class RedisSubscriber {

    private static final System.Logger LOG = System.getLogger("matchmaking.redis");

    /** How long to wait before rebuilding the subscription after the connection dies. */
    static final long RECONNECT_DELAY_MS = 1_000;

    private RedisSubscriber() {
    }

    public static void subscribe(Vertx vertx, String redisUri, String channel, Consumer<JsonObject> handler) {
        Redis client = Redis.createClient(vertx, new RedisOptions().setConnectionString(redisUri));
        connect(vertx, client, channel, handler, 1);
    }

    private static void connect(Vertx vertx, Redis client, String channel, Consumer<JsonObject> handler,
            int attempt) {
        client.connect().onSuccess(connection -> {
            AtomicBoolean repairing = new AtomicBoolean();
            Runnable repair = () -> {
                if (repairing.compareAndSet(false, true)) {
                    LOG.log(System.Logger.Level.WARNING,
                            "redis.reconnecting channel={0} attempt={1} inMs={2}",
                            channel, attempt, RECONNECT_DELAY_MS);
                    vertx.setTimer(RECONNECT_DELAY_MS, id -> connect(vertx, client, channel, handler, attempt + 1));
                }
            };
            connection.handler(response -> {
                if (response == null || response.size() < 3) {
                    return; // subscription confirmation frame
                }
                if ("message".equals(response.get(0).toString())) {
                    handler.accept(new JsonObject(response.get(2).toString()));
                }
            });
            connection.exceptionHandler(err -> {
                LOG.log(System.Logger.Level.ERROR, "redis.connection.error channel=" + channel, err);
                repair.run();
            });
            connection.endHandler(v -> repair.run());
            connection.send(Request.cmd(Command.SUBSCRIBE).arg(channel))
                    .onSuccess(v -> LOG.log(System.Logger.Level.INFO,
                            "redis.subscribed channel={0} attempt={1}", channel, attempt))
                    .onFailure(err -> {
                        LOG.log(System.Logger.Level.ERROR, "redis.subscribe.failed channel=" + channel, err);
                        repair.run();
                    });
        }).onFailure(err -> {
            LOG.log(System.Logger.Level.WARNING,
                    "redis.connect.failed channel={0} attempt={1} inMs={2}",
                    channel, attempt, RECONNECT_DELAY_MS);
            vertx.setTimer(RECONNECT_DELAY_MS, id -> connect(vertx, client, channel, handler, attempt + 1));
        });
    }
}
