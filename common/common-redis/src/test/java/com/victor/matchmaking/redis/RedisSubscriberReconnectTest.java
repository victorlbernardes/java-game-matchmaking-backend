package com.victor.matchmaking.redis;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * A subscription survives the connection it lives on (W18, ADR 0024).
 *
 * <p>Pub/Sub state lives on the connection, so a Redis restart, a failover or an idle timeout takes
 * it with the socket. The subscriber used to connect once and never look again, which is a silent
 * failure by construction: nothing throws, nothing logs, and the only symptom is the messages that
 * stop arriving. Measured on the local stack, after `docker restart redis`: the engine received no
 * confirmation, so every match it formed dissolved at its 30 s deadline while both processes looked
 * healthy.
 *
 * <p>This test kills the subscriber's connection the way a restart does (`CLIENT KILL TYPE pubsub`)
 * and requires the next message to be delivered anyway: it fails against a caller that subscribes
 * once and forgets, and against one that reconnects without re-subscribing. The kill is server-wide
 * by command design — it takes down *every* Pub/Sub connection, which is what a restart does too
 * (the running services simply re-subscribe; the full suite runs with them stopped, runbook item 5).
 */
class RedisSubscriberReconnectTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String CHANNEL = "w18-reconnect-channel";

    private static Vertx vertx;
    private static Redis redis;

    @BeforeAll
    static void connect() {
        vertx = Vertx.vertx();
        redis = Redis.createClient(vertx, RedisClients.options(REDIS_URI));
    }

    @AfterAll
    static void disconnect() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void aKilledSubscriptionIsRebuiltAndStillDelivers() throws Exception {
        List<JsonObject> received = new CopyOnWriteArrayList<>();
        RedisSubscriber.subscribe(vertx, REDIS_URI, CHANNEL, received::add);
        awaitEventually(() -> subscribers(CHANNEL) == 1);
        assertTrue(subscribers(CHANNEL) == 1, "the subscriber is connected before anything is killed");

        // What a Redis restart does to every subscription it was serving.
        long killed = await(redis.send(Request.cmd(Command.CLIENT).arg("KILL").arg("TYPE").arg("pubsub")))
                .toLong();
        assertTrue(killed >= 1, "the test killed the subscriber's connection (killed=" + killed + ")");
        awaitEventually(() -> subscribers(CHANNEL) == 0);

        // The repair is not instant, and the test must not race it: publish until it lands.
        awaitEventually(() -> {
            await(redis.send(Request.cmd(Command.PUBLISH).arg(CHANNEL)
                    .arg(new JsonObject().put("after", "reconnect").encode())));
            return !received.isEmpty();
        });

        assertTrue(received.get(0).getString("after").equals("reconnect"),
                "the message arrived on the rebuilt subscription: " + received.get(0));
        assertTrue(subscribers(CHANNEL) == 1,
                "and exactly one subscription was rebuilt, not one per failure handler");
    }

    private static long subscribers(String channel) throws Exception {
        Response response = await(redis.send(Request.cmd(Command.PUBSUB).arg("NUMSUB").arg(channel)));
        return response.size() > 1 ? response.get(1).toLong() : 0L;
    }

    /** Polls until the condition holds; the subscriber's repair is asynchronous by design. */
    private static void awaitEventually(java.util.concurrent.Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new AssertionError("condition not met within 20s");
    }

    private static <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
