package com.victor.matchmaking.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.api.application.PendingConfirmationLookup;
import com.victor.matchmaking.api.application.PlayerPresence;
import com.victor.matchmaking.api.infrastructure.InMemoryTicketRepository;
import com.victor.matchmaking.api.presentation.ConnectionRegistry;
import com.victor.matchmaking.api.presentation.HttpRoutes;
import com.victor.matchmaking.redis.message.Payloads;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;

/**
 * The property W19 exists for (ADR 0026): a player whose notification was never delivered — their
 * socket died with the instance that held it, and they reconnected somewhere else — gets the
 * pending confirmation the next time their heartbeat asks.
 *
 * <p>Without the re-presentation this test has nothing to read: the heartbeat answers {@code PONG}
 * and the pending match, which is sitting in Redis, is never mentioned again. The client then
 * watches a match it cannot answer dissolve at its deadline. Measured before this (W16, and again
 * in W19 on the two-instance scenario): every match formed while the player's instance was down
 * dissolved, with both services healthy.
 *
 * <p>The lookup is a stub on purpose: the Redis read is covered by {@code RedisPendingConfirmation}
 * (engine side, where the index is written) and by the routing scenario end to end. What this test
 * pins is the wire: a heartbeat must re-present, and a heartbeat with nothing owed must not invent
 * a notification.
 */
class RePresentationTest {

    private static final String PLAYER = "player-1";
    private static final String MATCH_ID = "match-represented";

    private static Vertx vertx;
    private static JWTAuth jwtAuth;
    private static int port;
    private static final LinkedBlockingQueue<String> FRAMES = new LinkedBlockingQueue<>();

    @BeforeAll
    static void start() throws Exception {
        vertx = Vertx.vertx();
        byte[] secret = "w19-test-secret-w19-test-secret".getBytes(StandardCharsets.UTF_8);
        jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(new JsonObject()
                .put("kty", "oct")
                .put("k", Base64.getUrlEncoder().withoutPadding().encodeToString(secret))));
        PendingConfirmationLookup lookup = playerId -> Future.succeededFuture(
                PLAYER.equals(playerId)
                        ? new Payloads.MatchPending(MATCH_ID, List.of(PLAYER, "player-2"))
                        : null);
        ConnectionRegistry registry = new ConnectionRegistry(null, "test-instance",
                PlayerPresence.none(), lookup, new SimpleMeterRegistry());
        Router router = HttpRoutes.createRouter(vertx, jwtAuth, new InMemoryTicketRepository(), registry);
        HttpServer server = vertx.createHttpServer().requestHandler(router);
        port = server.listen(0).toCompletionStage().toCompletableFuture()
                .get(10, TimeUnit.SECONDS).actualPort();
    }

    @AfterAll
    static void stop() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void heartbeatRePresentsThePendingConfirmationTheClientMissed() throws Exception {
        FRAMES.clear();
        HttpClient client = HttpClient.newHttpClient();
        try (client) {
            WebSocket ws = client.newWebSocketBuilder()
                    .buildAsync(URI.create("ws://localhost:" + port + "/v1/ws?token=" + token(PLAYER)),
                            new WebSocket.Listener() {
                                @Override
                                public CompletionStage<?> onText(WebSocket webSocket, CharSequence data,
                                        boolean last) {
                                    FRAMES.add(data.toString());
                                    // The default demand is one frame: without asking for the
                                    // next, the re-presentation that follows the PONG is never
                                    // delivered to this listener.
                                    webSocket.request(1);
                                    return null;
                                }
                            })
                    .get(10, TimeUnit.SECONDS);
            ws.sendText("{\"type\":\"PING\",\"payload\":{}}", true).get(10, TimeUnit.SECONDS);

            JsonObject pong = nextFrame("PONG");
            assertEquals("PONG", pong.getString("type"));

            JsonObject represented = nextFrame("MATCH_PENDING_CONFIRMATION");
            assertEquals(MATCH_ID, represented.getJsonObject("payload").getString("matchId"),
                    "the re-presented match must be the pending one");
            assertTrue(represented.getJsonObject("payload").getJsonArray("playerIds").contains(PLAYER));
            ws.abort();
        }
    }

    @Test
    void heartbeatWithNothingOwedDoesNotInventANotification() throws Exception {
        FRAMES.clear();
        HttpClient client = HttpClient.newHttpClient();
        try (client) {
            WebSocket ws = client.newWebSocketBuilder()
                    .buildAsync(URI.create("ws://localhost:" + port + "/v1/ws?token=" + token("player-none")),
                            new WebSocket.Listener() {
                                @Override
                                public CompletionStage<?> onText(WebSocket webSocket, CharSequence data,
                                        boolean last) {
                                    FRAMES.add(data.toString());
                                    // The default demand is one frame: without asking for the
                                    // next, the re-presentation that follows the PONG is never
                                    // delivered to this listener.
                                    webSocket.request(1);
                                    return null;
                                }
                            })
                    .get(10, TimeUnit.SECONDS);
            ws.sendText("{\"type\":\"PING\",\"payload\":{}}", true).get(10, TimeUnit.SECONDS);

            assertEquals("PONG", nextFrame("PONG").getString("type"));
            String unexpected = FRAMES.poll(500, TimeUnit.MILLISECONDS);
            assertNull(unexpected, "a player with no pending match must receive only PONG");
            ws.abort();
        }
    }

    private static JsonObject nextFrame(String expectedType) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            String frame = FRAMES.poll(200, TimeUnit.MILLISECONDS);
            if (frame == null) {
                continue;
            }
            JsonObject json = new JsonObject(frame);
            if (expectedType.equals(json.getString("type"))) {
                return json;
            }
        }
        fail("expected a " + expectedType + " frame");
        return null;
    }
    private static String token(String playerId) {
        return jwtAuth.generateToken(new JsonObject().put("sub", playerId),
                new JWTOptions().setAlgorithm("HS256").setExpiresInSeconds(3600));
    }
}
