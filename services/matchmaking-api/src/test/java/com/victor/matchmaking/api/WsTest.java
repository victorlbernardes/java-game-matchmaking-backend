package com.victor.matchmaking.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;

class WsTest {

    private static Vertx vertx;
    private static int port;
    private static JWTAuth jwtAuth;

    @BeforeAll
    static void start() throws Exception {
        vertx = Vertx.vertx();
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair keyPair = gen.generateKeyPair();

        JsonObject fullJwk = new JsonObject()
                .put("kty", "RSA").put("use", "sig").put("alg", "RS256").put("kid", "test-key")
                .put("n", b64(((RSAPublicKey) keyPair.getPublic()).getModulus().toByteArray()))
                .put("e", b64(((RSAPublicKey) keyPair.getPublic()).getPublicExponent().toByteArray()))
                .put("d", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrivateExponent().toByteArray()))
                .put("p", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeP().toByteArray()))
                .put("q", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeQ().toByteArray()))
                .put("dp", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeExponentP().toByteArray()))
                .put("dq", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeExponentQ().toByteArray()))
                .put("qi", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getCrtCoefficient().toByteArray()));

        jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(fullJwk));
        Router router = MatchmakingApiApplication.createRouter(vertx, jwtAuth);

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router);
        port = server.listen(0).toCompletionStage().toCompletableFuture()
                .get(10, TimeUnit.SECONDS).actualPort();
    }

    @AfterAll
    static void stop() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static String b64(byte[] bytes) {
        if (bytes.length > 1 && bytes[0] == 0) {
            bytes = java.util.Arrays.copyOfRange(bytes, 1, bytes.length);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static String issueToken(String playerId) {
        return jwtAuth.generateToken(new JsonObject().put("sub", playerId),
                new JWTOptions().setAlgorithm("RS256").setExpiresInSeconds(3600));
    }

    @Test
    void wsRequiresValidToken() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        boolean[] reached = {false};
        try (client) {
            try {
                client.newWebSocketBuilder()
                        .buildAsync(URI.create("ws://localhost:" + port + "/v1/ws?token=invalid"),
                                new WebSocket.Listener() {})
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                reached[0] = true;
            }
            assertTrue(reached[0], "handshake with invalid token must fail");
        }
    }

    @Test
    void pingGetsPong() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        CompletableFuture<String> pong = new CompletableFuture<>();
        try (client) {
            WebSocket ws = client.newWebSocketBuilder()
                    .buildAsync(URI.create("ws://localhost:" + port + "/v1/ws?token=" + issueToken("player-1")),
                            new WebSocket.Listener() {
                                @Override
                                public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                                    pong.complete(data.toString());
                                    return null;
                                }
                            })
                    .get(10, TimeUnit.SECONDS);
            ws.sendText("{\"type\":\"PING\",\"payload\":{}}", true).get(10, TimeUnit.SECONDS);
            String response = pong.get(10, TimeUnit.SECONDS);
            assertEquals("PONG", new JsonObject(response).getString("type"));
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // fall through to abort
            }
            ws.abort();
        }
    }
}
