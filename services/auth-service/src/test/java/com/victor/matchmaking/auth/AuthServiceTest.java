package com.victor.matchmaking.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPublicKey;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;

class AuthServiceTest {

    private static Vertx vertx;
    private static int port;

    @BeforeAll
    static void start() throws Exception {
        vertx = Vertx.vertx();
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair keyPair = gen.generateKeyPair();

        io.vertx.core.json.JsonObject fullJwk = new io.vertx.core.json.JsonObject()
                .put("kty", "RSA").put("use", "sig").put("alg", "RS256").put("kid", "dev-key-1")
                .put("n", java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(((RSAPublicKey) keyPair.getPublic()).getModulus().toByteArray()))
                .put("e", java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(((RSAPublicKey) keyPair.getPublic()).getPublicExponent().toByteArray()))
                .put("d", java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrivateExponent().toByteArray()))
                .put("p", java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeP().toByteArray()))
                .put("q", java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeQ().toByteArray()))
                .put("dp", java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeExponentP().toByteArray()))
                .put("dq", java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeExponentQ().toByteArray()))
                .put("qi", java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString(((RSAPrivateCrtKey) keyPair.getPrivate()).getCrtCoefficient().toByteArray()));

        JWTAuth jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(fullJwk));
        Router router = AuthServiceApplication.createRouter(vertx, jwtAuth, keyPair);

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

    @Test
    void loginIssuesTokenAndJwksExposesPublicKey() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {

        HttpResponse<String> login = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v1/auth/login"))
                        .POST(HttpRequest.BodyPublishers.ofString("{\"playerId\":\"player-2\"}"))
                        .header("content-type", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, login.statusCode());
        String token = new JsonObject(login.body()).getString("token");
        assertNotNull(token);

        HttpResponse<String> jwks = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/.well-known/jwks.json"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, jwks.statusCode());
        JsonObject key = new JsonObject(jwks.body()).getJsonArray("keys").getJsonObject(0);
        assertEquals("RS256", key.getString("alg"));
        assertEquals("dev-key-1", key.getString("kid"));
        assertTrue(key.containsKey("n"));
        }
    }

    @Test
    void loginWithoutPlayerIdReturns400() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v1/auth/login"))
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .header("content-type", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode());
        }
    }
}
