package com.victor.matchmaking.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
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

class MatchmakingApiTest {

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
        JsonObject claims = new JsonObject().put("sub", playerId);
        return jwtAuth.generateToken(claims, new JWTOptions()
                .setAlgorithm("RS256")
                .setExpiresInSeconds(3600));
    }

    @Test
    void createTicketReturns201AndIdempotentKeyReturnsSameTicket() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String token = issueToken("player-2");
        String body = "{\"gameMode\":\"5v5\",\"region\":\"SA\",\"maxLatencyMs\":60}";

        HttpResponse<String> first = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets"))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .header("Authorization", "Bearer " + token)
                        .header("content-type", "application/json")
                        .header("Idempotency-Key", "key-1").build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(201, first.statusCode());
        JsonObject created = new JsonObject(first.body());
        assertNotNull(created.getString("ticketId"));
        assertEquals("SEARCHING", created.getString("status"));
        assertEquals(48, created.getJsonObject("searchRange").getInteger("minSkill"));
        assertEquals(52, created.getJsonObject("searchRange").getInteger("maxSkill"));

        HttpResponse<String> retry = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets"))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .header("Authorization", "Bearer " + token)
                        .header("content-type", "application/json")
                        .header("Idempotency-Key", "key-1").build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, retry.statusCode());
        assertEquals(created.getString("ticketId"), new JsonObject(retry.body()).getString("ticketId"));
    }

    @Test
    void getTicketByIdReturnsOwnerState() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String token = issueToken("player-7");
        HttpResponse<String> created = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets"))
                        .POST(HttpRequest.BodyPublishers.ofString("{\"gameMode\":\"1v1\",\"region\":\"NA\",\"maxLatencyMs\":80}"))
                        .header("Authorization", "Bearer " + token)
                        .header("content-type", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
        String id = new JsonObject(created.body()).getString("ticketId");

        HttpResponse<String> fetched = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets/" + id))
                        .header("Authorization", "Bearer " + token).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, fetched.statusCode());
        assertEquals(id, new JsonObject(fetched.body()).getString("ticketId"));
    }

    @Test
    void deleteTicketCancelsAndSecondDeleteReturns409() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String token = issueToken("player-9");
        HttpResponse<String> created = client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"gameMode\":\"5v5\",\"region\":\"EU\",\"maxLatencyMs\":40}"))
                .header("Authorization", "Bearer " + token)
                .header("content-type", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
        String id = new JsonObject(created.body()).getString("ticketId");

        HttpResponse<String> cancelled = client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets/" + id))
                .DELETE()
                .header("Authorization", "Bearer " + token).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, cancelled.statusCode());
        assertEquals("CANCELLED", new JsonObject(cancelled.body()).getString("status"));

        HttpResponse<String> again = client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets/" + id))
                .DELETE()
                .header("Authorization", "Bearer " + token).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(409, again.statusCode());
    }

    @Test
    void createTicketWithoutTokenReturns401() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets"))
                        .POST(HttpRequest.BodyPublishers.ofString("{\"gameMode\":\"1v1\",\"region\":\"NA\",\"maxLatencyMs\":80}"))
                        .header("content-type", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(401, response.statusCode());
    }

    @Test
    void getUnknownTicketReturns404() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String token = issueToken("player-1");
        HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets/ticket-nope"))
                        .header("Authorization", "Bearer " + token).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
    }
}
