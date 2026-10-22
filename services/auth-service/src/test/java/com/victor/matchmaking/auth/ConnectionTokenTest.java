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
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.auth.infrastructure.RsaKeyPairProvider;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;

/**
 * Property: the connection token is a credential a game server can verify <b>offline</b>, and it says
 * exactly one thing (W23, ADR 0033).
 *
 * <p>Four ways this goes wrong, and each one is a real incident:
 *
 * <ul>
 *   <li><b>not verifiable</b> without calling the matchmaking: the game server must be able to check
 *       the signature with the JWKS it already has, so the test verifies the token with a
 *       {@code JWTAuth} built from the <em>published</em> public JWK - the same material the server
 *       would use, no private key involved;</li>
 *   <li><b>not scoped</b>: a token minted for one match must not open another, and one minted for one
 *       player must not name another. The identity comes from the caller's own token, never from the
 *       request body;</li>
 *   <li><b>not short-lived</b>: two minutes, not an hour;</li>
 *   <li><b>interchangeable with the session token</b>: a game server that only checks the signature
 *       would accept the hour-long identity. The {@code scope} claim is what separates them, so the
 *       test also asserts the session token does not carry it.</li>
 * </ul>
 */
class ConnectionTokenTest {

    private static final int EXPECTED_TTL_SECONDS = 120;

    private static Vertx vertx;
    private static int port;
    private static JWTAuth jwtAuth;

    @BeforeAll
    static void start() throws Exception {
        vertx = Vertx.vertx();
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();

        jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions()
                .addJwk(RsaKeyPairProvider.fullJwkOf(keyPair)));
        HttpServer server = vertx.createHttpServer();
        server.requestHandler(AuthServiceApplication.createRouter(vertx, jwtAuth, keyPair));
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
    void theTokenVerifiesAgainstThePublishedJwksAndCarriesItsScope() throws Exception {
        String session = sessionToken("player-1");

        HttpResponse<String> response = post(session, "{\"matchId\":\"match-42\",\"team\":2}");

        assertEquals(200, response.statusCode(), response.body());
        JsonObject body = new JsonObject(response.body());
        String connectionToken = body.getString("connectionToken");
        assertNotNull(connectionToken);

        // What a game server does: fetch the public key once, verify offline.
        JWTAuth verifier = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(publishedJwk()));
        // authenticate() is the signature check a game server performs offline; the claims are read
        // from the payload so the assertion is about the token, not about the verifier's projection.
        assertNotNull(verifiedClaims(verifier, connectionToken), "the published JWK must verify it");
        JsonObject claims = claimsOf(connectionToken);

        assertEquals("player-1", claims.getString("sub"));
        assertEquals("match-42", claims.getString("matchId"));
        assertEquals(2, claims.getInteger("team"));
        assertEquals("game-connect", claims.getString("scope"));

        // A NumericDate comes back from the decoder as a number, not necessarily a Long.
        long expiresAt = claims.getNumber("exp").longValue();
        long now = Instant.now().getEpochSecond();
        assertEquals(EXPECTED_TTL_SECONDS, expiresAt - now, 5,
                "two minutes, not an hour: a leaked connection token is worth minutes");
        assertTrue(body.getLong("expiresInSeconds") == EXPECTED_TTL_SECONDS,
                "the response says the same thing the token does");
    }

    @Test
    void aSessionTokenIsNotAConnectionToken() throws Exception {
        JsonObject claims = claimsOf(sessionToken("player-2"));

        // The signature is valid (it is the same key), so this is exactly what a game server sees
        // when somebody tries to connect with his session token: a player, and no scope.
        assertEquals("player-2", claims.getString("sub"));
        assertTrue(claims.getString("scope") == null,
                "the session token must not carry scope=game-connect; the scope is what a game "
                        + "server checks to refuse it");
    }

    @Test
    void theSubjectComesFromTheCallersTokenNotFromTheBody() throws Exception {
        // A body that names another player is ignored: the token says who is asking.
        HttpResponse<String> response = post(sessionToken("player-3"),
                "{\"matchId\":\"match-42\",\"team\":1,\"playerId\":\"player-4\"}");

        assertEquals(200, response.statusCode(), response.body());
        JsonObject claims = claimsOf(new JsonObject(response.body()).getString("connectionToken"));
        assertEquals("player-3", claims.getString("sub"),
                "the issuer signs for the subject it verified, never for the one in the body");
    }

    @Test
    void aRequestWithoutASessionTokenIsRefused() throws Exception {
        assertEquals(401, post(null, "{\"matchId\":\"match-42\",\"team\":1}").statusCode());
    }

    @Test
    void aForgedSessionTokenIsRefused() throws Exception {
        String forged = sessionToken("player-5");
        String tampered = forged.substring(0, forged.length() - 4) + "AAAA";
        assertEquals(401, post(tampered, "{\"matchId\":\"match-42\",\"team\":1}").statusCode());
    }

    @Test
    void anIncompleteRequestIsRefused() throws Exception {
        String session = sessionToken("player-6");
        assertEquals(400, post(session, "{\"team\":1}").statusCode(), "matchId is required");
        assertEquals(400, post(session, "{\"matchId\":\"match-42\"}").statusCode(), "team is required");
        assertEquals(400, post(session, "{\"matchId\":\"match-42\",\"team\":3}").statusCode(),
                "team is 1 or 2");
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────

    private static String sessionToken(String playerId) {
        return jwtAuth.generateToken(new JsonObject().put("sub", playerId),
                new io.vertx.ext.auth.JWTOptions().setAlgorithm("RS256").setExpiresInSeconds(3600));
    }

    private static HttpResponse<String> post(String bearer, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/auth/connection-tokens"))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (bearer != null) {
            request.header("Authorization", "Bearer " + bearer);
        }
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static JsonObject publishedJwk() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/.well-known/jwks.json")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            return new JsonObject(response.body()).getJsonArray("keys").getJsonObject(0);
        }
    }

    /** The claims without verifying: the test only reads what the token says here. */
    private static JsonObject claimsOf(String token) {
        String payload = token.split("\\.")[1];
        return new JsonObject(new String(Base64.getUrlDecoder().decode(payload)));
    }

    private static JsonObject verifiedClaims(JWTAuth verifier, String token) throws Exception {
        return verifier.authenticate(new TokenCredentials(token))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
                .principal();
    }
}
