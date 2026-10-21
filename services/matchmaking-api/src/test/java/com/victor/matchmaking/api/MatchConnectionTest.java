package com.victor.matchmaking.api;

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
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.api.application.ConnectionTokenIssuer;
import com.victor.matchmaking.api.application.PlayerLatency;
import com.victor.matchmaking.api.application.RegionPlacement;
import com.victor.matchmaking.api.infrastructure.DeterministicSkillProvider;
import com.victor.matchmaking.api.infrastructure.InMemoryPartyStore;
import com.victor.matchmaking.api.infrastructure.InMemoryTicketRepository;
import com.victor.matchmaking.api.presentation.HttpRoutes;
import com.victor.matchmaking.api.presentation.MatchConnections;
import com.victor.matchmaking.api.presentation.Readiness;
import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.db.MatchConnectionRepository;
import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Tuple;

/**
 * Property: the gateway hands out an address and a credential <b>only</b> when there is a server, and
 * only to a player of that match (W23, ADR 0033).
 *
 * <p>The failure this catches is the tempting shortcut: answer {@code 200} with whatever the row has,
 * including "no server yet", and let the client retry. A player who receives a connection token for a
 * match that is not on a server connects to nothing, burns the token, and cannot tell whether to wait,
 * reconnect or give up. So the answers are distinct and the tests pin each one: {@code 200} served,
 * {@code 409 not_allocated} while waiting, {@code 409 match_rejected} with the reason, {@code 409
 * match_finished} after the release, {@code 404} for a match the caller is not in (which must read the
 * same as a match that does not exist), and {@code 503} when the issuer is down.
 *
 * <p>The issuer is a fake here on purpose: what is being tested is the decision, not the signature
 * (that one is {@code ConnectionTokenTest} in the auth service, verified against a public JWK).
 *
 * <p>Requires Postgres ({@code docker compose up -d postgres}) and cleans up after itself.
 */
class MatchConnectionTest {

    private static final String PREFIX = "match-w23-conn-";
    private static final String SERVER_ID = "gs-w23-conn-1";
    private static final String SERVER_ADDRESS = "10.0.0.7:7777";

    private static Vertx vertx;
    private static Pool pool;
    private static MatchConnectionRepository connections;
    private static FakeIssuer issuer;
    private static JWTAuth jwtAuth;
    private static int port;
    private static String playerToken;

    @BeforeAll
    static void start() throws Exception {
        vertx = Vertx.vertx();
        DatabaseConfig config = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(config);
        pool = DatabasePool.connect(vertx, config);
        connections = new MatchConnectionRepository(pool);
        issuer = new FakeIssuer();

        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        JsonObject fullJwk = fullJwk(keyPair);
        jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(fullJwk));
        playerToken = jwtAuth.generateToken(new JsonObject().put("sub", "player-conn-1"),
                new JWTOptions().setAlgorithm("RS256").setExpiresInSeconds(3600));

        Router router = HttpRoutes.createRouter(vertx, jwtAuth, new InMemoryTicketRepository(), null, null,
                new DeterministicSkillProvider(), new InMemoryPartyStore(), new SimpleMeterRegistry(), null,
                Readiness.none(), RegionPlacement.all(), PlayerLatency.none(),
                new MatchConnections(connections, issuer));

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router);
        port = server.listen(0).toCompletionStage().toCompletableFuture()
                .get(10, TimeUnit.SECONDS).actualPort();
    }

    @AfterAll
    static void stop() throws Exception {
        deleteTestRows();
        pool.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @BeforeEach
    void clean() {
        deleteTestRows();
        issuer.reset();
    }

    @Test
    void anAllocatedMatchGivesTheAddressAndATokenForThisPlayer() throws Exception {
        String matchId = project(AllocationStatus.ALLOCATED, SERVER_ID, null);

        HttpResponse<String> response = get(matchId, playerToken);

        assertEquals(200, response.statusCode(), response.body());
        JsonObject body = new JsonObject(response.body());
        assertEquals(SERVER_ADDRESS, body.getString("serverAddress"));
        assertEquals("ALLOCATED", body.getString("status"));
        assertEquals(1, body.getInteger("team"));
        assertEquals("token-for-player-conn-1", body.getString("connectionToken"));
        assertNotNull(body.getString("expiresAt"));
        assertEquals(120L, body.getLong("expiresInSeconds"));
        assertEquals(playerToken, issuer.lastPlayerToken,
                "the caller's own token is the proof forwarded to the issuer");
    }

    @Test
    void aMatchWaitingForAServerIsNotAllocated() throws Exception {
        String matchId = project(AllocationStatus.PENDING_ALLOCATION, null, null);

        HttpResponse<String> response = get(matchId, playerToken);

        assertEquals(409, response.statusCode(), response.body());
        assertEquals("not_allocated", new JsonObject(response.body()).getString("error"));
        assertTrue(issuer.calls == 0, "no token is minted for a match that has nowhere to play");
    }

    @Test
    void aRejectedMatchSaysWhy() throws Exception {
        String matchId = project(AllocationStatus.REJECTED, SERVER_ID, "server_failed");

        HttpResponse<String> response = get(matchId, playerToken);

        assertEquals(409, response.statusCode(), response.body());
        JsonObject body = new JsonObject(response.body());
        assertEquals("match_rejected", body.getString("error"));
        assertEquals("server_failed", body.getString("reason"),
                "the reason is what makes the rejection actionable");
    }

    @Test
    void aFinishedMatchIsFinished() throws Exception {
        String matchId = project(AllocationStatus.RELEASED, SERVER_ID, null);

        HttpResponse<String> response = get(matchId, playerToken);

        assertEquals(409, response.statusCode(), response.body());
        assertEquals("match_finished", new JsonObject(response.body()).getString("error"));
    }

    @Test
    void aMatchWithoutItsServerRowDoesNotInventAnAddress() throws Exception {
        // ALLOCATED, but the pool row is gone (a server deleted under a live allocation). The
        // watchdog rejects it on its next tick; until then there is no address, and no token either.
        String matchId = project(AllocationStatus.ALLOCATED, "gs-w23-conn-missing", null);

        HttpResponse<String> response = get(matchId, playerToken);

        assertEquals(409, response.statusCode(), response.body());
        assertEquals("not_allocated", new JsonObject(response.body()).getString("error"));
        assertTrue(issuer.calls == 0);
    }

    @Test
    void aPlayerOutsideTheMatchGetsTheSameAnswerAsAMatchThatDoesNotExist() throws Exception {
        String matchId = project(AllocationStatus.ALLOCATED, SERVER_ID, null);
        String stranger = jwtAuth.generateToken(new JsonObject().put("sub", "player-not-in-it"),
                new JWTOptions().setAlgorithm("RS256").setExpiresInSeconds(3600));

        HttpResponse<String> strangerResponse = get(matchId, stranger);
        HttpResponse<String> unknownResponse = get(PREFIX + UUID.randomUUID(), stranger);

        assertEquals(404, strangerResponse.statusCode());
        assertEquals(404, unknownResponse.statusCode());
        assertEquals(new JsonObject(unknownResponse.body()).getString("error"),
                new JsonObject(strangerResponse.body()).getString("error"),
                "the two must be indistinguishable: the match's existence is not the stranger's business");
    }

    @Test
    void anotherPlayersMatchIsNotVisible() throws Exception {
        String matchId = project(AllocationStatus.ALLOCATED, SERVER_ID, null);
        // A second match, of another player, allocated on the same server.
        String otherMatch = PREFIX + UUID.randomUUID();
        projectFor(otherMatch, "player-somebody-else", AllocationStatus.ALLOCATED, SERVER_ID);

        assertEquals(200, get(matchId, playerToken).statusCode());
        assertEquals(404, get(otherMatch, playerToken).statusCode(),
                "the roster decides, not the server or the status");
    }

    @Test
    void anIssuerOutageIsATemporaryFailure() throws Exception {
        String matchId = project(AllocationStatus.ALLOCATED, SERVER_ID, null);
        issuer.fail = true;

        HttpResponse<String> response = get(matchId, playerToken);

        assertEquals(503, response.statusCode(), response.body());
        assertEquals("token_issuer_unavailable", new JsonObject(response.body()).getString("error"));
        assertNotNull(response.headers().firstValue("Retry-After").orElse(null),
                "503 without Retry-After teaches the client to give up instead of retrying");
    }

    @Test
    void aRequestWithoutATokenIsUnauthorized() throws Exception {
        String matchId = project(AllocationStatus.ALLOCATED, SERVER_ID, null);
        assertEquals(401, get(matchId, null).statusCode());
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────

    private static String project(AllocationStatus status, String serverId, String rejectionReason) {
        String matchId = PREFIX + UUID.randomUUID();
        projectFor(matchId, "player-conn-1", status, serverId);
        if (rejectionReason != null) {
            await(pool.preparedQuery("UPDATE match_allocations SET rejection_reason = ? WHERE match_id = ?")
                    .execute(Tuple.of(rejectionReason, matchId)).mapEmpty());
        }
        return matchId;
    }

    /** The row the inbox would have written, with one player in the roster. */
    private static void projectFor(String matchId, String playerId, AllocationStatus status, String serverId) {
        await(pool.preparedQuery("INSERT INTO game_servers (server_id, region, endpoint, game_modes, capacity, "
                + "allocated, draining, last_seen) VALUES (?, ?, ?, ARRAY[?], ?, 1, false, now()) "
                + "ON CONFLICT (server_id) DO NOTHING")
                .execute(Tuple.of(SERVER_ID, Region.NA.name(), SERVER_ADDRESS, GameMode.ONE_V_ONE.label(), 10))
                .mapEmpty());
        await(pool.preparedQuery("INSERT INTO match_allocations (match_id, game_mode, region, player_count, "
                + "status, confirmed_at, server_id) VALUES (?, ?, ?, ?, ?, now(), ?)")
                .execute(Tuple.of(matchId, GameMode.ONE_V_ONE.label(), Region.NA.name(), 1, status.name(),
                        serverId))
                .mapEmpty());
        await(pool.preparedQuery("INSERT INTO match_roster (match_id, player_id, ticket_id, team, skill_at_match) "
                + "VALUES (?, ?, ?, 1, 50)")
                .execute(Tuple.of(matchId, playerId, matchId + "-ticket")).mapEmpty());
    }

    private static HttpResponse<String> get(String matchId, String bearer) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/matches/" + matchId + "/connection"))
                .GET();
        if (bearer != null) {
            request.header("Authorization", "Bearer " + bearer);
        }
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static void deleteTestRows() {
        await(pool.preparedQuery("DELETE FROM match_allocations WHERE match_id LIKE ?")
                .execute(Tuple.of(PREFIX + "%")).mapEmpty());
        await(pool.preparedQuery("DELETE FROM game_servers WHERE server_id LIKE ?")
                .execute(Tuple.of(SERVER_ID + "%")).mapEmpty());
    }

    private static JsonObject fullJwk(KeyPair keyPair) {
        return new JsonObject()
                .put("kty", "RSA").put("use", "sig").put("alg", "RS256").put("kid", "test-key")
                .put("n", b64(((RSAPublicKey) keyPair.getPublic()).getModulus().toByteArray()))
                .put("e", b64(((RSAPublicKey) keyPair.getPublic()).getPublicExponent().toByteArray()))
                .put("d", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrivateExponent().toByteArray()))
                .put("p", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeP().toByteArray()))
                .put("q", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeQ().toByteArray()))
                .put("dp", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeExponentP().toByteArray()))
                .put("dq", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getPrimeExponentQ().toByteArray()))
                .put("qi", b64(((RSAPrivateCrtKey) keyPair.getPrivate()).getCrtCoefficient().toByteArray()));
    }

    private static String b64(byte[] bytes) {
        if (bytes.length > 1 && bytes[0] == 0) {
            bytes = java.util.Arrays.copyOfRange(bytes, 1, bytes.length);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    /** The issuer, replaced: the decision is what is under test, not the signature. */
    private static final class FakeIssuer implements ConnectionTokenIssuer {

        private volatile boolean fail;
        private volatile String lastPlayerToken;
        private volatile int calls;

        @Override
        public Future<IssuedConnectionToken> issue(String playerToken, String matchId, int team) {
            if (fail) {
                return Future.failedFuture(new RuntimeException("issuer down"));
            }
            calls++;
            lastPlayerToken = playerToken;
            return Future.succeededFuture(new IssuedConnectionToken("token-for-" + principalOf(playerToken),
                    Instant.now().plusSeconds(120), 120));
        }

        private void reset() {
            fail = false;
            lastPlayerToken = null;
            calls = 0;
        }

        /** The token is opaque to the route; the fake only needs a stable name for the assertion. */
        private static String principalOf(String token) {
            String payload = token.split("\\.")[1];
            return new JsonObject(new String(Base64.getUrlDecoder().decode(payload))).getString("sub");
        }
    }
}
