package com.victor.matchmaking.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.api.infrastructure.DeterministicSkillProvider;
import com.victor.matchmaking.api.infrastructure.InMemoryPartyStore;
import com.victor.matchmaking.api.presentation.HttpRoutes;
import com.victor.matchmaking.api.presentation.Readiness;
import com.victor.matchmaking.api.presentation.ConnectionRegistry;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.metrics.MetricNames;
import com.victor.matchmaking.redis.TicketRepository;

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
 * What the API says when a dependency is down (W18, ADR 0024).
 *
 * <p>Measured before this, with Redis stopped: {@code POST /v1/matchmaking/tickets} answered
 * {@code 500 {"error":"Connection refused: localhost/127.0.0.1:6379"}} — the wrong status (it says
 * "we broke", not "not now"), a {@code Retry-After} the client could have used but did not get, and
 * the internal address of the dependency in the body. And {@code /health} answered {@code 200 UP}
 * while nothing the instance existed for could work.
 *
 * <p>These tests pin the contract: 503 with a dependency name, never an internal address, counted
 * per dependency; {@code /health/ready} red and naming the dependency while {@code /health} stays
 * green because the process is fine; and a real bug still a 500, without the driver's message in
 * the body.
 */
class DependencyDegradationTest {

    private static final AtomicReference<Exception> HOT_STATE_FAILURE =
            new AtomicReference<>(new ConnectException("Connection refused: localhost/127.0.0.1:6379"));
    private static final AtomicReference<Exception> SOURCE_OF_TRUTH_FAILURE =
            new AtomicReference<>();

    private static Vertx vertx;
    private static int port;
    private static JWTAuth jwtAuth;
    private static SimpleMeterRegistry registry;

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

        registry = new SimpleMeterRegistry();
        UnavailableRepository repository = new UnavailableRepository();
        // The checks read the same switches the tests set: one server, wired once, asked the
        // questions the readiness endpoint will ask in production.
        Readiness readiness = new Readiness(List.of(
                new Readiness.Check("redis", () -> probe(HOT_STATE_FAILURE)),
                new Readiness.Check("postgres", () -> probe(SOURCE_OF_TRUTH_FAILURE))));
        Router router = HttpRoutes.createRouter(vertx, jwtAuth, repository, new ConnectionRegistry(null), null,
                new DeterministicSkillProvider(), new InMemoryPartyStore(), registry, null, readiness);
        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router);
        port = server.listen(0).toCompletionStage().toCompletableFuture()
                .get(10, TimeUnit.SECONDS).actualPort();
    }

    @BeforeEach
    void resetDependencies() {
        HOT_STATE_FAILURE.set(new ConnectException("Connection refused: localhost/127.0.0.1:6379"));
        SOURCE_OF_TRUTH_FAILURE.set(null);
        registry.clear();
    }

    @AfterAll
    static void stop() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void anUnavailableHotStateIsA503WithRetryAfterAndNoInternalAddress() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                            .uri(URI.create(base() + "/v1/matchmaking/tickets"))
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "{\"gameMode\":\"1v1\",\"region\":\"NA\",\"maxLatencyMs\":80}"))
                            .header("Authorization", "Bearer " + issueToken("w18-degraded-1"))
                            .header("content-type", "application/json").build(),
                    HttpResponse.BodyHandlers.ofString());

            assertEquals(503, response.statusCode(),
                    "an unreachable dependency is \"not now\", not \"we are broken\"");
            assertEquals("1", response.headers().firstValue("Retry-After").orElse(null),
                    "the client is told when to come back");
            JsonObject body = new JsonObject(response.body());
            assertEquals("hot state unavailable", body.getString("error"));
            assertEquals("redis", body.getString("dependency"));
            assertFalse(response.body().contains("localhost"), "no internal address in a response");
            assertFalse(response.body().contains("6379"), "no internal address in a response");
            assertEquals(1.0, registry.counter(MetricNames.DEPENDENCY_ERRORS,
                    MetricNames.TAG_DEPENDENCY, MetricNames.TAG_DEPENDENCY_REDIS).count(),
                    "the refusal is counted against the dependency that caused it");
        }
    }

    @Test
    void readinessNamesTheDependencyThatIsDownAndLivenessStaysUp() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> ready = client.send(HttpRequest.newBuilder()
                            .uri(URI.create(base() + "/health/ready")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(503, ready.statusCode(), "a dependency is down: do not route traffic here");
            JsonObject body = new JsonObject(ready.body());
            assertEquals("DOWN", body.getString("status"));
            assertEquals("DOWN", body.getJsonObject("checks").getString("redis"),
                    "readiness names which dependency is down");
            assertEquals("UP", body.getJsonObject("checks").getString("postgres"),
                    "and which ones are fine");

            HttpResponse<String> health = client.send(HttpRequest.newBuilder()
                            .uri(URI.create(base() + "/health")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, health.statusCode(),
                    "liveness is about the process: killing it would not fix the dependency");
        }
    }

    @Test
    void readinessIsUpWhenEveryDependencyAnswers() throws Exception {
        HOT_STATE_FAILURE.set(null);
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                            .uri(URI.create(base() + "/health/ready")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            JsonObject body = new JsonObject(response.body());
            assertEquals("UP", body.getString("status"));
            assertEquals("UP", body.getJsonObject("checks").getString("redis"));
            assertEquals("UP", body.getJsonObject("checks").getString("postgres"));
        }
    }

    @Test
    void aPooledConnectionTheServerClosedIsAlsoA503() throws Exception {
        // The wording a real outage produced (W18): the API's Postgres pool had an established
        // connection when Postgres stopped, so the failure is not "refused" but "closed" - a
        // detection that only knows how to read "connection refused" answers 500 here.
        HOT_STATE_FAILURE.set(new io.vertx.core.VertxException("This connection has been closed."));
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                            .uri(URI.create(base() + "/v1/matchmaking/tickets/ticket-w18-any"))
                            .header("Authorization", "Bearer " + issueToken("w18-degraded-4"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(503, response.statusCode(),
                    "a connection the server closed is an outage, not a bug in this service");
            assertEquals("redis", new JsonObject(response.body()).getString("dependency"));
        }
    }

    @Test
    void aBugIsStillA500AndTheBodyDoesNotCarryTheDriverMessage() throws Exception {
        HOT_STATE_FAILURE.set(new IllegalStateException(
                "column \"secret_shape\" does not exist at db-internal-host:5432"));
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                            .uri(URI.create(base() + "/v1/matchmaking/tickets"))
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "{\"gameMode\":\"1v1\",\"region\":\"NA\",\"maxLatencyMs\":80}"))
                            .header("Authorization", "Bearer " + issueToken("w18-degraded-2"))
                            .header("content-type", "application/json").build(),
                    HttpResponse.BodyHandlers.ofString());

            assertEquals(500, response.statusCode(), "a bug is a bug: 500, not a retry hint");
            assertTrue(response.body().contains("internal error"), response.body());
            assertFalse(response.body().contains("db-internal-host"), "the details stay in the log");
        }
    }

    @Test
    void readingATicketWithTheHotStateDownIsA503() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                            .uri(URI.create(base() + "/v1/matchmaking/tickets/ticket-w18-any"))
                            .header("Authorization", "Bearer " + issueToken("w18-degraded-3"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(503, response.statusCode(),
                    "every route that reads the hot state answers the same way when it is gone");
            assertEquals("redis", new JsonObject(response.body()).getString("dependency"));
        }
    }

    private static Future<Boolean> probe(AtomicReference<Exception> failure) {
        Exception current = failure.get();
        return current == null ? Future.succeededFuture(Boolean.TRUE) : Future.failedFuture(current);
    }

    private static String base() {
        return "http://localhost:" + port;
    }

    private static String issueToken(String playerId) {
        return jwtAuth.generateToken(new JsonObject().put("sub", playerId),
                new JWTOptions().setAlgorithm("RS256").setExpiresInSeconds(3600));
    }

    private static String b64(byte[] bytes) {
        if (bytes.length > 1 && bytes[0] == 0) {
            bytes = java.util.Arrays.copyOfRange(bytes, 1, bytes.length);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /** The hot state as the API sees it when Redis is gone: a connection that refuses. */
    private static final class UnavailableRepository implements TicketRepository {

        @Override
        public Future<String> createIdempotent(String idempotencyKey, MatchmakingTicket ticket) {
            return Future.failedFuture(HOT_STATE_FAILURE.get());
        }

        @Override
        public Future<Void> save(String idempotencyKey, MatchmakingTicket ticket) {
            return Future.failedFuture(HOT_STATE_FAILURE.get());
        }

        @Override
        public Future<MatchmakingTicket> findById(String ticketId) {
            return Future.failedFuture(HOT_STATE_FAILURE.get());
        }

        @Override
        public Future<MatchmakingTicket> findByIdempotencyKey(String key) {
            return Future.failedFuture(HOT_STATE_FAILURE.get());
        }

        @Override
        public Future<List<String>> createPartyTickets(List<MatchmakingTicket> tickets) {
            return Future.failedFuture(HOT_STATE_FAILURE.get());
        }

        @Override
        public Future<RequeueOutcome> requeue(MatchmakingTicket ticket) {
            return Future.failedFuture(HOT_STATE_FAILURE.get());
        }

        @Override
        public Future<Long> queueDepth(String gameModeLabel, String regionName) {
            return Future.failedFuture(HOT_STATE_FAILURE.get());
        }

        @Override
        public String newTicketId() {
            return "ticket-w18-unavailable";
        }

        @Override
        public java.time.Instant now() {
            return java.time.Instant.now();
        }
    }
}
