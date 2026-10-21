package com.victor.matchmaking.api;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;

import com.victor.matchmaking.redis.RedisTicketRepository;
import com.victor.matchmaking.api.presentation.HttpRoutes;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.redis.client.Redis;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;

/** Composition root: wires config, JWKS and HTTP server. */
public final class MatchmakingApiApplication {

    private static final int DEFAULT_PORT = 8080;
    private static final System.Logger LOG = System.getLogger("matchmaking.api");

    private MatchmakingApiApplication() {
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %5$s%n");
        java.util.Locale.setDefault(java.util.Locale.ENGLISH);
        // The registry exists before the Vertx instance: Vert.x binds its own meters (HTTP
        // server, event loop, JVM) to it, and the domain KPIs are added to the same one.
        com.victor.matchmaking.metrics.MatchmakingMetrics metrics =
                new com.victor.matchmaking.metrics.MatchmakingMetrics();
        Vertx vertx = Vertx.vertx(metrics.vertxOptions());
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", String.valueOf(DEFAULT_PORT)));
        int metricsPort = com.victor.matchmaking.metrics.MetricsEndpoint.portFromEnv(
                com.victor.matchmaking.metrics.MetricsEndpoint.DEFAULT_PORT);
        String jwksUrl = System.getenv().getOrDefault("AUTH_JWKS_URL", "http://localhost:8081/.well-known/jwks.json");

        String redisUri = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
        Redis redis = Redis.createClient(vertx, com.victor.matchmaking.redis.RedisClients.options(redisUri));
        int queueMaxDepth = com.victor.matchmaking.api.AdmissionConfig.queueMaxDepth();
        // The same QUEUE_PARTITIONS the engine runs with (W20, ADR 0028): both sides must agree
        // on how many keys a queue has, or an arrival lands in a partition nobody pairs from.
        int queuePartitions = com.victor.matchmaking.redis.QueuePartitions.partitionsFromEnv();
        com.victor.matchmaking.redis.TicketRepository repository =
                new RedisTicketRepository(redis, queueMaxDepth, queuePartitions);
        com.victor.matchmaking.redis.RateLimiter rateLimiter = com.victor.matchmaking.api.AdmissionConfig
                .rateLimiterFromEnv(redis);
        com.victor.matchmaking.redis.RedisPublisher publisher = new com.victor.matchmaking.redis.RedisPublisher(redis);
        // The instance's name (W19, ADR 0026). Two instances behind the same Redis must be
        // tellable apart: the name is logged, returned by /health and tagged on the socket-layer
        // metrics, so "which instance is this player on" has an answer that is not "guess".
        String instanceId = System.getenv().getOrDefault("INSTANCE_ID", "api-" + port);
        com.victor.matchmaking.api.presentation.ConnectionRegistry connectionRegistry =
                new com.victor.matchmaking.api.presentation.ConnectionRegistry(publisher, instanceId,
                        new com.victor.matchmaking.api.infrastructure.RedisPlayerPresence(redis),
                        new com.victor.matchmaking.api.infrastructure.RedisPendingConfirmationLookup(redis),
                        metrics.registry());
        com.victor.matchmaking.db.DatabaseConfig dbConfig = com.victor.matchmaking.db.DatabaseConfig.fromEnv();
        com.victor.matchmaking.db.DatabaseMigrator.migrate(dbConfig);
        com.victor.matchmaking.db.MatchRepository matchRepository =
                com.victor.matchmaking.db.MatchRepository.connect(vertx, dbConfig);
        // Readiness is built from the two dependencies the instance actually needs to serve a
        // player: the hot state it reads and writes, and the source of truth behind history
        // (W18, ADR 0024). /health stays liveness - the process is up - and is not a claim about
        // either of them.
        com.victor.matchmaking.api.presentation.Readiness readiness =
                new com.victor.matchmaking.api.presentation.Readiness(java.util.List.of(
                        new com.victor.matchmaking.api.presentation.Readiness.Check("redis",
                                () -> redis.send(io.vertx.redis.client.Request.cmd(
                                        io.vertx.redis.client.Command.PING)).map(response -> true)),
                        new com.victor.matchmaking.api.presentation.Readiness.Check("postgres",
                                () -> matchRepository.ping().map(ignored -> true))));
        com.victor.matchmaking.redis.RedisSubscriber.subscribe(vertx, redisUri,
                com.victor.matchmaking.redis.RedisKeys.CHANNEL_PLAYER_NOTIFICATIONS,
                msg -> connectionRegistry.deliver(com.victor.matchmaking.redis.RoutedEnvelope.fromJson(msg)));
        JWTAuth jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(fetchPublicJwk(jwksUrl)));
        // One shared, Redis-backed party store: a party created by one route must
        // be the same party the ticket (enqueue/cancel) routes resolve.
        com.victor.matchmaking.redis.PartyStore partyStore = new com.victor.matchmaking.redis.RedisPartyStore(redis);
        com.victor.matchmaking.domain.player.SkillProvider skillProvider =
                new com.victor.matchmaking.api.infrastructure.DeterministicSkillProvider();
        // The regions this deployment serves and the operator's declared distances (W21, ADR 0039).
        // Both are read once: a region list that changes under a running process would leave tickets
        // in queues nobody pairs, and a distance that changes per request is not a policy.
        com.victor.matchmaking.domain.ticket.ServedRegions servedRegions =
                com.victor.matchmaking.domain.ticket.ServedRegions.parse(
                        System.getenv(com.victor.matchmaking.domain.ticket.ServedRegions.ENV));
        com.victor.matchmaking.api.application.RegionPlacement placement =
                com.victor.matchmaking.api.application.RegionPlacement.parse(servedRegions,
                        System.getenv(com.victor.matchmaking.api.application.RegionPlacement.ENV_REGION_RTT_MS));
        com.victor.matchmaking.api.application.PlayerLatency playerLatency =
                new com.victor.matchmaking.api.infrastructure.RedisPlayerLatency(redis);
        // Where the player plays, and who signs the credential for it (W23, ADR 0033). The read model
        // is the orchestrator's projection; the signature is the auth service's, because the private
        // key lives in exactly one process.
        com.victor.matchmaking.api.presentation.MatchConnections matchConnections =
                new com.victor.matchmaking.api.presentation.MatchConnections(
                        com.victor.matchmaking.db.MatchConnectionRepository.connect(vertx, dbConfig),
                        new com.victor.matchmaking.api.infrastructure.ConnectionTokenClient(vertx,
                                com.victor.matchmaking.api.infrastructure.ConnectionTokenClient.baseUrlFromEnv()));
        Router router = HttpRoutes.createRouter(vertx, jwtAuth, repository, connectionRegistry, matchRepository,
                skillProvider, partyStore, metrics.registry(), rateLimiter, readiness, placement, playerLatency,
                matchConnections);

        com.victor.matchmaking.metrics.MetricsEndpoint.start(vertx, metrics.registry(), metricsPort)
                .onSuccess(s -> LOG.log(System.Logger.Level.INFO,
                        "metrics.started port={0} path={1}", String.valueOf(s.actualPort()),
                        com.victor.matchmaking.metrics.MetricsEndpoint.PATH))
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                        "metrics.start.failed port=" + metricsPort, err));

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router)
                .listen(port)
                .onSuccess(s -> LOG.log(System.Logger.Level.INFO,
                        "api.started port={0} instance={1} regions={2} declaredRtts={3} jwksUrl={4} authBaseUrl={5}",
                        String.valueOf(s.actualPort()), instanceId, servedRegions,
                        String.valueOf(placement.hasDeclaredRtts()), jwksUrl,
                        com.victor.matchmaking.api.infrastructure.ConnectionTokenClient.baseUrlFromEnv()))
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR, "api.start.failed port=" + port, err));
    }

    static Router createRouter(Vertx vertx, JWTAuth jwtAuth, com.victor.matchmaking.redis.TicketRepository repository) {
        return HttpRoutes.createRouter(vertx, jwtAuth, repository);
    }

    static Router createRouter(Vertx vertx, JWTAuth jwtAuth) {
        return HttpRoutes.createRouter(vertx, jwtAuth);
    }

    private static JsonObject fetchPublicJwk(String jwksUrl) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder().uri(URI.create(jwksUrl)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("failed to fetch JWKS: HTTP " + response.statusCode());
            }
            JsonArray keys = new JsonObject(response.body()).getJsonArray("keys");
            if (keys == null || keys.isEmpty()) {
                throw new IllegalStateException("JWKS has no keys");
            }
            return keys.getJsonObject(0);
        }
    }
}
