package com.victor.matchmaking.skill;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.events.EventSerialization;
import com.victor.matchmaking.events.EventTopics;
import com.victor.matchmaking.outbox.KafkaEventPublisher;
import com.victor.matchmaking.outbox.OutboxRelay;
import com.victor.matchmaking.outbox.PostgresOutboxQueue;
import com.victor.matchmaking.skill.infrastructure.KafkaMatchFinishedConsumer;
import com.victor.matchmaking.skill.infrastructure.PostgresSkillRepository;
import com.victor.matchmaking.skill.infrastructure.RedisSkillCache;
import com.victor.matchmaking.skill.presentation.SkillRoutes;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.redis.client.Redis;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * Skill service: consumes MATCH_FINISHED, recalculates Elo with calibration,
 * serves GET /v1/skills/{playerId} (W24, ADR 0035).
 */
public final class SkillServiceApplication {

    /** Prometheus scrapes this service here. */
    static final int METRICS_PORT = 9092;

    private static final System.Logger LOG = System.getLogger("skill-service");

    private SkillServiceApplication() {
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %5$s%n");
        java.util.Locale.setDefault(java.util.Locale.ENGLISH);

        DatabaseConfig database = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(database);

        com.victor.matchmaking.metrics.MatchmakingMetrics metrics =
                new com.victor.matchmaking.metrics.MatchmakingMetrics();
        Vertx vertx = Vertx.vertx(metrics.vertxOptions());

        // JWKS from auth-service for token validation.
        String jwksUrl = System.getenv().getOrDefault("AUTH_JWKS_URL", "http://localhost:8081/.well-known/jwks.json");
        JsonObject jwk = fetchPublicJwk(jwksUrl);
        JWTAuth jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(jwk));

        // Redis for skill cache.
        String redisUri = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
        Redis redis = Redis.createClient(vertx, com.victor.matchmaking.redis.RedisClients.options(redisUri));
        RedisSkillCache cache = new RedisSkillCache(redis);

        // Outbox + relay (generic, from common-outbox).
        com.victor.matchmaking.db.DatabaseConfig dbConfig = com.victor.matchmaking.db.DatabaseConfig.fromEnv();
        io.vertx.sqlclient.Pool pool = DatabasePool.connect(vertx, dbConfig);
        PostgresOutboxQueue outbox = new PostgresOutboxQueue(pool);
        KafkaEventPublisher kafkaPublisher = KafkaEventPublisher.fromEnv();
        OutboxRelay relay = new OutboxRelay(outbox, kafkaPublisher, Clock.systemUTC(), "skill-service-outbox");

        // Skill repository (uses the outbox).
        PostgresSkillRepository skillRepo = new PostgresSkillRepository(pool,
                KafkaMatchFinishedConsumer.CONSUMER_GROUP, outbox);

        // Consumer for MATCH_FINISHED.
        KafkaMatchFinishedConsumer consumer = KafkaMatchFinishedConsumer.fromEnv(skillRepo);

        // HTTP server.
        SkillRoutes routes = new SkillRoutes(skillRepo, cache, jwtAuth);
        int httpPort = SkillRoutes.portFromEnv();
        vertx.createHttpServer()
                .requestHandler(routes.router())
                .listen(httpPort)
                .onSuccess(server -> LOG.log(System.Logger.Level.INFO,
                        "skill-service.http.started port={0}", httpPort))
                .onFailure(err -> {
                    LOG.log(System.Logger.Level.ERROR, "skill-service.http.failed port={0} error={1}",
                            httpPort, err.getMessage());
                    vertx.close();
                });

        // Metrics endpoint.
        com.victor.matchmaking.metrics.MetricsEndpoint.start(vertx, metrics.registry(),
                        com.victor.matchmaking.metrics.MetricsEndpoint.portFromEnv(METRICS_PORT))
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                        "skill-service.metrics.failed port=" + METRICS_PORT, err));

        // Timers.
        vertx.setPeriodic(250, id -> relay.drainOnce()
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR, "skill-service.outbox.failed", err)));

        // Consumer thread.
        Thread.ofPlatform().name("match-finished-consumer").start(consumer::run);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            outbox.close();
            vertx.close();
        }, "skill-service-shutdown"));

        LOG.log(System.Logger.Level.INFO, "skill-service.started topic={0} group={1}",
                EventTopics.MATCH_FINISHED, KafkaMatchFinishedConsumer.CONSUMER_GROUP);
    }

    private static JsonObject fetchPublicJwk(String jwksUrl) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder().uri(URI.create(jwksUrl)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("failed to fetch JWKS: HTTP " + response.statusCode());
            }
            JsonObject keys = new JsonObject(response.body()).getJsonArray("keys").getJsonObject(0);
            return keys;
        }
    }
}