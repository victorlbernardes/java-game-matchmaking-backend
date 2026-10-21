package com.victor.matchmaking.orchestrator;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.db.GameServerRepository;
import com.victor.matchmaking.events.EventTopics;
import com.victor.matchmaking.orchestrator.infrastructure.KafkaMatchConfirmedConsumer;
import com.victor.matchmaking.orchestrator.infrastructure.PostgresConfirmedMatchInbox;
import com.victor.matchmaking.orchestrator.presentation.GameServerRoutes;

import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;

/**
 * Match orchestrator: consumes confirmed matches and places them on a game server.
 *
 * <p>W13 built the consuming half - a consumer group that projects each match once, however
 * often the event is delivered (see {@code ProcessConfirmedMatch}). W22 adds the half the
 * projection was waiting for (ADR 0031): a pool of game servers that register and heartbeat,
 * an allocation that shares the event's transaction so it cannot happen twice, and an
 * internal HTTP surface ({@code GameServerRoutes}) so a server has something to register
 * against. The pool is also what the engine reads to decide whether forming a match is worth
 * doing at all.
 */
public final class MatchOrchestratorApplication {

    /** Prometheus scrapes this service here; 9090 and 9091 already belong to the API and engine. */
    static final int METRICS_PORT = 9093;

    private static final System.Logger LOG = System.getLogger("orchestrator");

    /**
     * How often the allocation lifecycle is inspected (W23). One second is the resolution of every
     * deadline the watchdog enforces (30 s, 60 s, 90 s), and it is the same order of magnitude as the
     * engine's pairing tick - a retry that lands a second late costs a second, not a match.
     */
    static final long WATCHDOG_INTERVAL_MS = 1000;

    private MatchOrchestratorApplication() {
    }

    public static void main(String[] args) {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %5$s%n");
        java.util.Locale.setDefault(java.util.Locale.ENGLISH);

        DatabaseConfig database = DatabaseConfig.fromEnv();
        // Idempotent, so several services can run it at boot - which is what lets this
        // one start on its own against a fresh database.
        DatabaseMigrator.migrate(database);

        // The registry before the Vertx instance (see MatchmakingMetrics): the HTTP server's
        // own meters have to land in the registry this process scrapes.
        com.victor.matchmaking.metrics.MatchmakingMetrics metrics =
                new com.victor.matchmaking.metrics.MatchmakingMetrics();
        Vertx vertx = Vertx.vertx(metrics.vertxOptions());

        // One pool for the whole process: the inbox's transaction and the pool's queries are
        // two users of the same connections, and a second pool would only double them.
        Pool pool = DatabasePool.connect(vertx, database);
        int serverTtlSeconds = GameServerRepository.ttlSecondsFromEnv();
        GameServerRepository servers = new GameServerRepository(pool, serverTtlSeconds);

        PostgresConfirmedMatchInbox inbox = new PostgresConfirmedMatchInbox(pool, servers, metrics.registry());
        KafkaMatchConfirmedConsumer consumer = KafkaMatchConfirmedConsumer.fromEnv(inbox);

        // The half that keeps a confirmed match from becoming an orphan (W23, ADR 0032): retries,
        // the two deadlines, a server that died, and the players who never connected.
        com.victor.matchmaking.db.AllocationRepository allocations =
                new com.victor.matchmaking.db.AllocationRepository(pool, servers);
        com.victor.matchmaking.orchestrator.application.AllocationWatchdog watchdog =
                new com.victor.matchmaking.orchestrator.application.AllocationWatchdog(allocations,
                        metrics.registry(),
                        com.victor.matchmaking.orchestrator.application.AllocationWatchdog
                                .allocationTimeoutFromEnv(),
                        com.victor.matchmaking.orchestrator.application.AllocationWatchdog
                                .startTimeoutFromEnv(),
                        com.victor.matchmaking.orchestrator.application.AllocationWatchdog
                                .connectWindowFromEnv(),
                        serverTtlSeconds);
        vertx.setPeriodic(WATCHDOG_INTERVAL_MS, id -> watchdog.tick()
                .onFailure(error -> LOG.log(System.Logger.Level.ERROR,
                        "orchestrator.watchdog.failed error={0}", error.getMessage())));

        com.victor.matchmaking.metrics.MetricsEndpoint.start(vertx, metrics.registry(),
                        com.victor.matchmaking.metrics.MetricsEndpoint.portFromEnv(METRICS_PORT))
                .onFailure(error -> LOG.log(System.Logger.Level.ERROR,
                        "orchestrator.metrics.failed port={0} error={1}", METRICS_PORT, error.getMessage()));

        int httpPort = GameServerRoutes.portFromEnv();
        GameServerRoutes routes = new GameServerRoutes(vertx, servers, allocations, serverTtlSeconds,
                GameServerRoutes.tokenFromEnv());
        vertx.createHttpServer()
                .requestHandler(routes.router())
                .listen(httpPort)
                .onSuccess(server -> LOG.log(System.Logger.Level.INFO,
                        "orchestrator.http.started port={0} serverTtlSeconds={1}", httpPort, serverTtlSeconds))
                // A pool nobody can register against is a deployment that allocates nothing;
                // failing here is louder than starting and answering 404 to every server.
                .onFailure(error -> {
                    LOG.log(System.Logger.Level.ERROR, "orchestrator.http.failed port={0} error={1}",
                            httpPort, error.getMessage());
                    vertx.close();
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            inbox.close();
            vertx.close();
        }, "orchestrator-shutdown"));

        // A platform thread, not a virtual one: the thread spends its life inside
        // KafkaConsumer.poll, which is a blocking native-ish call with no benefit from
        // being unmounted, and pinning one thread buys nothing here.
        Thread.ofPlatform().name("match-confirmed-consumer").start(consumer::run);

        LOG.log(System.Logger.Level.INFO, "orchestrator.started topic={0} group={1}",
                EventTopics.MATCH_CONFIRMED, KafkaMatchConfirmedConsumer.CONSUMER_GROUP);
    }
}
