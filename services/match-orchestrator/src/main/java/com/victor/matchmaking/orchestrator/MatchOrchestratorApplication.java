package com.victor.matchmaking.orchestrator;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.events.EventTopics;
import com.victor.matchmaking.orchestrator.infrastructure.KafkaMatchConfirmedConsumer;
import com.victor.matchmaking.orchestrator.infrastructure.PostgresConfirmedMatchInbox;

import io.vertx.core.Vertx;

/**
 * Match orchestrator: consumes confirmed matches and keeps them ready for a game
 * server.
 *
 * <p>W13 covers the consuming half - a consumer group that projects each match once,
 * however often the event is delivered (see {@code ProcessConfirmedMatch}). Choosing
 * a server, and the capacity awareness that goes with it, is W22.
 */
public final class MatchOrchestratorApplication {

    private static final System.Logger LOG = System.getLogger("orchestrator");

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

        Vertx vertx = Vertx.vertx();
        PostgresConfirmedMatchInbox inbox = PostgresConfirmedMatchInbox.connect(vertx, database);
        KafkaMatchConfirmedConsumer consumer = KafkaMatchConfirmedConsumer.fromEnv(inbox);

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
