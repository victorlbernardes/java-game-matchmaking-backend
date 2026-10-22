package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.PendingMatch;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.engine.infrastructure.PostgresOutboxQueue;
import com.victor.matchmaking.engine.infrastructure.RedisPendingMatchStore;
import com.victor.matchmaking.redis.RedisClients;
import com.victor.matchmaking.redis.RedisPlayerNotifier;
import com.victor.matchmaking.redis.RedisPublisher;
import com.victor.matchmaking.redis.RedisTicketRepository;
import com.victor.matchmaking.redis.TicketRepository;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

/**
 * A confirmed match is announced once.
 *
 * <p>In a real burst the two players answer at the same moment and the engine reads both CONFIRM
 * messages while the match's confirmation set is already complete, so both callers used to persist
 * the match. The match row survived it - the id is the primary key and the insert is an upsert -
 * but the event next to it did not: the outbox got one row per confirmation. Measured by the W16
 * load test: 312 MATCH_CONFIRMED events for 204 matches, every duplicate a new event id that a
 * consumer cannot recognise as the same fact.
 *
 * <p>What pins the property now is Redis, not the database: the caller that deletes the
 * confirmation set is the only one allowed to confirm (the DEL is atomic and exactly one caller
 * can win it). This test fires many confirmations for one match at once and requires exactly one
 * match row and exactly one event. It needs Redis and Postgres - the duplicate hid in the
 * interaction between the two - and cleans up after itself.
 */
class RepeatedConfirmationTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String MATCH_ID = "match-w16-repeated-confirm";
    private static final String PLAYER_A = "w16-confirm-a";
    private static final String PLAYER_B = "w16-confirm-b";
    private static final String TICKET_A = "ticket-w16-confirm-a";
    private static final String TICKET_B = "ticket-w16-confirm-b";
    /** The burst: a message per player, repeated. */
    private static final int MESSAGES_PER_PLAYER = 6;

    private static Vertx vertx;
    private static Redis redis;
    private static Pool probe;
    private static PostgresOutboxQueue outbox;

    @BeforeAll
    static void connect() {
        DatabaseConfig config = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(config);
        vertx = Vertx.vertx();
        redis = Redis.createClient(vertx, RedisClients.options(REDIS_URI));
        outbox = PostgresOutboxQueue.connect(vertx, config);
        probe = DatabasePool.connect(vertx, config);
    }

    @BeforeEach
    void cleanBefore() throws Exception {
        clean();
    }

    @AfterAll
    static void cleanup() throws Exception {
        clean();
        outbox.close();
        probe.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void manyConfirmationsOfOneMatchWriteOneMatchAndOneEvent() throws Exception {
        TicketRepository tickets = new RedisTicketRepository(redis);
        PendingMatchStore pending = new RedisPendingMatchStore(redis);
        RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
        EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());

        Instant now = Instant.now();
        await(tickets.save(null, confirming(TICKET_A, PLAYER_A, now)));
        await(tickets.save(null, confirming(TICKET_B, PLAYER_B, now)));
        await(pending.save(pendingMatch(now)));

        // The burst, launched together: the race is between the callers' Redis round trips, so
        // releasing them at once is what makes the duplicate happen without the claim.
        Runnable messageA = () -> MatchmakingEngineApplication.handleConfirmation(
                confirm(PLAYER_A), redis, tickets, pending, notifier, outbox, kpis);
        Runnable messageB = () -> MatchmakingEngineApplication.handleConfirmation(
                confirm(PLAYER_B), redis, tickets, pending, notifier, outbox, kpis);
        ExecutorService senders = Executors.newFixedThreadPool(2 * MESSAGES_PER_PLAYER);
        try {
            for (int i = 0; i < MESSAGES_PER_PLAYER; i++) {
                senders.submit(messageA);
                senders.submit(messageB);
            }
        } finally {
            senders.shutdown();
            senders.awaitTermination(10, TimeUnit.SECONDS);
        }

        awaitMatchWritten();
        // Nothing is left in flight after this: a duplicate would have been written by now.
        TimeUnit.MILLISECONDS.sleep(1500);

        assertEquals(1, count("SELECT count(*) AS total FROM matches WHERE id = ?", MATCH_ID),
                "one match, however many confirmations arrived");
        assertEquals(2, count("SELECT count(*) AS total FROM match_players WHERE match_id = ?", MATCH_ID),
                "the roster is written once, with the match");
        assertEquals(1, count("SELECT count(*) AS total FROM outbox_events WHERE partition_key = ?", MATCH_ID),
                "one event per match: a second MATCH_CONFIRMED is a second fact nobody can undo");
        assertEquals(1, commandLong(Command.EXISTS, "match-confirmed-claim:" + MATCH_ID),
                "the match is spoken for: the claim is what refuses the next confirmation");
        assertTrue(commandLong(Command.TTL, "match-confirmed-claim:" + MATCH_ID) > 0,
                "the claim expires on its own (ADR 0025: every key has a TTL or a delete)");
        assertTrue(commandLong(Command.TTL, "match-confirmed:" + MATCH_ID) != -1,
                "a confirmation key never survives without an expiry (ADR 0025)");
    }

    private static PendingMatch pendingMatch(Instant now) {
        return new PendingMatch(MATCH_ID,
                List.of(new PendingMatch.PendingPlayer(PLAYER_A, TICKET_A),
                        new PendingMatch.PendingPlayer(PLAYER_B, TICKET_B)),
                List.of(), now.plusSeconds(60));
    }

    private static MatchmakingTicket confirming(String ticketId, String playerId, Instant queuedAt) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                queuedAt, TicketState.CONFIRMING, 1L, null);
    }

    private static JsonObject confirm(String playerId) {
        return new JsonObject().put("matchId", MATCH_ID).put("playerId", playerId).put("action", "CONFIRM");
    }

    /** The write is async; the test waits for the effect, and only then judges the count. */
    private static void awaitMatchWritten() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (count("SELECT count(*) AS total FROM matches WHERE id = ?", MATCH_ID) > 0) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
    }

    private static long count(String sql, String param) {
        Row row = await(probe.preparedQuery(sql).execute(Tuple.of(param))).iterator().next();
        return row.getLong("total");
    }

    private static long commandLong(Command command, String key) {
        var response = await(redis.send(Request.cmd(command).arg(key)));
        return response != null ? response.toLong() : 0L;
    }

    private static void clean() throws Exception {
        await(probe.preparedQuery("DELETE FROM outbox_events WHERE partition_key = ?")
                .execute(Tuple.of(MATCH_ID)).mapEmpty());
        await(probe.preparedQuery("DELETE FROM match_players WHERE match_id = ?")
                .execute(Tuple.of(MATCH_ID)).mapEmpty());
        await(probe.preparedQuery("DELETE FROM matches WHERE id = ?")
                .execute(Tuple.of(MATCH_ID)).mapEmpty());
        redis.send(Request.cmd(Command.DEL).arg("match-pending:" + MATCH_ID)
                .arg("match-confirmed:" + MATCH_ID)
                .arg("match-confirmed-claim:" + MATCH_ID)
                .arg("ticket:" + TICKET_A).arg("ticket:" + TICKET_B))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    /**
     * Bounded on purpose: this test talks to Postgres and Redis through a container, and a
     * stalled container must fail the test in a minute instead of hanging the suite (measured
     * twice during W16: an unbounded join turned a slow Docker VM into a 54-minute "run").
     */
    private static <T> T await(Future<T> future) {
        try {
            return future.toCompletionStage().toCompletableFuture().get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("the test's own await timed out", e);
        }
    }
}
