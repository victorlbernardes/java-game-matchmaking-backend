package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.OutboxQueue;
import com.victor.matchmaking.engine.application.OutboxRecord;
import com.victor.matchmaking.engine.application.PendingMatch;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.engine.infrastructure.RedisPendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.WriteGate;
import com.victor.matchmaking.metrics.MetricNames;
import com.victor.matchmaking.redis.RedisClients;
import com.victor.matchmaking.redis.RedisKeys;
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
import io.vertx.redis.client.Response;

/**
 * Property: a slow database changes what the engine does, not how much it asks of the database
 * (W17, ADR 0022).
 *
 * <p>Two halves of the same failure. The pairing is the source of the confirmations and one tick
 * can form hundreds of matches; if it keeps forming them while the database is behind, the
 * database receives every transaction at once and the engine's own throughput becomes the outage.
 * So:
 *
 * <ul>
 *   <li>while the write gate is saturated, a queue full of pairable players forms <b>no</b> match;</li>
 *   <li>a burst of confirmations never puts more than the limit of transactions in flight, and
 *       every one of them is eventually written (the deferred ones retry, they are not dropped).</li>
 * </ul>
 *
 * <p>The database is a fake here, and deliberately: what is being tested is the shape of the load
 * the engine produces, which is exactly what a fake can count. The real Postgres is what
 * {@code requests/backpressure/verify_load_shedding.sh} slows down with a trigger to measure the
 * same property end to end.
 */
class DbBackpressureTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String QUEUE_KEY = "queue:1v1:NA";
    private static final int LIMIT = 2;
    private static final int MATCHES = 12;
    /** The "slow database": long enough that several confirmations overlap. */
    private static final long WRITE_MILLIS = 120;

    private static Vertx vertx;
    private static Redis redis;

    @BeforeAll
    static void connect() throws Exception {
        vertx = Vertx.vertx();
        redis = Redis.createClient(vertx, RedisClients.options(REDIS_URI));
        // The engine loads the script at startup; the test runs the same pairing tick by hand.
        MatchmakingEngineApplication.PAIR_SCRIPT = MatchmakingEngineApplication.pairScript(redis);
        MatchmakingEngineApplication.PAIR_SCRIPT.load()
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @AfterAll
    static void close() throws Exception {
        clean();
        redis.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @BeforeEach
    void cleanBefore() throws Exception {
        clean();
    }

    @Test
    void aSaturatedDatabaseStopsThePairingInsteadOfFormingMoreMatches() throws Exception {
        TicketRepository tickets = new RedisTicketRepository(redis);
        PendingMatchStore pending = new RedisPendingMatchStore(redis);
        RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
        EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());

        Instant base = Instant.now();
        await(tickets.save(null, searching("ticket-w17-a", "w17-player-a", base)));
        await(tickets.save(null, searching("ticket-w17-b", "w17-player-b", base)));

        // The database is at its limit: one write in flight against a limit of one.
        WriteGate gate = WriteGate.of(1);
        assertTrue(gate.tryEnter());
        assertTrue(gate.saturated());

        MatchmakingEngineApplication.tryPairFromQueue(QUEUE_KEY, redis, tickets, pending, notifier, kpis, gate);

        // Nothing pairs while the database is behind: the players stay where they are, and the
        // pressure lands on the queue (where the admission cap can shed it), not on the database.
        TimeUnit.MILLISECONDS.sleep(500);
        assertEquals(2L, zcard(), "the players must still be waiting: the pairing is paused");
        assertNull(pendingMatchHolding("ticket-w17-a"),
                "no match may be formed while the database is saturated");
        gate.leave();

        // The moment the database is not behind, the same queue pairs again - the pause is a
        // pause, not a lost match.
        MatchmakingEngineApplication.tryPairFromQueue(QUEUE_KEY, redis, tickets, pending, notifier, kpis, gate);
        String matchId = awaitPendingMatchHolding("ticket-w17-a", 5);
        assertNotNull(matchId, "with a free write slot the queue pairs as usual");
        assertEquals(0L, zcard(), "the roster left the queue");
    }

    @Test
    void aConfirmationBurstNeverPutsMoreThanTheLimitInFlightAndWritesAllOfThem() throws Exception {
        TicketRepository tickets = new RedisTicketRepository(redis);
        PendingMatchStore pending = new RedisPendingMatchStore(redis);
        RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EngineKpis kpis = new EngineKpis(registry);
        SlowOutbox outbox = new SlowOutbox(vertx, WRITE_MILLIS);
        WriteGate gate = WriteGate.of(LIMIT);

        Instant now = Instant.now();
        List<String> matchIds = new ArrayList<>();
        for (int i = 0; i < MATCHES; i++) {
            String matchId = "match-w17-backpressure-" + i;
            matchIds.add(matchId);
            await(tickets.save(null, confirming("ticket-w17-" + i + "-a", "w17-" + i + "-a", now)));
            await(tickets.save(null, confirming("ticket-w17-" + i + "-b", "w17-" + i + "-b", now)));
            await(pending.save(new PendingMatch(matchId,
                    List.of(new PendingMatch.PendingPlayer("w17-" + i + "-a", "ticket-w17-" + i + "-a"),
                            new PendingMatch.PendingPlayer("w17-" + i + "-b", "ticket-w17-" + i + "-b")),
                    List.of(), now.plusSeconds(60))));
        }

        // Every match's two players confirm at once: MATCHES confirmations arrive together and
        // each one wants a transaction.
        for (int i = 0; i < MATCHES; i++) {
            String matchId = matchIds.get(i);
            MatchmakingEngineApplication.handleConfirmation(confirm(matchId, "w17-" + i + "-a"),
                    redis, tickets, pending, notifier, outbox, kpis, gate, vertx);
            MatchmakingEngineApplication.handleConfirmation(confirm(matchId, "w17-" + i + "-b"),
                    redis, tickets, pending, notifier, outbox, kpis, gate, vertx);
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (outbox.writes.get() < MATCHES && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        assertEquals(MATCHES, outbox.writes.get(), "every confirmed match must be written, none dropped");
        assertTrue(outbox.maxConcurrent.get() <= LIMIT,
                "the database must never see more than " + LIMIT + " writes at once, saw "
                        + outbox.maxConcurrent.get());
        double deferred = registry.counter(MetricNames.DB_WRITE_DEFERRED).count();
        assertTrue(deferred > 0,
                "with " + MATCHES + " matches and a limit of " + LIMIT + " some confirmations must wait");
        // The count of writes is incremented when a transaction starts; the slot comes back when
        // it finishes, so the last one needs a moment. Bounded: a permit that never comes back is
        // the failure this assertion exists to catch.
        long settleBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (gate.inFlight() > 0 && System.nanoTime() < settleBy) {
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertEquals(0, gate.inFlight(), "every permit comes back, even after the retries");
    }

    private static JsonObject confirm(String matchId, String playerId) {
        return new JsonObject().put("matchId", matchId).put("playerId", playerId).put("action", "CONFIRM");
    }

    private static MatchmakingTicket searching(String ticketId, String playerId, Instant queuedAt) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                queuedAt, TicketState.SEARCHING, 0L, null);
    }

    private static MatchmakingTicket confirming(String ticketId, String playerId, Instant queuedAt) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                queuedAt, TicketState.CONFIRMING, 1L, null);
    }

    /** A database that takes its time and counts how many transactions it is holding. */
    private static final class SlowOutbox implements OutboxQueue {

        private final Vertx vertx;
        private final long millis;
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();
        private final AtomicInteger writes = new AtomicInteger();

        SlowOutbox(Vertx vertx, long millis) {
            this.vertx = vertx;
            this.millis = millis;
        }

        @Override
        public Future<Void> writeMatchWithEvent(Match match, List<MatchPlayer> players, OutboxRecord event) {
            int now = concurrent.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            writes.incrementAndGet();
            return Future.future(promise -> vertx.setTimer(millis, id -> {
                concurrent.decrementAndGet();
                promise.complete();
            }));
        }

        @Override
        public Future<List<OutboxRecord>> claim(String relayId, int batchSize) {
            return Future.succeededFuture(List.of());
        }

        @Override
        public Future<Void> markPublished(String eventId, Instant publishedAt) {
            return Future.succeededFuture();
        }

        @Override
        public Future<Void> markFailed(String eventId, String error, Instant nextAttemptAt) {
            return Future.succeededFuture();
        }

        @Override
        public Future<Void> park(String eventId, String error) {
            return Future.succeededFuture();
        }
    }

    private static long zcard() throws Exception {
        Response response = await(redis.send(Request.cmd(Command.ZCARD).arg(QUEUE_KEY)));
        return response != null ? response.toLong() : 0L;
    }

    /** The pending match whose roster holds this ticket, or null. The engine's "a match formed". */
    private static String pendingMatchHolding(String ticketId) throws Exception {
        for (String key : allKeys("match-pending:*")) {
            Response json = await(redis.send(Request.cmd(Command.GET).arg(key)));
            if (json != null && json.toString().contains(ticketId)) {
                return key.substring("match-pending:".length());
            }
        }
        return null;
    }

    private static String awaitPendingMatchHolding(String ticketId, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            String matchId = pendingMatchHolding(ticketId);
            if (matchId != null) {
                return matchId;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        return null;
    }

    private static List<String> allKeys(String pattern) throws Exception {
        List<String> keys = new ArrayList<>();
        Response response = await(redis.send(Request.cmd(Command.KEYS).arg(pattern)));
        if (response != null) {
            for (int i = 0; i < response.size(); i++) {
                keys.add(response.get(i).toString());
            }
        }
        return keys;
    }

    /** Removes everything this test can write, so the suite never inherits its state. */
    private static void clean() throws Exception {
        await(redis.send(Request.cmd(Command.DEL)
                .arg(QUEUE_KEY).arg("ticket-w17-a").arg("ticket-w17-b")
                .arg("match-pending:match-w17-backpressure")));

        Request del = Request.cmd(Command.DEL);
        boolean anyPending = false;
        for (String key : allKeys("match-pending:*")) {
            String matchId = key.substring("match-pending:".length());
            // The prefix is not enough: the code under test names matches `match-<uuid>`, so a
            // pending match this test formed survived the clean-up and the next run's
            // `assertNull(pendingMatchHolding(...))` found it — the scenario answering with a
            // previous run's leftovers. The payload is what identifies the roster.
            if (matchId.startsWith("match-w17") || mentionsThisRoster(key)) {
                del = del.arg(key)
                        .arg(RedisKeys.matchConfirmedKey(matchId))
                        .arg(RedisKeys.matchConfirmedClaimKey(matchId));
                anyPending = true;
            }
        }
        for (String key : allKeys("player-pending:w17-*")) {
            del = del.arg(key);
            anyPending = true;
        }
        if (anyPending) {
            await(redis.send(del));
        }

        Request delTickets = Request.cmd(Command.DEL);
        boolean anyTicket = false;
        for (String key : allKeys("ticket:ticket-w17-*")) {
            delTickets = delTickets.arg(key);
            anyTicket = true;
        }
        // The confirmation counter and its claim outlive the pending match (they carry their own
        // TTL), and the pending key is already gone when the confirmation completes — so the loop
        // above finds nothing to clean them through. Left behind, they make the *next* run's first
        // confirmation a duplicate (`count=2 required=2`, `ignored=true`) and the burst writes
        // nothing at all: 12 expected, 0 written, 60 s of waiting.
        for (String key : allKeys("match-confirmed:match-w17-*")) {
            delTickets = delTickets.arg(key);
            anyTicket = true;
        }
        for (String key : allKeys("match-confirmed-claim:match-w17-*")) {
            delTickets = delTickets.arg(key);
            anyTicket = true;
        }
        if (anyTicket) {
            await(redis.send(delTickets));
        }
    }

    /** A pending match key whose payload names one of this test's tickets. */
    private static boolean mentionsThisRoster(String key) throws Exception {
        Response json = await(redis.send(Request.cmd(Command.GET).arg(key)));
        return json != null && json.toString().contains("ticket-w17-");
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.toCompletionStage().toCompletableFuture().get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("the test's own await timed out", e);
        }
    }
}
