package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.ConnectException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
 * The durable fact goes first; the hot state follows (W18, ADR 0024).
 *
 * <p>Before this, a confirmation moved the tickets to {@code CONFIRMED} and only then tried to
 * write the match. With the database unavailable the tickets announced a match that was never
 * written — and the timeout sweep could not undo it, because it only requeues tickets that are
 * still {@code CONFIRMING}. Measured on the local stack: the row count for the match stayed 0, the
 * pending match was dissolved at its deadline, and both tickets stayed {@code CONFIRMED} with no
 * match to look up, permanently.
 *
 * <p>This test fails if the order regresses: with a source of truth that refuses every write, no
 * ticket may reach {@code CONFIRMED}, and once the match's claim expires the sweep must put the
 * players back in the queue (2/2 back to {@code SEARCHING}). It needs Redis; it does not need
 * Postgres, because the point is the state the engine leaves behind when Postgres is not there.
 */
class ConfirmationOrderingTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String MATCH_ID = "match-w18-ordering";
    private static final String PLAYER_A = "w18-order-a";
    private static final String PLAYER_B = "w18-order-b";
    private static final String TICKET_A = "ticket-w18-order-a";
    private static final String TICKET_B = "ticket-w18-order-b";
    private static final String QUEUE_KEY = "queue:1v1:NA";

    private static Vertx vertx;
    private static Redis redis;

    @BeforeAll
    static void connect() {
        vertx = Vertx.vertx();
        redis = Redis.createClient(vertx, RedisClients.options(REDIS_URI));
    }

    @BeforeEach
    void cleanBefore() throws Exception {
        clean();
    }

    @AfterAll
    static void cleanup() throws Exception {
        clean();
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void aFailedDurableWriteLeavesTheTicketsConfirmingAndTheSweepRequeuesThem() throws Exception {
        TicketRepository tickets = new RedisTicketRepository(redis);
        PendingMatchStore pending = new RedisPendingMatchStore(redis);
        RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
        EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());

        Instant now = Instant.now();
        await(tickets.save(null, confirming(TICKET_A, PLAYER_A, now)));
        await(tickets.save(null, confirming(TICKET_B, PLAYER_B, now)));
        // Already past its deadline: the sweep judges it as soon as it is asked to.
        await(pending.save(pendingMatch(now.minusSeconds(1))));

        // Both players confirm; the engine cannot reach the source of truth.
        await(confirmInBackground(PLAYER_A, tickets, pending, notifier, kpis));
        await(confirmInBackground(PLAYER_B, tickets, pending, notifier, kpis));
        awaitEventually(() -> exists(RedisKeys.matchConfirmedClaimKey(MATCH_ID)));

        // The property: the hot state did NOT move. No ticket says CONFIRMED for a match with no
        // row, and the pending match is still there for the sweep to act on.
        assertEquals(TicketState.CONFIRMING, await(tickets.findById(TICKET_A)).status(),
                "a ticket must not be CONFIRMED before the match is durable");
        assertEquals(TicketState.CONFIRMING, await(tickets.findById(TICKET_B)).status(),
                "a ticket must not be CONFIRMED before the match is durable");
        assertNotNull(await(pending.findById(MATCH_ID)),
                "the pending match survives the failed write: it is what the recovery works from");

        // The claim owns the match while a write is in flight; once it expires the sweep is free to
        // dissolve. Expiring it here rather than sleeping out its real TTL keeps the test honest
        // about the sequence and quick about the wait.
        command(Request.cmd(Command.PEXPIRE)
                .arg(RedisKeys.matchConfirmedClaimKey(MATCH_ID)).arg("50"));
        TimeUnit.MILLISECONDS.sleep(200);

        MatchmakingEngineApplication.sweepExpired(redis, tickets, pending, notifier, kpis);
        awaitEventually(() -> "SEARCHING".equals(statusOf(tickets, TICKET_A))
                && "SEARCHING".equals(statusOf(tickets, TICKET_B)));

        assertEquals(TicketState.SEARCHING, await(tickets.findById(TICKET_A)).status(),
                "the sweep returns the player to the queue");
        assertEquals(TicketState.SEARCHING, await(tickets.findById(TICKET_B)).status(),
                "the sweep returns the player to the queue");
        assertEquals(2, await(queueDepth()), "both players are back in the queue, not stranded");
        assertNull(await(pending.findById(MATCH_ID)), "the expired match is gone");
    }

    @Test
    void aSuccessfulWriteMovesTheTicketsToConfirmedAfterTheFact() throws Exception {
        TicketRepository tickets = new RedisTicketRepository(redis);
        PendingMatchStore pending = new RedisPendingMatchStore(redis);
        RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
        EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());
        RecordingOutbox outbox = new RecordingOutbox();

        Instant now = Instant.now();
        await(tickets.save(null, confirming(TICKET_A, PLAYER_A, now)));
        await(tickets.save(null, confirming(TICKET_B, PLAYER_B, now)));
        await(pending.save(pendingMatch(now.plusSeconds(60))));

        await(confirmInBackground(PLAYER_A, tickets, pending, notifier, kpis, outbox));
        await(confirmInBackground(PLAYER_B, tickets, pending, notifier, kpis, outbox));
        awaitEventually(() -> outbox.writtenMatches.size() == 1);

        assertEquals(1, outbox.writtenMatches.size(), "the match is written once, by the winning claim");
        assertEquals(MATCH_ID, outbox.writtenMatches.get(0).id());
        assertEquals(2, outbox.writtenPlayers.size(), "the roster travels with the match");
        awaitEventually(() -> "CONFIRMED".equals(statusOf(tickets, TICKET_A))
                && "CONFIRMED".equals(statusOf(tickets, TICKET_B)));
        assertEquals(TicketState.CONFIRMED, await(tickets.findById(TICKET_A)).status(),
                "after the row exists, the ticket may say CONFIRMED");
        assertEquals(TicketState.CONFIRMED, await(tickets.findById(TICKET_B)).status(),
                "after the row exists, the ticket may say CONFIRMED");
        awaitEventually(() -> pendingDeleted(pending));
        assertEquals(0L, await(queueDepth()), "a confirmed ticket leaves the queue");
        assertNull(await(pending.findById(MATCH_ID)), "a confirmed match stops being pending");
    }

    private static Future<Void> confirmInBackground(String playerId, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis) {
        return confirmInBackground(playerId, tickets, pending, notifier, kpis, new UnavailableOutbox());
    }

    private static Future<Void> confirmInBackground(String playerId, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis, OutboxQueue outbox) {
        // The 7-argument entry point runs without a scheduler, which is what makes the assertions
        // deterministic: a failed write is not retried behind the test's back.
        MatchmakingEngineApplication.handleConfirmation(confirm(playerId), redis, tickets, pending,
                notifier, outbox, kpis);
        return Future.succeededFuture();
    }

    private static boolean pendingDeleted(PendingMatchStore pending) {
        try {
            return pending.findById(MATCH_ID).toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS) == null;
        } catch (Exception e) {
            return false;
        }
    }

    private static String statusOf(TicketRepository tickets, String ticketId) {
        try {
            MatchmakingTicket ticket = tickets.findById(ticketId)
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            return ticket != null ? ticket.status().name() : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Future<Long> queueDepth() {
        return redis.send(Request.cmd(Command.ZCARD).arg(QUEUE_KEY))
                .map(response -> response != null ? response.toLong() : 0L);
    }

    private static boolean exists(String key) {
        try {
            Response response = redis.send(Request.cmd(Command.EXISTS).arg(key))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            return response != null && response.toLong() == 1L;
        } catch (Exception e) {
            return false;
        }
    }

    /** Polls until the condition holds (the engine's work is async); fails after 10 s. */
    private static void awaitEventually(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("condition not met within 10s");
    }

    private static PendingMatch pendingMatch(Instant expiresAt) {
        return new PendingMatch(MATCH_ID,
                List.of(new PendingMatch.PendingPlayer(PLAYER_A, TICKET_A),
                        new PendingMatch.PendingPlayer(PLAYER_B, TICKET_B)),
                List.of(), expiresAt);
    }

    private static MatchmakingTicket confirming(String ticketId, String playerId, Instant queuedAt) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                queuedAt, TicketState.CONFIRMING, 1L, null);
    }

    private static JsonObject confirm(String playerId) {
        return new JsonObject().put("matchId", MATCH_ID).put("playerId", playerId).put("action", "CONFIRM");
    }

    private static void clean() throws Exception {
        command(Request.cmd(Command.DEL)
                .arg("match-pending:" + MATCH_ID)
                .arg(RedisKeys.matchConfirmedKey(MATCH_ID))
                .arg(RedisKeys.matchConfirmedClaimKey(MATCH_ID))
                .arg("ticket:" + TICKET_A).arg("ticket:" + TICKET_B)
                .arg(QUEUE_KEY));
    }

    private static Response command(Request request) throws Exception {
        return redis.send(request).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    /** A source of truth that refuses every write: Postgres, as the engine sees it when it is down. */
    private static final class UnavailableOutbox implements OutboxQueue {
        @Override
        public Future<Void> writeMatchWithEvent(Match match, List<MatchPlayer> players, OutboxRecord event) {
            return Future.failedFuture(new ConnectException("Connection refused: localhost:5432"));
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

    /** An outbox that accepts everything and remembers what it was given. */
    private static final class RecordingOutbox implements OutboxQueue {
        private final List<Match> writtenMatches = new ArrayList<>();
        private final List<MatchPlayer> writtenPlayers = new ArrayList<>();

        @Override
        public Future<Void> writeMatchWithEvent(Match match, List<MatchPlayer> players, OutboxRecord event) {
            writtenMatches.add(match);
            writtenPlayers.addAll(players);
            return Future.succeededFuture();
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
}
