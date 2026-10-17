package com.victor.matchmaking.orchestrator.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.orchestrator.domain.AllocationRequest;
import com.victor.matchmaking.orchestrator.domain.AllocationStatus;
import com.victor.matchmaking.orchestrator.domain.ProcessedEvent;
import com.victor.matchmaking.orchestrator.domain.ProcessingOutcome;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

/**
 * The idempotency property, against the database that enforces it.
 *
 * <p>A mock would test the call order; the guarantee lives in a unique index and a
 * transaction, so the test needs both. This is Redis-like infrastructure for the
 * engine tests: a real database, because a fake one would delete the property.
 *
 * <p>Requires Postgres (docker compose up -d postgres) and cleans up after itself.
 */
class PostgresConfirmedMatchInboxTest {

    private static final String CONSUMER_GROUP = "w13-test";
    private static final String MATCH_ID_PREFIX = "match-w13-";
    private static final Instant CONFIRMED_AT = Instant.parse("2026-10-17T02:47:34.356185Z");

    private static Vertx vertx;
    private static PostgresConfirmedMatchInbox inbox;
    private static Pool probe;

    @BeforeAll
    static void connect() {
        DatabaseConfig config = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(config);
        vertx = Vertx.vertx();
        inbox = PostgresConfirmedMatchInbox.connect(vertx, config);
        // Second pool, used only to read what the inbox wrote.
        probe = DatabasePool.connect(vertx, config);
    }

    @AfterAll
    static void cleanup() {
        await(probe.preparedQuery("DELETE FROM match_allocations WHERE match_id LIKE ?")
                .execute(Tuple.of(MATCH_ID_PREFIX + "%")).mapEmpty());
        await(probe.preparedQuery("DELETE FROM processed_events WHERE consumer_group = ?")
                .execute(Tuple.of(CONSUMER_GROUP)).mapEmpty());
        inbox.close();
        probe.close();
        vertx.close();
    }

    @Test
    void theSameEventDeliveredTwiceIsAppliedOnce() {
        String matchId = MATCH_ID_PREFIX + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        AllocationRequest allocation = allocation(matchId);

        assertEquals(ProcessingOutcome.APPLIED, await(inbox.applyOnce(allocation, event(eventId))));
        // The same record again: a rebalance, a consumer restart, an operator resetting
        // the offsets to replay the topic. It is a success, not a second effect.
        assertEquals(ProcessingOutcome.DUPLICATE, await(inbox.applyOnce(allocation, event(eventId))));

        assertEquals(1, countAllocations(matchId));
        assertEquals(1, countProcessedEvents(eventId));
        assertProjection(allocation);
    }

    @Test
    void concurrentDeliveriesOfTheSameEventAreAppliedOnce() throws Exception {
        String matchId = MATCH_ID_PREFIX + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        AllocationRequest allocation = allocation(matchId);
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService consumers = Executors.newFixedThreadPool(2);
        List<ProcessingOutcome> outcomes;
        try {
            // Two consumers of the same group racing on one record: exactly what a
            // rebalance looks like from the database's point of view.
            java.util.concurrent.Future<ProcessingOutcome> first =
                    consumers.submit(consumer(start, allocation, eventId));
            java.util.concurrent.Future<ProcessingOutcome> second =
                    consumers.submit(consumer(start, allocation, eventId));
            start.countDown();
            outcomes = List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS));
        } finally {
            consumers.shutdownNow();
        }

        assertEquals(1, outcomes.stream().filter(outcome -> outcome == ProcessingOutcome.APPLIED).count());
        assertEquals(1, outcomes.stream().filter(outcome -> outcome == ProcessingOutcome.DUPLICATE).count());
        assertEquals(1, countAllocations(matchId));
        assertEquals(1, countProcessedEvents(eventId));
    }

    @Test
    void aSecondEventForTheSameMatchFailsInsteadOfBeingAbsorbed() {
        String matchId = MATCH_ID_PREFIX + UUID.randomUUID();
        UUID firstEvent = UUID.randomUUID();
        UUID secondEvent = UUID.randomUUID();
        AllocationRequest allocation = allocation(matchId);

        assertEquals(ProcessingOutcome.APPLIED, await(inbox.applyOnce(allocation, event(firstEvent))));
        // A different event id for the same match is a data conflict, not a redelivery:
        // the projection refuses the duplicate row and the whole transaction rolls
        // back, including the inbox row - so the record stays unhandled and unmarked.
        assertThrows(RuntimeException.class, () -> await(inbox.applyOnce(allocation, event(secondEvent))));

        assertEquals(1, countAllocations(matchId));
        assertEquals(1, countProcessedEvents(firstEvent));
        assertEquals(0, countProcessedEvents(secondEvent));
    }

    private static Callable<ProcessingOutcome> consumer(CountDownLatch start, AllocationRequest allocation,
            UUID eventId) {
        return () -> {
            start.await(30, TimeUnit.SECONDS);
            return await(inbox.applyOnce(allocation, event(eventId)));
        };
    }

    private static AllocationRequest allocation(String matchId) {
        return new AllocationRequest(matchId, GameMode.FIVE_V_FIVE, Region.EU, 10,
                AllocationStatus.PENDING_ALLOCATION, CONFIRMED_AT);
    }

    private static ProcessedEvent event(UUID eventId) {
        return new ProcessedEvent(eventId, CONSUMER_GROUP, "matchmaking.events.match-confirmed", 0, 7L, Instant.now());
    }

    private static void assertProjection(AllocationRequest expected) {
        Row row = await(probe.preparedQuery("SELECT game_mode, region, player_count, status, confirmed_at "
                + "FROM match_allocations WHERE match_id = ?").execute(Tuple.of(expected.matchId())))
                .iterator().next();
        assertEquals(expected.gameMode().label(), row.getString("game_mode"));
        assertEquals(expected.region().name(), row.getString("region"));
        assertEquals(expected.playerCount(), row.getInteger("player_count"));
        assertEquals(expected.status().name(), row.getString("status"));
        assertEquals(expected.confirmedAt(), row.getOffsetDateTime("confirmed_at").toInstant());
    }

    private static long countAllocations(String matchId) {
        return await(probe.preparedQuery("SELECT count(*) AS total FROM match_allocations WHERE match_id = ?")
                .execute(Tuple.of(matchId))).iterator().next().getLong("total");
    }

    private static long countProcessedEvents(UUID eventId) {
        return await(probe.preparedQuery("SELECT count(*) AS total FROM processed_events WHERE event_id = ?")
                .execute(Tuple.of(eventId))).iterator().next().getLong("total");
    }

    private static <T> T await(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }
}
