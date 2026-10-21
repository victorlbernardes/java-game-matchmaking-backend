package com.victor.matchmaking.orchestrator.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.db.GameServerRepository;
import com.victor.matchmaking.db.GameServerRepository.Registration;
import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.metrics.MetricNames;
import com.victor.matchmaking.orchestrator.domain.AllocationRequest;
import com.victor.matchmaking.orchestrator.domain.ProcessedEvent;
import com.victor.matchmaking.orchestrator.domain.ProcessingOutcome;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

/**
 * Property: the allocation is part of the event's effect, so it happens exactly once and never
 * over-commits a server (W22, ADR 0031).
 *
 * <p>Two ways to get this wrong, and the two that matter:
 *
 * <ul>
 *   <li>a <b>redelivery</b> takes a second slot — the inbox prevents it, because the slot is taken
 *       in the same transaction as the insert that proves the event was new;</li>
 *   <li>two <b>confirmations arriving together</b> take the same last slot — the row lock and the
 *       {@code allocated < capacity} predicate prevent it, and the loser stays
 *       {@code PENDING_ALLOCATION} instead of disappearing.</li>
 * </ul>
 *
 * <p>Requires Postgres (docker compose up -d postgres) and cleans up after itself.
 */
class AllocationOnConfirmationTest {

    private static final String CONSUMER_GROUP = "w22-test";
    private static final String MATCH_PREFIX = "match-w22-";
    private static final String SERVER_PREFIX = "gs-w22-";
    private static final String TOPIC = "matchmaking.events.match-confirmed";
    private static final Instant CONFIRMED_AT = Instant.parse("2026-10-20T14:00:00Z");

    private static Vertx vertx;
    private static Pool pool;
    private static GameServerRepository servers;
    private static SimpleMeterRegistry registry;
    private static PostgresConfirmedMatchInbox inbox;

    @BeforeAll
    static void connect() {
        DatabaseConfig config = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(config);
        vertx = Vertx.vertx();
        pool = DatabasePool.connect(vertx, config);
        servers = new GameServerRepository(pool);
        registry = new SimpleMeterRegistry();
        inbox = new PostgresConfirmedMatchInbox(pool, servers, registry);
    }

    @BeforeEach
    void clean() {
        await(pool.preparedQuery("DELETE FROM match_allocations WHERE match_id LIKE ?")
                .execute(Tuple.of(MATCH_PREFIX + "%")).mapEmpty());
        await(pool.preparedQuery("DELETE FROM processed_events WHERE consumer_group = ?")
                .execute(Tuple.of(CONSUMER_GROUP)).mapEmpty());
        await(pool.preparedQuery("DELETE FROM game_servers WHERE server_id LIKE ?")
                .execute(Tuple.of(SERVER_PREFIX + "%")).mapEmpty());
        // The registry outlives one test: without this the counters would be asserted as whether
        // the suite ran before, not as what this test did.
        registry.clear();
    }

    @AfterAll
    static void close() {
        // A game_servers row is live state that a running engine reads: leaving one behind makes
        // the region authoritative with zero slots and stops every match in it (W22: a leftover
        // `gs-w22-busy` from this class broke the E2E until somebody deleted it).
        await(pool.preparedQuery("DELETE FROM match_allocations WHERE match_id LIKE ?")
                .execute(Tuple.of(MATCH_PREFIX + "%")).mapEmpty());
        await(pool.preparedQuery("DELETE FROM processed_events WHERE consumer_group = ?")
                .execute(Tuple.of(CONSUMER_GROUP)).mapEmpty());
        await(pool.preparedQuery("DELETE FROM game_servers WHERE server_id LIKE ?")
                .execute(Tuple.of(SERVER_PREFIX + "%")).mapEmpty());
        pool.close();
        vertx.close();
    }

    @Test
    void aConfirmedMatchTakesASlotAndTheRowNamesTheServer() {
        await(servers.register(server("one", 2)));

        assertEquals(ProcessingOutcome.APPLIED, apply(event(), allocation(MATCH_PREFIX + "one")));

        Row allocation = findAllocation(MATCH_PREFIX + "one");
        assertEquals(AllocationStatus.ALLOCATED.name(), allocation.getString("status"));
        assertEquals(SERVER_PREFIX + "one", allocation.getString("server_id"));
        assertTrue(allocation.getValue("allocated_at") != null, "the row says when it took the slot");
        assertEquals(1, await(servers.find(SERVER_PREFIX + "one")).orElseThrow().allocated());
        assertEquals(1.0, counted(MetricNames.RESULT_ALLOCATED), "one allocation, counted after the commit");
    }

    @Test
    void aRedeliveryDoesNotTakeASecondSlot() {
        await(servers.register(server("once", 2)));
        AllocationRequest allocation = allocation(MATCH_PREFIX + "once");
        ProcessedEvent event = event();

        assertEquals(ProcessingOutcome.APPLIED, apply(event, allocation));
        assertEquals(ProcessingOutcome.DUPLICATE, apply(event, allocation),
                "the inbox has seen this event id: nothing is applied again");

        assertEquals(1, await(servers.find(SERVER_PREFIX + "once")).orElseThrow().allocated(),
                "one match, one slot - a redelivery is not a second match");
        assertEquals(1.0, counted(MetricNames.RESULT_ALLOCATED), "and the counter does not count it again");
    }

    @Test
    void aFullPoolLeavesTheMatchPending() {
        await(servers.register(server("busy", 1)));
        assertEquals(ProcessingOutcome.APPLIED, apply(event(), allocation(MATCH_PREFIX + "first")));
        assertEquals(ProcessingOutcome.APPLIED, apply(event(), allocation(MATCH_PREFIX + "second")));

        Row second = findAllocation(MATCH_PREFIX + "second");
        assertEquals(AllocationStatus.PENDING_ALLOCATION.name(), second.getString("status"),
                "confirmed, projected, and waiting for a server (W23 gives it a deadline)");
        assertNull(second.getString("server_id"));
        assertEquals(1, await(servers.find(SERVER_PREFIX + "busy")).orElseThrow().allocated());
        assertEquals(1.0, counted(MetricNames.RESULT_NO_CAPACITY));
    }

    @Test
    void twoConfirmationsCannotTakeTheSameLastSlot() {
        await(servers.register(server("last", 1)));
        String firstMatch = MATCH_PREFIX + "race-" + UUID.randomUUID();
        String secondMatch = MATCH_PREFIX + "race-" + UUID.randomUUID();

        // Both events are new and both are applied: the inbox is not the guard here, the
        // pool's row lock is - which is exactly the case a rebalance does not produce.
        await(Future.all(
                inbox.applyOnce(allocation(firstMatch), event()),
                inbox.applyOnce(allocation(secondMatch), event())));

        List<String> statuses = statuses(firstMatch, secondMatch);
        assertEquals(1, statuses.stream().filter(AllocationStatus.ALLOCATED.name()::equals).count(),
                "exactly one match gets the slot, got " + statuses);
        assertEquals(1, statuses.stream().filter(AllocationStatus.PENDING_ALLOCATION.name()::equals).count());
        assertEquals(1, await(servers.find(SERVER_PREFIX + "last")).orElseThrow().allocated(),
                "the server is never over-committed");
        assertEquals(1.0, counted(MetricNames.RESULT_ALLOCATED));
        assertEquals(1.0, counted(MetricNames.RESULT_NO_CAPACITY));
    }

    @Test
    void aReleasedSlotCanBeUsedByTheNextMatch() {
        await(servers.register(server("cycle", 1)));
        apply(event(), allocation(MATCH_PREFIX + "running"));

        assertTrue(await(servers.release(SERVER_PREFIX + "cycle", MATCH_PREFIX + "running")).isPresent());
        assertEquals(ProcessingOutcome.APPLIED, apply(event(), allocation(MATCH_PREFIX + "next")));

        assertEquals(AllocationStatus.ALLOCATED.name(),
                findAllocation(MATCH_PREFIX + "next").getString("status"),
                "the slot the finished match gave back is available again");
        assertEquals(1, await(servers.find(SERVER_PREFIX + "cycle")).orElseThrow().allocated());
    }

    private static Registration server(String suffix, int capacity) {
        return new Registration(SERVER_PREFIX + suffix, Region.NA, "10.0.0.9:7777",
                List.of(GameMode.ONE_V_ONE), capacity);
    }

    private static AllocationRequest allocation(String matchId) {
        return new AllocationRequest(matchId, GameMode.ONE_V_ONE, Region.NA, roster(2),
                AllocationStatus.PENDING_ALLOCATION, CONFIRMED_AT);
    }

    private static ProcessedEvent event() {
        return new ProcessedEvent(UUID.randomUUID(), CONSUMER_GROUP, TOPIC, 0, 42L, Instant.now());
    }

    /** The roster the allocation needs since W23: one ticket per player, teams split in half. */
    private static java.util.List<AllocationRequest.RosterEntry> roster(int players) {
        java.util.List<AllocationRequest.RosterEntry> entries = new java.util.ArrayList<>(players);
        for (int i = 1; i <= players; i++) {
            entries.add(new AllocationRequest.RosterEntry("player-" + i, "ticket-" + i,
                    i <= players / 2 ? 1 : 2, 50));
        }
        return entries;
    }

    private static ProcessingOutcome apply(ProcessedEvent event, AllocationRequest allocation) {
        return await(inbox.applyOnce(allocation, event));
    }

    private static Row findAllocation(String matchId) {
        RowSet<Row> rows = await(pool.preparedQuery("SELECT status, server_id, allocated_at FROM match_allocations "
                + "WHERE match_id = ?").execute(Tuple.of(matchId)));
        var iterator = rows.iterator();
        assertTrue(iterator.hasNext(), "the projection row exists for " + matchId);
        return iterator.next();
    }

    private static List<String> statuses(String firstMatch, String secondMatch) {
        RowSet<Row> rows = await(pool.preparedQuery("SELECT status FROM match_allocations WHERE match_id IN (?, ?)")
                .execute(Tuple.of(firstMatch, secondMatch)));
        List<String> statuses = new java.util.ArrayList<>();
        rows.forEach(row -> statuses.add(row.getString("status")));
        return statuses;
    }

    private static double counted(String result) {
        Counter counter = registry.find(MetricNames.ALLOCATIONS).tag(MetricNames.TAG_RESULT, result).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.toCompletionStage().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
