package com.victor.matchmaking.orchestrator.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.db.AllocationRepository;
import com.victor.matchmaking.db.AllocationRepository.PendingMatch;
import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.db.GameServerRepository;
import com.victor.matchmaking.db.GameServerRepository.Registration;
import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.domain.match.RejectionReason;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.metrics.MetricNames;
import com.victor.matchmaking.orchestrator.application.AllocationWatchdog;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

/**
 * Property: a confirmed match never becomes an orphan (W23, ADR 0032).
 *
 * <p>The failures this has to catch, each one a match that dies in a different way:
 *
 * <ul>
 *   <li>a match <b>waits forever</b> because nobody retries the allocation — it must be allocated on
 *       the first slot that appears, and rejected at the deadline when none does;</li>
 *   <li>a match <b>holds a slot it never uses</b> because the server never started it — the slot must
 *       go back before it starves the next match;</li>
 *   <li>a match <b>holds a slot on a dead server</b> because the {@code release} never arrived — the
 *       staleness is what has to end it;</li>
 *   <li>and the retry itself must not <b>leak</b> a slot when the row stops being pending between the
 *       reserve and the flip.</li>
 * </ul>
 *
 * <p>Requires Postgres ({@code docker compose up -d postgres}) and cleans up after itself: a leftover
 * {@code game_servers} row makes a region authoritative with zero slots.
 */
class AllocationTimeoutTest {

    private static final String MATCH_PREFIX = "match-w23-";
    private static final String SERVER_PREFIX = "gs-w23-";

    private static final int ALLOCATION_TIMEOUT_SECONDS = 30;
    private static final int START_TIMEOUT_SECONDS = 60;
    private static final int CONNECT_WINDOW_SECONDS = 90;
    private static final int SERVER_TTL_SECONDS = 30;

    private static Vertx vertx;
    private static Pool pool;
    private static GameServerRepository servers;
    private static AllocationRepository allocations;
    private static SimpleMeterRegistry registry;
    private static AllocationWatchdog watchdog;

    @BeforeAll
    static void connect() {
        DatabaseConfig config = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(config);
        vertx = Vertx.vertx();
        pool = DatabasePool.connect(vertx, config);
        servers = new GameServerRepository(pool);
        allocations = new AllocationRepository(pool, servers);
        registry = new SimpleMeterRegistry();
        watchdog = new AllocationWatchdog(allocations, registry, ALLOCATION_TIMEOUT_SECONDS,
                START_TIMEOUT_SECONDS, CONNECT_WINDOW_SECONDS, SERVER_TTL_SECONDS);
    }

    @BeforeEach
    void clean() {
        deleteTestRows();
        registry.clear();
    }

    @AfterAll
    static void close() {
        deleteTestRows();
        pool.close();
        vertx.close();
    }

    // ── the deadline: waiting has an end ------------------------------------------------

    @Test
    void aMatchThatWaitsPastItsDeadlineIsRejectedWithItsReason() {
        // 31 s of waiting against a 30 s deadline: the match is old enough to give up on.
        project(MATCH_PREFIX + "late", AllocationStatus.PENDING_ALLOCATION,
                Instant.now().minusSeconds(ALLOCATION_TIMEOUT_SECONDS + 1), null, null);

        await(watchdog.tick());

        Row allocation = findAllocation(MATCH_PREFIX + "late");
        assertEquals(AllocationStatus.REJECTED.name(), allocation.getString("status"),
                "a match that never got a server must not stay pending forever");
        assertEquals(RejectionReason.NO_CAPACITY.wireName(), allocation.getString("rejection_reason"));
        assertTrue(allocation.getValue("rejected_at") != null, "the rejection has a timestamp");
        assertEquals(1.0, counted(MetricNames.MATCH_REJECTIONS, MetricNames.TAG_REASON,
                RejectionReason.NO_CAPACITY.wireName()), "and it is counted by reason");
    }

    @Test
    void aMatchStillInsideItsDeadlineIsNotRejected() {
        project(MATCH_PREFIX + "waiting", AllocationStatus.PENDING_ALLOCATION,
                Instant.now().minusSeconds(1), null, null);

        await(watchdog.tick());

        assertEquals(AllocationStatus.PENDING_ALLOCATION.name(),
                findAllocation(MATCH_PREFIX + "waiting").getString("status"),
                "the watchdog retries before it gives up; being early is not a failure");
    }

    // ── the retry: a slot that appears is a match that runs ------------------------------

    @Test
    void aSlotFreedBeforeTheDeadlineIsTakenByTheMatchThatWasWaiting() {
        await(servers.register(server("retry", 1)));
        String serverId = SERVER_PREFIX + "retry";
        await(pool.withTransaction(connection ->
                servers.reserve(connection, GameMode.ONE_V_ONE, Region.NA)));
        project(MATCH_PREFIX + "running", AllocationStatus.ALLOCATED, Instant.now().minusSeconds(5),
                serverId, Instant.now().minusSeconds(5));
        project(MATCH_PREFIX + "waiting", AllocationStatus.PENDING_ALLOCATION,
                Instant.now().minusSeconds(5), null, null);

        await(watchdog.tick());
        assertEquals(AllocationStatus.PENDING_ALLOCATION.name(),
                findAllocation(MATCH_PREFIX + "waiting").getString("status"),
                "the pool is full: the waiting match stays where it is");

        // The running match ends and gives its slot back.
        assertTrue(await(servers.release(serverId, MATCH_PREFIX + "running")).isPresent());

        await(watchdog.tick());

        Row waiting = findAllocation(MATCH_PREFIX + "waiting");
        assertEquals(AllocationStatus.ALLOCATED.name(), waiting.getString("status"),
                "the first tick after a release must place the match that was already waiting");
        assertEquals(serverId, waiting.getString("server_id"));
        assertEquals(1, await(servers.find(serverId)).orElseThrow().allocated(),
                "one slot, one match: the retry takes the one that was freed, not a second one");
        assertEquals(1.0, counted(MetricNames.ALLOCATION_RETRIES, MetricNames.TAG_RESULT,
                MetricNames.RESULT_ALLOCATED));
    }

    /**
     * The race the retry has to survive: it reserved a slot, and by the time it flips the row the row
     * is no longer pending (another writer rejected it). Returning the slot is the difference between
     * a lost match and a slot nobody can use until the next release.
     */
    @Test
    void aRetryThatLosesTheRaceGivesTheSlotBack() {
        await(servers.register(server("race", 1)));
        project(MATCH_PREFIX + "gone", AllocationStatus.REJECTED, Instant.now(),
                null, null);

        boolean allocated = await(allocations.tryAllocate(
                new PendingMatch(MATCH_PREFIX + "gone", GameMode.ONE_V_ONE, Region.NA)));

        assertFalse(allocated, "a match that is no longer pending cannot be allocated");
        assertEquals(0, await(servers.find(SERVER_PREFIX + "race")).orElseThrow().allocated(),
                "and the slot the retry took is back in the pool");
    }

    // ── the slot: held by a match that never starts -------------------------------------

    @Test
    void anAllocatedMatchThatNeverStartsGivesTheSlotBack() {
        await(servers.register(server("stuck", 1)));
        String serverId = SERVER_PREFIX + "stuck";
        await(pool.withTransaction(connection ->
                servers.reserve(connection, GameMode.ONE_V_ONE, Region.NA)));
        project(MATCH_PREFIX + "stuck", AllocationStatus.ALLOCATED, Instant.now().minusSeconds(70),
                serverId, Instant.now().minusSeconds(START_TIMEOUT_SECONDS + 1));
        project(MATCH_PREFIX + "next", AllocationStatus.PENDING_ALLOCATION, Instant.now().minusSeconds(2),
                null, null);

        await(watchdog.tick());

        Row stuck = findAllocation(MATCH_PREFIX + "stuck");
        assertEquals(AllocationStatus.REJECTED.name(), stuck.getString("status"));
        assertEquals(RejectionReason.START_TIMEOUT.wireName(), stuck.getString("rejection_reason"),
                "a server that took a match and never began is a server that is not running it");
        assertEquals(0, await(servers.find(serverId)).orElseThrow().allocated(), "the slot is back");

        // And the slot is not merely free: the next tick hands it to the match that was waiting.
        await(watchdog.tick());
        assertEquals(AllocationStatus.ALLOCATED.name(),
                findAllocation(MATCH_PREFIX + "next").getString("status"));
    }

    @Test
    void aMatchOnADeadServerIsRejectedAndItsSlotFreed() {
        await(servers.register(server("dead", 1)));
        String serverId = SERVER_PREFIX + "dead";
        await(pool.withTransaction(connection ->
                servers.reserve(connection, GameMode.ONE_V_ONE, Region.NA)));
        project(MATCH_PREFIX + "orphan", AllocationStatus.ALLOCATED, Instant.now().minusSeconds(10),
                serverId, Instant.now().minusSeconds(10));

        // The heartbeat stops: last_seen goes past the pool's TTL without anybody releasing the slot.
        await(pool.preparedQuery("UPDATE game_servers SET last_seen = now() - make_interval(secs => ?) "
                + "WHERE server_id = ?")
                .execute(Tuple.of(SERVER_TTL_SECONDS + 10, serverId)).mapEmpty());

        await(watchdog.tick());

        Row orphan = findAllocation(MATCH_PREFIX + "orphan");
        assertEquals(AllocationStatus.REJECTED.name(), orphan.getString("status"),
                "a server that stopped talking cannot be running the match");
        assertEquals(RejectionReason.SERVER_FAILED.wireName(), orphan.getString("rejection_reason"));
        assertEquals(0, await(servers.find(serverId)).orElseThrow().allocated(),
                "the release that never arrived is what this replaces");
    }

    @Test
    void aDrainingServerKeepsTheMatchItIsRunning() {
        await(servers.register(server("draining", 1)));
        String serverId = SERVER_PREFIX + "draining";
        await(pool.withTransaction(connection ->
                servers.reserve(connection, GameMode.ONE_V_ONE, Region.NA)));
        project(MATCH_PREFIX + "finishing", AllocationStatus.ALLOCATED, Instant.now().minusSeconds(10),
                serverId, Instant.now().minusSeconds(10));
        await(servers.drain(serverId));

        await(watchdog.tick());

        assertEquals(AllocationStatus.ALLOCATED.name(),
                findAllocation(MATCH_PREFIX + "finishing").getString("status"),
                "draining means no new matches, never stop the ones already running");
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────

    private static void project(String matchId, AllocationStatus status, Instant confirmedAt, String serverId,
            Instant allocatedAt) {
        await(pool.preparedQuery("INSERT INTO match_allocations (match_id, game_mode, region, player_count, "
                + "status, confirmed_at, server_id, allocated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                .execute(Tuple.of(matchId, GameMode.ONE_V_ONE.label(), Region.NA.name(), 2, status.name(),
                        timestamp(confirmedAt), serverId, allocatedAt == null ? null : timestamp(allocatedAt)))
                .mapEmpty());
        for (int i = 1; i <= 2; i++) {
            await(pool.preparedQuery("INSERT INTO match_roster (match_id, player_id, ticket_id, team, "
                    + "skill_at_match) VALUES (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(matchId, matchId + "-player-" + i, matchId + "-ticket-" + i, i, 50))
                    .mapEmpty());
        }
    }

    private static java.sql.Timestamp timestamp(Instant instant) {
        return java.sql.Timestamp.from(instant);
    }

    private static Row findAllocation(String matchId) {
        return await(pool.preparedQuery("SELECT status, server_id, rejection_reason, rejected_at "
                + "FROM match_allocations WHERE match_id = ?").execute(Tuple.of(matchId)))
                .iterator().next();
    }

    private static Registration server(String suffix, int capacity) {
        return new Registration(SERVER_PREFIX + suffix, Region.NA, "10.0.0.9:7777",
                List.of(GameMode.ONE_V_ONE), capacity);
    }

    private static double counted(String metric, String tag, String value) {
        Counter counter = registry.find(metric).tag(tag, value).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static void deleteTestRows() {
        await(pool.preparedQuery("DELETE FROM match_allocations WHERE match_id LIKE ?")
                .execute(Tuple.of(MATCH_PREFIX + "%")).mapEmpty());
        await(pool.preparedQuery("DELETE FROM game_servers WHERE server_id LIKE ?")
                .execute(Tuple.of(SERVER_PREFIX + "%")).mapEmpty());
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.toCompletionStage().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
