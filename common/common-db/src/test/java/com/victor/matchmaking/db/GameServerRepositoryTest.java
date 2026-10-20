package com.victor.matchmaking.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.db.GameServerRepository.CapacityBelowAllocated;
import com.victor.matchmaking.db.GameServerRepository.GameServer;
import com.victor.matchmaking.db.GameServerRepository.Registration;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

/**
 * The pool's properties, against the database that enforces them (W22, ADR 0031).
 *
 * <p>Three of these are the sprint: a server that stops heartbeating leaves the capacity without
 * anybody sweeping it, a full server cannot be over-allocated, and the slot comes back exactly once
 * when the match ends. The fourth - two transactions racing for the last slot - is the one that
 * cannot be faked: it needs the row lock and the {@code allocated < capacity} predicate in the same
 * statement, so a fake would delete the property it is supposed to test.
 *
 * <p>Requires Postgres (docker compose up -d postgres) and cleans up after itself.
 */
class GameServerRepositoryTest {

    private static final String SERVER_PREFIX = "gs-w22-";
    private static final String MATCH_PREFIX = "match-w22-";
    private static final GameMode MODE = GameMode.ONE_V_ONE;
    private static final Region REGION = Region.NA;

    private static Vertx vertx;
    private static Pool pool;
    private static GameServerRepository servers;

    @BeforeAll
    static void connect() {
        DatabaseConfig config = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(config);
        vertx = Vertx.vertx();
        pool = DatabasePool.connect(vertx, config);
        // The default TTL (30 s): the stale test backdates `last_seen` instead of sleeping, so the
        // assertion is about the predicate and not about how patient the test is.
        servers = new GameServerRepository(pool);
    }

    @BeforeEach
    void clean() {
        deleteRows();
    }

    @AfterAll
    static void close() {
        // The pool is durable state a *running* engine reads: a row left behind would make this
        // region authoritative with zero slots and stop matching for everybody using this database
        // (found the hard way in W22 - a leftover row from this class broke the E2E until it was
        // deleted). Tests clean up after themselves for the same reason they clean up before.
        deleteRows();
        pool.close();
        vertx.close();
    }

    private static void deleteRows() {
        await(pool.preparedQuery("DELETE FROM match_allocations WHERE match_id LIKE ?")
                .execute(Tuple.of(MATCH_PREFIX + "%")).mapEmpty());
        await(pool.preparedQuery("DELETE FROM game_servers WHERE server_id LIKE ?")
                .execute(Tuple.of(SERVER_PREFIX + "%")).mapEmpty());
    }

    @Test
    void aHeartbeatIsTheRegistration() {
        GameServer first = await(servers.register(registration("one", 2, "10.0.0.1:7777")));
        assertEquals(0, first.allocated());
        assertEquals(2, first.free());

        GameServer renewed = await(servers.register(registration("one", 2, "10.0.0.2:7777")));

        assertEquals(1, await(servers.list()).size(), "renewing is the same row, not a second server");
        assertEquals("10.0.0.2:7777", renewed.endpoint(), "the offer is a full statement, not a delta");
        assertEquals(GameServerRepository.ServerState.READY,
                renewed.stateAt(Instant.now(), servers.ttlSeconds()));
    }

    @Test
    void aServerThatStopsHeartbeatingLeavesTheCapacityWithNoSweep() {
        await(servers.register(registration("silent", 2, "10.0.0.3:7777")));
        assertEquals(2, regionCapacity().free(MODE));

        // The heartbeat stops: backdate it past the TTL. Nothing runs, nothing is marked.
        await(pool.preparedQuery("UPDATE game_servers SET last_seen = now() - interval '1 hour' WHERE server_id = ?")
                .execute(Tuple.of(SERVER_PREFIX + "silent")).mapEmpty());

        GameServerRepository.RegionCapacity capacity = regionCapacity();
        assertEquals(0, capacity.free(MODE), "a silent server offers nothing");
        assertEquals(0, capacity.servers(MODE));
        assertTrue(capacity.authoritative(),
                "the region still has a pool: zero is the answer, not \"unknown\"");
        assertEquals(GameServerRepository.ServerState.STALE,
                await(servers.find(SERVER_PREFIX + "silent")).orElseThrow()
                        .stateAt(Instant.now(), servers.ttlSeconds()),
                "the listing derives STALE; the pool itself only stops picking it");
    }

    @Test
    void aRegionWithNoPoolIsUnknownRatherThanEmpty() {
        assertFalse(regionCapacityOf(Region.SA).authoritative(),
                "no server ever registered in SA: the engine must not read that as zero capacity");
    }

    @Test
    void allocationTakesASlotUntilTheServerIsFull() {
        await(servers.register(registration("full", 1, "10.0.0.4:7777")));

        Optional<String> first = allocate();
        Optional<String> second = allocate();
        assertEquals(Optional.of(SERVER_PREFIX + "full"), first);
        assertEquals(Optional.empty(), second, "capacity 1 means exactly one match");
        assertEquals(1, await(servers.find(SERVER_PREFIX + "full")).orElseThrow().allocated());
    }

    /**
     * The property the allocator exists for: the last slot goes to exactly one of two confirmations
     * that arrive together.
     *
     * <p>Two open transactions, the first holding the row. The second must answer "no server" - not
     * block, and above all not read the same slot. Remove {@code SKIP LOCKED} and this test hangs
     * until the await timeout; remove {@code allocated < capacity} from the update and both take
     * the same slot and {@code allocated} becomes 2.
     */
    @Test
    void twoTransactionsRacingForTheLastSlotGetExactlyOne() throws Exception {
        await(servers.register(registration("last", 1, "10.0.0.5:7777")));

        SqlConnection first = await(pool.getConnection());
        SqlConnection second = await(pool.getConnection());
        try {
            io.vertx.sqlclient.Transaction firstTx = await(first.begin());
            Optional<String> winner = await(servers.reserve(first, MODE, REGION));

            io.vertx.sqlclient.Transaction secondTx = await(second.begin());
            Optional<String> loser = await(servers.reserve(second, MODE, REGION));

            await(firstTx.commit());
            await(secondTx.commit());

            assertTrue(winner.isPresent(), "the first confirmation takes the slot");
            assertTrue(loser.isEmpty(), "the second finds the server locked and full, and waits for W23");
        } finally {
            first.close();
            second.close();
        }
        assertEquals(1, await(servers.find(SERVER_PREFIX + "last")).orElseThrow().allocated(),
                "one slot was taken, not two");
    }

    @Test
    void releaseGivesTheSlotBackExactlyOnce() {
        String matchId = MATCH_PREFIX + "release";
        await(servers.register(registration("release", 1, "10.0.0.6:7777")));
        assertEquals(Optional.of(SERVER_PREFIX + "release"), allocate());
        await(recordAllocation(matchId, SERVER_PREFIX + "release"));

        Optional<GameServer> released = await(servers.release(SERVER_PREFIX + "release", matchId));
        assertEquals(0, released.orElseThrow().allocated(), "the slot came back");
        assertEquals(1, released.orElseThrow().free());

        assertTrue(await(servers.release(SERVER_PREFIX + "release", matchId)).isEmpty(),
                "a repeated release finds nothing to flip");
        assertEquals(0, await(servers.find(SERVER_PREFIX + "release")).orElseThrow().allocated(),
                "and cannot return a second slot");
    }

    @Test
    void aDrainedServerStopsReceivingUntilItOffersAgain() {
        await(servers.register(registration("drained", 2, "10.0.0.7:7777")));
        await(servers.drain(SERVER_PREFIX + "drained"));

        Optional<String> whileDraining = allocate();
        assertEquals(Optional.empty(), whileDraining, "draining stops new matches");
        assertEquals(0, regionCapacity().free(MODE), "and the capacity the engine reads agrees");

        // The heartbeat is an offer: a server that says it is here and can host N is available.
        await(servers.register(registration("drained", 2, "10.0.0.7:7777")));
        Optional<String> afterOffer = allocate();
        assertTrue(afterOffer.isPresent(), "an offering server is picked again");
    }

    @Test
    void capacityCannotShrinkBelowWhatIsAllocated() {
        await(servers.register(registration("shrink", 1, "10.0.0.8:7777")));
        assertEquals(Optional.of(SERVER_PREFIX + "shrink"), allocate());

        assertThrows(CapacityBelowAllocated.class, () -> await(servers.register(registration("shrink", 0,
                "10.0.0.8:7777"))));
        assertEquals(1, await(servers.find(SERVER_PREFIX + "shrink")).orElseThrow().capacity(),
                "the refused registration left the row untouched");
    }

    private static GameServerRepository.RegionCapacity regionCapacity() {
        return regionCapacityOf(REGION);
    }

    private static GameServerRepository.RegionCapacity regionCapacityOf(Region region) {
        Map<Region, GameServerRepository.RegionCapacity> snapshot = await(servers.snapshot());
        return snapshot.getOrDefault(region, GameServerRepository.RegionCapacity.UNKNOWN);
    }

    private static Optional<String> allocate() {
        return await(pool.withTransaction(connection -> servers.reserve(connection, MODE, REGION)));
    }

    private static Registration registration(String suffix, int capacity, String endpoint) {
        return new Registration(SERVER_PREFIX + suffix, REGION, endpoint, List.of(MODE), capacity);
    }

    /** The projection row the inbox writes before it allocates; release reads it to be idempotent. */
    private static Future<Object> recordAllocation(String matchId, String serverId) {
        return pool.preparedQuery("INSERT INTO match_allocations (match_id, game_mode, region, player_count, "
                + "status, confirmed_at, server_id, allocated_at) VALUES (?, ?, ?, ?, 'ALLOCATED', now(), ?, now())")
                .execute(Tuple.of(matchId, MODE.label(), REGION.name(), 2, serverId))
                .mapEmpty();
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.toCompletionStage().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception failure) {
            // The caller asserts on the failure type (e.g. CapacityBelowAllocated), so the
            // ExecutionException is unwrapped instead of replacing it.
            if (failure.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(failure);
        }
    }
}
