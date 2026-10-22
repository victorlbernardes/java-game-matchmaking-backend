package com.victor.matchmaking.orchestrator.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.db.AllocationRepository;
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
 * Property: one absent player does not kill a match, and a match nobody joined does not hold a slot
 * (W23, ADR 0034).
 *
 * <p>The naive rule - "a player did not connect, dissolve the match" - punishes everybody for one
 * person's connection. The other naive rule - "ignore it" - leaves a match running with an empty team
 * and a slot that is never used. The property is the one in between: the silent player is marked, the
 * match keeps running for the ones who showed up, and only a match where <em>nobody</em> connected is
 * rejected and gives the slot back.
 *
 * <p>It also pins the two reports a game server makes: {@code started} moves the allocation and is
 * idempotent, {@code connected} is only accepted for a started match and only for its own roster.
 *
 * <p>Requires Postgres ({@code docker compose up -d postgres}) and cleans up after itself.
 */
class NoShowTest {

    private static final String MATCH_PREFIX = "match-w23-noshow-";
    private static final String SERVER_PREFIX = "gs-w23-noshow-";

    private static final int CONNECT_WINDOW_SECONDS = 90;

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
        watchdog = new AllocationWatchdog(allocations, registry, 30, 60, CONNECT_WINDOW_SECONDS, 30);
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

    @Test
    void aPlayerWhoNeverConnectsIsMarkedAndTheMatchKeepsRunning() {
        String matchId = MATCH_PREFIX + "one-silent";
        await(servers.register(server("one-silent", 1)));
        allocatedAndStarted(matchId, Instant.now().minusSeconds(CONNECT_WINDOW_SECONDS + 1));
        connected(matchId, player(1));

        await(watchdog.tick());

        Row present = findPlayer(matchId, player(1));
        assertNotNull(present.getValue("connected_at"), "the player who arrived stays connected");
        assertNull(present.getValue("no_show_at"), "and is not marked absent");

        Row silent = findPlayer(matchId, player(2));
        assertNotNull(silent.getValue("no_show_at"), "the player who did not arrive is marked");

        assertEquals(AllocationStatus.STARTED.name(), findAllocation(matchId).getString("status"),
                "one absent player must not dissolve the match for the other one");
        assertEquals(1, await(servers.find(SERVER_PREFIX + "one-silent")).orElseThrow().allocated(),
                "and the slot stays with the match that is still running");
        assertEquals(1.0, countedNoShows());
    }

    @Test
    void aMatchWhereNobodyConnectedIsRejectedAndTheSlotComesBack() {
        String matchId = MATCH_PREFIX + "nobody";
        await(servers.register(server("nobody", 1)));
        allocatedAndStarted(matchId, Instant.now().minusSeconds(CONNECT_WINDOW_SECONDS + 1));

        await(watchdog.tick());

        Row allocation = findAllocation(matchId);
        assertEquals(AllocationStatus.REJECTED.name(), allocation.getString("status"),
                "a match nobody joined is not a match");
        assertEquals(RejectionReason.ALL_NO_SHOW.wireName(), allocation.getString("rejection_reason"));
        assertEquals(0, await(servers.find(SERVER_PREFIX + "nobody")).orElseThrow().allocated(),
                "the slot is back in the pool");
    }

    @Test
    void aMatchInsideTheConnectWindowIsLeftAlone() {
        String matchId = MATCH_PREFIX + "fresh";
        await(servers.register(server("fresh", 1)));
        allocatedAndStarted(matchId, Instant.now().minusSeconds(10));

        await(watchdog.tick());

        assertEquals(AllocationStatus.STARTED.name(), findAllocation(matchId).getString("status"));
        assertNull(findPlayer(matchId, player(2)).getValue("no_show_at"),
                "the window is still open: calling somebody absent early is worse than waiting");
    }

    @Test
    void startedIsIdempotentAndKeepsTheFirstInstant() {
        String matchId = MATCH_PREFIX + "started-twice";
        await(servers.register(server("started-twice", 1)));
        allocated(matchId);

        var first = await(allocations.start(SERVER_PREFIX + "started-twice", matchId));
        var second = await(allocations.start(SERVER_PREFIX + "started-twice", matchId));

        assertEquals(AllocationRepository.Transition.APPLIED, first.transition());
        assertEquals(AllocationRepository.Transition.ALREADY, second.transition(),
                "a server retrying its own report is not an error");
        assertEquals(first.at(), second.at(), "and the instant does not move");
    }

    @Test
    void aConnectionIsOnlyAcceptedWhenTheMatchHasStarted() {
        String matchId = MATCH_PREFIX + "early";
        await(servers.register(server("early", 1)));
        allocated(matchId);

        var early = await(allocations.connect(SERVER_PREFIX + "early", matchId, player(1)));
        assertEquals(AllocationRepository.Transition.INVALID_TRANSITION, early.transition(),
                "a connection before the start is not a connection");

        await(allocations.start(SERVER_PREFIX + "early", matchId));
        var late = await(allocations.connect(SERVER_PREFIX + "early", matchId, player(1)));
        assertEquals(AllocationRepository.Transition.APPLIED, late.transition());

        var again = await(allocations.connect(SERVER_PREFIX + "early", matchId, player(1)));
        assertEquals(AllocationRepository.Transition.ALREADY, again.transition());
        assertEquals(late.at(), again.at(), "the first connection is the one that counts");
    }

    @Test
    void aPlayerOutsideTheRosterIsNotConnected() {
        String matchId = MATCH_PREFIX + "stranger";
        await(servers.register(server("stranger", 1)));
        allocated(matchId);
        await(allocations.start(SERVER_PREFIX + "stranger", matchId));

        var stranger = await(allocations.connect(SERVER_PREFIX + "stranger", matchId, "somebody-else"));

        assertEquals(AllocationRepository.Transition.PLAYER_NOT_IN_MATCH, stranger.transition(),
                "the roster is the only list of players that may join");
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────

    private static String player(int index) {
        return MATCH_PREFIX + "player-" + index;
    }

    /** An allocation with the slot already taken and a roster of two, as the inbox would leave it. */
    private static void allocated(String matchId) {
        String serverId = serverOf(matchId);
        await(pool.withTransaction(connection -> servers.reserve(connection, GameMode.ONE_V_ONE, Region.NA)));
        await(pool.preparedQuery("INSERT INTO match_allocations (match_id, game_mode, region, player_count, "
                + "status, confirmed_at, server_id, allocated_at) VALUES (?, ?, ?, ?, ?, now(), ?, now())")
                .execute(Tuple.of(matchId, GameMode.ONE_V_ONE.label(), Region.NA.name(), 2,
                        AllocationStatus.ALLOCATED.name(), serverId))
                .mapEmpty());
        for (int i = 1; i <= 2; i++) {
            await(pool.preparedQuery("INSERT INTO match_roster (match_id, player_id, ticket_id, team, "
                    + "skill_at_match) VALUES (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(matchId, player(i), matchId + "-ticket-" + i, i, 50))
                    .mapEmpty());
        }
    }

    /** The same row, already started at the given instant. */
    private static void allocatedAndStarted(String matchId, Instant startedAt) {
        allocated(matchId);
        await(pool.preparedQuery("UPDATE match_allocations SET status = ?, started_at = ? WHERE match_id = ?")
                .execute(Tuple.of(AllocationStatus.STARTED.name(), java.sql.Timestamp.from(startedAt), matchId))
                .mapEmpty());
    }

    private static void connected(String matchId, String playerId) {
        await(allocations.connect(serverOf(matchId), matchId, playerId));
    }

    private static String serverOf(String matchId) {
        return SERVER_PREFIX + matchId.substring(MATCH_PREFIX.length());
    }

    private static Row findAllocation(String matchId) {
        return await(pool.preparedQuery("SELECT status, server_id, rejection_reason FROM match_allocations "
                + "WHERE match_id = ?").execute(Tuple.of(matchId))).iterator().next();
    }

    private static Row findPlayer(String matchId, String playerId) {
        return await(pool.preparedQuery("SELECT connected_at, no_show_at FROM match_roster "
                + "WHERE match_id = ? AND player_id = ?").execute(Tuple.of(matchId, playerId)))
                .iterator().next();
    }

    private static Registration server(String suffix, int capacity) {
        return new Registration(SERVER_PREFIX + suffix, Region.NA, "10.0.0.9:7777",
                List.of(GameMode.ONE_V_ONE), capacity);
    }

    private static double countedNoShows() {
        Counter counter = registry.find(MetricNames.NO_SHOWS).counter();
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
