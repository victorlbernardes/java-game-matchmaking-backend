package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.db.RejectedMatchRepository;
import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.MatchRequeueSweeper;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.metrics.MetricNames;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.RedisTicketRepository;
import com.victor.matchmaking.redis.TicketRepository;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Tuple;

/**
 * Property: a rejected match's players come back to the queue <b>where they were</b>, exactly once
 * (W23, ADR 0032).
 *
 * <p>The queue is a priority queue ordered by when the player arrived. A requeue that takes the ticket
 * out and puts it back with {@code now()} as the score is not a requeue — it is a player sent to the
 * back of a line he had already waited in, and it is the most common way to get this wrong because
 * every test that only counts the members passes. The four properties below are the ones that fail
 * when that happens:
 *
 * <ol>
 *   <li>the score is the ticket's <b>original</b> {@code queuedAt};</li>
 *   <li>a sweep repeated after a crash (the marker written last, for exactly this reason) does not
 *       <b>duplicate</b> the member;</li>
 *   <li>a ticket that stopped waiting (cancelled, already back) is <b>left alone</b>;</li>
 *   <li>a ticket whose TTL ran out is <b>counted</b>, not guessed at, and a rejection of another
 *       region is <b>not</b> this engine's to act on.</li>
 * </ol>
 *
 * <p>Requires Redis and Postgres ({@code docker compose up -d redis postgres}) and cleans up after
 * itself. It works in {@code EU}/{@code 1v1} on purpose: the other engine tests share
 * {@code queue:1v1:NA}, and a leftover member there pairs into somebody else's scenario.
 */
class RequeuePreservesScoreTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final GameMode MODE = GameMode.ONE_V_ONE;
    private static final Region REGION = Region.EU;
    private static final Region OTHER_REGION = Region.NA;
    /**
     * The starvation test's region. It is neither this class's main region nor the one another
     * region's rejections use, and no other test writes a rejected SA match: the batch of one in
     * that test is therefore its own row, not a leftover from a previous run.
     */
    private static final Region STARVED_REGION = Region.SA;
    private static final String MATCH_PREFIX = "match-w23-requeue-";
    private static final String TICKET_PREFIX = "ticket-w23-requeue-";

    private static Vertx vertx;
    private static Redis redis;
    private static Pool pool;
    private static TicketRepository tickets;
    private static RejectedMatchRepository rejected;
    private static SimpleMeterRegistry registry;
    private static MatchRequeueSweeper sweeper;

    @BeforeAll
    static void connect() {
        DatabaseConfig config = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(config);
        vertx = Vertx.vertx();
        redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        pool = DatabasePool.connect(vertx, config);
        // One partition keeps the key historical (queue:1v1:EU) and the assertions readable; the
        // partition is storage, and the requeue uses the ticket's own key either way (W20).
        tickets = new RedisTicketRepository(redis, RedisTicketRepository.UNBOUNDED_QUEUE, 1);
        rejected = new RejectedMatchRepository(pool);
        registry = new SimpleMeterRegistry();
        sweeper = new MatchRequeueSweeper(List.of(REGION), rejected, tickets,
                new EngineKpis(registry), Clock.systemUTC(), 32);
    }

    @BeforeEach
    void clean() throws Exception {
        deleteTestRows();
        deleteTestKeys();
        registry.clear();
    }

    @AfterAll
    static void close() throws Exception {
        deleteTestRows();
        deleteTestKeys();
        redis.close();
        pool.close();
        vertx.close();
    }

    @Test
    void thePlayersComeBackWithTheScoreTheyHad() throws Exception {
        Instant queuedAt = Instant.now().minusSeconds(40);
        String ticketA = awaitConfirmedTicket("a", "player-a", queuedAt);
        String ticketB = awaitConfirmedTicket("b", "player-b", queuedAt);
        String matchId = rejectMatch(REGION, ticketA, ticketB);

        assertEquals(0, zcard(REGION), "pairing took both members off the queue");

        int requeued = await(sweeper.sweepOnce());

        assertEquals(1, requeued, "one rejected match came back");
        assertEquals(2, zcard(REGION), "one member per ticket, no more");
        assertEquals((double) queuedAt.toEpochMilli(), zscore(REGION, ticketA), 0.5,
                "the player keeps the 40 s he already waited: the score is his own queuedAt");
        assertEquals(TicketState.SEARCHING, await(tickets.findById(ticketA)).status(),
                "and the ticket is searchable again");
        assertNotNull(requeuedAt(matchId), "the match is marked, so the next sweep leaves it alone");
        assertEquals(2.0, counted(MetricNames.PLAYERS_REQUEUED));
    }

    /**
     * The marker is written <em>after</em> the requeue on purpose, so the crash window repeats the
     * requeue instead of losing it. The repeat has to be harmless — and it is, because the script
     * refuses a ticket that is already searching.
     */
    @Test
    void aSweepRepeatedAfterACrashDoesNotDuplicateTheMember() throws Exception {
        Instant queuedAt = Instant.now().minusSeconds(10);
        String ticketA = awaitConfirmedTicket("repeat", "player-repeat", queuedAt);
        String matchId = rejectMatch(REGION, ticketA);
        await(sweeper.sweepOnce());
        assertEquals(1, zcard(REGION));

        // The engine died between the ZADD and the marker: the row looks untouched.
        await(pool.preparedQuery("UPDATE match_allocations SET requeued_at = NULL WHERE match_id = ?")
                .execute(Tuple.of(matchId)).mapEmpty());

        int secondPass = await(sweeper.sweepOnce());

        assertEquals(1, secondPass, "the row was processed again");
        assertEquals(1, zcard(REGION), "and the queue still holds exactly one member");
        assertEquals((double) queuedAt.toEpochMilli(), zscore(REGION, ticketA), 0.5);
    }

    @Test
    void aTicketThatStoppedWaitingIsLeftAlone() throws Exception {
        Instant queuedAt = Instant.now().minusSeconds(5);
        // A ticket that is still in the queue (cancelled by its owner and re-queued, say) must not be
        // added a second time: the script's state check is what keeps it to one member.
        MatchmakingTicket searching = ticket("searching", "player-searching", REGION, queuedAt);
        await(tickets.createIdempotent(null, searching));
        String matchId = rejectMatch(REGION, searching.id());

        await(sweeper.sweepOnce());

        assertEquals(1, zcard(REGION), "the member was already there");
        assertEquals(TicketState.SEARCHING, await(tickets.findById(searching.id())).status());
        assertNotNull(requeuedAt(matchId), "the match is still marked: there was nothing left to do");
    }

    @Test
    void anExpiredTicketIsCountedInsteadOfGuessed() throws Exception {
        Instant queuedAt = Instant.now().minusSeconds(5);
        String real = awaitConfirmedTicket("expired", "player-expired", queuedAt);
        // One ticket whose key is gone (its 900 s TTL ran out while the match waited) and one roster
        // row from an event that predates W23 and carries no ticket at all.
        String matchId = rejectMatch(REGION, real, TICKET_PREFIX + "no-such-ticket");
        await(pool.preparedQuery("INSERT INTO match_roster (match_id, player_id, ticket_id, team, "
                + "skill_at_match) VALUES (?, ?, NULL, 2, 50)")
                .execute(Tuple.of(matchId, MATCH_PREFIX + "player-without-ticket")).mapEmpty());

        await(sweeper.sweepOnce());

        assertEquals(1, zcard(REGION), "only the ticket that exists comes back");
        assertEquals(1.0, countedSkipped(MetricNames.REASON_TICKET_EXPIRED),
                "the expired ticket is reported, not invented");
        assertEquals(1.0, countedSkipped(MetricNames.REASON_NO_TICKET),
                "and an old event's player is reported as what he is");
        assertNotNull(requeuedAt(matchId));
    }

    @Test
    void anotherRegionsRejectionIsNotThisEngines() throws Exception {
        Instant queuedAt = Instant.now().minusSeconds(5);
        MatchmakingTicket elsewhere = ticket("other-region", "player-other-region", OTHER_REGION, queuedAt);
        await(tickets.createIdempotent(null, elsewhere));
        await(tickets.save(null, confirmed(elsewhere)));
        String matchId = rejectMatch(OTHER_REGION, elsewhere.id());

        int requeued = await(sweeper.sweepOnce());

        assertEquals(0, requeued, "this engine pairs EU; a NA rejection belongs to the NA engine (W21)");
        assertNull(requeuedAt(matchId));
        assertEquals(TicketState.CONFIRMED, await(tickets.findById(elsewhere.id())).status());
    }

    /**
     * Property: a rejection the sweep <b>cannot act on</b> must not be able to starve one it can.
     *
     * <p>The batch is the oldest {@code LIMIT} rows, so a row that produces no work and is never
     * marked is the head of every pass — and every later rejection waits behind it forever. That is
     * not hypothetical: a projection written before W23 (or from an event that carried no players)
     * has no {@code match_roster} rows at all, and the inner join this test pins down dropped them,
     * so 18 930 of them blocked the sweep on the local stack while the new rejections sat untouched.
     *
     * <p>The marker means "this rejection was acted on", not "players came back": a row with nothing
     * to give back is marked all the same, and it is not counted as a requeue.
     */
    @Test
    void aRejectionWithNoRosterDoesNotStarveTheSweep() throws Exception {
        Instant queuedAt = Instant.now().minusSeconds(20);
        MatchmakingTicket waiting = new MatchmakingTicket(TICKET_PREFIX + "behind", "player-behind", MODE,
                STARVED_REGION, 80, 50, queuedAt, TicketState.SEARCHING, 0L, null);
        await(tickets.createIdempotent(null, waiting));
        await(tickets.save(null, confirmed(waiting)));
        // The oldest row of the region, and one nobody can requeue: it has no roster.
        String rosterless = rosterlessRejection();
        String behind = rejectMatch(STARVED_REGION, waiting.id());

        // One match per pass: the first pass is exactly the rosterless row.
        MatchRequeueSweeper oneAtATime = new MatchRequeueSweeper(List.of(STARVED_REGION), rejected, tickets,
                new EngineKpis(registry), Clock.systemUTC(), 1);

        assertEquals(0, await(oneAtATime.sweepOnce()), "a row with nothing to requeue is not a requeue");
        assertNotNull(requeuedAt(rosterless), "but it is marked, so it leaves the head of the batch");
        assertEquals(1, await(oneAtATime.sweepOnce()), "and the next pass reaches the match behind it");
        assertEquals(1, zcard(STARVED_REGION), "the player behind it is back in his queue");
        assertEquals((double) queuedAt.toEpochMilli(), zscore(STARVED_REGION, waiting.id()), 0.5);
        assertNotNull(requeuedAt(behind));
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────

    private static MatchmakingTicket ticket(String suffix, String playerId, Region region, Instant queuedAt) {
        return new MatchmakingTicket(TICKET_PREFIX + suffix, playerId, MODE, region, 80, 50, queuedAt,
                TicketState.SEARCHING, 0L, null);
    }

    private static MatchmakingTicket confirmed(MatchmakingTicket ticket) {
        return new MatchmakingTicket(ticket.id(), ticket.playerId(), ticket.gameMode(), ticket.region(),
                ticket.maxLatencyMs(), ticket.skill(), ticket.queuedAt(), TicketState.CONFIRMED, 1L,
                ticket.partyId());
    }

    /** Created and then confirmed, which takes it off the queue exactly as pairing does. */
    private static String awaitConfirmedTicket(String suffix, String playerId, Instant queuedAt) throws Exception {
        MatchmakingTicket created = ticket(suffix, playerId, REGION, queuedAt);
        await(tickets.createIdempotent(null, created));
        await(tickets.save(null, confirmed(created)));
        return created.id();
    }

    /** A rejected match whose roster was never projected: nothing to requeue, but it must be marked. */
    private static String rosterlessRejection() {
        String matchId = MATCH_PREFIX + UUID.randomUUID();
        await(pool.preparedQuery("INSERT INTO match_allocations (match_id, game_mode, region, player_count, "
                + "status, confirmed_at, rejected_at, rejection_reason) "
                + "VALUES (?, ?, ?, 2, ?, now() - interval '1 hour', now() - interval '1 hour', ?)")
                .execute(Tuple.of(matchId, MODE.label(), STARVED_REGION.name(),
                        AllocationStatus.REJECTED.name(), "no_capacity"))
                .mapEmpty());
        return matchId;
    }

    private static String rejectMatch(Region region, String... ticketIds) {
        String matchId = MATCH_PREFIX + UUID.randomUUID();
        await(pool.preparedQuery("INSERT INTO match_allocations (match_id, game_mode, region, player_count, "
                + "status, confirmed_at, rejected_at, rejection_reason) VALUES (?, ?, ?, ?, ?, now(), now(), ?)")
                .execute(Tuple.of(matchId, MODE.label(), region.name(), ticketIds.length,
                        AllocationStatus.REJECTED.name(), "no_capacity"))
                .mapEmpty());
        for (int i = 0; i < ticketIds.length; i++) {
            await(pool.preparedQuery("INSERT INTO match_roster (match_id, player_id, ticket_id, team, "
                    + "skill_at_match) VALUES (?, ?, ?, ?, ?)")
                    .execute(Tuple.of(matchId, MATCH_PREFIX + "player-" + i, ticketIds[i], i % 2 + 1, 50))
                    .mapEmpty());
        }
        return matchId;
    }

    private static Object requeuedAt(String matchId) {
        return await(pool.preparedQuery("SELECT requeued_at FROM match_allocations WHERE match_id = ?")
                .execute(Tuple.of(matchId))).iterator().next().getValue("requeued_at");
    }

    private static String queueKey(Region region) {
        return RedisKeys.queueKey(MODE.label(), region.name(), 0, 1);
    }

    private static long zcard(Region region) throws Exception {
        Response size = redis.send(Request.cmd(Command.ZCARD).arg(queueKey(region)))
                .toCompletionStage().toCompletableFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);
        return size != null ? size.toLong() : 0L;
    }

    private static double zscore(Region region, String ticketId) throws Exception {
        Response score = redis.send(Request.cmd(Command.ZSCORE).arg(queueKey(region)).arg(ticketId))
                .toCompletionStage().toCompletableFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);
        assertNotNull(score, "the ticket must be in the queue");
        return score.toDouble();
    }

    private static double counted(String metric) {
        Counter counter = registry.find(metric).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static double countedSkipped(String reason) {
        Counter counter = registry.find(MetricNames.REQUEUE_SKIPPED).tag(MetricNames.TAG_REASON, reason).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static void deleteTestRows() {
        await(pool.preparedQuery("DELETE FROM match_allocations WHERE match_id LIKE ?")
                .execute(Tuple.of(MATCH_PREFIX + "%")).mapEmpty());
    }

    /** Only this class's keys: the other engine tests share the same Redis. */
    private static void deleteTestKeys() throws Exception {
        List<String> toDelete = new ArrayList<>(List.of(queueKey(REGION), queueKey(OTHER_REGION),
                queueKey(STARVED_REGION)));
        Response found = redis.send(Request.cmd(Command.KEYS).arg(TICKET_PREFIX + "*"))
                .toCompletionStage().toCompletableFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);
        for (int i = 0; found != null && i < found.size(); i++) {
            toDelete.add(found.get(i).toString());
        }
        Request del = Request.cmd(Command.DEL);
        toDelete.forEach(del::arg);
        redis.send(del).toCompletionStage().toCompletableFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.toCompletionStage().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
