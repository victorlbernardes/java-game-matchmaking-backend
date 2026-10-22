package com.victor.matchmaking.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * Property: a queue with a capacity of C never holds more than C tickets, however many arrivals
 * race it at once (W17, ADR 0022).
 *
 * <p>The check and the write have to be one atomic step. Splitting them - {@code ZCARD} in Java,
 * then {@code ZADD} - makes the cap a suggestion: every concurrent caller reads a depth below the
 * cap and every one of them writes, so the queue overshoots by as many callers as raced. This test
 * fails on that implementation and passes only because {@code create.lua} does both in one
 * execution.
 */
class QueueCapacityTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final int CAPACITY = 25;
    private static final int ARRIVALS = 200;
    private static final GameMode MODE = GameMode.ONE_V_ONE;
    private static final Region REGION = Region.NA;

    private static Vertx vertx;
    private static Redis redis;
    private static RedisTicketRepository repository;

    @BeforeAll
    static void setUp() throws Exception {
        vertx = Vertx.vertx();
        redis = Redis.createClient(vertx, RedisClients.options(REDIS_URI));
        repository = new RedisTicketRepository(redis, CAPACITY);
        clearW17Keys();
    }

    @AfterAll
    static void tearDown() throws Exception {
        clearW17Keys();
        redis.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    /** Removes everything this test can write: the queue, its tickets and its idempotency keys. */
    private static void clearW17Keys() throws Exception {
        Request del = Request.cmd(Command.DEL).arg(RedisKeys.queueKey(MODE.label(), REGION.name()));
        redis.send(del).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

        Response idempotencyKeys = redis.send(Request.cmd(Command.KEYS).arg("idem:w17-*"))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        if (idempotencyKeys != null && idempotencyKeys.size() > 0) {
            Request delIdem = Request.cmd(Command.DEL);
            for (int i = 0; i < idempotencyKeys.size(); i++) {
                delIdem = delIdem.arg(idempotencyKeys.get(i).toString());
            }
            redis.send(delIdem).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }

        Response tickets = redis.send(Request.cmd(Command.KEYS).arg("ticket:w17-*"))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        if (tickets != null && tickets.size() > 0) {
            Request delTickets = Request.cmd(Command.DEL);
            for (int i = 0; i < tickets.size(); i++) {
                delTickets = delTickets.arg(tickets.get(i).toString());
            }
            redis.send(delTickets).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static MatchmakingTicket ticket(int index) {
        return new MatchmakingTicket("w17-" + index, "player-w17-" + index, MODE, REGION, 80, 50,
                Instant.now(), TicketState.SEARCHING, 0L, null);
    }

    private static void enqueue(int index) throws Exception {
        repository.createIdempotent("w17-" + index, ticket(index))
                .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static long depth() throws Exception {
        return await(repository.queueDepth(MODE.label(), REGION.name()));
    }

    private static <T> T await(Future<T> future) throws Exception {
        try {
            return future.toCompletionStage().toCompletableFuture().get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("the test's own await timed out", e);
        }
    }

    @Test
    void concurrentArrivalsNeverExceedTheCap() throws Exception {
        clearW17Keys();
        AtomicInteger admitted = new AtomicInteger();
        AtomicInteger refused = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

        // Issued together and awaited once: awaiting each one would make the arrivals sequential
        // and there would be no race for the cap to survive.
        List<Future<String>> inFlight = new ArrayList<>();
        for (int index = 0; index < ARRIVALS; index++) {
            inFlight.add(repository.createIdempotent("w17-" + index, ticket(index)));
        }
        // Every arrival is already in flight; waiting for them one by one waits for all of them,
        // and each wait is bounded so a stalled Redis fails the test instead of hanging it.
        for (Future<String> arrival : inFlight) {
            try {
                arrival.toCompletionStage().toCompletableFuture().get(60, TimeUnit.SECONDS);
                admitted.incrementAndGet();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof QueueFullException) {
                    refused.incrementAndGet();
                } else {
                    failed.incrementAndGet();
                }
            }
        }

        long depth = depth();
        assertEquals(0, failed.get(), "no arrival may fail for a reason other than the cap");
        assertEquals(CAPACITY, admitted.get(), "the cap is all that is admitted: " + admitted.get());
        assertEquals(ARRIVALS - CAPACITY, refused.get(), "every other arrival is shed");
        assertEquals(CAPACITY, depth, "the queue holds exactly the cap");

        // A shed arrival must write nothing at all: the idempotency index holds exactly one entry
        // per admitted arrival, whatever order the race resolved in.
        Response idempotencyKeys = await(redis.send(Request.cmd(Command.KEYS).arg("idem:w17-*")));
        assertEquals(CAPACITY, idempotencyKeys.size(),
                "only the admitted arrivals may leave an idempotency record");
    }

    @Test
    void arrivalsWithoutAnIdempotencyKeyAreCappedToo() throws Exception {
        // The regression this test exists for, found by the W17 load run: the cap was checked only
        // on the path that had an Idempotency-Key, and a client that sends none (the load
        // generator, a curl, a player's second device) went around it. Measured before the fix:
        // 4000 arrivals against a cap of 1000, zero refusals, the queue at 3146.
        clearW17Keys();
        List<Future<String>> inFlight = new ArrayList<>();
        for (int index = 10000; index < 10000 + ARRIVALS; index++) {
            inFlight.add(repository.createIdempotent(null, ticket(index)));
        }
        int admitted = 0;
        int refused = 0;
        for (Future<String> arrival : inFlight) {
            try {
                arrival.toCompletionStage().toCompletableFuture().get(60, TimeUnit.SECONDS);
                admitted++;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof QueueFullException) {
                    refused++;
                } else {
                    throw e;
                }
            }
        }

        assertEquals(CAPACITY, admitted, "a caller without an Idempotency-Key is still an arrival");
        assertEquals(ARRIVALS - CAPACITY, refused, "and is still shed when the queue is full");
        assertEquals(CAPACITY, depth(), "the queue holds exactly the cap");
        Response idempotencyKeys = await(redis.send(Request.cmd(Command.KEYS).arg("idem:w17-*")));
        assertEquals(0, idempotencyKeys.size(),
                "an arrival with no key must leave no idempotency record behind");
    }

    @Test
    void aReplayIsAnsweredEvenWhenTheQueueIsFull() throws Exception {
        clearW17Keys();
        String idempotencyKey = "w17-1950";
        String created = repository.createIdempotent(idempotencyKey, ticket(1950))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertEquals("w17-1950", created);

        for (int index = 1951; index < 1951 + CAPACITY - 1; index++) {
            enqueue(index);
        }
        assertEquals(CAPACITY, depth(), "the queue is full");

        // The player did enqueue; refusing his retry would lose a ticket that exists.
        String again = repository.createIdempotent(idempotencyKey, ticket(1950))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertEquals(created, again, "a replay answers with the ticket that exists");

        // A brand new arrival, though, is shed: the cap is the cap.
        try {
            enqueue(9000);
            fail("a new arrival must be refused while the queue is full");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof QueueFullException,
                    "the refusal must be the cap: " + e.getCause());
        }
    }
}
