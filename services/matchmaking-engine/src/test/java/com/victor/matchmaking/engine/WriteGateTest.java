package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.engine.infrastructure.WriteGate;

/**
 * Property: at most {@code limit} writes are in flight, and no permit is lost (W17, ADR 0022).
 *
 * <p>What this pins: the claim is a compare-and-set, not a read-then-write. A gate built as
 * {@code if (inFlight < limit) { inFlight++ }} lets N threads pass together and turns the bound
 * into an average - the same race as a {@code ZCARD} before a {@code ZADD}, and the reason both
 * live in one atomic step.
 *
 * <p>The second half is the other failure mode: a permit that is not given back shrinks the
 * capacity forever, so the test also requires every thread to finish (with one permit released
 * per claim there is always progress).
 */
class WriteGateTest {

    private static final int LIMIT = 8;
    private static final int WRITERS = 64;

    @Test
    void neverLetsMoreThanTheLimitInAtOnce() throws Exception {
        WriteGate gate = WriteGate.of(LIMIT);
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(WRITERS);

        for (int i = 0; i < WRITERS; i++) {
            Thread.ofPlatform().start(() -> {
                try {
                    while (!gate.tryEnter()) {
                        Thread.onSpinWait();
                    }
                    int now = concurrent.incrementAndGet();
                    maxConcurrent.accumulateAndGet(now, Math::max);
                    Thread.sleep(2);
                    concurrent.decrementAndGet();
                    gate.leave();
                    completed.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(30, TimeUnit.SECONDS), "the gate must not deadlock");
        assertEquals(WRITERS, completed.get(), "every writer must get its turn: no permit may be lost");
        assertTrue(maxConcurrent.get() <= LIMIT,
                "at most " + LIMIT + " writes in flight, observed " + maxConcurrent.get());
        assertEquals(0, gate.inFlight(), "every write released its slot");
    }

    @Test
    void refusesWhileSaturatedAndAdmitsWhenASlotFrees() {
        WriteGate gate = WriteGate.of(2);
        assertFalse(gate.saturated(), "an empty gate is not saturated");

        assertTrue(gate.tryEnter());
        assertTrue(gate.tryEnter());
        assertTrue(gate.saturated(), "two of two slots taken means the pairing must stop");
        assertFalse(gate.tryEnter(), "a third write is refused, not queued");
        assertEquals(2, gate.inFlight());

        gate.leave();
        assertFalse(gate.saturated(), "one slot free is enough to pair again");
        assertTrue(gate.tryEnter());
        gate.leave();
        gate.leave();
        assertEquals(0, gate.inFlight());
    }

    @Test
    void unboundedNeverSaturates() {
        WriteGate unlimited = WriteGate.unbounded();
        List<Boolean> admitted = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            admitted.add(unlimited.tryEnter());
        }
        assertTrue(admitted.stream().allMatch(Boolean::booleanValue),
                "an unbounded gate is the pre-W17 behaviour and must never refuse");
        assertEquals(1000, unlimited.inFlight());
    }
}
