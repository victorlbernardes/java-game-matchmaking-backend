package com.victor.matchmaking.engine.infrastructure;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounds how many writes the engine may have in flight against Postgres at once (W17, ADR 0022).
 *
 * <p>Why a bound at all: the pairing is the <em>source</em> of the confirmations, and one pairing
 * tick can form hundreds of matches whose confirmations arrive independently a moment later. With
 * a healthy database that is throughput; with a slow one it is the failure this gate exists to
 * stop - every confirmation opens a transaction immediately, the database falls further behind,
 * latency grows, more transactions pile up, and the database goes down under a load the engine
 * itself created. A database that is behind is not helped by being asked harder.
 *
 * <p>Two effects, and both matter:
 *
 * <ul>
 *   <li><b>The confirmation waits</b> instead of issuing: {@link #tryEnter()} refuses while
 *       {@code limit} writes are already in flight, and the caller retries shortly. The database
 *       never sees more concurrent transactions than it can take.</li>
 *   <li><b>The pairing stops</b> ({@link #saturated()}): while the database is at its limit, no new
 *       match is formed, so the backlog cannot grow. The queue grows instead, which is load the
 *       other control (the admission cap, ADR 0022) knows how to shed - the pressure ends up on the
 *       boundary that can say no to a player, not on the database that cannot.</li>
 * </ul>
 *
 * <p>A gate, not a pool: permits are not waited for asynchronously because the caller has to be
 * able to give up and report (a confirmation that cannot write is a fact the operator must see),
 * and because the wait naturally belongs to the retry the caller already schedules.
 */
public final class WriteGate {

    /** No bound: the pairing and the confirmation write as fast as they can. */
    private static final int UNBOUNDED = 0;

    private final int limit;
    private final AtomicInteger inFlight = new AtomicInteger();

    private WriteGate(int limit) {
        this.limit = Math.max(UNBOUNDED, limit);
    }

    /** A gate that never refuses. What the tests and the in-memory wiring use. */
    public static WriteGate unbounded() {
        return new WriteGate(UNBOUNDED);
    }

    /**
     * @param limit writes allowed in flight at once; 0 or less means no bound
     */
    public static WriteGate of(int limit) {
        return new WriteGate(limit);
    }

    /**
     * Claims a write slot, or refuses when the database is already at its limit.
     *
     * <p>The compare-and-set is the whole mechanism: an increment followed by a comparison lets N
     * callers pass together and makes the bound an average (the same race as a {@code ZCARD} before
     * a {@code ZADD}, ADR 0022).
     */
    public boolean tryEnter() {
        while (true) {
            int current = inFlight.get();
            if (limit > UNBOUNDED && current >= limit) {
                return false;
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /** Releases a slot claimed by {@link #tryEnter()}. */
    public void leave() {
        inFlight.decrementAndGet();
    }

    /** True while no slot is free: the pairing must not form more matches. */
    public boolean saturated() {
        return limit > UNBOUNDED && inFlight.get() >= limit;
    }

    /** Writes in flight right now: the number the gauge publishes and the tests assert on. */
    public int inFlight() {
        return inFlight.get();
    }

    public int limit() {
        return limit;
    }
}
