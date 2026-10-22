package com.victor.matchmaking.orchestrator.application;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.victor.matchmaking.db.AllocationRepository;
import com.victor.matchmaking.db.AllocationRepository.PendingMatch;
import com.victor.matchmaking.db.AllocationRepository.RejectedAllocation;
import com.victor.matchmaking.domain.match.RejectionReason;
import com.victor.matchmaking.metrics.MetricNames;

import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;

/**
 * What keeps a confirmed match from becoming an orphan (W23, ADR 0032).
 *
 * <p>One tick, five questions, in this order - and the order matters:
 *
 * <ol>
 *   <li><b>Retry.</b> Every pending match tries to take a slot. This is what turns a released slot
 *       into a running match without waiting for another confirmation: the players already said
 *       yes, they are only missing a server.</li>
 *   <li><b>Did a pending match run out of time?</b> One statement rejects them all at once
 *       ({@code ALLOCATION_TIMEOUT_SECONDS}, default 30) - a match that never got a server must be
 *       dissolved while the players are still interested, not hours later.</li>
 *   <li><b>Did an allocated match start in time?</b> ({@code START_TIMEOUT_SECONDS}, default 60).
 *       The slot goes back: a server that took a match and never began is a server that is either
 *       broken or full, and either way the slot is not being used.</li>
 *   <li><b>Is the server that holds a match still alive?</b> A server that left the pool
 *       ({@code last_seen} past the TTL) cannot be running anything. This is the {@code release}
 *       that never arrives, the leak the W22 ADR named.</li>
 *   <li><b>Did the players show up?</b> After {@code CONNECT_WINDOW_SECONDS} (default 90) from the
 *       start, a silent player is marked absent and the match keeps running; if <em>nobody</em>
 *       connected, the match is rejected and the slot goes back.</li>
 * </ol>
 *
 * <p>Every rejection writes a reason. The row is the durable decision the engine acts on (it
 * requeues the roster), so this class never touches the queue: it decides, and the table carries
 * the decision to whoever owns the thing that must be undone.
 */
public final class AllocationWatchdog {

    public static final String ENV_ALLOCATION_TIMEOUT_SECONDS = "ALLOCATION_TIMEOUT_SECONDS";
    public static final String ENV_START_TIMEOUT_SECONDS = "START_TIMEOUT_SECONDS";
    public static final String ENV_CONNECT_WINDOW_SECONDS = "CONNECT_WINDOW_SECONDS";

    public static final int DEFAULT_ALLOCATION_TIMEOUT_SECONDS = 30;
    public static final int DEFAULT_START_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_CONNECT_WINDOW_SECONDS = 90;

    /** Bounded work per tick: the next tick is one second away, and a slow query must not pile up. */
    static final int BATCH = 64;

    private final AllocationRepository allocations;
    private final MeterRegistry registry;
    private final int allocationTimeoutSeconds;
    private final int startTimeoutSeconds;
    private final int connectWindowSeconds;
    private final int serverTtlSeconds;

    /** One tick at a time: a database that is slower than the timer must not stack work. */
    private final AtomicBoolean inFlight = new AtomicBoolean();

    public AllocationWatchdog(AllocationRepository allocations, MeterRegistry registry, int allocationTimeoutSeconds,
            int startTimeoutSeconds, int connectWindowSeconds, int serverTtlSeconds) {
        this.allocations = allocations;
        this.registry = registry;
        this.allocationTimeoutSeconds = positive(allocationTimeoutSeconds, ENV_ALLOCATION_TIMEOUT_SECONDS);
        this.startTimeoutSeconds = positive(startTimeoutSeconds, ENV_START_TIMEOUT_SECONDS);
        this.connectWindowSeconds = positive(connectWindowSeconds, ENV_CONNECT_WINDOW_SECONDS);
        this.serverTtlSeconds = positive(serverTtlSeconds, "SERVER_TTL_SECONDS");
    }

    public static int allocationTimeoutFromEnv() {
        return secondsFromEnv(ENV_ALLOCATION_TIMEOUT_SECONDS, DEFAULT_ALLOCATION_TIMEOUT_SECONDS);
    }

    public static int startTimeoutFromEnv() {
        return secondsFromEnv(ENV_START_TIMEOUT_SECONDS, DEFAULT_START_TIMEOUT_SECONDS);
    }

    public static int connectWindowFromEnv() {
        return secondsFromEnv(ENV_CONNECT_WINDOW_SECONDS, DEFAULT_CONNECT_WINDOW_SECONDS);
    }

    /** Runs one pass. The returned future completes when the pass is over, whatever it found. */
    public Future<Void> tick() {
        if (!inFlight.compareAndSet(false, true)) {
            return Future.succeededFuture();
        }
        return retryPending()
                .compose(ignored -> rejectExpiredPending())
                .compose(ignored -> rejectExpiredStarts())
                .compose(ignored -> rejectStaleServers())
                .compose(ignored -> closeTheConnectWindow())
                .onComplete(done -> inFlight.set(false));
    }

    private Future<Void> retryPending() {
        return allocations.pendingForRetry(BATCH).compose(pending -> {
            Future<Void> chain = Future.succeededFuture();
            for (PendingMatch match : pending) {
                chain = chain.compose(ignored -> allocations.tryAllocate(match).map(allocated -> {
                    registry.counter(MetricNames.ALLOCATION_RETRIES, MetricNames.TAG_RESULT,
                            allocated ? MetricNames.RESULT_ALLOCATED : MetricNames.RESULT_NO_CAPACITY).increment();
                    return null;
                }));
            }
            return chain;
        });
    }

    private Future<Void> rejectExpiredPending() {
        return allocations.rejectExpiredPending(allocationTimeoutSeconds).map(rejected -> {
            countRejections(RejectionReason.NO_CAPACITY, rejected);
            return null;
        });
    }

    private Future<Void> rejectExpiredStarts() {
        return allocations.expiredStarts(startTimeoutSeconds, BATCH)
                .compose(found -> rejectAll(found, RejectionReason.START_TIMEOUT));
    }

    private Future<Void> rejectStaleServers() {
        return allocations.staleServers(serverTtlSeconds, BATCH)
                .compose(found -> rejectAll(found, RejectionReason.SERVER_FAILED));
    }

    private Future<Void> rejectAll(List<RejectedAllocation> found, RejectionReason reason) {
        Future<Void> chain = Future.succeededFuture();
        for (RejectedAllocation allocation : found) {
            chain = chain.compose(ignored -> allocations.reject(allocation, reason).map(rejected -> {
                if (rejected) {
                    countRejections(reason, 1);
                }
                return null;
            }));
        }
        return chain;
    }

    private Future<Void> closeTheConnectWindow() {
        return allocations.markNoShows(connectWindowSeconds).map(noShows -> {
            registry.counter(MetricNames.NO_SHOWS).increment(noShows);
            return null;
        }).compose(ignored -> allocations.allNoShow(connectWindowSeconds, BATCH)
                .compose(found -> rejectAll(found, RejectionReason.ALL_NO_SHOW)));
    }

    private void countRejections(RejectionReason reason, int matches) {
        if (matches > 0) {
            registry.counter(MetricNames.MATCH_REJECTIONS, MetricNames.TAG_REASON, reason.wireName())
                    .increment(matches);
        }
    }

    private static int secondsFromEnv(String name, int fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        return positive(Integer.parseInt(raw.trim()), name);
    }

    private static int positive(int seconds, String name) {
        if (seconds <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + seconds);
        }
        return seconds;
    }
}
