package com.victor.matchmaking.engine.application;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.victor.matchmaking.db.RejectedMatchRepository;
import com.victor.matchmaking.db.RejectedMatchRepository.RejectedMatch;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.redis.TicketRepository;
import com.victor.matchmaking.redis.TicketRepository.RequeueOutcome;

import io.vertx.core.Future;

/**
 * The engine's half of W23 (ADR 0032): puts the players of a rejected match back in the queue.
 *
 * <p>The orchestrator decides that a match cannot run and writes {@code REJECTED}; this class reads
 * those rows every second, requeues every ticket of the roster and then marks the match. The order
 * is the design, not a detail:
 *
 * <ol>
 *   <li><b>Requeue first, mark after.</b> A crash between the two repeats the requeue on the next
 *       tick, and the requeue is idempotent (the ticket is already {@code SEARCHING}, the Lua script
 *       answers {@code UNCHANGED}). The opposite order would lose the players of every match the
 *       engine was marking when it died.</li>
 *   <li><b>The ticket's own partition and score.</b> The queue entry goes back where it was
 *       ({@code queuedAt} as the score, the same jump hash as the create), so a player who waited
 *       40 s does not start over at the end of the queue.</li>
 *   <li><b>Only this region's matches.</b> An engine requeues what it would pair (W21).</li>
 * </ol>
 *
 * <p>Nothing here retries a failed write: the row stays {@code requeued_at IS NULL} and the next
 * tick tries again. That is the retry, and it costs one query per second.
 */
public final class MatchRequeueSweeper {

    private final List<Region> regions;
    private final RejectedMatchRepository rejected;
    private final TicketRepository tickets;
    private final EngineKpis kpis;
    private final Clock clock;
    private final int batchPerRegion;

    /** One sweep at a time: a slow database must not stack ticks on top of each other. */
    private final AtomicBoolean inFlight = new AtomicBoolean();

    public MatchRequeueSweeper(List<Region> regions, RejectedMatchRepository rejected, TicketRepository tickets,
            EngineKpis kpis, Clock clock, int batchPerRegion) {
        this.regions = List.copyOf(regions);
        this.rejected = rejected;
        this.tickets = tickets;
        this.kpis = kpis;
        this.clock = clock;
        this.batchPerRegion = batchPerRegion;
    }

    /**
     * @return how many matches were requeued in this pass (a number, so the caller can log a tick
     *         that did something without logging one that did not)
     */
    public Future<Integer> sweepOnce() {
        if (!inFlight.compareAndSet(false, true)) {
            return Future.succeededFuture(0);
        }
        AtomicInteger requeued = new AtomicInteger();
        Future<Void> chain = Future.succeededFuture();
        for (Region region : regions) {
            chain = chain.compose(ignored -> requeueRegion(region, requeued));
        }
        return chain
                .map(ignored -> requeued.get())
                .onComplete(done -> inFlight.set(false));
    }

    private Future<Void> requeueRegion(Region region, AtomicInteger requeued) {
        return rejected.pendingRequeue(region, batchPerRegion).compose(matches -> {
            Future<Void> chain = Future.succeededFuture();
            for (RejectedMatch match : matches) {
                chain = chain.compose(ignored -> requeueMatch(match).map(counted -> {
                    if (counted) {
                        requeued.incrementAndGet();
                    }
                    return null;
                }));
            }
            return chain;
        });
    }

    /** @return true when the match was marked requeued by this pass */
    private Future<Boolean> requeueMatch(RejectedMatch match) {
        Future<Void> chain = Future.succeededFuture();
        AtomicInteger back = new AtomicInteger();
        AtomicInteger expired = new AtomicInteger();
        MatchmakingTicket[] reference = new MatchmakingTicket[1];
        for (String ticketId : match.ticketIds()) {
            chain = chain.compose(ignored -> tickets.findById(ticketId).compose(ticket -> {
                if (ticket == null) {
                    // The ticket's TTL ran out while the match waited: there is no queue entry to
                    // give back, and the player is not in the system any more.
                    expired.incrementAndGet();
                    return Future.succeededFuture();
                }
                reference[0] = reference[0] == null ? ticket : reference[0];
                return tickets.requeue(ticket).map(outcome -> {
                    if (outcome == RequeueOutcome.REQUEUED) {
                        back.incrementAndGet();
                    }
                    // UNCHANGED is the idempotent repeat of a requeue (a crash after the ZADD and
                    // before the marker): the ticket is already back, and that is not a skip.
                    return null;
                });
            }));
        }
        return chain.compose(ignored -> rejected.markRequeued(match.matchId()))
                .map(marked -> {
                    // A pass that put nobody back (a repeat after a crash, an expired ticket) is not
                    // a requeue: counting it would make "how many matches came back" a count of ticks.
                    if (reference[0] != null && back.get() > 0) {
                        kpis.matchRequeued(reference[0].gameMode(), reference[0].region(), back.get(),
                                Duration.between(rejectedAt(match), clock.instant()));
                    }
                    // An event from before W23 carries no ticket: those players cannot be found in
                    // the queue, and the count is how an operator sees a replayed old match instead
                    // of a silent gap.
                    kpis.requeueSkipped(expired.get(), com.victor.matchmaking.metrics.MetricNames.REASON_TICKET_EXPIRED);
                    kpis.requeueSkipped(match.playersWithoutTicket(),
                            com.victor.matchmaking.metrics.MetricNames.REASON_NO_TICKET);
                    // A rejection with no roster has nothing to give back, and it is not a requeue:
                    // it is marked (so the next pass gets past it) and counted as neither.
                    return marked && !match.entries().isEmpty();
                });
    }

    /**
     * The rejection instant is the row's, not the ticket's: the wait that matters is how long the
     * players were out of the queue because of a decision, and the clock that made it is the row's.
     */
    private Instant rejectedAt(RejectedMatch match) {
        return match.rejectedAt() != null ? match.rejectedAt() : clock.instant();
    }
}
