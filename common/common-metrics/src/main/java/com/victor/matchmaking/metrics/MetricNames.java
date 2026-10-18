package com.victor.matchmaking.metrics;

/**
 * The metric contract: every name and tag a dashboard, an alert or a test may rely on.
 *
 * <p>The names are written the Micrometer way (dots). Prometheus exposes them with
 * underscores ({@code matchmaking.queue.depth} becomes {@code matchmaking_queue_depth}),
 * so the exposition is renaming, never a different metric. Counters get the {@code _total}
 * suffix from Micrometer itself.
 *
 * <p>Names live here, in a module with no domain knowledge, so the same string cannot drift
 * between the service that writes the number and the panel that reads it.
 */
public final class MetricNames {

    // ── The domain: is the matchmaker doing its job? ────────────────────────────────

    /** Tickets that entered a queue. The denominator of every rate. */
    public static final String TICKETS_ENQUEUED = "matchmaking.tickets.enqueued";
    /** Matches the pairing script formed (players were taken off the queues). */
    public static final String MATCHES_FORMED = "matchmaking.matches.formed";
    /** Matches where every player confirmed: the matchmaker's output. */
    public static final String MATCHES_CONFIRMED = "matchmaking.matches.confirmed";
    /** Matches that fell apart before confirmation, tagged by reason. */
    public static final String MATCHES_DISSOLVED = "matchmaking.matches.dissolved";
    /** Time from a ticket entering the queue to the match being formed. */
    public static final String MATCH_WAIT = "matchmaking.match.wait";

    /** Players waiting right now, per mode and region. */
    public static final String QUEUE_DEPTH = "matchmaking.queue.depth";
    /**
     * How long the player at the head of a queue has been waiting. The depth alone lies -
     * a deep queue that moves is healthy, a shallow queue whose head is 10 minutes old is not.
     */
    public static final String QUEUE_OLDEST_WAIT_SECONDS = "matchmaking.queue.oldest.wait.seconds";

    // ── Admission control: what the system said no to, and why (W17, ADR 0022/0023) ──

    /**
     * Arrivals the boundary refused, tagged by reason. A refusal is a load-shedding decision, and
     * a decision nobody counts is indistinguishable from a system that is down: the queue full is
     * the system working, the rate limit is a caller misbehaving, and both must be visible.
     */
    public static final String TICKETS_REJECTED = "matchmaking.tickets.rejected";
    /** A queue was at its admission limit (503, try again shortly). */
    public static final String REASON_QUEUE_FULL = "queue_full";
    /** The caller exceeded its own request budget (429). */
    public static final String REASON_RATE_LIMITED = "rate_limited";

    // ── Backpressure: what the engine does when the database is behind (W17, ADR 0022) ──

    /**
     * Writes the engine has in flight against Postgres. The bound is the point: a number that
     * climbs to the limit and stays there says the database is the constraint, and it says it
     * before the database goes down (which is the failure it exists to prevent).
     */
    public static final String DB_WRITES_IN_FLIGHT = "matchmaking.db.writes.in.flight";
    /**
     * Confirmations that found the database at its limit and were asked to wait. Each one is a
     * match whose players confirmed and whose row is not written yet: a count that grows is a
     * database that needs attention, not a matchmaker that is broken.
     */
    public static final String DB_WRITE_DEFERRED = "matchmaking.db.write.deferred";


    // ── The outbox: is what happened getting out? (ADR 0018) ────────────────────────

    /** Rows per status: PENDING is the backlog, PARKED is what needs a human. */
    public static final String OUTBOX_BACKLOG = "matchmaking.outbox.backlog";
    /** Age of the oldest row still waiting to be published. The relay's real lag. */
    public static final String OUTBOX_OLDEST_PENDING_SECONDS = "matchmaking.outbox.oldest.pending.seconds";
    /** Time from the fact happening to the broker having it, for the newest published row. */
    public static final String OUTBOX_PUBLISH_LAG_SECONDS = "matchmaking.outbox.publish.lag.seconds";
    /** Rows the relay published (one per attempt acked by the broker, so duplicates count once). */
    public static final String OUTBOX_PUBLISHED = "matchmaking.outbox.published";
    /** Failed attempts the relay rescheduled with backoff. */
    public static final String OUTBOX_RETRIED = "matchmaking.outbox.retried";
    /** Rows the relay gave up on; each one needs a human. */
    public static final String OUTBOX_PARKED = "matchmaking.outbox.parked";

    // ── Tags ────────────────────────────────────────────────────────────────────────

    public static final String TAG_MODE = "mode";
    public static final String TAG_REGION = "region";
    public static final String TAG_STATUS = "status";
    public static final String TAG_REASON = "reason";

    // ── Tag values that are not an enum constant ────────────────────────────────────

    /** A match dissolved because the confirmation window closed (the roster stalled). */
    public static final String REASON_TIMEOUT = "timeout";
    /** A match dissolved because a player declined it. */
    public static final String REASON_DECLINED = "declined";

    // ── Dependencies: who is down, and is it us? (W18, ADR 0024) ─────────────────────

    /**
     * Requests the API refused because a dependency is unreachable. Counted where the refusal is
     * decided, tagged by dependency ({@link #TAG_DEPENDENCY}), so "the hot state is down" is a
     * series and not a hunt through 500s. An unavailable dependency is a 503: nothing is broken,
     * the answer is "not now" - and a 500 here teaches a client to retry the wrong way (or not
     * at all).
     */
    public static final String DEPENDENCY_ERRORS = "matchmaking.dependency.errors";

    /** The hot state: tickets, queues, the rate-limit buckets (Redis). */
    public static final String TAG_DEPENDENCY_REDIS = "redis";
    /** The source of truth: match history (PostgreSQL). */
    public static final String TAG_DEPENDENCY_POSTGRES = "postgres";

    public static final String TAG_DEPENDENCY = "dependency";

    private MetricNames() {
    }
}
