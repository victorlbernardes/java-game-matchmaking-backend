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
     * Players waiting in one partition of a queue (W20, ADR 0028). The sum is {@link #QUEUE_DEPTH};
     * the per-partition series is what makes a hotspot visible instead of implied.
     */
    public static final String QUEUE_PARTITION_DEPTH = "matchmaking.queue.partition.depth";

    /**
     * Max partition depth divided by the mean, per queue (W20). 1.0 is a perfectly even queue;
     * {@code P} means one partition holds everything, which is what a single-key queue always was.
     */
    public static final String QUEUE_HOTSPOT_RATIO = "matchmaking.queue.hotspot.ratio";

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
    /** The ticket asked for a region this deployment does not serve (422, W21). */
    public static final String REASON_REGION_UNSUPPORTED = "region_unsupported";
    /** The player's server-side distance to the region is above its own ceiling (422, W21). */
    public static final String REASON_LATENCY_BUDGET = "latency_budget";

    // ── Regions and the distance to them (W21, ADR 0039) ────────────────────────────

    /**
     * The player's measured round trip to a region, as the server timed it (millis, in the name so
     * no client has to guess the unit). The distribution the placement decision is made on - and,
     * next to the declared matrix, the evidence that a region a player was moved to really costs
     * what the model says.
     */
    public static final String PLAYER_RTT = "matchmaking.player.rtt.ms";
    /**
     * Probes per outcome: {@code measured} (a nonce came back and was timed) or
     * {@code invalid_nonce} (unknown, expired, replayed, or another player's). A probe is a
     * placement input, so what the server refused to measure is a number too.
     */
    public static final String LATENCY_PROBES = "matchmaking.latency.probes";
    public static final String RESULT_MEASURED = "measured";
    public static final String RESULT_INVALID_NONCE = "invalid_nonce";

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

    // ── Capacity and allocation: is there a server for the match? (W22, ADR 0031) ────

    /**
     * Matches the orchestrator tried to place on a game server, tagged by {@link #TAG_RESULT}.
     * {@link #RESULT_ALLOCATED} is a match that will run somewhere; {@link #RESULT_NO_CAPACITY} is
     * one that was confirmed and has nowhere to go — the number W23's timeout works through, and
     * the reason the engine also refuses to form matches when the pool is full.
     */
    public static final String ALLOCATIONS = "matchmaking.allocations";
    public static final String RESULT_ALLOCATED = "allocated";
    public static final String RESULT_NO_CAPACITY = "no_capacity";

    /**
     * Slots a region still has, per mode, as the engine reads them (W22). Zero with a registered
     * pool means matching is suspended for that (mode, region) — the gauge the gate decides on, and
     * the one to look at when arrivals stop pairing and nothing looks broken.
     */
    public static final String CAPACITY_FREE = "matchmaking.capacity.free";

    /**
     * Pairing ticks that found no capacity and left the queue alone (W22). Not an error: the player
     * waits instead of being confirmed into a match that cannot run. A rate that stays above zero
     * is a pool that needs servers, said in the place an operator already looks.
     */
    public static final String PAIRING_SUSPENDED = "matchmaking.pairing.suspended";

    // ── Tags ────────────────────────────────────────────────────────────────────────

    public static final String TAG_MODE = "mode";
    public static final String TAG_REGION = "region";
    /** Which partition of a queue a sample is about (W20, ADR 0028). */
    public static final String TAG_PARTITION = "partition";
    public static final String TAG_STATUS = "status";
    public static final String TAG_REASON = "reason";
    /** The outcome of an operation the caller can act on (e.g. a delivery: delivered or missed). */
    public static final String TAG_RESULT = "result";

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

    // ── The socket layer: who is connected, and did the notification land? (W19, ADR 0026) ──

    /**
     * WebSocket connections open on this instance right now, tagged by {@link #TAG_INSTANCE_ID}.
     * Per instance on purpose: the total across instances is the interesting number, and it is the
     * only one that can be summed from series that each say where they came from.
     */
    public static final String WS_CONNECTIONS = "matchmaking.ws.connections";
    /**
     * A player opened a new socket while this instance still held an older one for them (the older
     * one is closed). A number that climbs says clients are reconnecting before the previous
     * connection is torn down - the ordinary shape of a reconnect, made visible.
     */
    public static final String WS_CONNECTIONS_REPLACED = "matchmaking.ws.connections.replaced";
    /**
     * Notifications this instance tried to deliver, tagged by {@link #TAG_RESULT}. The two results
     * are the whole question of W19: {@link #RESULT_DELIVERED} is a message that found a socket
     * here, and {@link #RESULT_NO_LOCAL_CONNECTION} is one that did not - the player is on another
     * instance, or on none. Nothing else can tell those two apart, and it is why a lost
     * notification used to be indistinguishable from one that was never sent.
     */
    public static final String WS_NOTIFICATIONS = "matchmaking.ws.notifications";
    public static final String RESULT_DELIVERED = "delivered";
    public static final String RESULT_NO_LOCAL_CONNECTION = "no_local_connection";
    /**
     * Heartbeats that re-sent a pending confirmation because the player still had one to answer
     * (W19, ADR 0026). Each one is a notification the pub/sub path did not deliver - the client
     * missed it and asked again - which is the repair path doing its job, not an error.
     */
    public static final String WS_REPRESENTED = "matchmaking.ws.represented";

    /**
     * Which instance answered. Not named {@code instance}: Prometheus adds an {@code instance}
     * label of its own at scrape time, and a metric that already has one is renamed rather than
     * merged - the series would read {@code exported_instance} and answer a different question.
     */
    public static final String TAG_INSTANCE_ID = "instance_id";

    private MetricNames() {
    }
}
