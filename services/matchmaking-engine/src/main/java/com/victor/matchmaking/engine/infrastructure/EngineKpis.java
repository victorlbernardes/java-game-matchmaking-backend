package com.victor.matchmaking.engine.infrastructure;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.engine.application.OutboxRelay;
import com.victor.matchmaking.metrics.MetricNames;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

/**
 * The domain KPIs of the engine, on the process registry (ADR 0020).
 *
 * <p>Two kinds of number, and the difference is deliberate:
 *
 * <ul>
 *   <li><b>Counted where the event happens</b> (matches formed, confirmed, dissolved, the wait
 *       of each matched ticket, the relay's own report). Only the code that makes the decision
 *       can count it, and a counter that resets on restart is what Prometheus expects -
 *       {@code rate()} handles the reset, and the durable truth stays in Postgres.</li>
 *   <li><b>Derived from state</b> (queue depth, the head's wait, the outbox backlog). These are
 *       polled asynchronously from Redis/Postgres every few seconds and published through a
 *       holder the gauge reads. A Micrometer gauge is read synchronously and the stores here
 *       are async: blocking a scrape would block the event loop, so the scrape reads the last
 *       poll (bounded staleness, {@code KPI_POLL_INTERVAL} in the application).</li>
 * </ul>
 */
public final class EngineKpis {

    private final MeterRegistry registry;
    private final Map<String, AtomicLong> queueDepth = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> queueOldestWaitSeconds = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> outboxBacklog = new ConcurrentHashMap<>();
    private final AtomicLong outboxOldestPendingSeconds = new AtomicLong();
    private final AtomicLong outboxPublishLagSeconds = new AtomicLong();

    public EngineKpis(MeterRegistry registry) {
        this.registry = registry;
        registerOutboxGauges();
    }

    /** A roster left the queues: one match formed, and one wait per player measured. */
    public void matchFormed(List<MatchmakingTicket> roster, Instant now) {
        MatchmakingTicket reference = roster.get(0);
        Tags tags = tagsOf(reference.gameMode(), reference.region());
        registry.counter(MetricNames.MATCHES_FORMED, tags).increment();
        // A percentile histogram, not just count/sum: "the slowest 5% of the players waited
        // 40 s" is the sentence that says whether the pairing policy is working, and it needs
        // buckets (Grafana reads it with histogram_quantile).
        Timer wait = Timer.builder(MetricNames.MATCH_WAIT).tags(tags)
                .description("Time from entering the queue to the match being formed")
                .publishPercentileHistogram()
                .register(registry);
        for (MatchmakingTicket ticket : roster) {
            wait.record(Duration.between(ticket.queuedAt(), now));
        }
    }

    /** Every player confirmed and the match (and its event) is committed. */
    public void matchConfirmed(GameMode gameMode, Region region) {
        registry.counter(MetricNames.MATCHES_CONFIRMED, tagsOf(gameMode, region)).increment();
    }

    /**
     * A match fell apart before confirmation. {@code reason} is one of the values in
     * {@link MetricNames} - never the free-form text of the log, which would explode the
     * cardinality of the series.
     *
     * <p>Only the reason is tagged: the pending match in Redis carries its roster and its
     * deadline, not the mode and region, and looking the tickets up just to tag a counter
     * would put a read on the dissolution path. How many matches of a mode/region fall apart
     * is the difference between {@code matches_formed} and {@code matches_confirmed} for that
     * mode/region - the same answer from numbers already collected.
     */
    public void matchDissolved(String reason) {
        registry.counter(MetricNames.MATCHES_DISSOLVED, Tags.of(MetricNames.TAG_REASON, reason)).increment();
    }

    /** What one pass of the relay did (ADR 0018): published, rescheduled, parked. */
    public void relayDrained(OutboxRelay.Report report) {
        if (report.published() > 0) {
            registry.counter(MetricNames.OUTBOX_PUBLISHED).increment(report.published());
        }
        if (report.retried() > 0) {
            registry.counter(MetricNames.OUTBOX_RETRIED).increment(report.retried());
        }
        if (report.parked() > 0) {
            registry.counter(MetricNames.OUTBOX_PARKED).increment(report.parked());
        }
    }

    /** Players waiting in one queue and how long the player at its head has been waiting. */
    public void queue(GameMode gameMode, Region region, long players, Duration oldestWait) {
        Tags tags = tagsOf(gameMode, region);
        String key = gameMode.label() + ":" + region.name();
        gauge(MetricNames.QUEUE_DEPTH, tags, queueDepth, key,
                "Players waiting in this queue").set(players);
        gauge(MetricNames.QUEUE_OLDEST_WAIT_SECONDS, tags, queueOldestWaitSeconds, key,
                "How long the player at the head of this queue has been waiting, in seconds")
                .set(oldestWait == null ? 0L : Math.max(0L, oldestWait.toSeconds()));
    }

    /** Rows of the outbox in one status: PENDING is the backlog, PARKED needs a human. */
    public void outboxStatus(String status, long rows) {
        gauge(MetricNames.OUTBOX_BACKLOG, Tags.of(MetricNames.TAG_STATUS, status), outboxBacklog, status,
                "Outbox rows per status").set(rows);
    }

    /** The age of the oldest row still waiting: the relay's real lag, not the poll interval. */
    public void outboxOldestPending(Duration age) {
        outboxOldestPendingSeconds.set(age == null ? 0L : Math.max(0L, age.toSeconds()));
    }

    /** Fact to broker for the newest published row, in seconds. */
    public void outboxPublishLag(Duration lag) {
        outboxPublishLagSeconds.set(lag == null ? 0L : Math.max(0L, lag.toSeconds()));
    }

    private AtomicLong gauge(String name, Tags tags, Map<String, AtomicLong> holders, String key,
            String description) {
        return holders.computeIfAbsent(key, ignored -> {
            AtomicLong holder = new AtomicLong();
            Gauge.builder(name, holder, AtomicLong::doubleValue)
                    .tags(tags)
                    .description(description)
                    .register(registry);
            return holder;
        });
    }

    private static Tags tagsOf(GameMode gameMode, Region region) {
        return Tags.of(MetricNames.TAG_MODE, gameMode.label(), MetricNames.TAG_REGION, region.name());
    }

    /**
     * Registers the two gauges that have a single series (no tag: there is one outbox), so the
     * exposition carries them even before the first poll.
     */
    private void registerOutboxGauges() {
        Gauge.builder(MetricNames.OUTBOX_OLDEST_PENDING_SECONDS, outboxOldestPendingSeconds,
                        AtomicLong::doubleValue)
                .description("Age of the oldest outbox row still waiting to be published, in seconds")
                .register(registry);
        Gauge.builder(MetricNames.OUTBOX_PUBLISH_LAG_SECONDS, outboxPublishLagSeconds,
                        AtomicLong::doubleValue)
                .description("Time from the fact to the broker for the newest published event, in seconds")
                .register(registry);
    }
}
