package com.victor.matchmaking.engine.infrastructure;

import io.vertx.core.Future;

/**
 * Reads the outbox from Postgres and publishes it as KPIs (ADR 0020).
 *
 * <p>The backlog is derived from the table, not counted in memory: the relay may have started
 * before this process, and a row parked yesterday is still parked. The two numbers that matter
 * operationally are the size of the backlog ({@code matchmaking_outbox_backlog}) and how long
 * the oldest row has been waiting ({@code matchmaking_outbox_oldest_pending_seconds}) - a
 * backlog that is not growing but has a 10-minute-old head is a stopped relay, not a busy one.
 */
public final class OutboxMetricsPoller {

    private final PostgresOutboxQueue outbox;
    private final EngineKpis kpis;

    public OutboxMetricsPoller(PostgresOutboxQueue outbox, EngineKpis kpis) {
        this.outbox = outbox;
        this.kpis = kpis;
    }

    /** One pass: rows per status, the head's age and the newest publish lag. */
    public Future<Void> pollOnce() {
        return outbox.countByStatus()
                .compose(counts -> {
                    counts.forEach(kpis::outboxStatus);
                    // A status with no rows left still has to be published as 0, for the same
                    // reason the queues do: the last value must not outlive the condition.
                    kpis.outboxStatus(PostgresOutboxQueue.PENDING,
                            counts.getOrDefault(PostgresOutboxQueue.PENDING, 0L));
                    kpis.outboxStatus(PostgresOutboxQueue.PUBLISHED,
                            counts.getOrDefault(PostgresOutboxQueue.PUBLISHED, 0L));
                    kpis.outboxStatus(PostgresOutboxQueue.PARKED,
                            counts.getOrDefault(PostgresOutboxQueue.PARKED, 0L));
                    return outbox.oldestPendingAge();
                })
                .compose(age -> {
                    kpis.outboxOldestPending(age);
                    return outbox.newestPublishLag();
                })
                .map(lag -> {
                    kpis.outboxPublishLag(lag);
                    return (Void) null;
                });
    }
}
