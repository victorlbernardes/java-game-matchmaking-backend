package com.victor.matchmaking.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

import com.google.protobuf.Message;
import com.victor.matchmaking.events.EventContracts;

import io.vertx.core.Future;

/**
 * Publishes what the outbox holds (ADR 0018).
 *
 * <p>This is the other half of the transactional outbox: the fact has already
 * committed, and the event is now a row in a table. The relay claims a batch, publishes
 * it in insertion order, and marks each row published only after the broker
 * acknowledged it. Nothing is deleted, so a crash between the publish and the mark
 * costs a duplicate record - which the consumer's inbox (ADR 0038) absorbs - instead of
 * costing the event.
 *
 * <p>The policy on failure is in the row, not in this process: a failed publish is
 * rescheduled with exponential backoff (1 s doubling up to 30 s, so a broker outage
 * does not burn every attempt in seconds), and after {@code maxAttempts} the row is
 * parked with its error. A parked row does not block the queue: the relay works by
 * insertion order per batch, and the next batch does not wait for it.
 */
public final class OutboxRelay {

    public static final int DEFAULT_BATCH_SIZE = 20;
    public static final int DEFAULT_MAX_ATTEMPTS = 10;
    public static final Duration DEFAULT_FIRST_BACKOFF = Duration.ofSeconds(1);
    public static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(30);

    private static final System.Logger LOG = System.getLogger(OutboxRelay.class.getName());

    private final OutboxQueue queue;
    private final EventPublisher publisher;
    private final Clock clock;
    private final String relayId;
    private final int batchSize;
    private final int maxAttempts;
    private final Duration firstBackoff;
    private final Duration maxBackoff;

    public OutboxRelay(OutboxQueue queue, EventPublisher publisher, Clock clock, String relayId) {
        this(queue, publisher, clock, relayId, DEFAULT_BATCH_SIZE, DEFAULT_MAX_ATTEMPTS,
                DEFAULT_FIRST_BACKOFF, DEFAULT_MAX_BACKOFF);
    }

    public OutboxRelay(OutboxQueue queue, EventPublisher publisher, Clock clock, String relayId,
            int batchSize, int maxAttempts, Duration firstBackoff, Duration maxBackoff) {
        this.queue = queue;
        this.publisher = publisher;
        this.clock = clock;
        this.relayId = relayId;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.firstBackoff = firstBackoff;
        this.maxBackoff = maxBackoff;
    }

    /** One pass: claim a batch and work through it, oldest row first. */
    public Future<Report> drainOnce() {
        return queue.claim(relayId, batchSize).compose(claimed -> {
            if (claimed.isEmpty()) {
                return Future.succeededFuture(Report.empty());
            }
            List<OutboxRecord> ordered = claimed.stream()
                    .sorted(Comparator.comparing(OutboxRecord::createdAt).thenComparing(OutboxRecord::eventId))
                    .toList();
            return publishAll(ordered, 0, Report.of(ordered.size()));
        }).onSuccess(report -> {
            if (!report.isEmpty()) {
                LOG.log(System.Logger.Level.INFO, "outbox.drain relay={0} claimed={1} published={2} retried={3} parked={4}",
                        relayId, report.claimed(), report.published(), report.retried(), report.parked());
            }
        });
    }

    private Future<Report> publishAll(List<OutboxRecord> events, int index, Report report) {
        if (index >= events.size()) {
            return Future.succeededFuture(report);
        }
        return publish(events.get(index))
                // Sequential on purpose: the rows of a batch go out in insertion order, so
                // the events of one match keep the order they were written in.
                .compose(outcome -> publishAll(events, index + 1, report.plus(outcome)));
    }

    private Future<Outcome> publish(OutboxRecord event) {
        Message message;
        try {
            message = EventContracts.parse(event.topic(), event.payload());
        } catch (RuntimeException e) {
            // No code in this build can turn these bytes into an event of this topic, so
            // retrying cannot help: park it with the reason instead of burning attempts.
            return giveUp(event, "undecodable payload: " + describe(e));
        }
        return publisher.publish(event.topic(), event.partitionKey(), message)
                .compose(ignored -> queue.markPublished(event.eventId(), clock.instant()).map(ignored2 -> {
                    LOG.log(System.Logger.Level.INFO, "outbox.published relay={0} eventId={1} topic={2} key={3} lagMs={4}",
                            relayId, event.eventId(), event.topic(), event.partitionKey(),
                            clock.millis() - event.occurredAt().toEpochMilli());
                    return Outcome.PUBLISHED;
                }))
                .recover(error -> reschedule(event, error));
    }

    private Future<Outcome> reschedule(OutboxRecord event, Throwable error) {
        int attempts = event.attempts() + 1;
        if (attempts >= maxAttempts) {
            return giveUp(event, describe(error));
        }
        Duration backoff = backoff(attempts);
        Instant nextAttemptAt = clock.instant().plus(backoff);
        LOG.log(System.Logger.Level.WARNING, "outbox.retry relay={0} eventId={1} topic={2} attempt={3} backoffMs={4} nextAttemptAt={5} error={6}",
                relayId, event.eventId(), event.topic(), attempts, backoff.toMillis(), nextAttemptAt, describe(error));
        return queue.markFailed(event.eventId(), describe(error), nextAttemptAt)
                .map(ignored -> Outcome.RETRIED);
    }

    private Future<Outcome> giveUp(OutboxRecord event, String error) {
        LOG.log(System.Logger.Level.ERROR, "outbox.parked relay={0} eventId={1} topic={2} attempts={3} error={4}",
                relayId, event.eventId(), event.topic(), event.attempts(), error);
        return queue.park(event.eventId(), error).map(ignored -> Outcome.PARKED);
    }

    private Duration backoff(int attempt) {
        // 1s, 2s, 4s, 8s, 16s, 30s, 30s...
        long ms = Math.min(firstBackoff.toMillis() * (1L << (attempt - 1)), maxBackoff.toMillis());
        return Duration.ofMillis(ms);
    }

    private static String describe(Throwable e) {
        return e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    /** The result of one drain pass. */
    public static final class Report {
        private final int claimed;
        private int published;
        private int retried;
        private int parked;

        private Report(int claimed) {
            this.claimed = claimed;
        }

        public static Report empty() {
            return new Report(0);
        }

        public static Report of(int claimed) {
            return new Report(claimed);
        }

        public boolean isEmpty() {
            return claimed == 0;
        }

        public int claimed() {
            return claimed;
        }

        public int published() {
            return published;
        }

        public int retried() {
            return retried;
        }

        public int parked() {
            return parked;
        }

        private Report plus(Outcome outcome) {
            return switch (outcome) {
                case PUBLISHED -> plusPublished();
                case RETRIED -> plusRetried();
                case PARKED -> plusParked();
            };
        }

        private Report plusPublished() {
            Report copy = new Report(claimed);
            copy.published = published + 1;
            copy.retried = retried;
            copy.parked = parked;
            return copy;
        }

        private Report plusRetried() {
            Report copy = new Report(claimed);
            copy.published = published;
            copy.retried = retried + 1;
            copy.parked = parked;
            return copy;
        }

        private Report plusParked() {
            Report copy = new Report(claimed);
            copy.published = published;
            copy.retried = retried;
            copy.parked = parked + 1;
            return copy;
        }
    }

    private enum Outcome {
        PUBLISHED, RETRIED, PARKED
    }
}