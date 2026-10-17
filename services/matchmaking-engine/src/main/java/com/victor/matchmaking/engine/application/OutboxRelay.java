package com.victor.matchmaking.engine.application;

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
 * <p>This is the other half of the transactional outbox: the confirmation has already
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
        return queue.markFailed(event.eventId(), describe(error), nextAttemptAt).map(ignored -> Outcome.RETRIED);
    }

    private Future<Outcome> giveUp(OutboxRecord event, String reason) {
        LOG.log(System.Logger.Level.ERROR, "outbox.parked relay={0} eventId={1} topic={2} attempts={3} reason={4}",
                relayId, event.eventId(), event.topic(), event.attempts() + 1, reason);
        return queue.park(event.eventId(), reason).map(ignored -> Outcome.PARKED);
    }

    /** 1 s, 2 s, 4 s … capped: a broker outage must not park a queue in a few seconds. */
    private Duration backoff(int attempts) {
        Duration backoff = firstBackoff.multipliedBy(1L << Math.min(attempts - 1, 16));
        return backoff.compareTo(maxBackoff) > 0 ? maxBackoff : backoff;
    }

    private static String describe(Throwable error) {
        String message = error.getMessage();
        return message == null ? error.getClass().getName() : error.getClass().getName() + ": " + message;
    }

    /** What the relay did with the rows it claimed. */
    public record Report(int claimed, int published, int retried, int parked) {

        static Report of(int claimed) {
            return new Report(claimed, 0, 0, 0);
        }

        static Report empty() {
            return new Report(0, 0, 0, 0);
        }

        boolean isEmpty() {
            return claimed == 0;
        }

        Report plus(Outcome outcome) {
            return switch (outcome) {
                case PUBLISHED -> new Report(claimed, published + 1, retried, parked);
                case RETRIED -> new Report(claimed, published, retried + 1, parked);
                case PARKED -> new Report(claimed, published, retried, parked + 1);
            };
        }
    }

    private enum Outcome {
        PUBLISHED, RETRIED, PARKED
    }
}
