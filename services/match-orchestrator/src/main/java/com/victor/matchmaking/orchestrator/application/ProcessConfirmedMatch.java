package com.victor.matchmaking.orchestrator.application;

import java.time.Instant;
import java.util.UUID;

import com.victor.matchmaking.orchestrator.domain.ProcessedEvent;
import com.victor.matchmaking.orchestrator.domain.ProcessingOutcome;

/**
 * Handles one record from the log: decode it, apply its effect once, and commit its
 * offset - with a bounded number of attempts and a dead letter queue behind them.
 *
 * <p>The order is the point. The offset is committed only after the outcome is durable,
 * whether that outcome is the effect or a dead letter; and the effect is applied through
 * the inbox, so a redelivery after a commit that never happened is a no-op instead of a
 * second effect. Committing before the outcome is durable would lose the record
 * silently, which is why the "dead letter failed" case leaves the offset untouched.
 *
 * <p><b>Why decoding is retried too:</b> a payload that does not decode may be genuinely
 * broken, but it may also be a Schema Registry that is briefly unreachable. Both are
 * worth a second and third attempt; a record that fails all of them goes to the dead
 * letter queue either way, with the stage recorded so the two can be told apart.
 */
public final class ProcessConfirmedMatch {

    private static final System.Logger LOG = System.getLogger("orchestrator.match-confirmed");

    private final String consumerGroup;
    private final MatchConfirmedDecoder decoder;
    private final ConfirmedMatchInbox inbox;
    private final OffsetCommitter committer;
    private final DeadLetterQueue deadLetters;
    private final RetryPolicy retryPolicy;

    public ProcessConfirmedMatch(String consumerGroup, MatchConfirmedDecoder decoder, ConfirmedMatchInbox inbox,
            OffsetCommitter committer, DeadLetterQueue deadLetters, RetryPolicy retryPolicy) {
        this.consumerGroup = consumerGroup;
        this.decoder = decoder;
        this.inbox = inbox;
        this.committer = committer;
        this.deadLetters = deadLetters;
        this.retryPolicy = retryPolicy;
    }

    public ProcessingOutcome handle(RawRecord record) {
        Failure lastFailure = null;
        for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
            DecodedEvent decoded = null;
            ProcessingOutcome outcome;
            try {
                // Decoded once per attempt: a retry after a registry blip decodes again.
                decoded = decoder.decode(record);
                outcome = inbox.applyOnce(decoded.allocation(), processedEventOf(record, decoded))
                        .toCompletionStage()
                        .toCompletableFuture()
                        // Blocks the consumer's own thread, never an event loop: a Kafka
                        // consumer is single-threaded by design, and the effect is the
                        // only thing that thread has to do.
                        .join();
            } catch (RuntimeException failure) {
                // Whether the payload never became an event or the effect failed is
                // read from how far this attempt got, not guessed from the exception.
                lastFailure = decoded == null
                        ? new Failure(FailureStage.DECODE, failure, null)
                        : new Failure(FailureStage.APPLY, failure, decoded.eventId());
                logRetry(record, lastFailure, attempt);
                pauseBeforeNextAttempt(attempt);
                continue;
            }
            // Outside the retry block on purpose: a commit that fails is the broker's
            // problem, and retrying the effect (or worse, dead-lettering a record whose
            // effect was applied) would be the wrong reaction to it.
            committer.markProcessed(record);
            LOG.log(System.Logger.Level.INFO,
                    "match.confirmed.handled matchId={0} eventId={1} outcome={2} topic={3} partition={4} offset={5} attempt={6}",
                    decoded.allocation().matchId(), decoded.eventId(), outcome, record.topic(),
                    String.valueOf(record.partition()), String.valueOf(record.offset()),
                    String.valueOf(attempt));
            return outcome;
        }
        return deadLetter(record, lastFailure);
    }

    private ProcessedEvent processedEventOf(RawRecord record, DecodedEvent decoded) {
        return new ProcessedEvent(decoded.eventId(), consumerGroup, record.topic(), record.partition(),
                record.offset(), Instant.now());
    }

    /**
     * Last resort: the original payload is published, and only then is the offset
     * committed. If publishing fails the exception escapes and nothing is committed, so
     * the record is delivered again - which is the whole difference between a dead letter
     * queue and a bin.
     */
    private ProcessingOutcome deadLetter(RawRecord record, Failure failure) {
        deadLetters.publish(new DeadLetter(record, consumerGroup, failure.stage(),
                retryPolicy.maxAttempts(), failure.error().getClass().getName(),
                String.valueOf(failure.error().getMessage()), failure.eventId()));
        committer.markProcessed(record);
        LOG.log(System.Logger.Level.ERROR,
                "match.confirmed.dead-lettered topic={0} partition={1} offset={2} stage={3} attempts={4} error={5}",
                record.topic(), String.valueOf(record.partition()), String.valueOf(record.offset()),
                failure.stage(), String.valueOf(retryPolicy.maxAttempts()), failure.error().toString());
        return ProcessingOutcome.DEAD_LETTERED;
    }

    private void logRetry(RawRecord record, Failure failure, int attempt) {
        System.Logger.Level level = attempt < retryPolicy.maxAttempts()
                ? System.Logger.Level.WARNING
                : System.Logger.Level.INFO;
        LOG.log(level, "match.confirmed.retrying topic={0} partition={1} offset={2} stage={3} attempt={4}/{5} error={6}",
                record.topic(), String.valueOf(record.partition()), String.valueOf(record.offset()),
                failure.stage(), String.valueOf(attempt), String.valueOf(retryPolicy.maxAttempts()),
                failure.error().toString());
    }

    private void pauseBeforeNextAttempt(int attempt) {
        if (attempt >= retryPolicy.maxAttempts()) {
            return;
        }
        try {
            Thread.sleep(retryPolicy.backoffBefore(attempt + 1).toMillis());
        } catch (InterruptedException e) {
            // Shutting down: the record stays uncommitted and is redelivered next time.
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting to retry attempt " + (attempt + 1), e);
        }
    }

    private record Failure(FailureStage stage, RuntimeException error, UUID eventId) {
    }
}
