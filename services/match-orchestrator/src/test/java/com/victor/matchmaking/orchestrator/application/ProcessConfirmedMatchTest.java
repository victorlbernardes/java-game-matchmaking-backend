package com.victor.matchmaking.orchestrator.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.orchestrator.domain.AllocationRequest;
import com.victor.matchmaking.orchestrator.domain.AllocationStatus;
import com.victor.matchmaking.orchestrator.domain.ProcessedEvent;
import com.victor.matchmaking.orchestrator.domain.ProcessingOutcome;

import io.vertx.core.Future;

/**
 * What the consumer does with a record it cannot handle, tested without a broker.
 *
 * <p>Three things here are invisible in the happy path and each of them loses data when it
 * is wrong: the effect happens before the offset is committed, a failure is retried a
 * bounded number of times and then dead-lettered rather than retried forever, and a record
 * whose dead letter cannot be written is never committed.
 */
class ProcessConfirmedMatchTest {

    private static final String CONSUMER_GROUP = "w13-test";
    private static final RawRecord RECORD =
            new RawRecord("matchmaking.events.match-confirmed", 0, 7L, "match-w13-handler", new byte[] {1, 2, 3});
    private static final AllocationRequest ALLOCATION = new AllocationRequest("match-w13-handler",
            GameMode.ONE_V_ONE, Region.NA, 2, AllocationStatus.PENDING_ALLOCATION, Instant.now());
    private static final UUID EVENT_ID = UUID.randomUUID();
    /** No sleeping in the tests: the backoff sequence is asserted on its own. */
    private static final RetryPolicy FAST_RETRY = new RetryPolicy(3, Duration.ZERO);

    @Test
    void appliesTheEffectBeforeCommittingTheOffset() {
        List<String> calls = new ArrayList<>();
        ProcessConfirmedMatch processing = processing(
                decoder(), inboxApplying(ProcessingOutcome.APPLIED, calls), calls, new RecordingDeadLetters());

        ProcessingOutcome outcome = processing.handle(RECORD);

        assertEquals(ProcessingOutcome.APPLIED, outcome);
        assertEquals(List.of("effect", "commit"), calls);
    }

    @Test
    void aDuplicateIsDoneAndIsCommitted() {
        List<String> calls = new ArrayList<>();
        RecordingDeadLetters deadLetters = new RecordingDeadLetters();
        ProcessConfirmedMatch processing = processing(
                decoder(), inboxApplying(ProcessingOutcome.DUPLICATE, calls), calls, deadLetters);

        ProcessingOutcome outcome = processing.handle(RECORD);

        assertEquals(ProcessingOutcome.DUPLICATE, outcome, "a duplicate must not be redelivered forever");
        assertEquals(List.of("effect", "commit"), calls);
        assertTrue(deadLetters.letters.isEmpty(), "a duplicate is not a failure");
    }

    @Test
    void retriesABrokenEffectBeforeGivingUp() {
        List<ProcessingOutcome> attempts = new ArrayList<>();
        ConfirmedMatchInbox failsTwice = (allocation, event) -> {
            attempts.add(ProcessingOutcome.APPLIED);
            // Fails twice, works on the third attempt: a database blip, not a bad record.
            return attempts.size() < 3
                    ? Future.failedFuture(new IllegalStateException("database is down"))
                    : Future.succeededFuture(ProcessingOutcome.APPLIED);
        };
        RecordingDeadLetters deadLetters = new RecordingDeadLetters();

        ProcessingOutcome outcome = processing(decoder(), failsTwice, new ArrayList<>(), deadLetters).handle(RECORD);

        assertEquals(ProcessingOutcome.APPLIED, outcome);
        assertEquals(3, attempts.size());
        assertTrue(deadLetters.letters.isEmpty(), "a record that succeeds on a retry is not dead-lettered");
    }

    @Test
    void deadLettersAfterTheLastAttemptAndCommitsTheOffset() {
        List<String> calls = new ArrayList<>();
        RecordingDeadLetters deadLetters = new RecordingDeadLetters();
        ConfirmedMatchInbox alwaysFails = (allocation, event) -> {
            calls.add("effect");
            return Future.failedFuture(new IllegalStateException("boom"));
        };
        ProcessConfirmedMatch processing = processing(decoder(), alwaysFails, calls, deadLetters);

        ProcessingOutcome outcome = processing.handle(RECORD);

        assertEquals(ProcessingOutcome.DEAD_LETTERED, outcome);
        assertEquals(1, deadLetters.letters.size());
        DeadLetter letter = deadLetters.letters.get(0);
        assertEquals(FailureStage.APPLY, letter.stage());
        assertEquals(3, letter.attempts());
        assertEquals(EVENT_ID, letter.eventId(), "the record was decoded, so its event id is known");
        assertEquals(List.of("effect", "effect", "effect", "commit"), calls,
                "three attempts, and the partition must move on: the dead letter is the outcome");
    }

    @Test
    void deadLettersARecordThatNeverDecodes() {
        List<String> calls = new ArrayList<>();
        RecordingDeadLetters deadLetters = new RecordingDeadLetters();
        MatchConfirmedDecoder brokenDecoder = record -> {
            throw new IllegalArgumentException("not a protobuf payload");
        };

        ProcessingOutcome outcome = processing(brokenDecoder, inboxApplying(ProcessingOutcome.APPLIED, calls),
                calls, deadLetters).handle(RECORD);

        assertEquals(ProcessingOutcome.DEAD_LETTERED, outcome);
        assertFalse(calls.contains("effect"), "a record that never decoded never reached the effect");
        assertEquals(List.of("commit"), calls, "but the dead letter moved it out of the way");
        assertEquals(1, deadLetters.letters.size());
        assertEquals(FailureStage.DECODE, deadLetters.letters.get(0).stage());
        assertEquals(null, deadLetters.letters.get(0).eventId(), "nothing decoded, so nothing is known about it");
    }

    @Test
    void commitsNothingWhenTheDeadLetterCannotBePublished() {
        List<String> calls = new ArrayList<>();
        DeadLetterQueue unreachable = letter -> {
            throw new IllegalStateException("dead letter topic does not exist");
        };
        MatchConfirmedDecoder brokenDecoder = record -> {
            throw new IllegalArgumentException("not a protobuf payload");
        };
        ConfirmedMatchInbox inbox = (allocation, event) -> Future.succeededFuture(ProcessingOutcome.APPLIED);
        OffsetCommitter committer = record -> calls.add("commit");

        ProcessConfirmedMatch processing = new ProcessConfirmedMatch(CONSUMER_GROUP, brokenDecoder, inbox,
                committer, unreachable, FAST_RETRY);

        assertThrows(RuntimeException.class, () -> processing.handle(RECORD));
        // The record is neither applied nor stored anywhere: committing now would delete
        // the only copy of it.
        assertTrue(calls.isEmpty(), "an unhandled record must stay uncommitted");
    }

    @Test
    void theInboxReceivesTheRecordCoordinatesAndTheGroup() {
        List<ProcessedEvent> processed = new ArrayList<>();
        ConfirmedMatchInbox inbox = (allocation, event) -> {
            processed.add(event);
            return Future.succeededFuture(ProcessingOutcome.APPLIED);
        };
        ProcessConfirmedMatch processing = processing(decoder(), inbox, new ArrayList<>(), new RecordingDeadLetters());

        processing.handle(RECORD);

        ProcessedEvent event = processed.get(0);
        assertEquals(EVENT_ID, event.eventId());
        assertEquals(CONSUMER_GROUP, event.consumerGroup());
        assertEquals(RECORD.topic(), event.topic());
        assertEquals(RECORD.partition(), event.partition());
        assertEquals(RECORD.offset(), event.offset());
    }

    private static ProcessConfirmedMatch processing(MatchConfirmedDecoder decoder, ConfirmedMatchInbox inbox,
            List<String> calls, DeadLetterQueue deadLetters) {
        OffsetCommitter committer = record -> calls.add("commit");
        return new ProcessConfirmedMatch(CONSUMER_GROUP, decoder, inbox, committer, deadLetters, FAST_RETRY);
    }

    private static MatchConfirmedDecoder decoder() {
        return record -> new DecodedEvent(ALLOCATION, EVENT_ID);
    }

    private static ConfirmedMatchInbox inboxApplying(ProcessingOutcome outcome, List<String> calls) {
        return (allocation, event) -> {
            calls.add("effect");
            return Future.succeededFuture(outcome);
        };
    }

    private static final class RecordingDeadLetters implements DeadLetterQueue {

        private final List<DeadLetter> letters = new ArrayList<>();

        @Override
        public void publish(DeadLetter letter) {
            letters.add(letter);
        }
    }
}
