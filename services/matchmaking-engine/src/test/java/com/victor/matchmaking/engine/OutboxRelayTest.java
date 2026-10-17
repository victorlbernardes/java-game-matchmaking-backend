package com.victor.matchmaking.engine.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;
import com.victor.matchmaking.events.EventTopics;
import com.victor.matchmaking.events.MatchConfirmed;
import com.victor.matchmaking.proto.common.EventMetadata;

import io.vertx.core.Future;

/**
 * The policy of the relay (ADR 0018): what it publishes, in what order, when it retries
 * and when it gives up.
 *
 * <p>Everything here is decided by the use case, so it is tested without a broker and
 * without a database - the fakes record the *sequence*, which is the property that
 * matters: a row is marked published only after the broker acknowledged it, and a row
 * that fails does not stop the rows behind it.
 */
class OutboxRelayTest {

    private static final Instant NOW = Instant.parse("2026-10-17T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final String RELAY = "engine-test";

    @Test
    void publishesWhatItClaimedInInsertionOrder() {
        List<String> sequence = new ArrayList<>();
        FakeQueue queue = new FakeQueue(sequence);
        // Out of order on purpose: the claim is allowed to hand the rows in any order, and
        // publishing in the order they were written is the relay's job.
        queue.claimable.addAll(List.of(
                record("c", NOW.plusSeconds(3), 0),
                record("a", NOW.plusSeconds(1), 0),
                record("b", NOW.plusSeconds(2), 0)));
        FakePublisher publisher = new FakePublisher(sequence);

        OutboxRelay.Report report = await(new OutboxRelay(queue, publisher, CLOCK, RELAY).drainOnce());

        assertEquals(List.of("match-a", "match-b", "match-c"), publisher.publishedKeys);
        assertEquals(3, report.published());
        assertEquals(0, report.retried());
        assertEquals(0, report.parked());
        assertEquals(List.of("claim", "publish:match-a", "markPublished:a", "publish:match-b",
                "markPublished:b", "publish:match-c", "markPublished:c"), sequence);
    }

    @Test
    void marksPublishedOnlyAfterTheBrokerAcknowledged() {
        List<String> sequence = new ArrayList<>();
        FakeQueue queue = new FakeQueue(sequence);
        queue.claimable.add(record("a", NOW, 0));
        FakePublisher publisher = new FakePublisher(sequence);

        await(new OutboxRelay(queue, publisher, CLOCK, RELAY).drainOnce());

        // The order of the two calls is the guarantee: a row marked published before the
        // ack would lose the event on a crash, which is the whole point of the outbox.
        assertEquals(List.of("claim", "publish:match-a", "markPublished:a"), sequence);
        assertEquals(List.of(NOW), queue.publishedInstants);
    }

    @Test
    void aFailedPublishIsRescheduledWithExponentialBackoff() {
        FakeQueue queue = new FakeQueue(new ArrayList<>());
        queue.claimable.addAll(List.of(
                record("first", NOW, 0),
                record("second", NOW.plusSeconds(1), 1),
                record("far", NOW.plusSeconds(2), 5)));
        FakePublisher publisher = new FakePublisher(new ArrayList<>());
        publisher.failingKeys.addAll(List.of("match-first", "match-second", "match-far"));

        OutboxRelay.Report report = await(new OutboxRelay(queue, publisher, CLOCK, RELAY).drainOnce());

        assertEquals(0, report.published());
        assertEquals(3, report.retried());
        // 1 s, 2 s and then the cap: a broker outage must not burn ten attempts in seconds.
        assertEquals(List.of(NOW.plusSeconds(1), NOW.plusSeconds(2), NOW.plus(Duration.ofSeconds(30))),
                queue.scheduledRetries);
        assertTrue(queue.parkedEventIds.isEmpty());
    }

    @Test
    void parksAfterTheLastAttemptInsteadOfRetryingForever() {
        FakeQueue queue = new FakeQueue(new ArrayList<>());
        queue.claimable.add(record("poison", NOW, OutboxRelay.DEFAULT_MAX_ATTEMPTS - 1));
        FakePublisher publisher = new FakePublisher(new ArrayList<>());
        publisher.failingKeys.add("match-poison");

        OutboxRelay.Report report = await(new OutboxRelay(queue, publisher, CLOCK, RELAY).drainOnce());

        assertEquals(0, report.retried());
        assertEquals(1, report.parked());
        assertEquals(List.of("poison"), queue.parkedEventIds);
        assertEquals(List.of(), queue.scheduledRetries);
    }

    @Test
    void aPayloadThisBuildCannotDecodeIsParkedWithoutPublishing() {
        List<String> sequence = new ArrayList<>();
        FakeQueue queue = new FakeQueue(sequence);
        queue.claimable.add(new OutboxRecord("a", EventTopics.MATCH_CONFIRMED, "match-1", "MATCH_CONFIRMED",
                new byte[] {0x01, 0x02, 0x03}, NOW, NOW, 0));
        FakePublisher publisher = new FakePublisher(sequence);

        OutboxRelay.Report report = await(new OutboxRelay(queue, publisher, CLOCK, RELAY).drainOnce());

        assertEquals(List.of(), publisher.publishedKeys);
        assertEquals(1, report.parked());
        assertEquals(List.of("a"), queue.parkedEventIds);
        // Retrying bytes no code can read only delays the diagnosis.
        assertTrue(queue.errors.get(0).startsWith("undecodable payload"));
    }

    @Test
    void aTopicWithoutAContractIsParkedWithoutPublishing() {
        FakeQueue queue = new FakeQueue(new ArrayList<>());
        queue.claimable.add(new OutboxRecord("a", "matchmaking.events.unknown", "match-1", "UNKNOWN",
                new byte[] {0x01}, NOW, NOW, 0));
        FakePublisher publisher = new FakePublisher(new ArrayList<>());

        await(new OutboxRelay(queue, publisher, CLOCK, RELAY).drainOnce());

        assertEquals(List.of(), publisher.publishedKeys);
        assertEquals(List.of("a"), queue.parkedEventIds);
    }

    @Test
    void oneBadRowDoesNotStopTheRowsBehindIt() {
        FakeQueue queue = new FakeQueue(new ArrayList<>());
        queue.claimable.addAll(List.of(record("bad", NOW, 0), record("good", NOW.plusSeconds(1), 0)));
        FakePublisher publisher = new FakePublisher(new ArrayList<>());
        publisher.failingKeys.add("match-bad");

        OutboxRelay.Report report = await(new OutboxRelay(queue, publisher, CLOCK, RELAY).drainOnce());

        assertEquals(List.of("match-good"), publisher.publishedKeys);
        assertEquals(1, report.published());
        assertEquals(1, report.retried());
    }

    @Test
    void anEmptyClaimDoesNothing() {
        List<String> sequence = new ArrayList<>();
        FakeQueue queue = new FakeQueue(sequence);

        OutboxRelay.Report report = await(new OutboxRelay(queue, new FakePublisher(sequence), CLOCK, RELAY)
                .drainOnce());

        assertEquals(0, report.claimed());
        assertEquals(List.of("claim"), sequence);
    }

    private static OutboxRecord record(String eventId, Instant createdAt, int attempts) {
        return new OutboxRecord(eventId, EventTopics.MATCH_CONFIRMED, "match-" + eventId, "MATCH_CONFIRMED",
                payload(eventId), createdAt.minusSeconds(1), createdAt, attempts);
    }

    /** A real MATCH_CONFIRMED, so the relay decodes what a producer would have written. */
    private static byte[] payload(String eventId) {
        return MatchConfirmed.newBuilder()
                .setMeta(EventMetadata.newBuilder()
                        .setEventId(eventId)
                        .setEventType("MATCH_CONFIRMED")
                        .setOccurredAt(Timestamp.newBuilder().setSeconds(NOW.getEpochSecond())))
                .setMatchId("match-" + eventId)
                .build()
                .toByteArray();
    }

    private static <T> T await(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }

    /** A queue that hands over whatever the test put in it and records the calls. */
    private static final class FakeQueue implements OutboxQueue {

        private final List<OutboxRecord> claimable = new ArrayList<>();
        private final List<String> sequence;
        private final List<Instant> publishedInstants = new ArrayList<>();
        private final List<Instant> scheduledRetries = new ArrayList<>();
        private final List<String> parkedEventIds = new ArrayList<>();
        private final List<String> errors = new ArrayList<>();

        FakeQueue(List<String> sequence) {
            this.sequence = sequence;
        }

        @Override
        public Future<Void> writeMatchWithEvent(Match match, List<MatchPlayer> players, OutboxRecord event) {
            throw new UnsupportedOperationException("the relay does not write matches");
        }

        @Override
        public Future<List<OutboxRecord>> claim(String relayId, int batchSize) {
            sequence.add("claim");
            List<OutboxRecord> claimed = List.copyOf(claimable.subList(0, Math.min(batchSize, claimable.size())));
            claimable.removeAll(claimed);
            return Future.succeededFuture(claimed);
        }

        @Override
        public Future<Void> markPublished(String eventId, Instant publishedAt) {
            sequence.add("markPublished:" + eventId);
            publishedInstants.add(publishedAt);
            return Future.succeededFuture();
        }

        @Override
        public Future<Void> markFailed(String eventId, String error, Instant nextAttemptAt) {
            sequence.add("markFailed:" + eventId);
            scheduledRetries.add(nextAttemptAt);
            errors.add(error);
            return Future.succeededFuture();
        }

        @Override
        public Future<Void> park(String eventId, String error) {
            sequence.add("park:" + eventId);
            parkedEventIds.add(eventId);
            errors.add(error);
            return Future.succeededFuture();
        }
    }

    /** A publisher that acknowledges, unless the key is in {@code failingKeys}. */
    private static final class FakePublisher implements EventPublisher {

        private final List<String> sequence;
        private final List<String> publishedKeys = new ArrayList<>();
        private final Set<String> failingKeys = new HashSet<>();

        FakePublisher(List<String> sequence) {
            this.sequence = sequence;
        }

        @Override
        public Future<Void> publish(String topic, String partitionKey, Message event) {
            if (failingKeys.contains(partitionKey)) {
                sequence.add("publish:" + partitionKey + ":failed");
                return Future.failedFuture(new RuntimeException("broker is down"));
            }
            sequence.add("publish:" + partitionKey);
            publishedKeys.add(partitionKey);
            return Future.succeededFuture();
        }
    }
}
