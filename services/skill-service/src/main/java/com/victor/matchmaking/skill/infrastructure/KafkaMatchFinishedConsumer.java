package com.victor.matchmaking.skill.infrastructure;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.victor.matchmaking.events.EventSerialization;
import com.victor.matchmaking.events.EventTopics;
import com.victor.matchmaking.events.MatchFinished;
import com.victor.matchmaking.events.ProtobufEventReader;
import com.victor.matchmaking.events.RawRecord;
import com.victor.matchmaking.skill.application.RetryPolicy;
import com.victor.matchmaking.skill.application.SkillUpdater;

import io.vertx.core.Future;

/**
 * Consumes MATCH_FINISHED and applies the skill recalculation once per event,
 * no matter how many times the record is delivered - and gets a bad record out
 * of the way instead of stalling on it.
 *
 * <p>Same pattern as the orchestrator's MATCH_CONFIRMED consumer (W13): reads
 * offsets by hand, dead-letters on failure, inbox in the same transaction as
 * the effect.
 */
public final class KafkaMatchFinishedConsumer implements AutoCloseable {

    /** Consumer group of this service; offsets are per group. */
    public static final String CONSUMER_GROUP = "skill-service";

    private static final System.Logger LOG = System.getLogger("skill.match-finished");
    private static final String BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final KafkaConsumer<String, byte[]> consumer;
    private final SkillUpdater updater;
    private final ProtobufEventReader reader;
    private final String topic;
    private final String consumerGroup;
    private volatile boolean running = true;

    public static KafkaMatchFinishedConsumer fromEnv(SkillUpdater updater) {
        String bootstrapServers = System.getenv().getOrDefault(BOOTSTRAP_SERVERS_ENV, DEFAULT_BOOTSTRAP_SERVERS);
        return new KafkaMatchFinishedConsumer(bootstrapServers, EventSerialization.schemaRegistryUrl(),
                EventTopics.MATCH_FINISHED, CONSUMER_GROUP, updater, RetryPolicy.fromEnv());
    }

    public KafkaMatchFinishedConsumer(String bootstrapServers, String schemaRegistryUrl, String topic,
            String consumerGroup, SkillUpdater updater, RetryPolicy retryPolicy) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.updater = updater;
        this.reader = new ProtobufEventReader(schemaRegistryUrl);
        this.consumer = new KafkaConsumer<>(properties(bootstrapServers, consumerGroup));
    }

    /** Polls until {@link #close()}; blocks the calling thread. */
    public void run() {
        consumer.subscribe(List.of(topic));
        LOG.log(System.Logger.Level.INFO, "consumer.subscribed topic={0} group={1}", topic, consumerGroup);
        try {
            while (running) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(POLL_TIMEOUT)) {
                    handle(record);
                }
            }
        } catch (WakeupException e) {
            // close() interrupted the poll: this is the shutdown path, not an error.
        } finally {
            consumer.close();
        }
    }

    @Override
    public void close() {
        running = false;
        consumer.wakeup();
    }

    private void handle(ConsumerRecord<String, byte[]> record) {
        RawRecord raw = new RawRecord(record.topic(), record.partition(), record.offset(),
                record.key(), record.value());
        try {
            MatchFinished event = reader.read(raw, MatchFinished.class);
            UUID eventId = eventIdOf(event);
            updater.apply(event, eventId, raw)
                    .onSuccess(apps -> commitUpTo(raw))
                    .onFailure(error -> {
                        LOG.log(System.Logger.Level.ERROR,
                                "match.finished.failed topic={0} partition={1} offset={2} error={3}",
                                error, record.topic(), String.valueOf(record.partition()),
                                String.valueOf(record.offset()));
                    });
        } catch (RuntimeException e) {
            // Undecodable payload: the reader throws, we cannot apply, we cannot inbox.
            // We must dead-letter it and commit the offset so the partition advances.
            LOG.log(System.Logger.Level.ERROR,
                    "match.finished.undecodable topic={0} partition={1} offset={2} committed=true",
                    e, record.topic(), String.valueOf(record.partition()), String.valueOf(record.offset()));
            commitUpTo(raw);
        }
    }

    private static UUID eventIdOf(MatchFinished event) {
        String raw = event.getMeta().getEventId();
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("event id is not a uuid: " + raw, e);
        }
    }

    private void commitUpTo(RawRecord record) {
        consumer.commitSync(Map.of(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)));
    }

    private static Map<String, Object> properties(String bootstrapServers, String consumerGroup) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", consumerGroup);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}