package com.victor.matchmaking.orchestrator.infrastructure;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.victor.matchmaking.events.EventTopics;
import com.victor.matchmaking.orchestrator.application.DeadLetter;
import com.victor.matchmaking.orchestrator.application.DeadLetterQueue;

/**
 * Publishes records the consumer gave up on to the dead letter topic, payload included.
 *
 * <p>The value is written as raw bytes through a {@link ByteArraySerializer}, and that is
 * not a shortcut: the payload may not decode, so there is no schema to serialize it
 * against. Forwarding it verbatim is what makes the record replayable once the cause is
 * fixed - re-encoding it would mean inventing a shape for data that has none.
 *
 * <p>{@code send} is waited on rather than left in flight: the offset is committed right
 * after this call returns, and the original bytes cannot be reconstructed from the
 * diagnostic headers if the write is lost.
 */
public final class KafkaDeadLetterQueue implements DeadLetterQueue, AutoCloseable {

    private static final System.Logger LOG = System.getLogger("orchestrator.match-confirmed");

    private static final String STAGE_HEADER = "dlq.stage";
    private static final String ERROR_CLASS_HEADER = "dlq.error-class";
    private static final String ERROR_MESSAGE_HEADER = "dlq.error-message";
    private static final String ATTEMPTS_HEADER = "dlq.attempts";
    private static final String CONSUMER_GROUP_HEADER = "dlq.consumer-group";
    private static final String ORIGINAL_TOPIC_HEADER = "dlq.original-topic";
    private static final String ORIGINAL_PARTITION_HEADER = "dlq.original-partition";
    private static final String ORIGINAL_OFFSET_HEADER = "dlq.original-offset";
    private static final String EVENT_ID_HEADER = "dlq.event-id";
    private static final String FAILED_AT_HEADER = "dlq.failed-at";

    /** Header values travel to every consumer of the topic; a stack trace does not belong there. */
    private static final int MAX_HEADER_LENGTH = 1_000;
    private static final int SEND_TIMEOUT_S = 10;

    private final Producer<String, byte[]> producer;
    private final String deadLetterTopic;

    public KafkaDeadLetterQueue(String bootstrapServers, String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        props.put("acks", "all");
        props.put("enable.idempotence", true);
        // Same bound as the engine's publisher: a broker outage must fail the publish
        // (and keep the offset uncommitted) instead of hanging the consumer forever.
        props.put("max.block.ms", 5_000);
        this.producer = new KafkaProducer<>(props);
        this.deadLetterTopic = EventTopics.deadLetterOf(topic);
        LOG.log(System.Logger.Level.INFO, "dead-letter.queue topic={0}", deadLetterTopic);
    }

    @Override
    public void publish(DeadLetter letter) {
        try {
            producer.send(recordOf(letter)).get(SEND_TIMEOUT_S, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while dead-lettering " + source(letter), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("could not dead-letter " + source(letter)
                    + " (topic " + deadLetterTopic + " must exist): " + e, e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    private ProducerRecord<String, byte[]> recordOf(DeadLetter letter) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(deadLetterTopic,
                letter.record().key(), letter.record().value());
        header(record, STAGE_HEADER, letter.stage().name());
        header(record, ERROR_CLASS_HEADER, letter.errorClass());
        header(record, ERROR_MESSAGE_HEADER, letter.errorMessage());
        header(record, ATTEMPTS_HEADER, String.valueOf(letter.attempts()));
        header(record, CONSUMER_GROUP_HEADER, letter.consumerGroup());
        header(record, ORIGINAL_TOPIC_HEADER, letter.record().topic());
        header(record, ORIGINAL_PARTITION_HEADER, String.valueOf(letter.record().partition()));
        header(record, ORIGINAL_OFFSET_HEADER, String.valueOf(letter.record().offset()));
        header(record, EVENT_ID_HEADER, letter.eventId() == null ? null : letter.eventId().toString());
        header(record, FAILED_AT_HEADER, Instant.now().toString());
        return record;
    }

    private static void header(ProducerRecord<String, byte[]> record, String name, String value) {
        if (value == null) {
            return;
        }
        record.headers().add(name, truncate(value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String truncate(String value) {
        return value.length() <= MAX_HEADER_LENGTH ? value : value.substring(0, MAX_HEADER_LENGTH) + "...";
    }

    private static String source(DeadLetter letter) {
        return letter.record().topic() + " partition " + letter.record().partition()
                + " offset " + letter.record().offset();
    }
}
