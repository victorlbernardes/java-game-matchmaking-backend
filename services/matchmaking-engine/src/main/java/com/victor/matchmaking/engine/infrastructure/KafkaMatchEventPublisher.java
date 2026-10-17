package com.victor.matchmaking.engine.infrastructure;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.protobuf.Message;
import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;
import com.victor.matchmaking.engine.application.MatchEventPublisher;
import com.victor.matchmaking.events.EventSerialization;
import com.victor.matchmaking.events.EventTopics;
import com.victor.matchmaking.events.MatchConfirmed;
import com.victor.matchmaking.events.MatchConfirmedMapper;

import io.vertx.core.Future;
import io.vertx.core.Promise;

/**
 * Publishes match events to Kafka, serialized as Protobuf against the Schema Registry.
 *
 * <p><b>Why not {@code vertx-kafka-client}:</b> the Confluent serializer is compiled
 * against Confluent's {@code kafka-clients} (7.7.1-ccs, Kafka 3.7 base) while Vert.x 5
 * resolves Kafka 4.x; two {@code kafka-clients} versions on one classpath is a runtime
 * {@code NoSuchMethodError} waiting to happen. {@code KafkaProducer.send} is already
 * asynchronous, so the reactive integration is the bridge below (callback to Vert.x
 * promise) and the event loop is never blocked waiting for the broker.
 *
 * <p>One producer serves every event topic: the schema is resolved per topic through
 * the subject name strategy, so a new event type does not need a second client.
 *
 * <p><b>Known limitation, closed by the Transactional Outbox (W14):</b> the record is
 * produced by the same process that writes the match row, so a crash between the two
 * can lose the event; and {@code send} may block up to {@code max.block.ms} on the
 * first record while it fetches metadata.
 */
public final class KafkaMatchEventPublisher implements MatchEventPublisher, AutoCloseable {

    private static final String BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    /** Bounds the initial metadata wait, so a broker outage cannot hang a confirmation. */
    private static final int MAX_BLOCK_MS = 5_000;

    private final Producer<String, Message> producer;

    /** Builds the publisher from the environment (see the runbook in requests/README.md). */
    public static KafkaMatchEventPublisher fromEnv() {
        return new KafkaMatchEventPublisher(
                System.getenv().getOrDefault(BOOTSTRAP_SERVERS_ENV, DEFAULT_BOOTSTRAP_SERVERS),
                EventSerialization.schemaRegistryUrl());
    }

    public KafkaMatchEventPublisher(String bootstrapServers, String schemaRegistryUrl) {
        Map<String, Object> props =
                new HashMap<>(EventSerialization.protobufValueSerializerProps(schemaRegistryUrl, false));
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        // Durability: the confirmation is only acknowledged to the engine after every
        // in-sync replica has it, and retries cannot duplicate records in the log.
        props.put("acks", "all");
        props.put("enable.idempotence", true);
        props.put("max.block.ms", MAX_BLOCK_MS);
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public Future<String> publishConfirmed(Match match, List<MatchPlayer> players) {
        MatchConfirmed event = MatchConfirmedMapper.toEvent(match, players, Instant.now(), match.id());
        // Key = matchId: every event of one match goes to the same partition, so a
        // consumer reads that match's history in order.
        ProducerRecord<String, Message> record = new ProducerRecord<>(EventTopics.MATCH_CONFIRMED, match.id(), event);
        Promise<Void> acknowledged = Promise.promise();
        try {
            producer.send(record, (metadata, error) -> {
                if (error != null) {
                    acknowledged.fail(error);
                } else {
                    acknowledged.complete();
                }
            });
        } catch (RuntimeException e) {
            // send() throws before queueing when the buffer is full after max.block.ms.
            acknowledged.fail(e);
        }
        return acknowledged.future().map(ignored -> event.getMeta().getEventId());
    }

    @Override
    public void close() {
        // Flushes records still in flight; without it a shutdown could drop an ack.
        producer.close();
    }
}
