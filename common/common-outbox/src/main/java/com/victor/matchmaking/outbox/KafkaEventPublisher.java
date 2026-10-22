package com.victor.matchmaking.outbox;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.protobuf.Message;
import com.victor.matchmaking.events.EventSerialization;
import com.victor.matchmaking.outbox.EventPublisher;

import io.vertx.core.Future;
import io.vertx.core.Promise;

/**
 * Publishes records to Kafka, serialized as Protobuf against the Schema Registry.
 *
 * <p>This is the transport of the outbox relay (ADR 0018), and it deliberately knows
 * nothing about matches: it is handed a topic, a key and an already-built message, so it
 * cannot change the event the outbox stored.
 *
 * <p><b>Why not {@code vertx-kafka-client}:</b> the Confluent serializer is compiled
 * against Confluent's {@code kafka-clients} (7.7.1-ccs, Kafka 3.7 base) while Vert.x 5
 * resolves Kafka 4.x; two {@code kafka-clients} versions on one classpath is a runtime
 * {@code NoSuchMethodError} waiting to happen. {@code KafkaProducer.send} is already
 * asynchronous, so the reactive integration is the bridge below (callback to Vert.x
 * promise) and the event loop is never blocked waiting for the broker.
 *
 * <p>One producer serves every event topic: the schema is resolved per topic through the
 * subject name strategy, so a new event type does not need a second client.
 *
 * <p>It is only ever called from the relay, which means a broker outage no longer sits
 * inside the confirmation: the match is durable without Kafka, and the events go out when
 * the broker comes back (ADR 0018).
 */
public final class KafkaEventPublisher implements EventPublisher, AutoCloseable {

    private static final String BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    /** Bounds the initial metadata wait, so a broker outage cannot hang a relay pass. */
    private static final int MAX_BLOCK_MS = 5_000;

    private final Producer<String, Message> producer;

    /** Builds the publisher from the environment (see the runbook in requests/README.md). */
    public static KafkaEventPublisher fromEnv() {
        return new KafkaEventPublisher(
                System.getenv().getOrDefault(BOOTSTRAP_SERVERS_ENV, DEFAULT_BOOTSTRAP_SERVERS),
                EventSerialization.schemaRegistryUrl());
    }

    public KafkaEventPublisher(String bootstrapServers, String schemaRegistryUrl) {
        Map<String, Object> props =
                new HashMap<>(EventSerialization.protobufValueSerializerProps(schemaRegistryUrl, false));
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        // Durability: the relay only marks a row published after every in-sync replica has
        // the record, and retries cannot duplicate records inside the log.
        props.put("acks", "all");
        props.put("enable.idempotence", true);
        props.put("max.block.ms", MAX_BLOCK_MS);
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public Future<Void> publish(String topic, String partitionKey, Message event) {
        // Key = matchId: every event of one match goes to the same partition, so a
        // consumer reads that match's history in order.
        ProducerRecord<String, Message> record = new ProducerRecord<>(topic, partitionKey, event);
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
        return acknowledged.future();
    }

    @Override
    public void close() {
        // Flushes records still in flight; without it a shutdown could drop an ack.
        producer.close();
    }
}