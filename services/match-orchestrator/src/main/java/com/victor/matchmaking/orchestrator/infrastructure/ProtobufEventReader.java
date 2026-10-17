package com.victor.matchmaking.orchestrator.infrastructure;

import org.apache.kafka.common.header.internals.RecordHeaders;

import com.victor.matchmaking.events.EventSerialization;
import com.victor.matchmaking.events.MatchConfirmed;
import com.victor.matchmaking.orchestrator.application.RawRecord;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

/**
 * Reads the bytes of a record into a {@link MatchConfirmed}, resolving the writer's schema
 * through the Schema Registry.
 *
 * <p>Separate from {@link MatchConfirmedTranslator} on purpose: this class knows about the
 * registry, the Confluent deserializer and the fact that a payload can be undecodable, and
 * the translator stays a pure function from message to records that a unit test can call
 * with an object it built itself.
 *
 * <p>The deserializer is instantiated and configured here instead of being handed to the
 * Kafka client, so that a bad payload fails per record - with the bytes still in hand, and
 * therefore still dead-letterable. Behind the client's own value deserializer it would blow
 * up inside {@code poll()}, where the offending record can no longer be identified.
 */
public final class ProtobufEventReader {

    private final KafkaProtobufDeserializer<MatchConfirmed> deserializer;

    public ProtobufEventReader(String schemaRegistryUrl) {
        KafkaProtobufDeserializer<MatchConfirmed> configured = new KafkaProtobufDeserializer<>();
        configured.configure(
                EventSerialization.protobufValueDeserializerProps(schemaRegistryUrl, MatchConfirmed.class), false);
        this.deserializer = configured;
    }

    /**
     * @throws IllegalArgumentException when the payload is empty or does not decode
     *         against the schema the registry resolves from its header
     */
    public MatchConfirmed read(RawRecord record) {
        if (record.value() == null) {
            throw new IllegalArgumentException("record carries no value (tombstone)");
        }
        MatchConfirmed event;
        try {
            // Empty headers, not the record's: the Confluent wire format carries the
            // schema id inside the payload, so the headers were never the schema's home.
            event = deserializer.deserialize(record.topic(), new RecordHeaders(), record.value());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("payload does not decode as " + MatchConfirmed.class.getSimpleName()
                    + ": " + e.getMessage(), e);
        }
        if (event == null) {
            throw new IllegalArgumentException("payload decoded to nothing");
        }
        return event;
    }
}
