package com.victor.matchmaking.events;

import java.util.Map;

import org.apache.kafka.common.header.internals.RecordHeaders;

import com.google.protobuf.Message;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

/**
 * Reads the bytes of a record into a specific Protobuf message type, resolving the writer's schema
 * through the Schema Registry.
 *
 * <p>The deserializer is instantiated and configured here instead of being handed to the
 * Kafka client, so that a bad payload fails per record - with the bytes still in hand, and
 * therefore still dead-letterable. Behind the client's own value deserializer it would blow
 * up inside {@code poll()}, where the offending record can no longer be identified.
 */
public final class ProtobufEventReader {

    private final Map<String, KafkaProtobufDeserializer<? extends Message>> deserializers;

    public ProtobufEventReader(String schemaRegistryUrl) {
        this.deserializers = new java.util.HashMap<>();
        // Pre-configure for known types (lazy init could also work, but this is explicit)
        register(MatchConfirmed.class, schemaRegistryUrl);
        register(MatchFinished.class, schemaRegistryUrl);
        register(SkillUpdated.class, schemaRegistryUrl);
    }

    private <T extends Message> void register(Class<T> type, String schemaRegistryUrl) {
        KafkaProtobufDeserializer<T> configured = new KafkaProtobufDeserializer<>();
        configured.configure(EventSerialization.protobufValueDeserializerProps(schemaRegistryUrl, type), false);
        deserializers.put(type.getName(), configured);
    }

    /**
     * @throws IllegalArgumentException when the payload is empty or does not decode
     *         against the schema the registry resolves from its header
     */
    public <T extends Message> T read(RawRecord record, Class<T> type) {
        if (record.value() == null) {
            throw new IllegalArgumentException("record carries no value (tombstone)");
        }
        @SuppressWarnings("unchecked")
        KafkaProtobufDeserializer<T> deserializer = (KafkaProtobufDeserializer<T>) deserializers.get(type.getName());
        if (deserializer == null) {
            throw new IllegalArgumentException("no deserializer configured for " + type.getName());
        }
        T event;
        try {
            // Empty headers: the Confluent wire format carries the schema id inside the payload.
            event = deserializer.deserialize(record.topic(), new RecordHeaders(), record.value());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("payload does not decode as " + type.getSimpleName()
                    + ": " + e.getMessage(), e);
        }
        if (event == null) {
            throw new IllegalArgumentException("payload decoded to nothing");
        }
        return event;
    }
}