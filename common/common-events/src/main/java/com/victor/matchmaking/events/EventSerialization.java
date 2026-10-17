package com.victor.matchmaking.events;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

/**
 * Wire settings shared by every producer and consumer of the event contracts, so
 * "how an event travels" is defined in one place and cannot drift per service.
 *
 * <p>Protobuf is not self-describing: a consumer needs the schema and resolves its
 * version from the {@code schemaId} in the record header, which is why the Schema
 * Registry URL is part of the serialization configuration rather than a service
 * detail.
 */
public final class EventSerialization {

    /** Environment variable holding the Schema Registry URL. */
    public static final String REGISTRY_URL_ENV = "SCHEMA_REGISTRY_URL";

    /** Dev default: the registry is published on host port 8082 (see docker-compose.yml). */
    public static final String DEFAULT_REGISTRY_URL = "http://localhost:8082";

    private EventSerialization() {
    }

    /** Schema Registry URL from the environment, or the development default. */
    public static String schemaRegistryUrl() {
        return System.getenv().getOrDefault(REGISTRY_URL_ENV, DEFAULT_REGISTRY_URL);
    }

    /**
     * Kafka properties for the Protobuf value serializer.
     *
     * @param schemaRegistryUrl     registry to resolve the schema id against
     * @param autoRegisterSchemas   {@code true} only for the build-time
     *                              {@link SchemaRegistrar}: services publish against
     *                              an already versioned schema, so an unregistered
     *                              change fails loudly instead of silently creating a
     *                              new subject version from a running pod
     */
    public static Map<String, Object> protobufValueSerializerProps(String schemaRegistryUrl,
            boolean autoRegisterSchemas) {
        Map<String, Object> props = new HashMap<>();
        props.put("value.serializer", KafkaProtobufSerializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas", autoRegisterSchemas);
        // Pinned instead of inherited from a default: the subject is part of the
        // contract every consumer depends on (`<topic>-value`).
        props.put("value.subject.name.strategy", TopicNameStrategy.class.getName());
        return props;
    }
}
