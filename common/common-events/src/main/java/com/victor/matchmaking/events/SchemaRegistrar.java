package com.victor.matchmaking.events;

import java.util.List;
import java.util.Map;

import com.google.protobuf.Message;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

/**
 * Versions the event contracts in the Schema Registry.
 *
 * <p>Services publish with {@code auto.register.schemas=false} (ADR 0016), so a
 * schema must exist <em>before</em> the first record is produced. This class is that
 * build-time half: it runs the real serializer with registration enabled, so the
 * schema and its imports end up registered exactly as the runtime serializer
 * resolves them.
 *
 * <p>It is also the compatibility gate. The registry is configured with {@code FULL}
 * (see docker-compose.yml): a version that would break an existing reader or writer
 * is rejected, the registration fails and so does the build. Task 3 wires this into
 * CI; today it is run by hand:
 *
 * <pre>
 * mvn -B -pl common/common-events exec:java \
 *     -Dexec.mainClass=com.victor.matchmaking.events.SchemaRegistrar
 * </pre>
 */
public final class SchemaRegistrar {

    /**
     * Topic to a sample value. Only the schema matters at registration time, so a
     * default instance is enough - no fixture has to be kept in sync with the contract.
     */
    private static final Map<String, Message> CONTRACTS = Map.of(
            EventTopics.MATCH_CONFIRMED, MatchConfirmed.getDefaultInstance());

    private SchemaRegistrar() {
    }

    public static void main(String[] args) {
        String registryUrl = EventSerialization.schemaRegistryUrl();
        SchemaRegistryClient client = new CachedSchemaRegistryClient(registryUrl, 100);
        int registered = 0;
        try (KafkaProtobufSerializer<Message> serializer = new KafkaProtobufSerializer<>()) {
            serializer.configure(EventSerialization.protobufValueSerializerProps(registryUrl, true), false);
            for (Map.Entry<String, Message> contract : CONTRACTS.entrySet()) {
                serializer.serialize(contract.getKey(), contract.getValue());
                registered += report(client, contract.getKey());
            }
        } catch (Exception e) {
            System.err.println("schema.registration.failed registry=" + registryUrl + " error=" + e);
            // The message alone does not carry the registry's verdict: without the
            // cause chain a red CI build says "registration failed" and hides which
            // field the registry considered incompatible.
            for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
                System.err.println("schema.registration.cause: " + cause);
            }
            System.exit(1);
        }
        if (registered < CONTRACTS.size()) {
            // Registering nothing and reporting success would be a false green: the
            // producer would fail later, at the worst possible moment.
            System.err.println("schema.registration.failed registry=" + registryUrl
                    + " reason=no subject found for " + CONTRACTS.keySet());
            System.exit(1);
        }
    }

    /** Prints what the registry now holds for the topic; returns the subjects found. */
    private static int report(SchemaRegistryClient client, String topic) throws Exception {
        List<String> subjects = client.getAllSubjects().stream()
                .filter(subject -> subject.startsWith(topic))
                .sorted()
                .toList();
        for (String subject : subjects) {
            SchemaMetadata metadata = client.getLatestSchemaMetadata(subject);
            System.out.printf("schema.registered subject=%s version=%d id=%d schemaType=%s%n",
                    subject, metadata.getVersion(), metadata.getId(), metadata.getSchemaType());
        }
        return subjects.size();
    }
}
