package com.victor.matchmaking.outbox;

import java.time.Instant;

/**
 * One row of the outbox (ADR 0018): an event written next to the fact it announces,
 * waiting for the relay to publish it.
 *
 * @param eventId      the event's own id ({@code meta.event_id}), the idempotency key a
 *                     consumer deduplicates by (ADR 0038)
 * @param topic        where the event goes
 * @param partitionKey the key that fixes the partition, and with it the per-key order
 * @param eventType    the event type, for whoever reads the table by hand
 * @param payload      the bare Protobuf message, without the Confluent wire framing
 * @param occurredAt   when the fact happened (the event's clock)
 * @param createdAt    when the row was written: what the relay publishes first
 * @param attempts     publish attempts so far (0 when the row is enqueued)
 */
public record OutboxRecord(
        String eventId,
        String topic,
        String partitionKey,
        String eventType,
        byte[] payload,
        Instant occurredAt,
        Instant createdAt,
        int attempts) {
}