package com.victor.matchmaking.orchestrator.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * One event a consumer group has already applied: a row of the inbox table.
 *
 * <p>The {@code eventId} is the deduplication key - the uuid the publisher generates
 * per delivery (ADR 0016) - so the same business fact recognised twice is applied
 * once. The remaining fields are forensics: they answer "which consumer read this
 * record, and when", which is what makes a replay auditable after the fact.
 *
 * @param eventId       {@code EventMetadata.event_id} of the processed event
 * @param consumerGroup group that processed it; a second group is a second reader
 * @param topic         topic the record came from
 * @param partition     partition the record came from
 * @param offset        offset of the record inside its partition
 * @param processedAt   when the effect was applied
 */
public record ProcessedEvent(UUID eventId, String consumerGroup, String topic, int partition, long offset,
        Instant processedAt) {
}
