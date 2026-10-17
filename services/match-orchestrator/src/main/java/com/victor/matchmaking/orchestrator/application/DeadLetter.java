package com.victor.matchmaking.orchestrator.application;

import java.util.UUID;

/**
 * A record the consumer gave up on, with everything known about the failure.
 *
 * @param record        the original record, payload included byte for byte
 * @param consumerGroup group that gave up on it
 * @param stage         which step failed
 * @param attempts      attempts made before giving up
 * @param errorClass    exception type, verbatim; the message alone loses the kind
 * @param errorMessage  what it said
 * @param eventId       the event's id when the payload decoded, {@code null} otherwise
 */
public record DeadLetter(RawRecord record, String consumerGroup, FailureStage stage, int attempts,
        String errorClass, String errorMessage, UUID eventId) {
}
