package com.victor.matchmaking.orchestrator.application;

/**
 * Turns the bytes of a record into the orchestrator's records.
 *
 * <p>This is the only step that can fail on the wire, and it is a port rather than a
 * static call so the retry and dead letter policy can be tested without a broker — and
 * so the byte-level decoding (Schema Registry, deserializer) stays out of the policy.
 */
public interface MatchConfirmedDecoder {

    /**
     * @throws RuntimeException when the payload does not decode or does not satisfy the
     *         contract; the caller counts that as a failed attempt and retries
     */
    DecodedEvent decode(RawRecord record);
}
