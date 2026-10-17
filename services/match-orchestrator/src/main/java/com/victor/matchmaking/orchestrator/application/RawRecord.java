package com.victor.matchmaking.orchestrator.application;

/**
 * A record as it arrived from the log: coordinates, key and raw value bytes, before
 * anything is interpreted.
 *
 * <p>The bytes are part of it on purpose. A record that cannot be decoded has to be
 * forwarded to the dead letter queue exactly as it came: it cannot be re-serialized,
 * because whatever the registered schema is, this payload does not match it.
 */
public record RawRecord(String topic, int partition, long offset, String key, byte[] value) {
}
