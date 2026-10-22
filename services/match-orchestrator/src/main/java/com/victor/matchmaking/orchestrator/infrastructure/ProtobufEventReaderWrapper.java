package com.victor.matchmaking.orchestrator.infrastructure;

import com.victor.matchmaking.events.ProtobufEventReader;
import com.victor.matchmaking.events.MatchConfirmed;
import com.victor.matchmaking.events.RawRecord;

/**
 * Thin wrapper that delegates to the common ProtobufEventReader for MatchConfirmed.
 * Exists only to keep the orchestrator's translator interface unchanged.
 */
public final class ProtobufEventReaderWrapper {

    private final ProtobufEventReader reader;

    public ProtobufEventReaderWrapper(String schemaRegistryUrl) {
        this.reader = new ProtobufEventReader(schemaRegistryUrl);
    }

    public MatchConfirmed read(RawRecord record) {
        return reader.read(record, MatchConfirmed.class);
    }
}
