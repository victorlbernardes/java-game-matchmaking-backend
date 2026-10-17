package com.victor.matchmaking.orchestrator.infrastructure;

import java.time.Instant;
import java.util.UUID;

import com.google.protobuf.Timestamp;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.events.MatchConfirmed;
import com.victor.matchmaking.orchestrator.application.DecodedEvent;
import com.victor.matchmaking.orchestrator.application.MatchConfirmedDecoder;
import com.victor.matchmaking.orchestrator.application.RawRecord;
import com.victor.matchmaking.orchestrator.domain.AllocationRequest;
import com.victor.matchmaking.orchestrator.domain.AllocationStatus;

/**
 * Translates the wire contract into the orchestrator's own records.
 *
 * <p>The protobuf types stay at the edge: everything the orchestrator acts on is checked
 * here rather than trusted. A payload that does not satisfy the contract fails loudly
 * instead of producing a plausible-looking row - the difference matters, because a wrong
 * row is worse than a missing one: nobody looks for it.
 */
public final class MatchConfirmedTranslator implements MatchConfirmedDecoder {

    /** {@code EventMetadata.event_type} this consumer accepts; mirrors the topic (ADR 0016). */
    public static final String EVENT_TYPE = "MATCH_CONFIRMED";

    private final ProtobufEventReader reader;

    public MatchConfirmedTranslator(ProtobufEventReader reader) {
        this.reader = reader;
    }

    @Override
    public DecodedEvent decode(RawRecord record) {
        MatchConfirmed event = reader.read(record);
        return new DecodedEvent(allocationOf(event), eventIdOf(event));
    }

    /**
     * The event's uuid: the deduplication key, and the only thing on this payload that has
     * to be well formed for the consumer to be correct. A malformed one is refused rather
     * than stored as text - every delivery would then look like a new event.
     */
    public static UUID eventIdOf(MatchConfirmed event) {
        String raw = event.getMeta().getEventId();
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("event id is not a uuid: " + raw, e);
        }
    }

    public static AllocationRequest allocationOf(MatchConfirmed event) {
        GameMode gameMode = gameMode(event.getGameMode());
        requireContract(event, gameMode);
        return new AllocationRequest(event.getMatchId(), gameMode, region(event.getRegion()),
                event.getPlayersCount(), AllocationStatus.PENDING_ALLOCATION, occurredAt(event));
    }

    private static void requireContract(MatchConfirmed event, GameMode gameMode) {
        if (!EVENT_TYPE.equals(event.getMeta().getEventType())) {
            throw new IllegalArgumentException("unexpected event type: " + event.getMeta().getEventType());
        }
        if (event.getMatchId().isBlank()) {
            throw new IllegalArgumentException("event carries a blank match id");
        }
        if (!event.getMeta().hasOccurredAt()) {
            throw new IllegalArgumentException("event carries no occurred_at");
        }
        if (event.getPlayersCount() != gameMode.playersPerMatch()) {
            throw new IllegalArgumentException("roster has " + event.getPlayersCount() + " players, "
                    + gameMode.label() + " takes " + gameMode.playersPerMatch());
        }
    }

    /** The event's clock, not ours: {@code confirmedAt} says when the match was confirmed. */
    private static Instant occurredAt(MatchConfirmed event) {
        Timestamp occurredAt = event.getMeta().getOccurredAt();
        return Instant.ofEpochSecond(occurredAt.getSeconds(), occurredAt.getNanos());
    }

    /**
     * The contract's enums are not the domain's. {@code UNSPECIFIED} is refused instead of
     * being mapped to a default: a match with an unknown game mode cannot be allocated, and
     * storing a wrong mode would send it to the wrong pool of servers.
     */
    private static GameMode gameMode(com.victor.matchmaking.proto.common.GameMode mode) {
        return switch (mode) {
            case GAME_MODE_1V1 -> GameMode.ONE_V_ONE;
            case GAME_MODE_5V5 -> GameMode.FIVE_V_FIVE;
            case GAME_MODE_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("unknown game mode: " + mode);
        };
    }

    /** Same rule as {@link #gameMode}: no region means no place to run the match. */
    private static Region region(com.victor.matchmaking.proto.common.Region region) {
        return switch (region) {
            case REGION_SA -> Region.SA;
            case REGION_NA -> Region.NA;
            case REGION_EU -> Region.EU;
            case REGION_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("unknown region: " + region);
        };
    }
}
