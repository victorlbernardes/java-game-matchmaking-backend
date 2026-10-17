package com.victor.matchmaking.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.google.protobuf.Timestamp;
import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;
import com.victor.matchmaking.proto.common.EventMetadata;
import com.victor.matchmaking.proto.common.GameMode;
import com.victor.matchmaking.proto.common.Region;

/**
 * Translates the matchmaker's domain into the published event contract.
 *
 * <p>A pure function of its inputs: no registry, no broker, no clock of its own, so
 * the mapping is unit-testable and reproducible. The event carries identifiers and
 * a skill snapshot, never the domain entities themselves.
 */
public final class MatchConfirmedMapper {

    /** Value of {@code EventMetadata.event_type}; mirrors the topic, not the class name. */
    public static final String EVENT_TYPE = "MATCH_CONFIRMED";

    /**
     * Version of the contract carried inside the payload, independent of the subject
     * version the registry assigns. Bump it when the meaning of a field changes.
     */
    public static final int CONTRACT_VERSION = 1;

    private static final int TEAM_ONE = 1;
    private static final int TEAM_TWO = 2;
    private static final String TEAM_LABEL_ONE = "A";
    private static final String TEAM_LABEL_TWO = "B";

    private MatchConfirmedMapper() {
    }

    /**
     * Builds the MATCH_CONFIRMED event for a match that was just persisted.
     *
     * @param occurredAt    when the match reached its confirmations
     * @param correlationId groups every event of one match; until OpenTelemetry
     *                      lands (W15) callers pass the match id
     */
    public static MatchConfirmed toEvent(Match match, List<MatchPlayer> players, Instant occurredAt,
            String correlationId) {
        MatchConfirmed.Builder event = MatchConfirmed.newBuilder()
                .setMeta(metadata(occurredAt, correlationId))
                .setMatchId(match.id())
                .setGameMode(gameMode(match.gameMode()))
                .setRegion(region(match.region()));
        for (MatchPlayer player : players) {
            event.addPlayers(MatchPlayerState.newBuilder()
                    .setPlayerId(player.playerId())
                    .setTeam(team(player.team()))
                    .setSkillAtMatch(player.skillAtMatch()));
        }
        return event.build();
    }

    /**
     * A fresh event id per delivery: consumers deduplicate by it, so a redelivery of
     * the same business fact is recognised instead of applied twice.
     */
    private static EventMetadata metadata(Instant occurredAt, String correlationId) {
        return EventMetadata.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(EVENT_TYPE)
                .setSchemaVersion(CONTRACT_VERSION)
                .setOccurredAt(timestamp(occurredAt))
                .setCorrelationId(correlationId == null ? "" : correlationId)
                .build();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private static GameMode gameMode(com.victor.matchmaking.domain.ticket.GameMode mode) {
        return switch (mode) {
            case ONE_V_ONE -> GameMode.GAME_MODE_1V1;
            case FIVE_V_FIVE -> GameMode.GAME_MODE_5V5;
        };
    }

    private static Region region(com.victor.matchmaking.domain.ticket.Region region) {
        return switch (region) {
            case SA -> Region.REGION_SA;
            case NA -> Region.REGION_NA;
            case EU -> Region.REGION_EU;
        };
    }

    /**
     * The domain labels teams A/B, the contract numbers them 1/2. An unknown label
     * fails here instead of being published as an empty (0) team: a silent default
     * would reach consumers as data that looks valid.
     */
    private static int team(String label) {
        if (TEAM_LABEL_ONE.equals(label)) {
            return TEAM_ONE;
        }
        if (TEAM_LABEL_TWO.equals(label)) {
            return TEAM_TWO;
        }
        throw new IllegalArgumentException("unknown team label: " + label);
    }
}
