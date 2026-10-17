package com.victor.matchmaking.orchestrator.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;
import com.victor.matchmaking.domain.match.MatchStatus;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.events.MatchConfirmed;
import com.victor.matchmaking.events.MatchConfirmedMapper;
import com.victor.matchmaking.orchestrator.domain.AllocationRequest;
import com.victor.matchmaking.orchestrator.domain.AllocationStatus;

/**
 * The translator is the only place that trusts nothing: it turns a record that the
 * broker accepted into values the orchestrator acts on. Every test here is a shape of
 * "the broker does not validate the payload for us".
 *
 * <p>Fixtures are built with the real producer mapper, so a change on the publishing
 * side that the consumer would refuse shows up here instead of in production.
 */
class MatchConfirmedTranslatorTest {

    private static final Instant OCCURRED_AT = Instant.parse("2026-10-17T02:47:34.356185Z");
    private static final String MATCH_ID = "match-w13-translator";

    @Test
    void mapsEveryFieldOfTheContract() {
        MatchConfirmed event = event(GameMode.ONE_V_ONE, Region.NA);

        AllocationRequest allocation = MatchConfirmedTranslator.allocationOf(event);

        assertEquals(MATCH_ID, allocation.matchId());
        assertEquals(GameMode.ONE_V_ONE, allocation.gameMode());
        assertEquals(Region.NA, allocation.region());
        assertEquals(GameMode.ONE_V_ONE.playersPerMatch(), allocation.playerCount());
        assertEquals(AllocationStatus.PENDING_ALLOCATION, allocation.status());
        // The event's clock, not the consumer's: this is when the match was confirmed.
        assertEquals(OCCURRED_AT, allocation.confirmedAt());
    }

    @Test
    void everyGameModeAndRegionSurvivesTheRoundTrip() {
        for (GameMode gameMode : GameMode.values()) {
            for (Region region : Region.values()) {
                AllocationRequest allocation =
                        MatchConfirmedTranslator.allocationOf(event(gameMode, region));
                assertEquals(gameMode, allocation.gameMode(), "game mode round trip");
                assertEquals(region, allocation.region(), "region round trip");
            }
        }
    }

    @Test
    void unspecifiedGameModeIsRefused() {
        MatchConfirmed event = eventWith(GameMode.ONE_V_ONE, Region.NA,
                builder -> builder.setGameMode(com.victor.matchmaking.proto.common.GameMode.GAME_MODE_UNSPECIFIED));

        assertThrows(IllegalArgumentException.class, () -> MatchConfirmedTranslator.allocationOf(event));
    }

    @Test
    void unspecifiedRegionIsRefused() {
        MatchConfirmed event = eventWith(GameMode.ONE_V_ONE, Region.NA,
                builder -> builder.setRegion(com.victor.matchmaking.proto.common.Region.REGION_UNSPECIFIED));

        assertThrows(IllegalArgumentException.class, () -> MatchConfirmedTranslator.allocationOf(event));
    }

    @Test
    void rosterSizeMustMatchTheGameMode() {
        // A 1v1 match with three players: the producer does not refuse it, the
        // consumer does - an allocation row for the wrong roster size would be
        // silently wrong for whoever reads it later.
        Match match = match(GameMode.ONE_V_ONE, Region.NA);
        List<MatchPlayer> players = players(match, 3);
        MatchConfirmed event = MatchConfirmedMapper.toEvent(match, players, OCCURRED_AT, match.id());

        assertThrows(IllegalArgumentException.class, () -> MatchConfirmedTranslator.allocationOf(event));
    }

    @Test
    void malformedEventIdIsRefused() {
        MatchConfirmed event = eventWith(GameMode.ONE_V_ONE, Region.NA, builder -> builder.setMeta(
                builder.getMeta().toBuilder().setEventId("not-a-uuid")));

        // The event id is the deduplication key: a malformed one would make every
        // delivery of the same event look like a new one.
        assertThrows(IllegalArgumentException.class, () -> MatchConfirmedTranslator.eventIdOf(event));
    }

    @Test
    void unexpectedEventTypeIsRefused() {
        MatchConfirmed event = eventWith(GameMode.ONE_V_ONE, Region.NA, builder -> builder.setMeta(
                builder.getMeta().toBuilder().setEventType("MATCH_FINISHED")));

        assertThrows(IllegalArgumentException.class, () -> MatchConfirmedTranslator.allocationOf(event));
    }

    @Test
    void missingOccurredAtIsRefused() {
        MatchConfirmed event = eventWith(GameMode.ONE_V_ONE, Region.NA,
                builder -> builder.setMeta(builder.getMeta().toBuilder().clearOccurredAt()));

        assertThrows(IllegalArgumentException.class, () -> MatchConfirmedTranslator.allocationOf(event));
    }

    @Test
    void theEventIdIsReadAsAUuid() {
        MatchConfirmed event = event(GameMode.ONE_V_ONE, Region.NA);

        assertEquals(UUID.fromString(event.getMeta().getEventId()), MatchConfirmedTranslator.eventIdOf(event));
    }

    private static MatchConfirmed event(GameMode gameMode, Region region) {
        return eventWith(gameMode, region, builder -> {
        });
    }

    private static MatchConfirmed eventWith(GameMode gameMode, Region region,
            java.util.function.Consumer<MatchConfirmed.Builder> mutation) {
        Match match = match(gameMode, region);
        MatchConfirmed mapped =
                MatchConfirmedMapper.toEvent(match, players(match, gameMode.playersPerMatch()), OCCURRED_AT, match.id());
        MatchConfirmed.Builder builder = mapped.toBuilder();
        mutation.accept(builder);
        return builder.build();
    }

    private static Match match(GameMode gameMode, Region region) {
        return new Match(MATCH_ID, gameMode, region, MatchStatus.PENDING, Instant.now(), null, null);
    }

    private static List<MatchPlayer> players(Match match, int count) {
        List<MatchPlayer> players = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            players.add(new MatchPlayer(match.id(), "p" + i, i < count / 2 ? "A" : "B", 50 + i));
        }
        return players;
    }
}
