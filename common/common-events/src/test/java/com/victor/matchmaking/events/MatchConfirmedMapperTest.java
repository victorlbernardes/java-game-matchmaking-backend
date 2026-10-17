package com.victor.matchmaking.events;

import static com.victor.matchmaking.proto.common.GameMode.GAME_MODE_1V1;
import static com.victor.matchmaking.proto.common.GameMode.GAME_MODE_5V5;
import static com.victor.matchmaking.proto.common.Region.REGION_EU;
import static com.victor.matchmaking.proto.common.Region.REGION_NA;
import static com.victor.matchmaking.proto.common.Region.REGION_SA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;
import com.victor.matchmaking.domain.match.MatchStatus;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

class MatchConfirmedMapperTest {

    private static final Instant OCCURRED_AT = Instant.parse("2026-10-16T12:00:00.123456Z");
    private static final String MATCH_ID = "match-1";
    private static final String CORRELATION_ID = "trace-9f2";

    @Test
    void mapsMatchAndPlayersIntoTheEvent() {
        List<MatchPlayer> players = List.of(
                player("player-1", "A", 62),
                player("player-2", "A", 58),
                player("player-3", "B", 47));

        MatchConfirmed event = MatchConfirmedMapper.toEvent(
                match(GameMode.FIVE_V_FIVE, Region.SA), players, OCCURRED_AT, CORRELATION_ID);

        assertEquals(MATCH_ID, event.getMatchId());
        assertEquals(GAME_MODE_5V5, event.getGameMode());
        assertEquals(REGION_SA, event.getRegion());
        assertEquals(3, event.getPlayersCount());
        assertEquals("player-1", event.getPlayers(0).getPlayerId());
        assertEquals(1, event.getPlayers(0).getTeam());
        assertEquals(62, event.getPlayers(0).getSkillAtMatch());
        assertEquals("player-3", event.getPlayers(2).getPlayerId());
        assertEquals(2, event.getPlayers(2).getTeam());
    }

    @Test
    void metadataIsFreshPerEventAndCarriesTheContractVersion() {
        MatchConfirmed first = MatchConfirmedMapper.toEvent(
                match(GameMode.ONE_V_ONE, Region.NA), List.of(player("p1", "A", 50)), OCCURRED_AT, CORRELATION_ID);
        MatchConfirmed second = MatchConfirmedMapper.toEvent(
                match(GameMode.ONE_V_ONE, Region.NA), List.of(player("p1", "A", 50)), OCCURRED_AT, CORRELATION_ID);

        assertEquals(MatchConfirmedMapper.EVENT_TYPE, first.getMeta().getEventType());
        assertEquals(MatchConfirmedMapper.CONTRACT_VERSION, first.getMeta().getSchemaVersion());
        assertEquals(CORRELATION_ID, first.getMeta().getCorrelationId());
        assertEquals(OCCURRED_AT.getEpochSecond(), first.getMeta().getOccurredAt().getSeconds());
        assertEquals(OCCURRED_AT.getNano(), first.getMeta().getOccurredAt().getNanos());
        // Idempotency depends on this: the same fact redelivered is recognised by id.
        assertNotEquals(first.getMeta().getEventId(), second.getMeta().getEventId());
    }

    @Test
    void everyGameModeMapsToADistinctNonDefaultEventValue() {
        Set<Integer> mapped = new HashSet<>();
        for (GameMode mode : GameMode.values()) {
            MatchConfirmed event = MatchConfirmedMapper.toEvent(
                    match(mode, Region.SA), List.of(), OCCURRED_AT, CORRELATION_ID);
            assertNotEquals(0, event.getGameModeValue(),
                    "game mode " + mode + " must not fall back to the proto3 zero value");
            mapped.add(event.getGameModeValue());
        }
        assertEquals(GameMode.values().length, mapped.size(), "each domain game mode needs its own event value");
    }

    @Test
    void everyRegionMapsToADistinctNonDefaultEventValue() {
        Set<Integer> mapped = new HashSet<>();
        for (Region region : Region.values()) {
            MatchConfirmed event = MatchConfirmedMapper.toEvent(
                    match(GameMode.ONE_V_ONE, region), List.of(), OCCURRED_AT, CORRELATION_ID);
            assertNotEquals(0, event.getRegionValue(),
                    "region " + region + " must not fall back to the proto3 zero value");
            mapped.add(event.getRegionValue());
        }
        assertEquals(Region.values().length, mapped.size(), "each domain region needs its own event value");
    }

    @Test
    void knownEnumsMapOneToOne() {
        assertEquals(GAME_MODE_1V1, MatchConfirmedMapper.toEvent(
                match(GameMode.ONE_V_ONE, Region.EU), List.of(), OCCURRED_AT, CORRELATION_ID).getGameMode());
        assertEquals(REGION_NA, MatchConfirmedMapper.toEvent(
                match(GameMode.ONE_V_ONE, Region.NA), List.of(), OCCURRED_AT, CORRELATION_ID).getRegion());
        assertEquals(REGION_EU, MatchConfirmedMapper.toEvent(
                match(GameMode.ONE_V_ONE, Region.EU), List.of(), OCCURRED_AT, CORRELATION_ID).getRegion());
    }

    @Test
    void unknownTeamLabelIsRejectedInsteadOfPublishedAsTeamZero() {
        assertThrows(IllegalArgumentException.class, () -> MatchConfirmedMapper.toEvent(
                match(GameMode.ONE_V_ONE, Region.SA), List.of(player("p1", "C", 50)), OCCURRED_AT, CORRELATION_ID));
    }

    @Test
    void eventSurvivesTheWireRoundTrip() throws Exception {
        MatchConfirmed event = MatchConfirmedMapper.toEvent(
                match(GameMode.FIVE_V_FIVE, Region.SA), List.of(player("player-1", "B", 71)), OCCURRED_AT,
                CORRELATION_ID);

        // What a consumer receives are bytes: they must decode back to the same event.
        MatchConfirmed decoded = MatchConfirmed.parseFrom(event.toByteArray());

        assertEquals(event, decoded);
    }

    private static Match match(GameMode mode, Region region) {
        return new Match(MATCH_ID, mode, region, MatchStatus.PENDING, OCCURRED_AT, null, null);
    }

    private static MatchPlayer player(String playerId, String team, int skillAtMatch) {
        return new MatchPlayer(MATCH_ID, playerId, team, skillAtMatch);
    }
}
