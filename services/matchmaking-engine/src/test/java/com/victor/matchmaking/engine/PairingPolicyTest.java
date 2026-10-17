package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.PairingPolicy;

class PairingPolicyTest {

    private static MatchmakingTicket ticket(String id, String playerId, GameMode mode, Region region, int skill,
            Instant queuedAt) {
        return new MatchmakingTicket(id, playerId, mode, region, 80, skill, queuedAt, TicketState.SEARCHING, 0, null);
    }

    private static Instant now() {
        return Instant.now();
    }

    @Test
    void acceptsExactlyThePlayersRequiredByTheGameMode() {
        List<MatchmakingTicket> oneVOne = List.of(
                ticket("t1", "p1", GameMode.ONE_V_ONE, Region.NA, 50, Instant.now()),
                ticket("t2", "p2", GameMode.ONE_V_ONE, Region.NA, 50, Instant.now()));
        assertTrue(PairingPolicy.canPair(oneVOne, Instant.now()));
    }

    @Test
    void rejectsWhenTheCandidateCountIsWrong() {
        // A 1v1 queue requires exactly two tickets.
        List<MatchmakingTicket> tooFew = List.of(ticket("t1", "p1", GameMode.ONE_V_ONE, Region.NA, 50, Instant.now()));
        assertFalse(PairingPolicy.canPair(tooFew, Instant.now()));

        // A 5v5 queue requires exactly ten tickets.
        List<MatchmakingTicket> tooMany = List.of(
                ticket("t1", "p1", GameMode.FIVE_V_FIVE, Region.NA, 50, Instant.now()),
                ticket("t2", "p2", GameMode.FIVE_V_FIVE, Region.NA, 50, Instant.now()));
        assertFalse(PairingPolicy.canPair(tooMany, Instant.now()));
    }

    @Test
    void rejectsSamePlayerDuplicateAndMixedModeRegion() {
        List<MatchmakingTicket> candidates = List.of(
                ticket("t1", "p1", GameMode.FIVE_V_FIVE, Region.NA, 50, Instant.now()),
                ticket("t2", "p1", GameMode.FIVE_V_FIVE, Region.NA, 50, Instant.now()));
        assertFalse(PairingPolicy.canPair(candidates, Instant.now()));

        List<MatchmakingTicket> mixedMode = List.of(
                ticket("t1", "p1", GameMode.FIVE_V_FIVE, Region.NA, 50, Instant.now()),
                ticket("t2", "p2", GameMode.ONE_V_ONE, Region.NA, 50, Instant.now()));
        assertFalse(PairingPolicy.canPair(mixedMode, Instant.now()));
    }

    @Test
    void acceptsSkillsInsideTheInitialWideningHalfRange() {
        List<MatchmakingTicket> candidates = List.of(
                ticket("t1", "p1", GameMode.ONE_V_ONE, Region.NA, 50, Instant.now()),
                ticket("t2", "p2", GameMode.ONE_V_ONE, Region.NA, 52, Instant.now()));
        assertTrue(PairingPolicy.canPair(candidates, Instant.now()));
    }

    @Test
    void rejectsSkillsOutsideTheInitialWideningHalfRange() {
        List<MatchmakingTicket> candidates = List.of(
                ticket("t1", "p1", GameMode.ONE_V_ONE, Region.NA, 50, Instant.now()),
                ticket("t2", "p2", GameMode.ONE_V_ONE, Region.NA, 53, Instant.now()));
        assertFalse(PairingPolicy.canPair(candidates, Instant.now()));
    }

    @Test
    void widensTheAcceptedRangeWithWaitTime() {
        // The difference of 5 is rejected with the initial half-range (2)...
        List<MatchmakingTicket> rejected = List.of(
                ticket("t1", "p1", GameMode.ONE_V_ONE, Region.NA, 50, Instant.now()),
                ticket("t2", "p2", GameMode.ONE_V_ONE, Region.NA, 55, Instant.now()));
        assertFalse(PairingPolicy.canPair(rejected, Instant.now()));

        // ...but accepted once the queue head has waited long enough for widening.
        Instant waited = now().minus(60, ChronoUnit.SECONDS);
        List<MatchmakingTicket> accepted = List.of(
                ticket("t1", "p1", GameMode.ONE_V_ONE, Region.NA, 50, waited),
                ticket("t2", "p2", GameMode.ONE_V_ONE, Region.NA, 55, waited));
        assertTrue(PairingPolicy.canPair(accepted, waited));
    }
}
