package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.PairingPolicy;

class PairingPolicyTest {

    private static MatchmakingTicket ticket(String id, String playerId, GameMode mode, Region region) {
        return new MatchmakingTicket(id, playerId, mode, region, 80, 50, Instant.now(), TicketState.SEARCHING, 0);
    }

    @Test
    void pairsDifferentPlayersInSameQueue() {
        assertTrue(PairingPolicy.canPair(
                ticket("t1", "p1", GameMode.FIVE_V_FIVE, Region.NA),
                ticket("t2", "p2", GameMode.FIVE_V_FIVE, Region.NA)));
    }

    @Test
    void rejectsSamePlayer() {
        assertFalse(PairingPolicy.canPair(
                ticket("t1", "p1", GameMode.FIVE_V_FIVE, Region.NA),
                ticket("t2", "p1", GameMode.FIVE_V_FIVE, Region.NA)));
    }

    @Test
    void rejectsDifferentQueues() {
        assertFalse(PairingPolicy.canPair(
                ticket("t1", "p1", GameMode.FIVE_V_FIVE, Region.NA),
                ticket("t2", "p2", GameMode.ONE_V_ONE, Region.NA)));
        assertFalse(PairingPolicy.canPair(
                ticket("t1", "p1", GameMode.FIVE_V_FIVE, Region.NA),
                ticket("t2", "p2", GameMode.FIVE_V_FIVE, Region.SA)));
    }
}
