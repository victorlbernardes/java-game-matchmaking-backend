package com.victor.matchmaking.domain.ticket;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class MatchmakingTicketTest {

    @Test
    void requiresPositiveMaxLatency() {
        assertThrows(IllegalArgumentException.class, () -> new MatchmakingTicket(
                "t1", "p1", GameMode.FIVE_V_FIVE, Region.SA, 0, 50, Instant.now(), TicketState.SEARCHING, 0));
    }

    @Test
    void rejectsNullGameMode() {
        assertThrows(IllegalArgumentException.class, () -> new MatchmakingTicket(
                "t1", "p1", null, Region.SA, 100, 50, Instant.now(), TicketState.SEARCHING, 0));
    }
}
