package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.engine.application.PendingMatch;

class PendingMatchTest {

    @Test
    void jsonRoundTrip() {
        PendingMatch pm = new PendingMatch("match-1",
                List.of(new PendingMatch.PendingPlayer("p1", "t1"), new PendingMatch.PendingPlayer("p2", "t2")),
                List.of("p1"), Instant.parse("2026-10-06T12:00:30Z"));
        PendingMatch restored = PendingMatch.fromJson(pm.toJson());
        assertEquals(pm.matchId(), restored.matchId());
        assertEquals(pm.playerIds(), restored.playerIds());
        assertEquals(pm.confirmations(), restored.confirmations());
        assertEquals(pm.expiresAt(), restored.expiresAt());
        assertEquals("t2", restored.ticketIdOf("p2"));
    }

    @Test
    void completenessRequiresAllPlayers() {
        PendingMatch pm = new PendingMatch("match-1",
                List.of(new PendingMatch.PendingPlayer("p1", "t1"), new PendingMatch.PendingPlayer("p2", "t2")),
                List.of(), Instant.now().plusSeconds(30));
        assertFalse(pm.isComplete());
        PendingMatch one = pm.withConfirmation("p1");
        assertFalse(one.isComplete());
        assertTrue(one.withConfirmation("p2").isComplete());
        assertTrue(one.withConfirmation("p1").hasConfirmed("p1"));
    }
}
