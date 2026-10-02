package com.victor.matchmaking.domain;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

class PartyTest {

    @Test
    void leaderMustBeMember() {
        assertThrows(IllegalArgumentException.class, () -> new Party(
                "party-1", "leader", List.of("p1", "p2"), Instant.now(), true));
    }

    @Test
    void enforcesMaxSize() {
        assertThrows(IllegalArgumentException.class, () -> new Party(
                "party-1", "leader", List.of("leader", "p1", "p2", "p3", "p4", "p5"), Instant.now(), true));
    }
}
