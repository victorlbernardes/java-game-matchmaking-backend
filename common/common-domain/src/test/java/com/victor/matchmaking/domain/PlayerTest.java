package com.victor.matchmaking.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class PlayerTest {

    @Test
    void acceptsSkillInRange() {
        Player player = new Player("p1", "nick", 50, Instant.now());
        assertEquals(50, player.skill());
    }

    @Test
    void rejectsSkillAbove100() {
        assertThrows(IllegalArgumentException.class,
                () -> new Player("p1", "nick", 101, Instant.now()));
    }
}
