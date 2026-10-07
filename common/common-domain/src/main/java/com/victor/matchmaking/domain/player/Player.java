package com.victor.matchmaking.domain.player;

import java.time.Instant;

/** A registered player. Skill is 0-100 and owned by the skill-service. */
public record Player(String id, String nickname, int skill, Instant createdAt) {

    public static final int MIN_SKILL = 0;
    public static final int MAX_SKILL = 100;

    public Player {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id is required");
        }
        if (nickname == null || nickname.isBlank()) {
            throw new IllegalArgumentException("nickname is required");
        }
        if (skill < MIN_SKILL || skill > MAX_SKILL) {
            throw new IllegalArgumentException("skill must be between 0 and 100");
        }
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt is required");
        }
    }
}
