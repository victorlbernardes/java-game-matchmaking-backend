package com.victor.matchmaking.domain.skill;

import java.time.Instant;

/**
 * One player's skill change caused by one match (W24).
 *
 * <p>Carried by {@code SKILL_UPDATED} for telemetry: the delta can be reconstructed,
 * but the starting point cannot.
 */
public record SkillUpdate(
        String playerId,
        double skillBefore,
        double skillAfter,
        int matchesPlayed,
        boolean provisional,
        Instant updatedAt) {
}