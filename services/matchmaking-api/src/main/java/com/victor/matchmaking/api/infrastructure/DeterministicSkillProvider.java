package com.victor.matchmaking.api.infrastructure;

import com.victor.matchmaking.domain.player.SkillProvider;

/**
 * Deterministic skill stub: derives a stable 0-100 skill from the playerId.
 *
 * <p>Uses a simple hash so the same player always gets the same skill in dev.
 * This is NOT a real skill system — W24 replaces it with Elo/Glicko-2 via the
 * skill-service gRPC API.</p>
 *
 * <p>It lives in matchmaking-api (not in skill-service) because services cannot
 * have compile-time dependencies on each other (AGENTS.md rule 5).</p>
 */
public final class DeterministicSkillProvider implements SkillProvider {

    @Override
    public int skillForPlayer(String playerId) {
        // Stable hash → skill 0..100. Same player always gets the same value.
        int hash = playerId.hashCode();
        return Math.abs(hash) % 101;
    }
}
