package com.victor.matchmaking.api.infrastructure;

import com.victor.matchmaking.domain.player.DeterministicSkill;
import com.victor.matchmaking.domain.player.SkillProvider;

/**
 * Deterministic skill stub: derives a stable 0-100 skill from the playerId.
 *
 * <p>Uses a simple hash so the same player always gets the same skill in dev.
 * This is NOT a real skill system — W24 replaces it with Elo/Glicko-2 via the
 * skill-service gRPC API.</p>
 *
 * <p>It lives in matchmaking-api (not in skill-service) because services cannot
 * have compile-time dependencies on each other (AGENTS.md rule 5). The hash itself is
 * {@link DeterministicSkill}'s, in the domain, because the scenario harness must generate ids with
 * the same skill the API will give them (the harness used to reimplement it in Python).</p>
 */
public final class DeterministicSkillProvider implements SkillProvider {

    @Override
    public int skillForPlayer(String playerId) {
        return DeterministicSkill.of(playerId);
    }
}
