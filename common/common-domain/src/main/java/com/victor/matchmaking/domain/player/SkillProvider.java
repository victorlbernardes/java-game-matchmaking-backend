package com.victor.matchmaking.domain.player;

/**
 * Port for retrieving a player's skill (0-100).
 *
 * <p>The skill value is owned by the skill-service. Today a deterministic stub
 * derives a stable skill from the playerId so the full flow can be exercised
 * without the skill-service; W24 replaces it with Elo/Glicko-2.</p>
 */
public interface SkillProvider {

    /**
     * Returns the skill (0-100) for the given player.
     *
     * @param playerId the player identifier
     * @return skill value between 0 and 100
     */
    int skillForPlayer(String playerId);
}
