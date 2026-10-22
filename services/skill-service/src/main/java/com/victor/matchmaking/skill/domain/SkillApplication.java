package com.victor.matchmaking.skill.domain;

/**
 * The result of applying a match's outcome to one player's skill.
 *
 * @param playerId      the player
 * @param skillBefore   the stored rating before the match (0–100)
 * @param skillAfter    the stored rating after the match (0–100)
 * @param matchesPlayed including the match that caused this update
 * @param provisional   whether the player is still in calibration
 */
public record SkillApplication(String playerId, double skillBefore, double skillAfter,
        int matchesPlayed, boolean provisional) {
}