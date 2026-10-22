package com.victor.matchmaking.domain.skill;

import java.time.Instant;

/**
 * A player's skill as the skill service knows it (W24, ADR 0035).
 *
 * <p>The rating is on the 0–100 scale the matchmaker uses. The internal Elo
 * calculation runs on a 400-point scale (100 rating = 400 Elo), so the stored
 * value is the rating (0–100), and the Elo is rating × 4.
 *
 * <p>A player without a row is provisional at 50: the first match the service
 * recalculates uses the {@code skill_at_match} from the event as the seed.
 */
public record PlayerSkill(
        String playerId,
        double rating,
        int matchesPlayed,
        boolean provisional,
        Instant updatedAt) {

    public static final double MIN_RATING = 0.0;
    public static final double MAX_RATING = 100.0;

    public PlayerSkill {
        if (playerId == null || playerId.isBlank()) {
            throw new IllegalArgumentException("playerId is required");
        }
        if (rating < MIN_RATING || rating > MAX_RATING) {
            throw new IllegalArgumentException("rating must be between 0 and 100");
        }
        if (matchesPlayed < 0) {
            throw new IllegalArgumentException("matchesPlayed cannot be negative");
        }
    }

    /** The default skill for a player the service has never seen (calibration start). */
    public static double defaultRating() {
        return 50.0;
    }

    /** The number of matches a player must play before leaving calibration. */
    public static int calibrationMatches() {
        return 10;
    }
}