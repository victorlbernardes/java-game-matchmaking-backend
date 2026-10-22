package com.victor.matchmaking.domain.skill;

import java.util.List;
import java.util.Map;

/**
 * The Elo rating rule on the 0–100 scale (W24, ADR 0035).
 *
 * <p>Internal scale: 100 rating points = 400 Elo. The expected score is
 * {@code E = 1 / (1 + 10^((opponentElo - playerElo) / 400))}, which simplifies to
 * {@code E = 1 / (1 + 10^((opponentRating - playerRating) / 100))} because the
 * 400 cancels with the 4x scaling.
 *
 * <p>K-factor:
 * <ul>
 *   <li>16 during calibration (first 10 matches) — a new player moves fast</li>
 *   <li>8 after calibration — a veteran moves little</li>
 * </ul>
 *
 * <p>Opponent rating for a team game is the mean of the opposing team's ratings at
 * match time (the event carries {@code skill_at_match}). A 5v5 is one number per
 * team, not five pairwise duels — the ADR records why.
 *
 * <p>The new rating is clamped to 0–100.
 */
public final class SkillRating {

    private static final double K_CALIBRATION = 16.0;
    private static final double K_ESTABLISHED = 8.0;
    private static final double ELO_SCALE = 100.0; // 100 rating = 400 Elo, so divide by 100

    private SkillRating() {
    }

    /**
     * Updates one player's rating given the match outcome and the opponent's rating.
     *
     * @param playerRating    the player's rating before the match (0–100)
     * @param opponentRating  the mean rating of the opposing team at match time (0–100)
     * @param outcome         win/draw/loss from the player's team perspective
     * @param isProvisional   whether the player is still in calibration
     * @return the new rating, clamped to 0–100
     */
    public static double update(double playerRating, double opponentRating,
            MatchOutcome outcome, boolean isProvisional) {
        double k = isProvisional ? K_CALIBRATION : K_ESTABLISHED;
        double expected = expectedScore(playerRating, opponentRating);
        double newRating = playerRating + k * (outcome.score() - expected);
        return clamp(newRating);
    }

    /**
     * Updates all players of a finished match.
     *
     * @param players the roster with each player's team and skill at match time
     * @param winner  the match result (TEAM_1, TEAM_2, DRAW)
     * @param skills  the current stored skill of each player (null = never seen)
     * @return the new rating for each player, in the same order
     */
    public static List<Double> updateAll(
            List<MatchResultPlayer> players,
            MatchWinner winner,
            Map<String, PlayerSkill> skills) {

        // Mean rating of each team at match time (from the event's snapshot).
        double sum1 = 0, sum2 = 0;
        int count1 = 0, count2 = 0;
        for (var p : players) {
            if (p.team() == 1) {
                sum1 += p.skillAtMatch();
                count1++;
            } else {
                sum2 += p.skillAtMatch();
                count2++;
            }
        }
        double mean1 = count1 == 0 ? 50.0 : sum1 / count1;
        double mean2 = count2 == 0 ? 50.0 : sum2 / count2;

        return players.stream().map(p -> {
            PlayerSkill skill = skills.get(p.playerId());
            double currentRating = skill != null ? skill.rating() : PlayerSkill.defaultRating();
            boolean provisional = skill != null ? skill.provisional() : true;
            MatchOutcome outcome = MatchOutcome.fromWireName(winner.wireName(), p.team());
            double opponentMean = p.team() == 1 ? mean2 : mean1;
            return update(currentRating, opponentMean, outcome, provisional);
        }).toList();
    }

    /**
     * The expected score against an opponent with the given rating (0–100).
     *
     * <p>{@code E = 1 / (1 + 10^((opponent - player) / 100))}
     */
    private static double expectedScore(double playerRating, double opponentRating) {
        double diff = opponentRating - playerRating;
        return 1.0 / (1.0 + Math.pow(10.0, diff / ELO_SCALE));
    }

    /** Clamps to the valid range. */
    private static double clamp(double rating) {
        if (rating < PlayerSkill.MIN_RATING) {
            return PlayerSkill.MIN_RATING;
        }
        if (rating > PlayerSkill.MAX_RATING) {
            return PlayerSkill.MAX_RATING;
        }
        return rating;
    }
}