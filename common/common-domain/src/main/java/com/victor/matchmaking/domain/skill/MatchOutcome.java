package com.victor.matchmaking.domain.skill;

/**
 * The result of a match from the perspective of one team (W24, ADR 0035).
 *
 * <p>It mirrors {@link MatchWinner} but is the domain's vocabulary: the skill
 * service does not depend on the match domain, it only needs to know "did this
 * player's team win, lose or draw".
 */
public enum MatchOutcome {

    TEAM_ONE_WON(1.0),
    DRAW(0.5),
    TEAM_TWO_WON(0.0);

    private final double score;

    MatchOutcome(double score) {
        this.score = score;
    }

    /** The Elo score: 1 for a win, 0.5 for a draw, 0 for a loss. */
    public double score() {
        return score;
    }

    /** Parses the wire name from the {@code MATCH_FINISHED} event. */
    public static MatchOutcome fromWireName(String wireWinner, int team) {
        return switch (wireWinner) {
            case "TEAM_1" -> team == 1 ? TEAM_ONE_WON : TEAM_TWO_WON;
            case "TEAM_2" -> team == 2 ? TEAM_ONE_WON : TEAM_TWO_WON;
            case "DRAW" -> DRAW;
            default -> throw new IllegalArgumentException("unknown winner: " + wireWinner);
        };
    }
}