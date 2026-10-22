package com.victor.matchmaking.domain.skill;

/**
 * The winner of a match, as the skill service knows it (W24).
 *
 * <p>This mirrors the wire contract's Winner enum but lives in the domain so
 * the skill calculation doesn't depend on protobuf types.
 */
public enum MatchWinner {

    TEAM_1(1),
    TEAM_2(2),
    DRAW(0);

    private final int team;

    MatchWinner(int team) {
        this.team = team;
    }

    /** The winning team number, or 0 for a draw. */
    public int team() {
        return team;
    }

    /** The wire name (TEAM_1, TEAM_2, DRAW). */
    public String wireName() {
        return name();
    }

    public static MatchWinner fromWireName(String raw) {
        if (raw != null) {
            try {
                return valueOf(raw.trim());
            } catch (IllegalArgumentException e) {
                // Fall through to error
            }
        }
        throw new IllegalArgumentException("unknown match winner: " + raw);
    }
}