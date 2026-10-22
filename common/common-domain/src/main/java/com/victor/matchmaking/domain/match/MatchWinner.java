package com.victor.matchmaking.domain.match;

/**
 * The result of a finished match (W24): which side won, or that neither did.
 *
 * <p>It is a three-valued answer, not a nullable one. A match that ended in a draw has a
 * result, and a consumer that cannot tell a draw from "no result reported" would have to
 * guess what to do with a rating — so "no result" is the absence of a {@code MatchWinner},
 * and a draw is {@link #DRAW}.
 *
 * <p>The wire name is the contract's spelling ({@code TEAM_1}, {@code TEAM_2}, {@code DRAW})
 * and the database stores it verbatim, so a row read by hand and a payload read by a
 * consumer say the same word.
 */
public enum MatchWinner {

    TEAM_ONE("TEAM_1", 1),
    TEAM_TWO("TEAM_2", 2),
    DRAW("DRAW", 0);

    private final String wireName;
    private final int team;

    MatchWinner(String wireName, int team) {
        this.wireName = wireName;
        this.team = team;
    }

    /** The contract's spelling; what the HTTP body and the database column carry. */
    public String wireName() {
        return wireName;
    }

    /** The winning team number, or 0 for a draw. */
    public int team() {
        return team;
    }

    /**
     * Parses the wire spelling.
     *
     * @throws IllegalArgumentException when the value is missing or unknown: a result the
     *         system invents is worse than one it refuses, because nobody looks for it
     */
    public static MatchWinner fromWireName(String raw) {
        if (raw != null) {
            for (MatchWinner winner : values()) {
                if (winner.wireName.equals(raw.trim())) {
                    return winner;
                }
            }
        }
        throw new IllegalArgumentException("unknown match result: " + raw);
    }
}
