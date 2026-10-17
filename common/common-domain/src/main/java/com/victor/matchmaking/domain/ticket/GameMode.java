package com.victor.matchmaking.domain.ticket;

/** Supported game modes. The wire label matches the API contract (e.g. "5v5"). */
public enum GameMode {
    ONE_V_ONE("1v1", 1, 2),
    FIVE_V_FIVE("5v5", 5, 10);

    private final String label;
    private final int teamSize;
    private final int playersPerMatch;

    GameMode(String label, int teamSize, int playersPerMatch) {
        this.label = label;
        this.teamSize = teamSize;
        this.playersPerMatch = playersPerMatch;
    }

    public String label() {
        return label;
    }

    /** Number of players forming one team in this game mode. */
    public int teamSize() {
        return teamSize;
    }

    /** Number of players forming the whole match in this game mode. */
    public int playersPerMatch() {
        return playersPerMatch;
    }
}
