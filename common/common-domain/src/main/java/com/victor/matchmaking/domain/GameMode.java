package com.victor.matchmaking.domain;

/** Supported game modes. The wire label matches the API contract (e.g. "5v5"). */
public enum GameMode {
    ONE_V_ONE("1v1", 1),
    FIVE_V_FIVE("5v5", 5);

    private final String label;
    private final int teamSize;

    GameMode(String label, int teamSize) {
        this.label = label;
        this.teamSize = teamSize;
    }

    public String label() {
        return label;
    }

    public int teamSize() {
        return teamSize;
    }
}
