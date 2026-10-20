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

    /**
     * The mode as the wire names it ({@code "1v1"}, {@code "5v5"}), for the boundaries that read a
     * label rather than a constant: the request body (W1) and the pool's {@code game_modes} column
     * (W22). An unknown label is an {@link IllegalArgumentException}, which a boundary turns into
     * a 400 and a deployment's configuration turns into a failed boot.
     */
    public static GameMode fromLabel(String label) {
        for (GameMode mode : values()) {
            if (mode.label.equals(label)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown game mode: '" + label + "'");
    }
}
