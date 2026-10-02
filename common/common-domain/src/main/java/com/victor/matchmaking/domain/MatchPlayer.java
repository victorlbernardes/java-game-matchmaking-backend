package com.victor.matchmaking.domain;

/** Relates a player to a match. */
public record MatchPlayer(String matchId, String playerId, String team, int skillAtMatch) {

    public MatchPlayer {
        if (matchId == null || matchId.isBlank() || playerId == null || playerId.isBlank()) {
            throw new IllegalArgumentException("matchId and playerId are required");
        }
        if (team == null || team.isBlank()) {
            throw new IllegalArgumentException("team is required");
        }
        if (skillAtMatch < Player.MIN_SKILL || skillAtMatch > Player.MAX_SKILL) {
            throw new IllegalArgumentException("skillAtMatch must be between 0 and 100");
        }
    }
}
