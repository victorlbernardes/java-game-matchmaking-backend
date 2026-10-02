package com.victor.matchmaking.domain;

import java.time.Instant;

/** A match created by the matchmaker. */
public record Match(
        String id,
        GameMode gameMode,
        Region region,
        MatchStatus status,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt) {

    public Match {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id is required");
        }
        if (gameMode == null || region == null || status == null || createdAt == null) {
            throw new IllegalArgumentException("gameMode, region, status and createdAt are required");
        }
    }
}
