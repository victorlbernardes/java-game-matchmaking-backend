package com.victor.matchmaking.domain.ticket;


import java.time.Instant;

import com.victor.matchmaking.domain.player.Player;

/** A matchmaking request. `status` always starts as SEARCHING. */
public record MatchmakingTicket(
        String id,
        String playerId,
        GameMode gameMode,
        Region region,
        int maxLatencyMs,
        int skill,
        Instant queuedAt,
        TicketState status,
        long version) {

    public MatchmakingTicket {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id is required");
        }
        if (playerId == null || playerId.isBlank()) {
            throw new IllegalArgumentException("playerId is required");
        }
        if (gameMode == null || region == null || status == null) {
            throw new IllegalArgumentException("gameMode, region and status are required");
        }
        if (maxLatencyMs <= 0) {
            throw new IllegalArgumentException("maxLatencyMs must be positive");
        }
        if (skill < Player.MIN_SKILL || skill > Player.MAX_SKILL) {
            throw new IllegalArgumentException("skill must be between 0 and 100");
        }
        if (queuedAt == null) {
            throw new IllegalArgumentException("queuedAt is required");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must be >= 0");
        }
    }
}
