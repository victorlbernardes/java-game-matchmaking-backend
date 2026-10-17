package com.victor.matchmaking.orchestrator.domain;

import java.time.Instant;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

/**
 * A confirmed match waiting for a game server: the orchestrator's projection of the
 * MATCH_CONFIRMED event.
 *
 * <p>It holds identifiers, the mode, the region and the roster size - never the
 * matchmaker's entities. The orchestrator routes a match to a server, it does not
 * need to know how the matchmaker paired it (ADR 0016).
 *
 * @param matchId      id of the confirmed match; one row per match
 * @param playerCount  roster size carried by the event
 * @param confirmedAt  when the match was confirmed (the event's clock, not ours)
 */
public record AllocationRequest(String matchId, GameMode gameMode, Region region, int playerCount,
        AllocationStatus status, Instant confirmedAt) {
}
