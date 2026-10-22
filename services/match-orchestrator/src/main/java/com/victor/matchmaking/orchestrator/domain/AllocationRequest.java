package com.victor.matchmaking.orchestrator.domain;

import java.time.Instant;
import java.util.List;

import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

/**
 * A confirmed match waiting for a game server: the orchestrator's projection of the
 * MATCH_CONFIRMED event.
 *
 * <p>It holds identifiers, the mode, the region and the roster - never the
 * matchmaker's entities. The orchestrator routes a match to a server, it does not
 * need to know how the matchmaker paired it (ADR 0016).
 *
 * <p>Since W23 the roster travels with the request, not only its size: the ticket of each player
 * is what puts him back in the queue when the match cannot run (ADR 0032), and the team is what
 * the connection token carries (ADR 0033).
 *
 * @param matchId      id of the confirmed match; one row per match
 * @param roster       the players, with their queue entry and team
 * @param confirmedAt  when the match was confirmed (the event's clock, not ours)
 */
public record AllocationRequest(String matchId, GameMode gameMode, Region region, List<RosterEntry> roster,
        AllocationStatus status, Instant confirmedAt) {

    public AllocationRequest {
        if (matchId == null || matchId.isBlank()) {
            throw new IllegalArgumentException("matchId is required");
        }
        if (gameMode == null || region == null || status == null || confirmedAt == null) {
            throw new IllegalArgumentException("gameMode, region, status and confirmedAt are required");
        }
        if (roster == null || roster.isEmpty()) {
            throw new IllegalArgumentException("a confirmed match has a roster");
        }
        roster = List.copyOf(roster);
    }

    /** Roster size as the event carried it; the number the allocation and the contract use. */
    public int playerCount() {
        return roster.size();
    }

    /**
     * One player of the match as the allocation needs him.
     *
     * @param ticketId the queue entry the player was paired with; null when the event predates
     *                 W23 (a replayed old event), which the requeue reports instead of guessing
     * @param team     1 or 2, as the contract numbers the teams
     */
    public record RosterEntry(String playerId, String ticketId, int team, int skillAtMatch) {

        public RosterEntry {
            if (playerId == null || playerId.isBlank()) {
                throw new IllegalArgumentException("playerId is required");
            }
            if (team != 1 && team != 2) {
                throw new IllegalArgumentException("team must be 1 or 2, got " + team);
            }
        }
    }
}
