package com.victor.matchmaking.api.application;

import java.util.List;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

/**
 * Input for ticket creation, already typed.
 *
 * <p>When {@code partyId} is set, the request enqueues every member of that
 * party atomically (W11).</p>
 */
public record CreateTicketCommand(GameMode gameMode, Region region, int maxLatencyMs, String partyId,
        List<String> members) {

    public CreateTicketCommand {
        if (gameMode == null || region == null) {
            throw new IllegalArgumentException("gameMode and region are required");
        }
        if (maxLatencyMs <= 0) {
            throw new IllegalArgumentException("maxLatencyMs must be positive");
        }
        if (partyId != null && partyId.isBlank()) {
            throw new IllegalArgumentException("partyId must not be blank");
        }
        if (partyId != null && members == null) {
            throw new IllegalArgumentException("members are required when a partyId is set");
        }
        if (partyId != null && members.isEmpty()) {
            throw new IllegalArgumentException("members must not be empty when a party is enqueued");
        }
    }
}
