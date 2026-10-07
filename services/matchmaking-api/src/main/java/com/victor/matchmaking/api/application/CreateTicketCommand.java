package com.victor.matchmaking.api.application;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

/** Input for ticket creation, already typed. */
public record CreateTicketCommand(GameMode gameMode, Region region, int maxLatencyMs) {

    public CreateTicketCommand {
        if (gameMode == null || region == null) {
            throw new IllegalArgumentException("gameMode and region are required");
        }
        if (maxLatencyMs <= 0) {
            throw new IllegalArgumentException("maxLatencyMs must be positive");
        }
    }
}
