package com.victor.matchmaking.api.presentation;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;

import io.vertx.core.json.JsonObject;

/** Single place where domain objects become HTTP JSON. */
public final class TicketJsonMapper {

    private static final int SKILL_RANGE_HALF_WIDTH = 2;
    private static final int MAX_SKILL = 100;

    private TicketJsonMapper() {
    }

    public static JsonObject toJson(MatchmakingTicket ticket) {
        return new JsonObject()
                .put("ticketId", ticket.id())
                .put("playerId", ticket.playerId())
                .put("status", ticket.status().name())
                .put("gameMode", ticket.gameMode().label())
                .put("region", ticket.region().name())
                .put("maxLatencyMs", ticket.maxLatencyMs())
                .put("queuedAt", ticket.queuedAt().toString())
                .put("skill", ticket.skill())
                .put("searchRange", searchRange(ticket.skill()));
    }

    public static JsonObject toCreateResponse(MatchmakingTicket ticket) {
        JsonObject full = toJson(ticket);
        full.remove("playerId");
        full.remove("maxLatencyMs");
        return full;
    }

    private static JsonObject searchRange(int skill) {
        return new JsonObject()
                .put("minSkill", Math.max(0, skill - SKILL_RANGE_HALF_WIDTH))
                .put("maxSkill", Math.min(MAX_SKILL, skill + SKILL_RANGE_HALF_WIDTH));
    }
}
