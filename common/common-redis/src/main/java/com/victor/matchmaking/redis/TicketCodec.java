package com.victor.matchmaking.redis;

import java.time.Instant;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;

import io.vertx.core.json.JsonObject;

/** Single place where domain objects become HTTP JSON. */
public final class TicketCodec {

    private static final int SKILL_RANGE_HALF_WIDTH = 2;
    private static final int MAX_SKILL = 100;

    private TicketCodec() {
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
                .put("version", ticket.version())
                .put("searchRange", searchRange(ticket.skill()));
    }

    /** Deserializes the JSON stored in Redis back into the domain record. */
    public static MatchmakingTicket fromJson(JsonObject json) {
        return new MatchmakingTicket(
                json.getString("ticketId"),
                json.getString("playerId"),
                parseGameMode(json.getString("gameMode")),
                Region.valueOf(json.getString("region")),
                json.getInteger("maxLatencyMs"),
                json.getInteger("skill"),
                Instant.parse(json.getString("queuedAt")),
                TicketState.valueOf(json.getString("status")),
                json.getInteger("version", 0));
    }

    private static GameMode parseGameMode(String label) {
        for (GameMode mode : GameMode.values()) {
            if (mode.label().equals(label)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown gameMode label: " + label);
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
