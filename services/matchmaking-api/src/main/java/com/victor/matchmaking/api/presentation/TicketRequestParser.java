package com.victor.matchmaking.api.presentation;

import java.util.List;

import com.victor.matchmaking.api.application.CreateTicketCommand;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/** Translates the HTTP JSON body into a typed command. */
public final class TicketRequestParser {

    private TicketRequestParser() {
    }

    public static CreateTicketCommand parseCreate(JsonObject body) {
        JsonObject safeBody = body != null ? body : new JsonObject();
        String gameModeRaw = safeBody.getString("gameMode");
        String regionRaw = safeBody.getString("region");
        Integer maxLatencyMs = safeBody.getInteger("maxLatencyMs");
        if (gameModeRaw == null || regionRaw == null || maxLatencyMs == null) {
            throw new IllegalArgumentException("gameMode, region and positive maxLatencyMs are required");
        }
        String partyId = safeBody.getString("partyId");
        List<String> members = parseMembers(safeBody);
        return new CreateTicketCommand(parseGameMode(gameModeRaw), parseRegion(regionRaw),
                maxLatencyMs, partyId, members);
    }

    /** Members are only sent when the request enqueues a party. */
    private static List<String> parseMembers(JsonObject body) {
        JsonArray rawMembers = body.getJsonArray("members");
        return rawMembers == null ? List.of() : rawMembers.stream().map(Object::toString).toList();
    }

    private static GameMode parseGameMode(String raw) {
        for (GameMode mode : GameMode.values()) {
            if (mode.label().equalsIgnoreCase(raw)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown gameMode: " + raw);
    }

    private static Region parseRegion(String raw) {
        try {
            return Region.valueOf(raw.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown region: " + raw);
        }
    }
}
