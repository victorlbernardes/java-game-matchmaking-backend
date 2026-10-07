package com.victor.matchmaking.db;

import java.time.Instant;
import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/** One row of the player's match history, per the API contract. */
public record MatchHistoryItem(
        String matchId,
        String gameMode,
        String region,
        String status,
        Instant startedAt,
        Instant finishedAt,
        String team) {

    public JsonObject toJson() {
        JsonObject json = new JsonObject()
                .put("matchId", matchId)
                .put("gameMode", gameMode)
                .put("region", region)
                .put("status", status)
                .put("team", team);
        json.put("startedAt", startedAt != null ? startedAt.toString() : null);
        json.put("finishedAt", finishedAt != null ? finishedAt.toString() : null);
        return json;
    }
}
