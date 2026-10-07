package com.victor.matchmaking.db;

import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/** One page of match history plus the cursor for the next page. */
public record MatchHistoryPage(List<MatchHistoryItem> items, String nextCursor) {

    public JsonObject toJson() {
        JsonArray itemsJson = new JsonArray();
        items.forEach(item -> itemsJson.add(item.toJson()));
        return new JsonObject().put("items", itemsJson).put("nextCursor", nextCursor);
    }
}
