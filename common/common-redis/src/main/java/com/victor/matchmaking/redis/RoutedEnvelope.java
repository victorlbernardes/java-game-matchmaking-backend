package com.victor.matchmaking.redis;

import io.vertx.core.json.JsonObject;

/** Notification frame routed over Pub/Sub to the API instance holding the player's WebSocket. */
public record RoutedEnvelope(String targetPlayerId, String envelopeJson) {

    public JsonObject toJson() {
        return new JsonObject().put("targetPlayerId", targetPlayerId).put("envelope", new JsonObject(envelopeJson));
    }

    public static RoutedEnvelope fromJson(JsonObject json) {
        return new RoutedEnvelope(json.getString("targetPlayerId"), json.getJsonObject("envelope").encode());
    }
}
