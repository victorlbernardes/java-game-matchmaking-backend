package com.victor.matchmaking.redis;

import java.time.Instant;
import java.util.function.Function;

import io.vertx.core.json.JsonObject;

/** Wire envelope for every WebSocket message; payload is typed outside this class. */
public record Envelope<T>(String type, T payload, Instant sentAt) {

    public static <T> Envelope<T> of(String type, T payload) {
        return new Envelope<>(type, payload, Instant.now());
    }

    public JsonObject toJson(Function<T, JsonObject> payloadEncoder) {
        return new JsonObject()
                .put("type", type)
                .put("payload", payload != null ? payloadEncoder.apply(payload) : new JsonObject())
                .put("sentAt", sentAt.toString());
    }

    public static <T> Envelope<T> fromJson(JsonObject json, Function<JsonObject, T> payloadDecoder) {
        JsonObject payloadJson = json.getJsonObject("payload");
        return new Envelope<>(
                json.getString("type"),
                payloadJson != null ? payloadDecoder.apply(payloadJson) : null,
                json.containsKey("sentAt") ? Instant.parse(json.getString("sentAt")) : Instant.now());
    }
}
