package com.victor.matchmaking.redis.message;

import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/** Payloads for each server → client WebSocket message type, all typed. */
public final class Payloads {

    private Payloads() {
    }

    public record Empty() {
        public JsonObject toJson() {
            return new JsonObject();
        }

        public static Empty fromJson(JsonObject json) {
            return new Empty();
        }
    }

    public record Error(String message) {
        public JsonObject toJson() {
            return new JsonObject().put("message", message);
        }

        public static Error fromJson(JsonObject json) {
            return new Error(json.getString("message"));
        }
    }

    public record MatchPending(String matchId, List<String> playerIds) {
        public JsonObject toJson() {
            return new JsonObject().put("matchId", matchId).put("playerIds", new JsonArray(playerIds));
        }

        public static MatchPending fromJson(JsonObject json) {
            return new MatchPending(json.getString("matchId"),
                    json.getJsonArray("playerIds").stream().map(Object::toString).toList());
        }
    }

    public record MatchConfirmed(String matchId) {
        public JsonObject toJson() {
            return new JsonObject().put("matchId", matchId);
        }

        public static MatchConfirmed fromJson(JsonObject json) {
            return new MatchConfirmed(json.getString("matchId"));
        }
    }

    public record MatchCancelled(String matchId, String reason) {
        public JsonObject toJson() {
            return new JsonObject().put("matchId", matchId).put("reason", reason);
        }

        public static MatchCancelled fromJson(JsonObject json) {
            return new MatchCancelled(json.getString("matchId"), json.getString("reason"));
        }
    }

    /** Client → server confirm/decline command published on the match_confirmations channel. */
    public record MatchConfirmation(String matchId, String playerId, Action action) {

        public enum Action {
            CONFIRM, DECLINE
        }

        public JsonObject toJson() {
            return new JsonObject().put("matchId", matchId).put("playerId", playerId).put("action", action.name());
        }

        public static MatchConfirmation fromJson(JsonObject json) {
            return new MatchConfirmation(json.getString("matchId"), json.getString("playerId"),
                    Action.valueOf(json.getString("action")));
        }
    }
}
