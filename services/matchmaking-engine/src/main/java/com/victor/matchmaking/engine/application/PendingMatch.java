package com.victor.matchmaking.engine.application;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/** A 1v1 match waiting for both players to confirm. Stored in Redis with TTL. */
public record PendingMatch(String matchId, List<PendingPlayer> players, List<String> confirmations, Instant expiresAt) {

    public PendingMatch {
        players = List.copyOf(players);
        confirmations = List.copyOf(confirmations);
    }

    public record PendingPlayer(String playerId, String ticketId) {
    }

    public List<String> playerIds() {
        return players.stream().map(PendingPlayer::playerId).toList();
    }

    public String ticketIdOf(String playerId) {
        return players.stream().filter(p -> p.playerId().equals(playerId)).map(PendingPlayer::ticketId).findFirst()
                .orElse(null);
    }

    public boolean isComplete() {
        return confirmations.size() == players.size();
    }

    public boolean hasConfirmed(String playerId) {
        return confirmations.contains(playerId);
    }

    public PendingMatch withConfirmation(String playerId) {
        List<String> updated = new ArrayList<>(confirmations);
        if (!updated.contains(playerId)) {
            updated.add(playerId);
        }
        return new PendingMatch(matchId, players, updated, expiresAt);
    }

    public JsonObject toJson() {
        JsonArray playersJson = new JsonArray();
        players.forEach(p -> playersJson.add(new JsonObject().put("playerId", p.playerId()).put("ticketId", p.ticketId())));
        return new JsonObject()
                .put("matchId", matchId)
                .put("players", playersJson)
                .put("confirmations", new JsonArray(confirmations))
                .put("expiresAt", expiresAt.toString());
    }

    public static PendingMatch fromJson(JsonObject json) {
        List<PendingPlayer> players = json.getJsonArray("players").stream()
                .map(o -> {
                    JsonObject obj = (JsonObject) o;
                    return new PendingPlayer(obj.getString("playerId"), obj.getString("ticketId"));
                })
                .toList();
        return new PendingMatch(
                json.getString("matchId"),
                players,
                json.getJsonArray("confirmations").stream().map(Object::toString).toList(),
                Instant.parse(json.getString("expiresAt")));
    }
}
