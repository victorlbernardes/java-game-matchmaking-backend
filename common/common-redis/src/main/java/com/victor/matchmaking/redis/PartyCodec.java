package com.victor.matchmaking.redis;

import java.time.Instant;
import java.util.List;

import com.victor.matchmaking.domain.player.Party;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/** Serializes party records for hot-state storage and HTTP responses. */
public final class PartyCodec {

    private PartyCodec() {
    }

    public static JsonObject toJson(Party party) {
        return new JsonObject()
                .put("partyId", party.id())
                .put("leaderId", party.leaderId())
                .put("members", new io.vertx.core.json.JsonArray(party.members()))
                .put("memberTicketIds", new io.vertx.core.json.JsonArray(party.memberTicketIds()))
                .put("createdAt", party.createdAt().toString())
                .put("active", party.active());
    }

    public static Party fromJson(JsonObject json) {
        JsonArray memberTicketIds = json.getJsonArray("memberTicketIds");
        return new Party(
                json.getString("partyId"),
                json.getString("leaderId"),
                json.getJsonArray("members").stream().map(Object::toString).toList(),
                Instant.parse(json.getString("createdAt")),
                json.getBoolean("active"),
                memberTicketIds == null ? List.of() : memberTicketIds.stream().map(Object::toString).toList());
    }
}
