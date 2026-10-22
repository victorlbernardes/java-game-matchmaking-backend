package com.victor.matchmaking.scenarios;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The pending matches the engine has announced, read out of Redis.
 *
 * <p>A pending match is the only place the roster lives before the confirmation, so every
 * scenario that needs to know "which match did the engine form for *these* players" asks here.
 * Filtering by the run's own ids matters: a pending match left over from a previous run must not
 * be confirmed, requeued or measured by this one.
 */
final class Pending {

    private Pending() {
    }

    /** Every pending match whose whole roster belongs to {@code ours}, by match id. */
    static Map<String, List<String>> forPlayers(Resp redis, List<String> ours) throws Exception {
        Map<String, List<String>> found = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> pending : all(redis).entrySet()) {
            if (!pending.getValue().isEmpty() && ours.containsAll(pending.getValue())) {
                found.put(pending.getKey(), pending.getValue());
            }
        }
        return found;
    }

    /**
     * Every pending match, whatever its roster: for the checks whose question is "the engine formed
     * nothing", where filtering by the run's ids would pass by finding nothing at all.
     */
    static Map<String, List<String>> all(Resp redis) throws Exception {
        Map<String, List<String>> found = new LinkedHashMap<>();
        for (String key : redis.keys("match-pending:*")) {
            String raw = redis.get(key);
            if (raw == null || raw.isEmpty()) {
                continue;
            }
            JsonNode pending = Json.parse(raw);
            List<String> roster = new ArrayList<>();
            for (JsonNode player : pending.path("players")) {
                roster.add(player.path("playerId").asText(""));
            }
            found.put(pending.path("matchId").asText(""), roster);
        }
        return found;
    }
}
