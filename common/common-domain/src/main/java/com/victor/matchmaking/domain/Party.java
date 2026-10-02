package com.victor.matchmaking.domain;

import java.time.Instant;
import java.util.List;

/** A group of players enqueued as a single unit. The leader creates and dissolves it. */
public record Party(String id, String leaderId, List<String> members, Instant createdAt, boolean active) {

    public static final int MAX_MEMBERS = 5;

    public Party {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id is required");
        }
        if (leaderId == null || leaderId.isBlank()) {
            throw new IllegalArgumentException("leaderId is required");
        }
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("members is required");
        }
        if (members.size() > MAX_MEMBERS) {
            throw new IllegalArgumentException("party exceeds max size");
        }
        if (!members.contains(leaderId)) {
            throw new IllegalArgumentException("leader must be a member");
        }
        members = List.copyOf(members);
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt is required");
        }
    }
}
