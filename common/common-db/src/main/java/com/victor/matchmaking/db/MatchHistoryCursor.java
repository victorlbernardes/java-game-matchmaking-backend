package com.victor.matchmaking.db;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

/** Opaque cursor for keyset pagination: base64url of "{createdAtEpochMs}:{matchId}". */
public final class MatchHistoryCursor {

    private MatchHistoryCursor() {
    }

    public static String encode(Instant createdAt, String matchId) {
        String raw = createdAt.toEpochMilli() + ":" + matchId;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /** Returns {createdAtEpochMs, matchId}, or null when the cursor is blank/invalid. */
    public static Decoded decode(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return null;
        }
        String raw = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
        int sep = raw.lastIndexOf(':');
        if (sep <= 0 || sep == raw.length() - 1) {
            throw new IllegalArgumentException("invalid cursor");
        }
        return new Decoded(Long.parseLong(raw.substring(0, sep)), raw.substring(sep + 1));
    }

    public record Decoded(long createdAtEpochMs, String matchId) {
    }
}
