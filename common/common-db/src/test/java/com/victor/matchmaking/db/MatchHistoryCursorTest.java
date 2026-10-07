package com.victor.matchmaking.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class MatchHistoryCursorTest {

    @Test
    void roundTrip() {
        String cursor = MatchHistoryCursor.encode(Instant.ofEpochMilli(1728000000000L), "match-abc");
        MatchHistoryCursor.Decoded decoded = MatchHistoryCursor.decode(cursor);
        assertEquals(1728000000000L, decoded.createdAtEpochMs());
        assertEquals("match-abc", decoded.matchId());
    }

    @Test
    void blankCursorDecodesToNull() {
        assertNull(MatchHistoryCursor.decode(null));
        assertNull(MatchHistoryCursor.decode("   "));
    }

    @Test
    void garbageCursorThrows() {
        assertThrows(IllegalArgumentException.class, () -> MatchHistoryCursor.decode("!!!not-base64!!!"));
        assertThrows(IllegalArgumentException.class, () -> MatchHistoryCursor.decode(
                java.util.Base64.getUrlEncoder().encodeToString("no-colon-here".getBytes())));
    }
}
