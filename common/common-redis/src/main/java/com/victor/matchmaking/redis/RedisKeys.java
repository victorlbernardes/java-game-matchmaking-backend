package com.victor.matchmaking.redis;

/** Redis key/channel naming and formatting, single source of truth. */
public final class RedisKeys {

    public static final String CHANNEL_PLAYER_NOTIFICATIONS = "player_notifications";
    public static final String CHANNEL_MATCH_CONFIRMATIONS = "match_confirmations";

    private RedisKeys() {
    }

    public static String ticketKey(String ticketId) {
        return "ticket:" + ticketId;
    }

    public static String idempotencyKey(String key) {
        return "idem:" + key;
    }

    public static String queueKey(String gameModeLabel, String regionName) {
        return "queue:" + gameModeLabel + ":" + regionName;
    }

    public static String matchPendingKey(String matchId) {
        return "match-pending:" + matchId;
    }
}
