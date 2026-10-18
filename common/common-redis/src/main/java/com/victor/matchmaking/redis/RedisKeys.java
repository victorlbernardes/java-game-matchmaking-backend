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

    /**
     * Every queue key that can exist: the clients pick a game mode and a region from the domain
     * enums, so the queues are exactly this product.
     *
     * <p>Building the names beats discovering them with {@code SCAN}. A {@code SCAN} page is not
     * the answer to "which queues are there" — it is one page of the keyspace, and reading a
     * single page cost the engine every queue at once when the keyspace grew (W16: with 404 keys,
     * {@code SCAN 0 MATCH queue:*} returned none of them). One trivial read per key is exact and
     * has no dependency on how much else Redis is holding.
     */
    public static java.util.List<String> allQueueKeys() {
        java.util.List<String> keys = new java.util.ArrayList<>();
        for (com.victor.matchmaking.domain.ticket.GameMode gameMode
                : com.victor.matchmaking.domain.ticket.GameMode.values()) {
            for (com.victor.matchmaking.domain.ticket.Region region
                    : com.victor.matchmaking.domain.ticket.Region.values()) {
                keys.add(queueKey(gameMode.label(), region.name()));
            }
        }
        return keys;
    }

    public static String matchPendingKey(String matchId) {
        return "match-pending:" + matchId;
    }

    public static String partyKey(String partyId) {
        return "party:" + partyId;
    }
}
