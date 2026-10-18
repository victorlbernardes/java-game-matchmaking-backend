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

    /**
     * One token bucket: {@code ratelimit:{scope}:{id}} (W17, ADR 0023). The scope names what is
     * being limited (a player's ticket creations, a caller's requests) so two different limits
     * never share a bucket.
     */
    public static String rateLimitKey(String scope, String id) {
        return "ratelimit:" + scope + ":" + id;
    }

    public static String matchPendingKey(String matchId) {
        return "match-pending:" + matchId;
    }

    /** The set of players who confirmed a match (W8). */
    public static String matchConfirmedKey(String matchId) {
        return "match-confirmed:" + matchId;
    }

    /**
     * The claim that says a match is spoken for (W16): the first confirmation to create it is
     * the one that persists the match, and every later one is refused. Its existence also says
     * "this match is confirmed and owed a row", which is what keeps the timeout sweep from
     * dissolving a match the players already accepted (W17).
     */
    public static String matchConfirmedClaimKey(String matchId) {
        return "match-confirmed-claim:" + matchId;
    }

    public static String partyKey(String partyId) {
        return "party:" + partyId;
    }
}
