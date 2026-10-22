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
     * One partition of a queue (W20, ADR 0028). {@code partitions <= 1} keeps the historical key
     * ({@code queue:{mode}:{region}}): a deployment that does not partition is byte-identical to
     * what it was, and the partition count is what changes the shape.
     */
    public static String queueKey(String gameModeLabel, String regionName, int partition, int partitions) {
        String base = queueKey(gameModeLabel, regionName);
        return partitions <= 1 ? base : base + ":" + partition;
    }

    /** Every partition of one queue, in index order. */
    public static java.util.List<String> queueKeys(String gameModeLabel, String regionName, int partitions) {
        int count = Math.max(1, partitions);
        java.util.List<String> keys = new java.util.ArrayList<>(count);
        for (int partition = 0; partition < count; partition++) {
            keys.add(queueKey(gameModeLabel, regionName, partition, count));
        }
        return keys;
    }

    /**
     * Every queue key that can exist for a partition count: the closed product of game modes,
     * regions and partitions. The engine reads the heads of all of them; the poller sums them per
     * (mode, region) and publishes each partition as its own series (W20).
     *
     * <p>Building the names beats discovering them with {@code SCAN}. A {@code SCAN} page is not
     * the answer to "which queues are there" — it is one page of the keyspace, and reading a
     * single page cost the engine every queue at once when the keyspace grew (W16: with 404 keys,
     * {@code SCAN 0 MATCH queue:*} returned none of them). One trivial read per key is exact and
     * has no dependency on how much else Redis is holding.
     */
    public static java.util.List<String> allQueueKeys(int partitions) {
        return allQueueKeys(partitions, java.util.List.of(
                com.victor.matchmaking.domain.ticket.Region.values()));
    }

    /**
     * Every queue key that can exist for a partition count, restricted to the regions a deployment
     * serves (W21, ADR 0039): a regional engine reads its own queues and publishes its own KPIs.
     */
    public static java.util.List<String> allQueueKeys(int partitions,
            java.util.List<com.victor.matchmaking.domain.ticket.Region> regions) {
        java.util.List<String> keys = new java.util.ArrayList<>();
        for (com.victor.matchmaking.domain.ticket.GameMode gameMode
                : com.victor.matchmaking.domain.ticket.GameMode.values()) {
            for (com.victor.matchmaking.domain.ticket.Region region : regions) {
                keys.addAll(queueKeys(gameMode.label(), region.name(), partitions));
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

    /**
     * One in-flight latency probe (W21, ADR 0039): the instant the server issued it, in epoch
     * millis, bound to the player and the region that asked. Short lived and single use - a nonce
     * that could be answered twice would let one round trip be reported as several.
     */
    public static String latencyProbeKey(String nonce) {
        return "latency-probe:" + nonce;
    }

    /**
     * The player's measured distance to each region (W21): a hash {@code region -> rttMs}, holding
     * the best recent sample. It lives in the Redis of the region that measured it, because a
     * region's measurements are region-local hot state, like its queue.
     */
    public static String playerLatencyKey(String playerId) {
        return "latency:" + playerId;
    }

    /**
     * The instance that holds a player's socket (W19, ADR 0026). Written by the API on connect and
     * refreshed on every heartbeat, removed on disconnect; the value is the instance id. It is how
     * "which instance is this player on" stops being a guess: with N instances, the key names the
     * one holding the socket, and a nameless entry (or none) says the player is not connected.
     */
    public static String presenceKey(String playerId) {
        return "presence:" + playerId;
    }

    /**
     * The match a player is being asked to confirm (W19, ADR 0026). The engine writes it for every
     * member of a formed match and removes it when the match is confirmed or dissolved; the API
     * reads it on a heartbeat to re-present a {@code MATCH_PENDING_CONFIRMATION} the player may
     * have missed (a notification published while their socket was reconnecting). It is a reverse
     * index over {@code match-pending:{matchId}}, which is keyed the other way round and cannot be
     * searched by player.
     */
    public static String playerPendingKey(String playerId) {
        return "player-pending:" + playerId;
    }

    /**
     * One player's skill cache (W24): a hash with fields {@code rating}, {@code matchesPlayed},
     * {@code provisional}, {@code updatedAt}. TTL configured by {@code SKILL_CACHE_TTL_SECONDS}
     * (default 1 h). The cache is an acceleration; losing it falls back to Postgres and then to
     * the deterministic provider.
     */
    public static String playerSkillKey(String playerId) {
        return "skill:" + playerId;
    }
}
