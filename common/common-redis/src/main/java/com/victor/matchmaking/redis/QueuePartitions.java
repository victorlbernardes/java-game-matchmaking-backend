package com.victor.matchmaking.redis;

/**
 * Which queue partition an arrival belongs to (W20, ADR 0028).
 *
 * <p>A queue key is a single Redis key, and a single key is a single shard: every arrival
 * ({@code ZADD}) and every pairing read ({@code ZRANGE}) of a (mode, region) lands on it. The
 * partition key spreads that storage over {@code P} keys without becoming a matching boundary —
 * the engine merges the heads of all partitions on every tick, so a player never waits because
 * their partner landed somewhere else (ADR 0028).
 *
 * <p>The hash is <b>jump consistent hashing</b> (Lamping &amp; Veach, "A Fast, Minimal Memory,
 * Consistent Hash Algorithm"): O(log P) time, no state to keep or synchronise, and when the number
 * of partitions changes only about {@code 1/N} of the keys move. A plain modulo is just as uniform
 * for a fixed {@code P}, but it remaps almost every key when {@code P} changes — the measured
 * difference is in {@code QueuePartitionsTest} and in ADR 0028.
 *
 * <p>The key hashed is the <b>enqueue key</b>: the party id for a party, so every member of the
 * same party lands in one partition and the atomic party enqueue stays one key; the player id for
 * a player alone.
 */
public final class QueuePartitions {

    /** Number of partitions per (mode, region). {@code 1} keeps the historical single-key layout. */
    public static final String ENV_QUEUE_PARTITIONS = "QUEUE_PARTITIONS";

    /**
     * One partition by default: a deployment that has not decided to partition keeps the key it
     * always had ({@code queue:{mode}:{region}}), so this change adds a capability instead of a
     * migration. A partition count is a deployment decision, and changing it drains the queue
     * (the key shape changes with it).
     */
    public static final int DEFAULT_PARTITIONS = 1;

    /** Bounds a typo: 1024 partitions is already far past what one queue needs. */
    public static final int MAX_PARTITIONS = 1024;

    private QueuePartitions() {
    }

    /** Reads {@value #ENV_QUEUE_PARTITIONS}, clamped to {@code [1, }{@value #MAX_PARTITIONS}{@code ]}. */
    public static int partitionsFromEnv() {
        String raw = System.getenv(ENV_QUEUE_PARTITIONS);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_PARTITIONS;
        }
        try {
            int value = Integer.parseInt(raw.trim());
            if (value < 1) {
                return 1;
            }
            return Math.min(value, MAX_PARTITIONS);
        } catch (NumberFormatException notANumber) {
            return DEFAULT_PARTITIONS;
        }
    }

    /**
     * @param key        the enqueue key (party id, or player id for a player alone)
     * @param partitions how many partitions the (mode, region) has; {@code <= 1} means "the one key"
     * @return the partition index in {@code [0, partitions)}
     */
    public static int partitionOf(String key, int partitions) {
        if (partitions <= 1) {
            return 0;
        }
        long hash = fnv1a64(key == null ? "" : key);
        long bucket = -1;
        long next = 0;
        while (next < partitions) {
            bucket = next;
            hash = hash * 2862933555777941757L + 1;
            next = (long) ((bucket + 1) * (double) (1L << 31) / (double) ((hash >>> 33) + 1));
        }
        return (int) bucket;
    }

    /** FNV-1a 64-bit: a stable hash across JVMs (unlike {@link String#hashCode()}, 32 bits). */
    static long fnv1a64(String value) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
