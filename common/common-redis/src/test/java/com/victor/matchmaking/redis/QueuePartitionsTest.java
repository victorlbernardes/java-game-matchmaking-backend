package com.victor.matchmaking.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Property: the partition key is stable, even, and moves as little as possible when the number of
 * partitions changes (W20, ADR 0028).
 *
 * <p>Three things are being asserted, and each one is a way a partition key can fail:
 *
 * <ol>
 *   <li><b>Stable.</b> The same key always lands in the same partition — across calls and across
 *       processes. A hash that is not stable sends a player to a partition the engine does not
 *       read, and the ticket waits forever. (This is why the algorithm does not use
 *       {@link String#hashCode()}, which is stable in practice but only 32 bits and not specified
 *       across JVMs.)</li>
 *   <li><b>Even.</b> A hotspot is the thing the sprint exists to remove: with 20 000 keys over 8
 *       partitions, the deepest partition may not hold more than 1.1× the mean. A key that hashed
 *       by, say, its first character would put a third of the players in three partitions.</li>
 *   <li><b>Consistent.</b> Going from 8 to 9 partitions moves about 1/9 of the keys and no more.
 *       A plain modulo passes the first two checks and fails this one: {@code h % 8 == h % 9} for
 *       almost nobody, so growing the queue reshuffles nearly every player.</li>
 * </ol>
 */
class QueuePartitionsTest {

    private static final int KEYS = 20_000;
    private static final int PARTITIONS = 8;

    @Test
    void theSameKeyAlwaysLandsInTheSamePartition() {
        for (int partitions : new int[] {1, 2, 8, 17}) {
            for (int i = 0; i < 100; i++) {
                String key = "player-" + i;
                assertEquals(QueuePartitions.partitionOf(key, partitions),
                        QueuePartitions.partitionOf(key, partitions),
                        "the same key must not move between calls");
            }
        }
    }

    @Test
    void onePartitionIsPartitionZero() {
        for (int i = 0; i < 100; i++) {
            assertEquals(0, QueuePartitions.partitionOf("player-" + i, 1),
                    "a deployment with one partition has exactly one key");
        }
    }

    @Test
    void twentyThousandKeysAreEvenlySpread() {
        Map<Integer, Integer> perPartition = distribution(PARTITIONS);
        double mean = (double) KEYS / PARTITIONS;
        int deepest = perPartition.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        int shallowest = perPartition.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        assertTrue(deepest / mean <= 1.10,
                "the deepest partition holds " + deepest + " of a mean " + mean
                        + " — a hotspot, not a distribution: " + perPartition);
        assertTrue(shallowest > 0, "every partition must be used: " + perPartition);
        assertEquals(KEYS, perPartition.values().stream().mapToInt(Integer::intValue).sum());
    }

    @Test
    void growingThePartitionCountMovesAboutOneOverN() {
        int from = 8;
        int to = 9;
        int moved = 0;
        for (int i = 0; i < KEYS; i++) {
            String key = "player-" + i;
            if (QueuePartitions.partitionOf(key, from) != QueuePartitions.partitionOf(key, to)) {
                moved++;
            }
        }
        double fraction = (double) moved / KEYS;
        double ideal = 1.0 / to;
        assertTrue(Math.abs(fraction - ideal) <= 0.02,
                "jump consistent hashing must move about 1/" + to + " of the keys, moved " + fraction);
    }

    /** The naive alternative, measured so the ADR has the number: modulo moves almost everything. */
    @Test
    void moduloIsNotConsistent() {
        int moved = 0;
        for (int i = 0; i < KEYS; i++) {
            long hash = QueuePartitions.fnv1a64("player-" + i);
            if (Math.floorMod(hash, 8) != Math.floorMod(hash, 9)) {
                moved++;
            }
        }
        double fraction = (double) moved / KEYS;
        assertTrue(fraction > 0.70,
                "modulo should remap most keys between 8 and 9 partitions, moved " + fraction);
    }

    private static Map<Integer, Integer> distribution(int partitions) {
        Map<Integer, Integer> perPartition = new HashMap<>();
        for (int i = 0; i < KEYS; i++) {
            int partition = QueuePartitions.partitionOf("player-" + i, partitions);
            perPartition.merge(partition, 1, Integer::sum);
        }
        return perPartition;
    }
}
