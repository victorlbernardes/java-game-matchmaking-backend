package com.victor.matchmaking.scenarios;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.victor.matchmaking.domain.player.DeterministicSkill;

/**
 * The ids a scenario enqueues: they must share one skill, or the engine's widening policy never
 * considers them compatible and the scenario waits for a match that cannot happen.
 *
 * <p>The skill comes from {@link DeterministicSkill} — the product's own function — instead of the
 * second implementation the Python probe carried. That duplication was the quiet kind: had the two
 * disagreed, every "the pair formed" check would fail, and the failure would look like a pairing
 * bug rather than a harness one.
 */
final class Pairing {

    /** The scan is generous on purpose: a bucket of 100+ ids is found within the first few thousand. */
    private static final int MINIMUM_SCAN = 20_000;
    private static final int SCAN_PER_PLAYER = 300;

    record Group(int skill, List<String> players) {
    }

    private Pairing() {
    }

    static int dispatch(String[] argv) {
        if (argv.length == 0) {
            throw new IllegalArgumentException("pairing <three> [--prefix X]");
        }
        Args args = Args.of(java.util.Arrays.copyOfRange(argv, 1, argv.length));
        switch (argv[0]) {
            case "pair" -> {
                Group pair = playersWithOneSkill(2, args.optional("prefix", "pair-"));
                System.out.println(pair.players().get(0) + " " + pair.players().get(1));
                return 0;
            }
            case "three" -> {
                // Two players who can be paired plus one the engine will never consider
                // compatible with them: the ghost at the head of the queue has to be a player the
                // matching cannot use, or the "ghost does not block" check would pass by pairing
                // the ghost itself.
                Group pair = playersWithOneSkill(2, args.optional("prefix", "pair-"));
                String stranger = null;
                for (int i = 1; i <= MINIMUM_SCAN && stranger == null; i++) {
                    String candidate = args.optional("prefix", "pair-") + "x" + i;
                    if (DeterministicSkill.of(candidate) != pair.skill()) {
                        stranger = candidate;
                    }
                }
                if (stranger == null) {
                    throw new IllegalStateException("could not find a player with a different skill");
                }
                System.out.println(pair.players().get(0) + " " + pair.players().get(1) + " " + stranger);
                return 0;
            }
            default -> throw new IllegalArgumentException("unknown pairing command: " + argv[0]);
        }
    }

    static Group playersWithOneSkill(int count, String prefix) {
        if (count <= 0) {
            throw new IllegalArgumentException("--count must be positive");
        }
        int scan = Math.max(MINIMUM_SCAN, SCAN_PER_PLAYER * count);
        Map<Integer, List<String>> buckets = new LinkedHashMap<>();
        for (int i = 1; i <= scan; i++) {
            String playerId = prefix + i;
            buckets.computeIfAbsent(DeterministicSkill.of(playerId), ignored -> new ArrayList<>()).add(playerId);
        }
        Map.Entry<Integer, List<String>> best = null;
        for (Map.Entry<Integer, List<String>> bucket : buckets.entrySet()) {
            // Strict: the first bucket of the maximum size wins, which is the choice Python's
            // max() made, so the same ids come out for the same prefix.
            if (best == null || bucket.getValue().size() > best.getValue().size()) {
                best = bucket;
            }
        }
        if (best == null || best.getValue().size() < count) {
            throw new IllegalStateException("only " + (best == null ? 0 : best.getValue().size())
                    + " ids share skill " + (best == null ? "?" : best.getKey()) + " out of " + scan + " scanned");
        }
        return new Group(best.getKey(), List.copyOf(best.getValue().subList(0, count)));
    }
}
