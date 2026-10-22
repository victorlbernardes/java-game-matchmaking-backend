package com.victor.matchmaking.domain.player;

/**
 * The deterministic skill of a player id: stable, 0-100, and the only implementation of it.
 *
 * <p>It lives in the domain because three callers must agree on the number: the API's dev
 * {@link SkillProvider}, the scenario harness that generates ids which can actually pair, and (W24)
 * the tests of the skill rule. It was first written twice — once here as a Java stub and once, by
 * hand, in the Python probes — which is exactly the kind of duplication where a disagreement is
 * silent: the ids the harness produced would simply stop pairing, and every scenario that waits
 * for a match would fail for a reason that is not the property it tests.
 *
 * <p>The hash is made positive before the modulus, and the absolute value is taken in a
 * {@code long}: {@link Math#abs(int)} of {@link Integer#MIN_VALUE} is still negative, which would
 * hand out a negative skill for one id in four billion. Every other id keeps exactly the number the
 * API handed out before this function moved here.
 */
public final class DeterministicSkill {

    /** Inclusive upper bound of the range, which is also the modulus. */
    public static final int MAX = 100;

    private DeterministicSkill() {
    }

    /** The skill of a player id, in {@code 0..100}. */
    public static int of(String playerId) {
        return (int) (Math.abs((long) playerId.hashCode()) % (MAX + 1));
    }
}
