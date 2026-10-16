package com.victor.matchmaking.engine.application;

import java.time.Instant;

/**
 * Progressive widening of the accepted skill range (W10).
 *
 * <p>Two players are compatible when their skills are close, but a player that
 * has already waited a long time must not be blocked by a slightly stronger
 * opponent. Instead of a fixed window, the half-range grows with the wait time
 * of the queued player, so the queue adapts without a live call to the
 * skill-service (the skill itself was already resolved when the ticket was
 * created).</p>
 */
public final class WideningPolicy {

    private static final long FIRST_THRESHOLD_SECONDS = 30;
    private static final long SECOND_THRESHOLD_SECONDS = 60;
    private static final long THIRD_THRESHOLD_SECONDS = 120;

    private static final int INITIAL_HALF_RANGE = 2;
    private static final int SECOND_HALF_RANGE = 5;
    private static final int THIRD_HALF_RANGE = 10;
    private static final int MAX_HALF_RANGE = 20;

    private WideningPolicy() {
    }

    /**
     * The half-range applied to the queued player's skill, in skill units.
     *
     * @param waitTimeSeconds how long the queued player has been waiting
     * @return the half-range tolerated for this wait time
     */
    public static int halfRange(long waitTimeSeconds) {
        if (waitTimeSeconds < FIRST_THRESHOLD_SECONDS) {
            return INITIAL_HALF_RANGE;
        }
        if (waitTimeSeconds < SECOND_THRESHOLD_SECONDS) {
            return SECOND_HALF_RANGE;
        }
        if (waitTimeSeconds < THIRD_THRESHOLD_SECONDS) {
            return THIRD_HALF_RANGE;
        }
        return MAX_HALF_RANGE;
    }
}
