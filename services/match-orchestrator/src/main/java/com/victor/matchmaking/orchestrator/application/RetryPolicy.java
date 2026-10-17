package com.victor.matchmaking.orchestrator.application;

import java.time.Duration;

/**
 * How many times a record is tried before it is dead-lettered, and how long the consumer
 * waits between attempts.
 *
 * <p>Bounded on purpose: at-least-once delivery with unbounded retries is not a
 * guarantee, it is a loop that never ends and a partition that never moves.
 *
 * @param maxAttempts  attempts in total, including the first one
 * @param firstBackoff wait after the first failure; doubles after each one
 */
public record RetryPolicy(int maxAttempts, Duration firstBackoff) {

    /** Three attempts, 250 ms doubling: ~750 ms of retrying before the dead letter queue. */
    public static final RetryPolicy DEFAULT = new RetryPolicy(3, Duration.ofMillis(250));

    private static final String MAX_ATTEMPTS_ENV = "CONSUMER_MAX_ATTEMPTS";
    private static final String FIRST_BACKOFF_ENV = "CONSUMER_FIRST_BACKOFF_MS";

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1: " + maxAttempts);
        }
        if (firstBackoff.isNegative()) {
            throw new IllegalArgumentException("firstBackoff must not be negative: " + firstBackoff);
        }
    }

    public static RetryPolicy fromEnv() {
        return new RetryPolicy(
                Integer.parseInt(System.getenv().getOrDefault(MAX_ATTEMPTS_ENV,
                        String.valueOf(DEFAULT.maxAttempts()))),
                Duration.ofMillis(Long.parseLong(System.getenv().getOrDefault(FIRST_BACKOFF_ENV,
                        String.valueOf(DEFAULT.firstBackoff().toMillis())))));
    }

    /**
     * Wait before {@code attempt} (1-indexed) is made: none for the first attempt, then
     * the first backoff, then double it. Exponential and capped by {@code maxAttempts},
     * so a database or registry outage cannot turn into a hot retry loop.
     */
    public Duration backoffBefore(int attempt) {
        if (attempt <= 1) {
            return Duration.ZERO;
        }
        return firstBackoff.multipliedBy(1L << (attempt - 2));
    }
}
