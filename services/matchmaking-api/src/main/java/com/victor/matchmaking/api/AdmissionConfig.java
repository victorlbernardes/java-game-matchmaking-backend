package com.victor.matchmaking.api;

import com.victor.matchmaking.redis.RateLimiter;

import io.vertx.redis.client.Redis;

/**
 * The two admission decisions of the boundary, read once at startup (W17, ADR 0022/0023).
 *
 * <p>Both default to a working value rather than to "off": a protective limit that has to be
 * turned on in production is a limit that is off on the day it matters. The defaults are also
 * chosen to be invisible to a well-behaved client - a player enqueues, occasionally cancels and
 * re-enqueues, so a burst of {@value #DEFAULT_RATE_LIMIT_BURST} at
 * {@value #DEFAULT_RATE_LIMIT_REFILL_PER_SECOND}/s is far above any human, and a queue of
 * {@value #DEFAULT_QUEUE_MAX_DEPTH} is minutes of work for the engine measured in W16
 * (~184 players/s): a depth that deep means something is already wrong.
 */
public final class AdmissionConfig {

    /**
     * Tickets one queue may hold before the boundary refuses new arrivals. 0 disables the cap.
     * Per queue (mode/region), because the pairer drains them independently.
     */
    public static final String ENV_QUEUE_MAX_DEPTH = "QUEUE_MAX_DEPTH";
    public static final int DEFAULT_QUEUE_MAX_DEPTH = 1000;

    /** Requests one player may issue back to back before being throttled. 0 disables the limiter. */
    public static final String ENV_RATE_LIMIT_BURST = "RATE_LIMIT_BURST";
    public static final int DEFAULT_RATE_LIMIT_BURST = 20;

    /** Sustained requests per second a player gets after the burst is spent. */
    public static final String ENV_RATE_LIMIT_REFILL_PER_SECOND = "RATE_LIMIT_REFILL_PER_SECOND";
    public static final double DEFAULT_RATE_LIMIT_REFILL_PER_SECOND = 5.0;

    private AdmissionConfig() {
    }

    /** Queue admission limit; 0 means unbounded, which is how the cap is switched off. */
    public static int queueMaxDepth() {
        return nonNegativeInt(ENV_QUEUE_MAX_DEPTH, DEFAULT_QUEUE_MAX_DEPTH);
    }

    /**
     * The ingress limiter, or {@code null} when it is disabled. A player's ticket creations share
     * one bucket in Redis ({@code ratelimit:tickets:{playerId}}), which is what makes the limit
     * hold across instances of this service.
     */
    public static RateLimiter rateLimiterFromEnv(Redis redis) {
        int burst = nonNegativeInt(ENV_RATE_LIMIT_BURST, DEFAULT_RATE_LIMIT_BURST);
        if (burst == 0) {
            return null;
        }
        double refill = positiveDouble(ENV_RATE_LIMIT_REFILL_PER_SECOND,
                DEFAULT_RATE_LIMIT_REFILL_PER_SECOND);
        return new RateLimiter(redis, burst, refill);
    }

    private static int nonNegativeInt(String name, int fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            int value = Integer.parseInt(raw.trim());
            if (value < 0) {
                throw new IllegalArgumentException(name + " must not be negative: " + raw);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " is not a number: " + raw, e);
        }
    }

    private static double positiveDouble(String name, double fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            double value = Double.parseDouble(raw.trim());
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive: " + raw);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " is not a number: " + raw, e);
        }
    }
}
