package com.victor.matchmaking.api.application;

import com.victor.matchmaking.domain.ticket.Region;

import io.vertx.core.Future;

/**
 * The player's measured distance to a region (W21, ADR 0039).
 *
 * <p>The measurement is the server's, not the client's: the client asks a region's endpoint for a
 * nonce, and the same server times the second request that returns it. A client can make its own
 * number worse (it can wait before answering) and cannot make it better, which is what makes the
 * value usable in a placement decision.
 */
public interface PlayerLatency {

    /** A probe the player may answer: one nonce, one region, a short life. */
    record Probe(String nonce, Region region) {
    }

    /** Issues a single-use nonce for {@code region}. */
    Future<Probe> issue(String playerId, Region region);

    /**
     * Closes a probe and records the measurement.
     *
     * @return the rtt in millis, or null when the nonce does not exist, expired, or belongs to
     *         another player/region
     */
    Future<Integer> complete(String playerId, Region region, String nonce);

    /** The best known rtt for this player in this region, or null when it was never measured. */
    Future<Integer> rtt(String playerId, Region region);

    /** Wiring for tests and the in-memory dev router: nothing was ever measured, and probing is off. */
    static PlayerLatency none() {
        return new PlayerLatency() {
            @Override
            public Future<Probe> issue(String playerId, Region region) {
                return Future.failedFuture("latency probes are not available in this environment");
            }

            @Override
            public Future<Integer> complete(String playerId, Region region, String nonce) {
                return Future.succeededFuture(null);
            }

            @Override
            public Future<Integer> rtt(String playerId, Region region) {
                return Future.succeededFuture(null);
            }
        };
    }
}
