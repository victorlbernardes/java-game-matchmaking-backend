package com.victor.matchmaking.api.application;

import io.vertx.core.Future;

/**
 * Where a player's socket lives, mirrored in Redis (W19, ADR 0026).
 *
 * <p>The socket itself only exists on one instance — that is unavoidable, and the design does not
 * pretend otherwise. What this port makes explicit is the difference between "the connection is
 * pinned here" and "the connection happens to be here": the truth about who is online is a shared
 * fact in Redis, written by whichever instance holds the socket, so it survives the instance and is
 * readable from every other one.
 *
 * <p>{@link #offline} is compare-and-clear: a disconnect only removes the entry if it is still
 * this instance's. Without that, the ordinary reconnect (the client opens a socket on instance B
 * before A's close handler runs) would delete B's entry, and presence would name the instance the
 * player just left.
 */
public interface PlayerPresence {

    /** Records, or refreshes, that {@code playerId} is connected on {@code instanceId}. */
    Future<Void> markOnline(String playerId, String instanceId);

    /** Clears the entry — but only while it still names {@code instanceId}. */
    Future<Void> offline(String playerId, String instanceId);

    /** For the in-memory wiring (tests, dev): nothing is shared, so there is nothing to record. */
    static PlayerPresence none() {
        return new PlayerPresence() {
            @Override
            public Future<Void> markOnline(String playerId, String instanceId) {
                return Future.succeededFuture();
            }

            @Override
            public Future<Void> offline(String playerId, String instanceId) {
                return Future.succeededFuture();
            }
        };
    }
}
