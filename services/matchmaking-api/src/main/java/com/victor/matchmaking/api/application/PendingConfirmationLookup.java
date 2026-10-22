package com.victor.matchmaking.api.application;

import com.victor.matchmaking.redis.message.Payloads;

import io.vertx.core.Future;

/**
 * Answers "is this player being asked to confirm a match right now?" (W19, ADR 0026).
 *
 * <p>This is the question a heartbeat has to be able to answer for the notification contract
 * (03, "Contrato de conexão WebSocket") to hold: the pub/sub delivery is best effort, so a client
 * that reconnects on another instance has to be able to ask the state for what it missed. The
 * answer is a {@link Payloads.MatchPending} — the same payload the original notification carried —
 * or {@code null} when there is nothing to confirm.
 */
public interface PendingConfirmationLookup {

    /** The pending match for the player, or {@code null}. Never fails into an exception for the caller. */
    Future<Payloads.MatchPending> findForPlayer(String playerId);

    /** For the in-memory wiring (tests, dev): no shared state, so nothing is ever pending. */
    static PendingConfirmationLookup none() {
        return playerId -> Future.succeededFuture(null);
    }
}
