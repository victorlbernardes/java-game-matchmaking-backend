package com.victor.matchmaking.engine.application;

import io.vertx.core.Future;

/** Port for pending-match persistence. */
public interface PendingMatchStore {

    Future<Void> save(PendingMatch pendingMatch);

    Future<PendingMatch> findById(String matchId);

    Future<Void> delete(String matchId);

    /**
     * Points every player at their pending match (W19, ADR 0026): {@code player-pending:{playerId}}
     * → {@code matchId}, with the pending match's own TTL. The store is keyed by match, and a
     * reconnecting client only knows its player id — this is the reverse index that lets the API
     * answer "what do I owe this player?" without scanning every pending match.
     */
    Future<Void> indexPlayers(PendingMatch pendingMatch);

    /** Removes the reverse index once the match is confirmed or dissolved (W19, ADR 0026). */
    Future<Void> clearPlayerIndex(PendingMatch pendingMatch);
}
