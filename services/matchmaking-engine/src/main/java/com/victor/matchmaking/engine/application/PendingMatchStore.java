package com.victor.matchmaking.engine.application;

import io.vertx.core.Future;

/** Port for pending-match persistence. */
public interface PendingMatchStore {

    Future<Void> save(PendingMatch pendingMatch);

    Future<PendingMatch> findById(String matchId);

    Future<Void> delete(String matchId);
}
