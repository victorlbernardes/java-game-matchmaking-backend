package com.victor.matchmaking.engine.application;

import java.util.List;

import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;

import io.vertx.core.Future;

/**
 * Publishes the match lifecycle to the durable event log.
 *
 * <p>A port so the engine depends on the fact it announces, not on the broker that
 * carries it: the matching logic stays testable without Kafka and the transport can
 * change (or become an outbox) without touching it.
 */
public interface MatchEventPublisher {

    /**
     * Publishes MATCH_CONFIRMED for a match that has just been persisted.
     *
     * @param match   the persisted match
     * @param players its roster, with the team and skill snapshot to publish
     * @return the event id, completed once the broker acknowledged the record
     */
    Future<String> publishConfirmed(Match match, List<MatchPlayer> players);
}
