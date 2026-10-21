package com.victor.matchmaking.api.presentation;

import com.victor.matchmaking.api.application.ConnectionTokenIssuer;
import com.victor.matchmaking.db.MatchConnectionRepository;

/**
 * The two collaborators {@code GET /v1/matches/{matchId}/connection} needs (W23, ADR 0033): where the
 * allocation is read from, and who signs the token.
 *
 * <p>They travel together because the route is meaningless with one and not the other, and
 * {@link #disabled()} keeps every older wiring (tests, in-memory, the dev router) constructing the
 * same router it always did - the route simply does not exist there, instead of existing and
 * answering 500.
 */
public record MatchConnections(MatchConnectionRepository repository, ConnectionTokenIssuer tokens) {

    public static MatchConnections disabled() {
        return new MatchConnections(null, null);
    }

    public boolean enabled() {
        return repository != null && tokens != null;
    }
}
