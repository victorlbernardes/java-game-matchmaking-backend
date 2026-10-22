package com.victor.matchmaking.auth.application;

import com.victor.matchmaking.domain.ticket.Region;

/** Port for session token issuance. */
public interface SessionTokenIssuer {

    /**
     * Issues a session token for the player.
     *
     * <p>The home region travels as a claim (W21, ADR 0039): it is an attribute of the account, so
     * the identity provider is where it is decided. The matchmaking API reads it from the token to
     * know the starting point of the distance to a region - a value the caller cannot choose per
     * request, which is what keeps the latency budget from being a client-declared preference.
     */
    IssuedToken issue(String playerId, Region homeRegion);

    record IssuedToken(String token, long expiresInSeconds) {
    }
}
