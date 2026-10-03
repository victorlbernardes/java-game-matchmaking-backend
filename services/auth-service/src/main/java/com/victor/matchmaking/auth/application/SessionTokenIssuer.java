package com.victor.matchmaking.auth.application;

/** Port for session token issuance. */
public interface SessionTokenIssuer {

    IssuedToken issue(String playerId);

    record IssuedToken(String token, long expiresInSeconds) {
    }
}
