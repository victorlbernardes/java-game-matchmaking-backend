package com.victor.matchmaking.auth.application;

import java.time.Instant;

/**
 * Port for the connection token: the short-lived credential a game server accepts a player with
 * (W23, ADR 0033).
 *
 * <p>It is deliberately <b>not</b> the session token. The session token says who the player is and
 * lives for an hour; this one says "this player may join this match, on this team, now" and lives for
 * two minutes. Handing a game server the session token would hand it an identity it can reuse for
 * everything else - the connection token is scoped so that the worst case is one match, once.
 */
public interface ConnectionTokenIssuer {

    /**
     * Issues the credential for one player in one match.
     *
     * @param playerId the subject the token names; taken from the caller's own token, never from the
     *                 request body, so one player cannot ask for another player's connection
     * @param matchId  the match the token is scoped to
     * @param team     1 or 2, the team the player was paired into
     */
    IssuedConnectionToken issue(String playerId, String matchId, int team);

    /** The token and the instant it stops being accepted. */
    record IssuedConnectionToken(String token, Instant expiresAt, long expiresInSeconds) {
    }
}
