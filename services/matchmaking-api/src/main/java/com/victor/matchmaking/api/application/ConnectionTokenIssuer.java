package com.victor.matchmaking.api.application;

import java.time.Instant;

import io.vertx.core.Future;

/**
 * Port for asking for a connection token (W23, ADR 0033).
 *
 * <p>The API does not sign it: the private key lives in the auth service, so a bug in the gateway
 * cannot mint a credential a game server would accept. The port exists so the route can be tested
 * without an issuer, and so the transport (HTTP today) is a detail of the adapter.
 */
public interface ConnectionTokenIssuer {

    /**
     * @param playerToken the caller's own session token, forwarded as the proof of who is asking;
     *                    the issuer signs only for the subject it can verify
     */
    Future<IssuedConnectionToken> issue(String playerToken, String matchId, int team);

    /** The credential and the instant it stops being accepted. */
    record IssuedConnectionToken(String token, Instant expiresAt, long expiresInSeconds) {
    }
}
