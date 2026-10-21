package com.victor.matchmaking.auth.infrastructure;

import java.time.Instant;

import com.victor.matchmaking.auth.application.ConnectionTokenIssuer;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.jwt.JWTAuth;

/**
 * RS256 connection token, signed with the same key pair whose public half the auth service already
 * publishes at {@code /.well-known/jwks.json} (W23, ADR 0033).
 *
 * <p>The reuse of the key is the point: a game server already has to trust the JWKS to know who a
 * player is, so validating the connection token costs it nothing new - no shared secret, no call
 * back to the matchmaking during the accept path. What changes is the claims: {@code scope},
 * {@code matchId} and {@code team} narrow an identity that would otherwise be good for everything.
 */
public final class JwtConnectionTokenIssuer implements ConnectionTokenIssuer {

    /** Env name, because the scenario needs a token that expires while it is still running. */
    public static final String ENV_TTL_SECONDS = "CONNECTION_TOKEN_TTL_SECONDS";

    /**
     * Two minutes: long enough for a client to receive the token, pick a server and connect; short
     * enough that a leaked token is worth almost nothing. The game server validates it on accept
     * and never stores it.
     */
    public static final int DEFAULT_TTL_SECONDS = 120;

    /**
     * The claim that tells a connection token apart from a session token. A game server that only
     * checks the signature would accept a session token as a connection token; checking the scope is
     * what makes the two credentials non-interchangeable.
     */
    public static final String SCOPE_GAME_CONNECT = "game-connect";

    private final JWTAuth jwtAuth;
    private final int ttlSeconds;

    public JwtConnectionTokenIssuer(JWTAuth jwtAuth, int ttlSeconds) {
        if (ttlSeconds <= 0) {
            throw new IllegalArgumentException(ENV_TTL_SECONDS + " must be positive, got " + ttlSeconds);
        }
        this.jwtAuth = jwtAuth;
        this.ttlSeconds = ttlSeconds;
    }

    public static int ttlFromEnv() {
        String raw = System.getenv(ENV_TTL_SECONDS);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_TTL_SECONDS;
        }
        return Integer.parseInt(raw.trim());
    }

    @Override
    public IssuedConnectionToken issue(String playerId, String matchId, int team) {
        if (playerId == null || playerId.isBlank() || matchId == null || matchId.isBlank()) {
            throw new IllegalArgumentException("playerId and matchId are required");
        }
        if (team != 1 && team != 2) {
            throw new IllegalArgumentException("team must be 1 or 2, got " + team);
        }
        // `sub` is the player the token is for, and the match/team claims are the scope: a token
        // minted for match A cannot open a socket in match B, and one minted for team 1 cannot be
        // replayed as a member of team 2.
        String token = jwtAuth.generateToken(
                new JsonObject()
                        .put("sub", playerId)
                        .put("matchId", matchId)
                        .put("team", team)
                        .put("scope", SCOPE_GAME_CONNECT),
                new JWTOptions().setAlgorithm("RS256").setExpiresInSeconds(ttlSeconds));
        return new IssuedConnectionToken(token, Instant.now().plusSeconds(ttlSeconds), ttlSeconds);
    }
}
