package com.victor.matchmaking.auth.infrastructure;

import com.victor.matchmaking.auth.application.SessionTokenIssuer;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.jwt.JWTAuth;

/** RS256 session token issuer backed by Vert.x JWTAuth. */
public final class JwtSessionTokenIssuer implements SessionTokenIssuer {

    private static final long TOKEN_TTL_SECONDS = 3600;

    private final JWTAuth jwtAuth;

    public JwtSessionTokenIssuer(JWTAuth jwtAuth) {
        this.jwtAuth = jwtAuth;
    }

    @Override
    public IssuedToken issue(String playerId) {
        String token = jwtAuth.generateToken(new JsonObject().put("sub", playerId),
                new JWTOptions().setAlgorithm("RS256").setExpiresInSeconds((int) TOKEN_TTL_SECONDS));
        return new IssuedToken(token, TOKEN_TTL_SECONDS);
    }
}
