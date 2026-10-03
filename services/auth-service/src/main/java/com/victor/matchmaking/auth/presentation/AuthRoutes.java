package com.victor.matchmaking.auth.presentation;

import java.security.KeyPair;

import com.victor.matchmaking.auth.application.SessionTokenIssuer;
import com.victor.matchmaking.auth.infrastructure.JwtSessionTokenIssuer;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

/** HTTP routes for the auth stub. */
public final class AuthRoutes {

    private AuthRoutes() {
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, KeyPair keyPair) {
        SessionTokenIssuer issuer = new JwtSessionTokenIssuer(jwtAuth);
        JsonObject publicJwk = com.victor.matchmaking.auth.infrastructure.RsaKeyPairProvider.publicJwkOf(keyPair);
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.post("/v1/auth/login").handler(ctx -> {
            String playerId = parsePlayerId(ctx.body() != null ? ctx.body().asJsonObject() : null);
            if (playerId == null) {
                ctx.response().setStatusCode(400)
                        .putHeader("content-type", "application/json")
                        .end("{\"error\":\"playerId is required\"}");
                return;
            }
            SessionTokenIssuer.IssuedToken token = issuer.issue(playerId);
            ctx.response().putHeader("content-type", "application/json")
                    .end(new TokenResponse(token.token(), token.expiresInSeconds()).toJson().encode());
        });

        router.get("/.well-known/jwks.json").handler(ctx -> ctx.response()
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("keys", new JsonArray().add(publicJwk)).encode()));

        return router;
    }

    private static String parsePlayerId(JsonObject body) {
        if (body == null) {
            return null;
        }
        String playerId = body.getString("playerId");
        return playerId != null && !playerId.isBlank() ? playerId : null;
    }
}
