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

    private static final System.Logger LOG = System.getLogger("matchmaking.auth");

    private AuthRoutes() {
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, KeyPair keyPair) {
        SessionTokenIssuer issuer = new JwtSessionTokenIssuer(jwtAuth);
        JsonObject publicJwk = com.victor.matchmaking.auth.infrastructure.RsaKeyPairProvider.publicJwkOf(keyPair);
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.post("/v1/auth/login").handler(ctx -> {
            JsonObject body = ctx.body() != null ? ctx.body().asJsonObject() : null;
            String playerId = parsePlayerId(body);
            if (playerId == null) {
                LOG.log(System.Logger.Level.WARNING, "login.rejected reason=playerId is required");
                ctx.response().setStatusCode(400)
                        .putHeader("content-type", "application/json")
                        .end("{\"error\":\"playerId is required\"}");
                return;
            }
            String regionRaw = parseRegion(body);
            if (regionRaw == null) {
                LOG.log(System.Logger.Level.WARNING, "login.rejected reason=unknown region playerId={0}", playerId);
                ctx.response().setStatusCode(400)
                        .putHeader("content-type", "application/json")
                        .end(new JsonObject().put("error", "unknown region; known: "
                                + java.util.List.of(com.victor.matchmaking.domain.ticket.Region.values())).encode());
                return;
            }
            com.victor.matchmaking.domain.ticket.Region homeRegion =
                    com.victor.matchmaking.domain.ticket.Region.valueOf(regionRaw);
            SessionTokenIssuer.IssuedToken token = issuer.issue(playerId, homeRegion);
            LOG.log(System.Logger.Level.INFO, "login.issued playerId={0} region={1}", playerId, homeRegion);
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

    /**
     * The account's home region (W21, ADR 0039). Absent means {@code NA} - the stub of W2 had no
     * region and every existing caller keeps working; present, it must name a region the domain
     * knows, because a typo here would follow the player into every latency decision.
     *
     * @return the region name, or null when the body names one that does not exist
     */
    private static String parseRegion(JsonObject body) {
        String raw = body != null ? body.getString("region") : null;
        if (raw == null || raw.isBlank()) {
            return com.victor.matchmaking.domain.ticket.Region.NA.name();
        }
        String normalized = raw.trim().toUpperCase(java.util.Locale.ENGLISH);
        for (com.victor.matchmaking.domain.ticket.Region region
                : com.victor.matchmaking.domain.ticket.Region.values()) {
            if (region.name().equals(normalized)) {
                return normalized;
            }
        }
        return null;
    }
}
