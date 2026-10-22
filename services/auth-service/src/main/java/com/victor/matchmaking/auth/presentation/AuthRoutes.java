package com.victor.matchmaking.auth.presentation;

import java.security.KeyPair;

import com.victor.matchmaking.auth.application.ConnectionTokenIssuer;
import com.victor.matchmaking.auth.application.SessionTokenIssuer;
import com.victor.matchmaking.auth.infrastructure.JwtConnectionTokenIssuer;
import com.victor.matchmaking.auth.infrastructure.JwtSessionTokenIssuer;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

/** HTTP routes for the auth stub. */
public final class AuthRoutes {

    private static final System.Logger LOG = System.getLogger("matchmaking.auth");

    private AuthRoutes() {
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, KeyPair keyPair) {
        return createRouter(vertx, jwtAuth, keyPair,
                new JwtConnectionTokenIssuer(jwtAuth, JwtConnectionTokenIssuer.ttlFromEnv()));
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, KeyPair keyPair,
            ConnectionTokenIssuer connectionTokens) {
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

        // Connection token (W23, ADR 0033). Internal: the caller is the matchmaking API, and the
        // proof it presents is the player's own session token - the issuer signs only for the
        // subject it can verify, so "issue me a token for player B" is unanswerable.
        router.post("/v1/auth/connection-tokens").handler(ctx -> {
            String header = ctx.request().getHeader("Authorization");
            if (header == null || !header.startsWith("Bearer ")) {
                fail(ctx, 401, "missing_token", "Authorization: Bearer <player session token> is required");
                return;
            }
            jwtAuth.authenticate(new TokenCredentials(header.substring("Bearer ".length())))
                    .onSuccess(user -> {
                        JsonObject body = ctx.body() != null ? ctx.body().asJsonObject() : null;
                        String matchId = body != null ? body.getString("matchId") : null;
                        Integer team = body != null ? body.getInteger("team") : null;
                        if (matchId == null || matchId.isBlank()) {
                            fail(ctx, 400, "match_id_required", "matchId is required");
                            return;
                        }
                        if (team == null || (team != 1 && team != 2)) {
                            fail(ctx, 400, "invalid_team", "team must be 1 or 2");
                            return;
                        }
                        ConnectionTokenIssuer.IssuedConnectionToken issued =
                                connectionTokens.issue(user.subject(), matchId, team);
                        LOG.log(System.Logger.Level.INFO,
                                "connection_token.issued playerId={0} matchId={1} team={2} expiresIn={3}",
                                user.subject(), matchId, team, issued.expiresInSeconds());
                        ctx.response().putHeader("content-type", "application/json")
                                .end(new JsonObject()
                                        .put("connectionToken", issued.token())
                                        .put("expiresAt", issued.expiresAt().toString())
                                        .put("expiresInSeconds", issued.expiresInSeconds())
                                        .encode());
                    })
                    .onFailure(err -> fail(ctx, 401, "invalid_token", "invalid token"));
        });

        router.get("/.well-known/jwks.json").handler(ctx -> ctx.response()
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("keys", new JsonArray().add(publicJwk)).encode()));

        return router;
    }

    private static void fail(io.vertx.ext.web.RoutingContext ctx, int status, String error, String message) {
        LOG.log(System.Logger.Level.WARNING, "request.rejected error={0} status={1}", error, status);
        ctx.response().setStatusCode(status)
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("error", error).put("message", message).encode());
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
