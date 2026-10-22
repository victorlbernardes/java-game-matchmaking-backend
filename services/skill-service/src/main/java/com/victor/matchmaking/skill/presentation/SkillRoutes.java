package com.victor.matchmaking.skill.presentation;

import com.victor.matchmaking.domain.skill.PlayerSkill;
import com.victor.matchmaking.skill.application.SkillUpdater;
import com.victor.matchmaking.skill.infrastructure.RedisSkillCache;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * HTTP routes for the skill service (W24).
 *
 * <p>Internal service: the caller is a player reading their own skill, authenticated by
 * the same JWT the API gateway issues (session token). The endpoint is not on the
 * matchmaking hot path; the matchmaker reads the Redis cache directly.
 */
public final class SkillRoutes {

    public static final String PORT_ENV = "SKILL_SERVICE_PORT";
    public static final int DEFAULT_PORT = 8088;

    private final SkillUpdater updater;
    private final RedisSkillCache cache;
    private final JWTAuth jwtAuth;

    public SkillRoutes(SkillUpdater updater, RedisSkillCache cache, JWTAuth jwtAuth) {
        this.updater = updater;
        this.cache = cache;
        this.jwtAuth = jwtAuth;
    }

    public static int portFromEnv() {
        String raw = System.getenv(PORT_ENV);
        return raw == null || raw.isBlank() ? DEFAULT_PORT : Integer.parseInt(raw.trim());
    }

    public Router router() {
        Router router = Router.router(Vertx.vertx());
        router.route().handler(BodyHandler.create());
        // GET /v1/skills/{playerId} - only the player themselves.
        router.get("/v1/skills/:playerId").handler(this::getSkill);
        // Health (liveness only - no dependencies).
        router.get("/health").handler(ctx -> ctx.response()
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("status", "UP").put("service", "skill-service").encode()));
        return router;
    }

    private void getSkill(RoutingContext ctx) {
        authenticate(ctx, jwtAuth, user -> {
            String playerId = ctx.pathParam("playerId");
            String subject = user.subject();
            if (!playerId.equals(subject)) {
                fail(ctx, 403, "forbidden", "players may only read their own skill");
                return;
            }
            // Try cache first.
            cache.get(playerId)
                    .compose(cached -> cached.map(Future::succeededFuture)
                            .orElseGet(() -> updater.getOrCreate(playerId)
                                    .compose(skill -> cache.set(playerId, skill).map(v -> skill))))
                    .onSuccess(skill -> respond(ctx, 200, toJson(skill)))
                    .onFailure(err -> fail(ctx, 500, "internal_error", err.getMessage()));
        });
    }

    private static void authenticate(RoutingContext ctx, JWTAuth jwtAuth,
            java.util.function.Consumer<User> onAuthorized) {
        String header = ctx.request().getHeader("Authorization");
        if (header == null || !header.startsWith("Bearer ")) {
            fail(ctx, 401, "unauthorized", "missing Bearer token");
            return;
        }
        String token = header.substring("Bearer ".length());
        jwtAuth.authenticate(new TokenCredentials(token))
                .onSuccess(user -> {
                    onAuthorized.accept(user);
                })
                .onFailure(err -> fail(ctx, 401, "unauthorized", "invalid token: " + err.getMessage()));
    }

    private JsonObject toJson(PlayerSkill skill) {
        return new JsonObject()
                .put("playerId", skill.playerId())
                .put("rating", skill.rating())
                .put("matchesPlayed", skill.matchesPlayed())
                .put("provisional", skill.provisional())
                .put("updatedAt", skill.updatedAt().toString());
    }

    private void respond(RoutingContext ctx, int status, JsonObject body) {
        ctx.response().setStatusCode(status)
                .putHeader("content-type", "application/json")
                .end(body.encode());
    }

    private static void fail(RoutingContext ctx, int status, String error, String message) {
        ctx.response().setStatusCode(status)
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("error", error).put("message", message).encode());
    }
}