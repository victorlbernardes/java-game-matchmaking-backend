package com.victor.matchmaking.orchestrator.presentation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.victor.matchmaking.db.GameServerRepository;
import com.victor.matchmaking.db.GameServerRepository.CapacityBelowAllocated;
import com.victor.matchmaking.db.GameServerRepository.GameServer;
import com.victor.matchmaking.db.GameServerRepository.Registration;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * The pool's HTTP surface, on the orchestrator because the orchestrator is what allocates
 * (W22, ADR 0031).
 *
 * <p>It is an <b>internal</b> API: the callers are game servers announcing themselves and an
 * operator asking what the pool looks like, never a player. That is why it carries a service
 * token instead of a player's JWT — a game server has no player identity to present, and the
 * gateway's data-tokens answer a different question (what a player may read).
 *
 * <p>The contract is a full statement of intent, not a delta: {@code PUT} says "I am here, in
 * this region, for these modes, and I can host N matches". A heartbeat that renews
 * {@code last_seen} is the same call, so an agent has exactly one thing to do and a missed
 * renewal is not a state anybody has to repair.
 */
public final class GameServerRoutes {

    public static final String PORT_ENV = "ORCHESTRATOR_PORT";
    public static final int DEFAULT_PORT = 8085;

    /** The service token every call must carry. A real deployment replaces this with mTLS. */
    public static final String TOKEN_ENV = "GAME_SERVER_TOKEN";
    private static final String DEFAULT_TOKEN = "local-game-server-token";

    private final Vertx vertx;
    private final GameServerRepository servers;
    private final int ttlSeconds;
    private final String token;

    public GameServerRoutes(Vertx vertx, GameServerRepository servers, int ttlSeconds, String token) {
        this.vertx = vertx;
        this.servers = servers;
        this.ttlSeconds = ttlSeconds;
        this.token = token;
    }

    public static int portFromEnv() {
        String raw = System.getenv(PORT_ENV);
        return raw == null || raw.isBlank() ? DEFAULT_PORT : Integer.parseInt(raw.trim());
    }

    public static String tokenFromEnv() {
        String raw = System.getenv(TOKEN_ENV);
        return raw == null || raw.isBlank() ? DEFAULT_TOKEN : raw.trim();
    }

    public Router router() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        // Liveness before the token: a probe that had to hold a secret would be a probe
        // that fails for the wrong reason.
        router.get("/health").handler(context -> context.response()
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("status", "UP").put("service", "match-orchestrator").encode()));
        router.route("/v1/*").handler(this::requireToken);
        router.put("/v1/game-servers/:serverId").handler(this::register);
        router.delete("/v1/game-servers/:serverId").handler(this::drain);
        router.post("/v1/game-servers/:serverId/allocations/:matchId/release").handler(this::release);
        router.get("/v1/game-servers").handler(this::list);
        router.get("/v1/capacity").handler(this::capacity);
        return router;
    }

    private void requireToken(RoutingContext context) {
        String header = context.request().getHeader("authorization");
        if (header != null && header.startsWith("Bearer ") && header.substring("Bearer ".length()).equals(token)) {
            context.next();
            return;
        }
        fail(context, 401, "unauthorized", "a game server token is required");
    }

    private void register(RoutingContext context) {
        Registration registration;
        try {
            registration = parse(context.pathParam("serverId"), body(context));
        } catch (IllegalArgumentException invalid) {
            fail(context, 400, "invalid_registration", invalid.getMessage());
            return;
        }
        servers.register(registration)
                .onSuccess(server -> respond(context, 200, toJson(server)))
                .onFailure(error -> {
                    if (error instanceof CapacityBelowAllocated) {
                        // Shrinking below what is already running is refused, not clamped: the
                        // matches on that server did not stop because its configuration changed.
                        fail(context, 409, "capacity_below_allocated", error.getMessage());
                    } else {
                        fail(context, 500, "internal_error", error.getMessage());
                    }
                });
    }

    private void drain(RoutingContext context) {
        servers.drain(context.pathParam("serverId"))
                .onSuccess(drained -> drained.ifPresentOrElse(
                        server -> respond(context, 200, toJson(server)),
                        () -> fail(context, 404, "server_not_found", "no such game server")))
                .onFailure(error -> fail(context, 500, "internal_error", error.getMessage()));
    }

    private void release(RoutingContext context) {
        String matchId = context.pathParam("matchId");
        servers.release(context.pathParam("serverId"), matchId)
                .onSuccess(released -> released.ifPresentOrElse(
                        server -> respond(context, 200, new JsonObject()
                                .put("serverId", server.serverId())
                                .put("matchId", matchId)
                                .put("allocated", server.allocated())
                                .put("free", server.free())),
                        // Nothing was allocated there: the match belongs to another server, or the
                        // slot already went back. Saying so is the idempotence of the release - a
                        // repeated call cannot return a second slot, because it has no row to flip.
                        () -> fail(context, 404, "allocation_not_found",
                                "match is not allocated on this server")))
                .onFailure(error -> fail(context, 500, "internal_error", error.getMessage()));
    }

    private void list(RoutingContext context) {
        Instant now = Instant.now();
        servers.list()
                .onSuccess(all -> {
                    JsonArray array = new JsonArray();
                    all.forEach(server -> array.add(toJson(server, now)));
                    respond(context, 200, new JsonObject().put("servers", array));
                })
                .onFailure(error -> fail(context, 500, "internal_error", error.getMessage()));
    }

    private void capacity(RoutingContext context) {
        GameMode gameMode;
        Region region;
        try {
            gameMode = GameMode.fromLabel(requireParam(context, "gameMode"));
            region = Region.valueOf(requireParam(context, "region"));
        } catch (IllegalArgumentException invalid) {
            fail(context, 400, "invalid_capacity_query", invalid.getMessage());
            return;
        }
        servers.snapshot()
                .onSuccess(snapshot -> {
                    GameServerRepository.RegionCapacity capacity =
                            snapshot.getOrDefault(region, GameServerRepository.RegionCapacity.UNKNOWN);
                    respond(context, 200, new JsonObject()
                            .put("gameMode", gameMode.label())
                            .put("region", region.name())
                            .put("free", capacity.free(gameMode))
                            .put("servers", capacity.servers(gameMode))
                            // The one field a reader must not guess: an unknown pool (no server
                            // ever registered here) is not an empty pool.
                            .put("authoritative", capacity.authoritative()));
                })
                .onFailure(error -> fail(context, 500, "internal_error", error.getMessage()));
    }

    private static String requireParam(RoutingContext context, String name) {
        String value = context.request().getParam(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static JsonObject body(RoutingContext context) {
        JsonObject body;
        try {
            body = context.body().asJsonObject();
        } catch (RuntimeException notJson) {
            throw new IllegalArgumentException("body must be a JSON object: " + notJson.getMessage());
        }
        if (body == null) {
            throw new IllegalArgumentException("body must be a JSON object");
        }
        return body;
    }

    private static Registration parse(String serverId, JsonObject body) {
        if (serverId == null || serverId.isBlank()) {
            throw new IllegalArgumentException("serverId is required");
        }
        String regionRaw = body.getString("region");
        if (regionRaw == null || regionRaw.isBlank()) {
            throw new IllegalArgumentException("region is required");
        }
        Region region;
        try {
            region = Region.valueOf(regionRaw);
        } catch (IllegalArgumentException unknown) {
            throw new IllegalArgumentException("unknown region: '" + regionRaw + "'");
        }
        JsonArray modesRaw = body.getJsonArray("gameModes");
        if (modesRaw == null || modesRaw.isEmpty()) {
            throw new IllegalArgumentException("gameModes must be a non-empty array");
        }
        List<GameMode> modes = new ArrayList<>();
        for (Object raw : modesRaw) {
            modes.add(GameMode.fromLabel(String.valueOf(raw)));
        }
        String endpoint = body.getString("endpoint");
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("endpoint is required");
        }
        Integer capacity = body.getInteger("capacity");
        if (capacity == null || capacity < 0) {
            throw new IllegalArgumentException("capacity must be a number >= 0");
        }
        return new Registration(serverId, region, endpoint, List.copyOf(modes), capacity);
    }

    private JsonObject toJson(GameServer server) {
        return toJson(server, Instant.now());
    }

    private JsonObject toJson(GameServer server, Instant now) {
        JsonArray modes = new JsonArray();
        server.gameModes().forEach(mode -> modes.add(mode.label()));
        return new JsonObject()
                .put("serverId", server.serverId())
                .put("region", server.region().name())
                .put("endpoint", server.endpoint())
                .put("gameModes", modes)
                .put("capacity", server.capacity())
                .put("allocated", server.allocated())
                .put("free", server.free())
                // Derived, never stored: the pool cannot disagree with its own clock.
                .put("state", server.stateAt(now, ttlSeconds).name())
                .put("lastSeen", server.lastSeen().toString());
    }

    private static void respond(RoutingContext context, int status, JsonObject body) {
        context.response()
                .setStatusCode(status)
                .putHeader("content-type", "application/json")
                .end(body.encode());
    }

    private static void fail(RoutingContext context, int status, String error, String message) {
        respond(context, status, new JsonObject().put("error", error).put("message", message));
    }
}
