package com.victor.matchmaking.api.presentation;

import com.victor.matchmaking.api.application.CancelTicketUseCase;
import com.victor.matchmaking.api.application.CreateTicketCommand;
import com.victor.matchmaking.api.application.CreateTicketUseCase;
import com.victor.matchmaking.api.application.GetTicketUseCase;
import com.victor.matchmaking.db.MatchRepository;
import com.victor.matchmaking.redis.TicketRepository;
import com.victor.matchmaking.api.infrastructure.InMemoryTicketRepository;
import com.victor.matchmaking.redis.TicketCodec;
import com.victor.matchmaking.redis.RedisPublisher;

import io.vertx.core.Vertx;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

/** HTTP routes for the matchmaking gateway. */
public final class HttpRoutes {

    private HttpRoutes() {
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth) {
        return createRouter(vertx, jwtAuth, new InMemoryTicketRepository());
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository) {
        return buildRouter(vertx, jwtAuth, new CreateTicketUseCase(repository), new GetTicketUseCase(repository),
                new CancelTicketUseCase(repository), new WsHub(null), null);
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, RedisPublisher publisher) {
        CreateTicketUseCase createTicket = new CreateTicketUseCase(repository);
        GetTicketUseCase getTicket = new GetTicketUseCase(repository);
        CancelTicketUseCase cancelTicket = new CancelTicketUseCase(repository);
        WsHub wsHub = new WsHub(publisher);
        return buildRouter(vertx, jwtAuth, createTicket, getTicket, cancelTicket, wsHub, null);
    }

    /** Full injection used by main: caller keeps the WsHub to route Pub/Sub notifications. */
    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub) {
        return buildRouter(vertx, jwtAuth, new CreateTicketUseCase(repository), new GetTicketUseCase(repository),
                new CancelTicketUseCase(repository), wsHub, null);
    }

    /** Full injection used by main when history is backed by PostgreSQL. */
    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub,
            MatchRepository matchRepository) {
        return buildRouter(vertx, jwtAuth, new CreateTicketUseCase(repository), new GetTicketUseCase(repository),
                new CancelTicketUseCase(repository), wsHub, matchRepository);
    }

    private static Router buildRouter(Vertx vertx, JWTAuth jwtAuth, CreateTicketUseCase createTicket,
            GetTicketUseCase getTicket, CancelTicketUseCase cancelTicket, WsHub wsHub, MatchRepository matchRepository) {

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.get("/health").handler(ctx -> ctx.response()
                .putHeader("content-type", "application/json")
                .end("{\"status\":\"UP\"}"));

        router.get("/v1/ws").handler(ctx -> {
            String token = bearerToken(ctx);
            if (token == null) {
                ctx.response().setStatusCode(401).putHeader("content-type", "application/json")
                        .end("{\"error\":\"missing token\"}");
                return;
            }
            jwtAuth.authenticate(new TokenCredentials(token))
                    .onSuccess(user -> ctx.request().toWebSocket()
                            .onSuccess(ws -> {
                                String playerId = user.subject();
                                wsHub.register(playerId, ws);
                                ws.textMessageHandler(text -> wsHub.handleClientMessage(playerId, ws, text));
                                ws.closeHandler(v -> wsHub.unregister(playerId, ws));
                                ws.exceptionHandler(err -> wsHub.unregister(playerId, ws));
                            })
                            .onFailure(err -> ctx.fail(400)))
                    .onFailure(err -> ctx.response().setStatusCode(401).putHeader("content-type", "application/json")
                            .end(new io.vertx.core.json.JsonObject().put("error", "invalid token").encode()));
        });

        router.post("/v1/matchmaking/tickets").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> {
                    try {
                        CreateTicketCommand command = TicketRequestParser.parseCreate(
                                ctx.body() != null ? ctx.body().asJsonObject() : null);
                        createTicket.execute(user.subject(), command, ctx.request().getHeader("Idempotency-Key"))
                                .onSuccess(result -> {
                                    if (!result.created()) {
                                        respond(ctx, 200, TicketCodec.toJson(result.ticket()));
                                        return;
                                    }
                                    respond(ctx, 201, TicketCodec.toCreateResponse(result.ticket()));
                                })
                                .onFailure(err -> fail(ctx, 500, err.getMessage()));
                    } catch (IllegalArgumentException e) {
                        fail(ctx, 400, e.getMessage());
                    }
                }));

        router.get("/v1/matchmaking/tickets/:ticketId").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> getTicket.execute(ctx.pathParam("ticketId"), user.subject())
                        .onSuccess(ticket -> respond(ctx, 200, TicketCodec.toJson(ticket)))
                        .onFailure(err -> failWithDomainError(ctx, err))));

        router.delete("/v1/matchmaking/tickets/:ticketId").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> cancelTicket.execute(ctx.pathParam("ticketId"), user.subject())
                        .onSuccess(ticket -> respond(ctx, 200, TicketCodec.toJson(ticket)))
                        .onFailure(err -> failWithDomainError(ctx, err))));

        router.get("/v1/players/:playerId/matches").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> {
                    if (!ctx.pathParam("playerId").equals(user.subject())) {
                        fail(ctx, 403, "history belongs to another player");
                        return;
                    }
                    if (matchRepository == null) {
                        fail(ctx, 503, "history store unavailable");
                        return;
                    }
                    int limit;
                    try {
                        String limitParam = ctx.request().getParam("limit");
                        limit = limitParam == null || limitParam.isBlank() ? 20 : Integer.parseInt(limitParam);
                        if (limit < 1 || limit > 100) {
                            fail(ctx, 400, "limit must be between 1 and 100");
                            return;
                        }
                    } catch (NumberFormatException e) {
                        fail(ctx, 400, "limit must be a number");
                        return;
                    }
                    String cursorParam = ctx.request().getParam("cursor");
                    try {
                        // cursor is validated by the repository's codec
                        matchRepository.historyForPlayer(user.subject(), cursorParam, limit)
                                .onSuccess(page -> respond(ctx, 200, page.toJson()))
                                .onFailure(err -> fail(ctx, 500, "history store error: " + err.getMessage()));
                    } catch (IllegalArgumentException e) {
                        fail(ctx, 400, e.getMessage());
                    }
                }));

        return router;
    }

    private static String bearerToken(RoutingContext ctx) {
        String header = ctx.request().getHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            return header.substring("Bearer ".length());
        }
        String queryToken = ctx.request().getParam("token");
        return queryToken != null && !queryToken.isBlank() ? queryToken : null;
    }

    private static void authenticate(RoutingContext ctx, JWTAuth jwtAuth,
            java.util.function.Consumer<io.vertx.ext.auth.User> onAuthorized) {
        String header = ctx.request().getHeader("Authorization");
        if (header == null || !header.startsWith("Bearer ")) {
            fail(ctx, 401, "missing Bearer token");
            return;
        }
        String token = header.substring("Bearer ".length());
        jwtAuth.authenticate(new TokenCredentials(token))
                .onSuccess(onAuthorized::accept)
                .onFailure(err -> fail(ctx, 401, "invalid token: " + err.getMessage()));
    }

    private static void failWithDomainError(RoutingContext ctx, Throwable err) {
        if (err instanceof GetTicketUseCase.TicketNotFoundException) {
            fail(ctx, 404, err.getMessage());
        } else if (err instanceof GetTicketUseCase.TicketForbiddenException) {
            fail(ctx, 403, err.getMessage());
        } else if (err instanceof CancelTicketUseCase.TicketConflictException) {
            fail(ctx, 409, err.getMessage());
        } else {
            fail(ctx, 500, err.getMessage());
        }
    }

    private static void respond(RoutingContext ctx, int statusCode, io.vertx.core.json.JsonObject body) {
        ctx.response().setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(body.encode());
    }

    private static void fail(RoutingContext ctx, int statusCode, String message) {
        ctx.response().setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(new io.vertx.core.json.JsonObject().put("error", message).encode());
    }
}
