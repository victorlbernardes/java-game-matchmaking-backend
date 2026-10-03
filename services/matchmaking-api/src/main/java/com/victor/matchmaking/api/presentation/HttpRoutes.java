package com.victor.matchmaking.api.presentation;

import com.victor.matchmaking.api.application.CreateTicketCommand;
import com.victor.matchmaking.api.application.CreateTicketUseCase;
import com.victor.matchmaking.api.application.GetTicketUseCase;
import com.victor.matchmaking.api.application.TicketRepository;
import com.victor.matchmaking.api.infrastructure.InMemoryTicketRepository;

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
        TicketRepository repository = new InMemoryTicketRepository();
        CreateTicketUseCase createTicket = new CreateTicketUseCase(repository);
        GetTicketUseCase getTicket = new GetTicketUseCase(repository);

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.get("/health").handler(ctx -> ctx.response()
                .putHeader("content-type", "application/json")
                .end("{\"status\":\"UP\"}"));

        router.post("/v1/matchmaking/tickets").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> {
                    try {
                        CreateTicketCommand command = TicketRequestParser.parseCreate(
                                ctx.body() != null ? ctx.body().asJsonObject() : null);
                        CreateTicketUseCase.Result result = createTicket.execute(user.subject(), command,
                                ctx.request().getHeader("Idempotency-Key"));
                        if (!result.created()) {
                            ctx.response().putHeader("content-type", "application/json")
                                    .end(TicketJsonMapper.toJson(result.ticket()).encode());
                            return;
                        }
                        ctx.response().setStatusCode(201)
                                .putHeader("content-type", "application/json")
                                .end(TicketJsonMapper.toCreateResponse(result.ticket()).encode());
                    } catch (IllegalArgumentException e) {
                        fail(ctx, 400, e.getMessage());
                    }
                }));

        router.get("/v1/matchmaking/tickets/:ticketId").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> {
                    try {
                        var ticket = getTicket.execute(ctx.pathParam("ticketId"), user.subject());
                        ctx.response().putHeader("content-type", "application/json")
                                .end(TicketJsonMapper.toJson(ticket).encode());
                    } catch (GetTicketUseCase.TicketNotFoundException e) {
                        fail(ctx, 404, e.getMessage());
                    } catch (GetTicketUseCase.TicketForbiddenException e) {
                        fail(ctx, 403, e.getMessage());
                    }
                }));

        return router;
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

    private static void fail(RoutingContext ctx, int statusCode, String message) {
        ctx.response().setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(new io.vertx.core.json.JsonObject().put("error", message).encode());
    }
}
