package com.victor.matchmaking.api;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;

/** HTTP gateway. Exposes liveness for W1; business routes arrive in W2. */
public final class MatchmakingApiApplication {

    private static final int DEFAULT_PORT = 8080;

    private MatchmakingApiApplication() {
    }

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", String.valueOf(DEFAULT_PORT)));

        Router router = Router.router(vertx);
        router.get("/health").handler(ctx -> ctx.response()
                .putHeader("content-type", "application/json")
                .end("{\"status\":\"UP\"}"));

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router)
                .listen(port)
                .onSuccess(s -> System.out.println("matchmaking-api listening on :" + s.actualPort()))
                .onFailure(err -> {
                    err.printStackTrace();
                    vertx.close();
                });
    }
}
