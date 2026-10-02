package com.victor.matchmaking.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;

class HealthCheckTest {

    private static Vertx vertx;
    private static int port;

    @BeforeAll
    static void start() throws Exception {
        vertx = Vertx.vertx();
        Router router = Router.router(vertx);
        router.get("/health").handler(ctx -> ctx.response()
                .putHeader("content-type", "application/json")
                .end("{\"status\":\"UP\"}"));
        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router);
        port = server.listen(0).toCompletionStage().toCompletableFuture()
                .get(10, TimeUnit.SECONDS).actualPort();
    }

    @AfterAll
    static void stop() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void healthEndpointReturnsUp() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/health"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"UP\""));
    }
}
