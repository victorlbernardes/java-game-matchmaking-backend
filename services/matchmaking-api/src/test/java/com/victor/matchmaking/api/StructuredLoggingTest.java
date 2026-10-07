package com.victor.matchmaking.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;

/** Fails if the structured request log (ADR 0005) is removed from the route chain. */
class StructuredLoggingTest {

    @Test
    void requestLogEmitsStructuredLine() throws Exception {
        // Force class loading so the static LOG field initializes the named logger
        Class.forName("com.victor.matchmaking.api.presentation.HttpRoutes");
        Logger httpLogger = LogManager.getLogManager().getLogger("matchmaking.api.http");
        List<LogRecord> records = new CopyOnWriteArrayList<>();
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        capture.setLevel(java.util.logging.Level.ALL);
        httpLogger.addHandler(capture);
        httpLogger.setLevel(java.util.logging.Level.ALL);

        try {
            Vertx vertx = Vertx.vertx();
            JWTAuth jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions());
            Router router = MatchmakingApiApplication.createRouter(vertx, jwtAuth);
            HttpServer server = vertx.createHttpServer();
            server.requestHandler(router);
            int port = server.listen(0).toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS).actualPort();

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/v1/matchmaking/tickets"))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(401, response.statusCode(), "expected 401 for missing token");

            // the endHandler logs asynchronously after the response ends
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline && records.stream()
                    .noneMatch(r -> formatted(r).contains("http POST /v1/matchmaking/tickets -> 401"))) {
                Thread.sleep(50);
            }
            boolean logged = records.stream()
                    .anyMatch(r -> formatted(r).contains("http POST /v1/matchmaking/tickets -> 401"));
            assertTrue(logged, "structured request log missing; captured records: "
                    + records.stream().map(StructuredLoggingTest::formatted).toList());
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } finally {
            httpLogger.removeHandler(capture);
        }
    }

    private static String formatted(LogRecord record) {
        Object[] params = record.getParameters();
        if (params == null || params.length == 0) {
            return String.valueOf(record.getMessage());
        }
        return MessageFormat.format(String.valueOf(record.getMessage()), params);
    }
}
