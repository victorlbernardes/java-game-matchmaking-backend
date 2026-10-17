package com.victor.matchmaking.metrics;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.micrometer.PrometheusScrapingHandler;

/**
 * The scrape endpoint, on a port of its own.
 *
 * <p>Keeping it off the public port is the point: the public API answers players, this one
 * answers Prometheus, and the two have different exposure rules (a real deployment binds
 * this to the management network or scrapes the sidecar; the local stack binds every
 * interface so the Prometheus container can reach the host).
 */
public final class MetricsEndpoint {

    public static final int DEFAULT_PORT = 9090;
    /** Environment variable that moves the port; both services run in one machine here. */
    public static final String PORT_ENV = "METRICS_PORT";
    public static final String PATH = "/metrics";

    private MetricsEndpoint() {
    }

    /** Starts the exposition. Fails the future when the port is taken, never silently. */
    public static Future<HttpServer> start(Vertx vertx, PrometheusMeterRegistry registry, int port) {
        Router router = Router.router(vertx);
        router.get(PATH).handler(PrometheusScrapingHandler.create(registry));
        router.get("/health").handler(context -> context.response()
                .putHeader("content-type", "text/plain")
                .end("UP\n"));
        return vertx.createHttpServer().requestHandler(router).listen(port);
    }

    public static int portFromEnv(int fallback) {
        String value = System.getenv(PORT_ENV);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value);
    }
}
