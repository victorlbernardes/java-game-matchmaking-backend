package com.victor.matchmaking.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Gauge;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;

/**
 * The exposition contract: a meter registered on the shared registry must come out of
 * {@code /metrics} under the Prometheus name a dashboard is written against, and the
 * transport meters of Vert.x must land on the same registry.
 */
class MetricsEndpointTest {

    @Test
    void theDomainMetersComeOutUnderTheirPrometheusNames() throws Exception {
        MatchmakingMetrics metrics = new MatchmakingMetrics("test-" + UUID.randomUUID());
        Vertx vertx = Vertx.vertx(metrics.vertxOptions());
        try {
            Gauge.builder(MetricNames.QUEUE_DEPTH, () -> 3d).register(metrics.registry());
            metrics.registry().counter(MetricNames.OUTBOX_PARKED).increment(2);

            HttpServer server = await(MetricsEndpoint.start(vertx, metrics.registry(), 0));
            String base = "http://localhost:" + server.actualPort();

            String scrape = get(base + MetricsEndpoint.PATH);
            // Dots become underscores; a counter carries _total. These two lines are the
            // contract the dashboards, the alerts and requests/observability/check_metrics.sh read.
            assertTrue(scrape.contains("matchmaking_queue_depth 3.0"), scrape);
            assertTrue(scrape.contains("matchmaking_outbox_parked_total 2.0"), scrape);

            assertEquals("UP\n", get(base + "/health"));
        } finally {
            await(vertx.close());
        }
    }

    private static String get(String url) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), url);
        return response.body();
    }

    private static <T> T await(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }
}
