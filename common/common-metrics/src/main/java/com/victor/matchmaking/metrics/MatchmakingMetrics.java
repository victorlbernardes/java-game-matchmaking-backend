package com.victor.matchmaking.metrics;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;

/**
 * One Prometheus registry per process, shared by Vert.x and the domain KPIs.
 *
 * <p>Vert.x does not own the registry here: the registry is created first and handed to
 * {@link BackendRegistries#setupBackend}, so the meters Vert.x adds (HTTP server, event
 * loop, JVM) and the ones a service adds ({@code matchmaking.*}) end up in the same
 * scrape. The order matters - the registry has to be registered before the {@code Vertx}
 * instance is created, or Vert.x would install a backend of its own first.
 *
 * <p>The embedded Prometheus server of Vert.x is switched off on purpose: the exposition is
 * served by {@link MetricsEndpoint} on a port of its own, so it never shares the public
 * HTTP port and the route can be moved behind whatever the deployment uses for metrics.
 */
public final class MatchmakingMetrics {

    /** Names the registry in Vert.x's backend table; two services in one JVM need two names. */
    public static final String DEFAULT_REGISTRY_NAME = "matchmaking";

    private final String registryName;
    private final PrometheusMeterRegistry registry;
    private final MicrometerMetricsOptions options;

    public MatchmakingMetrics() {
        this(DEFAULT_REGISTRY_NAME);
    }

    /**
     * @param registryName the name Vert.x indexes the backend by; distinct per process, and
     *        distinct in tests that build more than one registry in the same JVM.
     */
    public MatchmakingMetrics(String registryName) {
        this.registryName = registryName;
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.options = new MicrometerMetricsOptions()
                .setEnabled(true)
                .setRegistryName(registryName)
                .setJvmMetricsEnabled(true)
                .setPrometheusOptions(new VertxPrometheusOptions()
                        .setEnabled(true)
                        .setStartEmbeddedServer(false));
        BackendRegistries.setupBackend(options, registry);
    }

    public PrometheusMeterRegistry registry() {
        return registry;
    }

    public String registryName() {
        return registryName;
    }

    /** Options for the {@code Vertx} instance: HTTP server, event-loop and JVM meters. */
    public VertxOptions vertxOptions() {
        return new VertxOptions().setMetricsOptions(options);
    }
}
