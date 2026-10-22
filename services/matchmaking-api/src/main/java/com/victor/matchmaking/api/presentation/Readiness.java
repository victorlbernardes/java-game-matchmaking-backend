package com.victor.matchmaking.api.presentation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.vertx.core.Future;

/**
 * Readiness: can this instance serve traffic right now? (W18, ADR 0024)
 *
 * <p>Liveness and readiness answer different questions, and conflating them is how an orchestrator
 * learns the wrong lesson. {@code GET /health} is liveness — the process is up, do not kill it.
 * This is readiness — the dependencies this instance needs are reachable, send traffic. Measured in
 * W18 before this existed: with Redis stopped, {@code /health} answered {@code 200 UP} while every
 * enqueue answered {@code 500}, so a load balancer would have kept sending players to the one thing
 * the instance could not do.
 *
 * <p>Each check is bounded by a timeout and a failure is an answer ({@code false}), not an
 * exception: a dependency that does not answer has already told us what we need to know, and a
 * probe that hangs is a probe that cannot be acted on.
 */
public final class Readiness {

    private static final System.Logger LOG = System.getLogger("matchmaking.api.readiness");
    /** A dependency that has not answered by now is down, as far as routing traffic is concerned. */
    private static final long PROBE_TIMEOUT_MS = 1_000;

    /** One dependency and how to ask it. The supplier is called on every probe. */
    public record Check(String name, Supplier<Future<Boolean>> probe) {
    }

    private final List<Check> checks;

    public Readiness(List<Check> checks) {
        this.checks = List.copyOf(checks);
    }

    /** Nothing to check: the in-memory wiring (tests, dev) is ready by definition. */
    public static Readiness none() {
        return new Readiness(List.of());
    }

    /** Every check, in declaration order: {@code true} means the dependency answered. */
    public Future<Map<String, Boolean>> probeAll() {
        List<Future<Map.Entry<String, Boolean>>> probes = checks.stream()
                .map(check -> check.probe().get()
                        .recover(err -> {
                            // The reason is worth a line: the status code says "not ready", only
                            // the log says which dependency and why.
                            LOG.log(System.Logger.Level.WARNING,
                                    "readiness.failed dependency={0} reason={1}",
                                    check.name(), String.valueOf(err.getMessage()));
                            return Future.succeededFuture(Boolean.FALSE);
                        })
                        .timeout(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .recover(err -> Future.succeededFuture(Boolean.FALSE))
                        .map(healthy -> Map.entry(check.name(), healthy)))
                .toList();
        return Future.all(probes).map(composite -> {
            Map<String, Boolean> result = new LinkedHashMap<>();
            for (Object entry : composite.list()) {
                @SuppressWarnings("unchecked")
                Map.Entry<String, Boolean> typed = (Map.Entry<String, Boolean>) entry;
                result.put(typed.getKey(), typed.getValue());
            }
            return result;
        });
    }

    /** True when every configured check answered; an instance with no checks is ready. */
    public boolean healthy(Map<String, Boolean> probed) {
        return probed.values().stream().allMatch(Boolean::booleanValue);
    }
}
