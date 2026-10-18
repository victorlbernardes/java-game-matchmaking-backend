package com.victor.matchmaking.redis;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.vertx.core.Future;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * A Lua script loaded once and **re-loaded when the server forgets it** (W18, ADR 0024).
 *
 * <p>Redis keeps its script cache next to the data, and the cache does not survive a restart, a
 * failover or a {@code SCRIPT FLUSH} — while the SHA the caller cached does. {@code EVALSHA} then
 * answers {@code NOSCRIPT} and, without this class, the same command keeps failing the same way
 * until the service is restarted: measured in W18, a Redis restart turned
 * {@code POST /v1/matchmaking/tickets} into a permanent {@code 500 NOSCRIPT No matching script}
 * for every arrival, with the matching loop stopped for the same reason. Restarting Redis is the
 * most routine operation there is; requiring every consumer to be restarted with it is an outage
 * with extra steps.
 *
 * <p>The recovery is one retry: evaluate with the cached SHA; if the server answers
 * {@code NOSCRIPT}, load the script again (which also refreshes the cached SHA) and evaluate once
 * more. A second {@code NOSCRIPT} is a real failure and is reported as one — the retry exists to
 * recover a lost cache, not to hide a broken script.
 *
 * <p>Callers build their own {@code EVALSHA} request from the SHA, so each script keeps its own
 * KEYS/ARGV shape and this class stays the one place that knows how to survive a lost cache.
 */
public final class RedisScript {

    private static final String MISSING_SCRIPT = "NOSCRIPT";

    private final Redis client;
    private final String classpathResource;
    private final AtomicReference<String> sha = new AtomicReference<>();

    /**
     * @param classpathResource the script, read from the classpath (e.g. {@code /create.lua})
     */
    public RedisScript(Redis client, String classpathResource) {
        this.client = client;
        this.classpathResource = classpathResource;
    }

    /** Evaluates the script, reloading it once if the server has lost it. */
    public Future<Response> eval(Function<String, Request> requestWithSha) {
        String cached = sha.get();
        if (cached == null) {
            return loadThenEval(requestWithSha);
        }
        return client.send(requestWithSha.apply(cached))
                .recover(err -> isMissingScript(err)
                        ? loadThenEval(requestWithSha)
                        : Future.failedFuture(err));
    }

    /** Loads the script and caches its SHA (the SHA Redis hands back is the cache key). */
    public Future<String> load() {
        String script;
        try {
            script = new String(getClass().getResourceAsStream(classpathResource).readAllBytes(),
                    StandardCharsets.UTF_8);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
        return client.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                .map(response -> {
                    String loaded = response != null ? response.toString() : null;
                    if (loaded != null) {
                        sha.set(loaded);
                    }
                    return loaded;
                });
    }

    /** The SHA this instance last loaded, or {@code null} before the first load. */
    public String cachedSha() {
        return sha.get();
    }

    private Future<Response> loadThenEval(Function<String, Request> requestWithSha) {
        return load().compose(loaded -> client.send(requestWithSha.apply(loaded)));
    }

    /** True when the server says the cached SHA is not in its cache: the one error worth a retry. */
    public static boolean isMissingScript(Throwable err) {
        String message = err != null ? err.getMessage() : null;
        return message != null && message.contains(MISSING_SCRIPT);
    }
}
