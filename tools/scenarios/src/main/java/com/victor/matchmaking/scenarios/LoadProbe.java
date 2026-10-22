package com.victor.matchmaking.scenarios;

import java.io.Closeable;
import java.math.BigDecimal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.victor.matchmaking.domain.player.DeterministicSkill;

/**
 * The load measurement: where the ceiling is, and what the system does at it.
 *
 * <p>Drives real players through the running stack (auth → API → engine → Redis/Postgres/Kafka),
 * with an auto-confirmer standing in for the players' clients — the engine's input contract for a
 * confirmation is a {@code match_confirmations} message, the same one the E2E publishes — and
 * reports the numbers the sprint is about: enqueue throughput and latency percentiles, the pairing
 * rate (players out of the queue per second, the ceiling), the wait percentiles the players actually
 * felt, the confirmation rate, and the cost in CPU, Redis operations and Postgres writes.
 *
 * <p>It also checks the invariant the load is most likely to break: <b>one match, one event</b>. The
 * rows of this run are counted in Postgres ({@code matches} vs {@code distinct partition_key} in
 * {@code outbox_events}) and a duplicate exits non-zero, because a second {@code MATCH_CONFIRMED} for
 * the same match is a second fact for every consumer downstream.
 *
 * <p>Two measurements carry the lessons of W16 and W17 and must not be simplified away:
 *
 * <ul>
 *   <li>The client is a socket and a RESP parser, not {@code docker exec redis-cli}. Measured with
 *       the exec-based subscriber, 24 of 1024 matches were announced and never arrived — each one
 *       dissolved at its 30 s deadline. A generator whose own plumbing drops 2 % of the input cannot
 *       measure a 99 % confirm objective.
 *   <li>The wait percentiles come from the <b>delta</b> of two scrapes' histogram buckets. The
 *       exposition is cumulative, and the tail of {@code matchmaking_match_wait_seconds} is exactly
 *       where a previous run's dissolved matches sit (a requeued ticket waits its window again).
 * </ul>
 *
 * <p>An arrival the API refuses is not an error: a shed ({@code 503}, the W17 admission cap) and a
 * rate limit ({@code 429}) are the system working, and both are counted separately — a run with a
 * cap would otherwise look like a run that lost two thirds of its players.
 */
final class LoadProbe {

    private static final String API = System.getenv().getOrDefault("API", "http://localhost:8080");
    private static final String AUTH = System.getenv().getOrDefault("AUTH", "http://localhost:8081");
    private static final String ENGINE_METRICS =
            System.getenv().getOrDefault("ENGINE_METRICS", "http://localhost:9091/metrics");
    private static final int REDIS_PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
    private static final String QUEUE_ZCARD = "queue:1v1:NA";

    private LoadProbe() {
    }

    static int run(String[] argv) throws Exception {
        Args args = Args.of(argv);
        int players = args.intOf("players", 400);
        int concurrency = args.intOf("concurrency", 64);
        String label = args.optional("label", "run");
        String mode = args.optional("mode", "1v1");
        String region = args.optional("region", "NA");
        double drainTimeout = doubleOf(args, "drain-timeout", 400);
        int drainFloor = args.intOf("drain-floor", 1);
        double settleTimeout = doubleOf(args, "settle-timeout", 60);
        int confirmRetries = args.intOf("confirm-retries", 1);
        String outPath = args.optional("out", "requests/load/results-" + label + ".json");

        long runStarted = System.nanoTime();
        System.out.println("== load test: " + players + " players, concurrency " + concurrency + ", "
                + mode + "/" + region + ", label '" + label + "'");

        int[] skillAndIds = new int[1];
        List<String> playerIds = playersWithOneSkill(players, skillAndIds);
        int targetSkill = skillAndIds[0];

        try (Redis redis = Redis.connect(); Postgres postgres = Postgres.connect()) {
            // A known start: queue empty. The counters are read as deltas, so the past does not mix in.
            redis.flushAll();
            Map<String, List<Scrape.Sample>> beforeEngine = scrapeOrEmpty(ENGINE_METRICS);
            long beforePg = number(postgres.scalar(
                    "SELECT xact_commit FROM pg_stat_database WHERE datname = 'matchmaking';"));
            // The database keeps everything previous runs wrote, so the rows of THIS run are the ones
            // written after this instant.
            String runStartedAt = postgres.scalar("SELECT now();");

            Run run = new Run(concurrency, mode, region, confirmRetries, playerIds, redis);
            System.out.println("   players share skill " + targetSkill + " (all pairable, none waiting on the policy)");

            long started = System.nanoTime();
            run.loginAll();
            double loginS = elapsedSeconds(started);
            System.out.println("1. logins: " + run.tokens.size() + "/" + players + " in "
                    + oneDecimal(loginS) + "s (" + Math.round(run.tokens.size() / loginS) + "/s)");

            run.startConfirmer();
            started = System.nanoTime();
            run.enqueueAll();
            double enqueueS = elapsedSeconds(started);
            System.out.println("2. enqueues: " + run.tickets.size() + "/" + players + " in "
                    + oneDecimal(enqueueS) + "s (" + Math.round(run.tickets.size() / enqueueS) + "/s), "
                    + "client p50 " + zeroDecimals(percentile(run.enqueueMs, 0.5)) + "ms p95 "
                    + zeroDecimals(percentile(run.enqueueMs, 0.95)) + "ms p99 "
                    + zeroDecimals(percentile(run.enqueueMs, 0.99)) + "ms");

            Drain drain = run.watchDrain(drainTimeout, drainFloor);
            Double windowPlayersPerS = observedRate(run.samples, "depth", true);
            Double windowMatchesPerS = observedRate(run.samples, "formed", false);
            if (drain.seconds() == null) {
                System.out.println("3. drain: DID NOT FINISH in " + zeroDecimals(drainTimeout) + "s (peak depth "
                        + drain.peakDepth() + ", still " + redis.zcard(QUEUE_ZCARD) + " queued, observed "
                        + windowPlayersPerS + " players/s over the window)");
            } else {
                System.out.println("3. drain: " + drain.peakDepth() + " players out of the queue in "
                        + oneDecimal(drain.seconds()) + "s (" + Math.round(drain.peakDepth() / drain.seconds())
                        + " players/s, " + twoDecimals(drain.peakDepth() / 2 / drain.seconds())
                        + " matches/s)");
            }
            run.settle(settleTimeout);
            int notificationsReceived = run.notificationsReceived();
            run.stopConfirmer();

            Map<String, List<Scrape.Sample>> afterEngine = scrapeOrEmpty(ENGINE_METRICS);
            long afterPg = number(postgres.scalar(
                    "SELECT xact_commit FROM pg_stat_database WHERE datname = 'matchmaking';"));
            double runSeconds = elapsedSeconds(runStarted);

            // One match, one event: the scenario counts its own rows instead of trusting the counters.
            int matchesWritten = postgres.count(
                    "SELECT count(*) FROM matches WHERE created_at >= '" + runStartedAt + "';");
            int eventsWritten = postgres.count(
                    "SELECT count(*) FROM outbox_events WHERE created_at >= '" + runStartedAt + "';");
            int eventsDistinct = postgres.count("SELECT count(DISTINCT partition_key) FROM outbox_events "
                    + "WHERE created_at >= '" + runStartedAt + "';");
            int duplicateEvents = Math.max(0, eventsWritten - eventsDistinct);

            double formed = delta(beforeEngine, afterEngine, "matchmaking_matches_formed_total");
            double confirmed = delta(beforeEngine, afterEngine, "matchmaking_matches_confirmed_total");
            double dissolvedTimeout = delta(beforeEngine, afterEngine, "matchmaking_matches_dissolved_total",
                    Map.of("reason", "timeout"));
            double enqueuedMetric = delta(beforeEngine, afterEngine, "matchmaking_tickets_enqueued_total");
            Double waitP50 = histogramQuantileDelta(beforeEngine, afterEngine,
                    "matchmaking_match_wait_seconds", 0.50);
            Double waitP95 = histogramQuantileDelta(beforeEngine, afterEngine,
                    "matchmaking_match_wait_seconds", 0.95);
            Double waitP99 = histogramQuantileDelta(beforeEngine, afterEngine,
                    "matchmaking_match_wait_seconds", 0.99);
            long pgTx = afterPg - beforePg;
            double drainSeconds = drain.seconds() == null ? 0.0 : drain.seconds();

            ObjectNode result = buildResult(run, label, players, concurrency, mode, region, targetSkill,
                    loginS, enqueueS, drain, drainSeconds, windowPlayersPerS, windowMatchesPerS,
                    notificationsReceived, beforeEngine, afterEngine, formed, confirmed, dissolvedTimeout,
                    enqueuedMetric, waitP50, waitP95, waitP99, pgTx, runSeconds, matchesWritten,
                    eventsWritten, eventsDistinct, duplicateEvents, redis);

            java.nio.file.Path out = java.nio.file.Path.of(outPath);
            if (out.getParent() != null) {
                java.nio.file.Files.createDirectories(out.getParent());
            }
            java.nio.file.Files.writeString(out, Json.pretty(result) + "\n");

            System.out.println();
            System.out.println("== result");
            System.out.println("   enqueue        " + result.at("/enqueue/per_second").asText() + " req/s  p50 "
                    + result.at("/enqueue/p50_ms").asText() + "ms p95 "
                    + result.at("/enqueue/p95_ms").asText() + "ms p99 "
                    + result.at("/enqueue/p99_ms").asText() + "ms");
            String peak = result.at("/drain/peak_players_per_second").asText();
            String window = drain.seconds() != null
                    ? result.at("/drain/players_per_second").asText()
                    : String.valueOf(result.at("/drain/observed_players_per_second"));
            String windowMatches = drain.seconds() != null
                    ? result.at("/drain/matches_per_second").asText()
                    : String.valueOf(result.at("/drain/observed_matches_per_second"));
            System.out.println("   pairing (ceiling) " + peak + " players/s peak, " + window
                    + " players/s over the window (" + windowMatches + " matches/s), peak depth "
                    + drain.peakDepth() + ", "
                    + (drain.seconds() != null
                            ? "drained in " + result.at("/drain/seconds").asText() + "s"
                            : "partial window"));
            System.out.println("   wait felt      p50 " + waitP50 + "s  p95 " + waitP95 + "s  p99 " + waitP99 + "s");
            System.out.println("   shed           " + result.at("/shed/count").asText() + " refused (capacity "
                    + result.at("/shed/capacity") + ", p50 " + result.at("/shed/p50_ms").asText() + "ms p99 "
                    + result.at("/shed/p99_ms").asText() + "ms), rate-limited "
                    + result.at("/shed/rate_limited").asText());
            System.out.println("   matches        formed " + zeroDecimals(formed) + "  confirmed "
                    + zeroDecimals(confirmed) + "  dissolved(timeout) " + zeroDecimals(dissolvedTimeout)
                    + "  confirm rate " + result.at("/matches/confirm_rate"));
            System.out.println("   client         " + run.confirmedMatches.size() + " matches seen, "
                    + run.pendingReceived + " pending frames of " + notificationsReceived + " received, "
                    + run.confirmationsSent + " confirmations sent (retries " + confirmRetries + ")");
            System.out.println("   resources      engine cpu peak " + result.at("/resources/engine_cpu_peak")
                    + "  system cpu peak " + result.at("/resources/system_cpu_peak") + "  heap peak "
                    + result.at("/resources/heap_used_peak_mb") + "MB  redis "
                    + result.at("/resources/redis_ops_peak") + " ops/s  postgres "
                    + result.at("/resources/postgres_tx_per_second") + " tx/s");
            System.out.println("   events         " + matchesWritten + " match rows, " + eventsWritten
                    + " events (" + eventsDistinct + " matches), duplicates " + duplicateEvents);
            System.out.println("   (written to " + outPath + ")");

            if (!run.errors.isEmpty()) {
                System.out.println("   !! " + run.errors.size() + " errors, first: " + run.errors.get(0));
                return 1;
            }
            if (duplicateEvents > 0) {
                System.out.println("   !! " + duplicateEvents + " duplicate events: one match must be announced once");
                return 1;
            }
            return 0;
        }
    }

    // ------------------------------------------------------------------ the result document

    private static ObjectNode buildResult(Run run, String label, int players, int concurrency, String mode,
            String region, int skill, double loginS, double enqueueS, Drain drain, double drainSeconds,
            Double windowPlayersPerS, Double windowMatchesPerS, int notificationsReceived,
            Map<String, List<Scrape.Sample>> beforeEngine, Map<String, List<Scrape.Sample>> afterEngine,
            double formed, double confirmed, double dissolvedTimeout, double enqueuedMetric, Double waitP50,
            Double waitP95, Double waitP99, long pgTx, double runSeconds, int matchesWritten,
            int eventsWritten, int eventsDistinct, int duplicateEvents, Redis redis) throws IOException {
        ObjectNode result = Json.object();
        result.put("label", label);
        result.put("players", players);
        result.put("concurrency", concurrency);
        result.put("mode", mode);
        result.put("region", region);
        result.put("skill", skill);

        ObjectNode logins = result.putObject("logins");
        logins.put("count", run.tokens.size());
        logins.put("seconds", decimals(loginS, 2));
        putNullable(logins, "per_second", loginS > 0 ? decimals(run.tokens.size() / loginS, 1) : null);

        ObjectNode enqueue = result.putObject("enqueue");
        enqueue.put("count", run.tickets.size());
        enqueue.put("seconds", decimals(enqueueS, 2));
        putNullable(enqueue, "per_second", enqueueS > 0 ? decimals(run.tickets.size() / enqueueS, 1) : null);
        putNullable(enqueue, "p50_ms", rounded(percentile(run.enqueueMs, 0.5), 1));
        putNullable(enqueue, "p95_ms", rounded(percentile(run.enqueueMs, 0.95), 1));
        putNullable(enqueue, "p99_ms", rounded(percentile(run.enqueueMs, 0.99), 1));
        putNullable(enqueue, "max_ms", run.enqueueMs.isEmpty() ? null : decimals(max(run.enqueueMs), 1));

        ObjectNode shed = result.putObject("shed");
        shed.put("count", run.shed.size());
        shed.put("rate_limited", run.rateLimited.get());
        JsonNode capacity = run.shed.isEmpty() ? null : run.shed.get(0).path("capacity");
        if (capacity == null || capacity.isMissingNode()) {
            shed.putNull("capacity");
        } else {
            shed.set("capacity", capacity);
        }
        putNullable(shed, "p50_ms", rounded(percentile(run.shedMs, 0.5), 1));
        putNullable(shed, "p95_ms", rounded(percentile(run.shedMs, 0.95), 1));
        putNullable(shed, "p99_ms", rounded(percentile(run.shedMs, 0.99), 1));
        putNullable(shed, "max_ms", run.shedMs.isEmpty() ? null : decimals(max(run.shedMs), 1));

        ObjectNode drainNode = result.putObject("drain");
        drainNode.put("peak_depth", drain.peakDepth());
        drainNode.put("leftover", run.leftover == null ? 0.0 : (double) run.leftover);
        drainNode.put("seconds", decimals(drainSeconds, 2));
        drainNode.put("completed", drain.seconds() != null);
        putNullable(drainNode, "players_per_second", drain.seconds() == null ? null : decimals(drain.peakDepth() / drainSeconds, 1));
        putNullable(drainNode, "matches_per_second", drain.seconds() == null ? null : decimals(drain.peakDepth() / 2 / drainSeconds, 2));
        drainNode.put("peak_players_per_second", decimals(run.peakPlayersPerSecond, 1));
        putNullable(drainNode, "observed_players_per_second",
                windowPlayersPerS == null ? null : decimals(windowPlayersPerS, 1));
        putNullable(drainNode, "observed_matches_per_second",
                windowMatchesPerS == null ? null : decimals(windowMatchesPerS, 2));

        ObjectNode wait = result.putObject("wait");
        putNullable(wait, "p50_s", waitP50);
        putNullable(wait, "p95_s", waitP95);
        putNullable(wait, "p99_s", waitP99);
        putNullable(wait, "queued_players_per_second_effective",
                drain.seconds() == null ? null : decimals(drain.peakDepth() / drainSeconds, 1));

        ObjectNode matches = result.putObject("matches");
        matches.put("enqueued_metric", enqueuedMetric);
        matches.put("formed", formed);
        matches.put("confirmed", confirmed);
        matches.put("dissolved_timeout", dissolvedTimeout);
        putNullable(matches, "confirm_rate", formed > 0 ? decimals(confirmed / formed, 4) : null);
        matches.put("confirmed_by_client", run.confirmedMatches.size());
        matches.put("notifications_received", notificationsReceived);
        matches.put("pending_notifications_received", run.pendingReceived);
        matches.put("confirmations_sent", run.confirmationsSent);

        ObjectNode resources = result.putObject("resources");
        putNullable(resources, "engine_cpu_peak", maxOf(run.samples, "engine_cpu"));
        putNullable(resources, "system_cpu_peak", maxOf(run.samples, "system_cpu"));
        resources.put("heap_used_peak_mb", decimals(maxOf(run.samples, "heap_mb") == null
                ? 0.0 : maxOf(run.samples, "heap_mb"), 1));
        putNullable(resources, "redis_ops_peak", maxOf(run.samples, "redis_ops"));
        putNullable(resources, "redis_memory_mb", redis.memoryMb());
        resources.put("postgres_tx_in_run", pgTx);
        putNullable(resources, "postgres_tx_per_second", runSeconds > 0 ? decimals(pgTx / runSeconds, 1) : null);

        ObjectNode outbox = result.putObject("outbox");
        outbox.put("publish_lag_s", total(afterEngine, "matchmaking_outbox_publish_lag_seconds", Map.of()));
        outbox.put("oldest_pending_s", total(afterEngine, "matchmaking_outbox_oldest_pending_seconds", Map.of()));

        ObjectNode events = result.putObject("events");
        events.put("matches_rows", matchesWritten);
        events.put("events", eventsWritten);
        events.put("distinct_matches", eventsDistinct);
        events.put("duplicate_events", duplicateEvents);

        ArrayNode errors = result.putArray("errors");
        run.errors.subList(0, Math.min(10, run.errors.size())).forEach(errors::add);

        ArrayNode samples = result.putArray("samples");
        for (Sample sample : run.samples) {
            ObjectNode entry = samples.addObject();
            entry.put("t", decimals(sample.t(), 2));
            entry.put("depth", sample.depth());
            entry.put("oldest_wait_s", decimals(sample.oldestWaitS(), 1));
            putNullable(entry, "engine_cpu", sample.engineCpu());
            putNullable(entry, "system_cpu", sample.systemCpu());
            entry.put("heap_mb", sample.heapMb());
            putNullable(entry, "redis_ops", sample.redisOps());
            entry.put("formed", sample.formed());
            entry.put("confirmed", sample.confirmed());
            entry.put("dissolved_timeout", sample.dissolvedTimeout());
            entry.put("outbox_pending", sample.outboxPending());
        }
        return result;
    }

    private static void putNullable(ObjectNode node, String field, Double value) {
        if (value == null || value.isNaN() || value.isInfinite()) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    private static Double maxOf(List<Sample> samples, String field) {
        Double max = null;
        for (Sample sample : samples) {
            Double value = switch (field) {
                case "engine_cpu" -> sample.engineCpu();
                case "system_cpu" -> sample.systemCpu();
                case "heap_mb" -> sample.heapMb();
                case "redis_ops" -> sample.redisOps();
                default -> null;
            };
            if (value != null && (max == null || value > max)) {
                max = value;
            }
        }
        return max;
    }

    // ------------------------------------------------------------------ the run

    /** One sample of the drain window: what the queue holds and what the engine is paying for it. */
    private record Sample(double t, long depth, double oldestWaitS, Double engineCpu, Double systemCpu,
            double heapMb, Double redisOps, double formed, double confirmed, double dissolvedTimeout,
            double outboxPending) {
    }

    private record Drain(long peakDepth, Double seconds) {
    }

    private static final class Run {

        private final int concurrency;
        private final String mode;
        private final String region;
        private final int confirmRetries;
        private final Set<String> playerIds;
        private final Redis redis;
        private final List<String> players;

        final Map<String, String> tokens = new ConcurrentHashMap<>();
        final Map<String, String> tickets = new ConcurrentHashMap<>();
        final List<Double> loginMs = Collections.synchronizedList(new ArrayList<>());
        final List<Double> enqueueMs = Collections.synchronizedList(new ArrayList<>());
        // W17: the arrivals the API refused on purpose. They are not errors — a bounded queue is the
        // system working — but they must be counted, or a run with a cap looks like a run that lost
        // two thirds of its players.
        final List<JsonNode> shed = Collections.synchronizedList(new ArrayList<>());
        final List<Double> shedMs = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger rateLimited = new AtomicInteger();
        final Set<String> confirmedMatches = ConcurrentHashMap.newKeySet();
        final List<Sample> samples = Collections.synchronizedList(new ArrayList<>());
        final List<String> errors = Collections.synchronizedList(new ArrayList<>());
        volatile Long leftover;
        volatile double peakPlayersPerSecond;
        volatile int pendingReceived;
        volatile int confirmationsSent;

        private Subscriber subscriber;
        private Confirmer publisher;

        Run(int concurrency, String mode, String region, int confirmRetries,
                List<String> playerIds, Redis redis) {
            this.concurrency = concurrency;
            this.mode = mode;
            this.region = region;
            this.confirmRetries = confirmRetries;
            this.players = playerIds;
            this.playerIds = new LinkedHashSet<>(playerIds);
            this.redis = redis;
        }

        // -- phase 1: tokens (the auth service is part of the path under test)
        void loginAll() throws Exception {
            eachPlayer(this::login);
        }

        private void login(String playerId) {
            long started = System.nanoTime();
            try {
                Http.Response response = Http.exchange("POST", AUTH + "/v1/auth/login", null,
                        Map.of("playerId", playerId));
                double elapsed = elapsedMillis(started);
                if (response.status() != 200) {
                    errors.add("login " + playerId + ": " + response.status() + " " + Json.write(response.body()));
                    return;
                }
                tokens.put(playerId, response.body().path("token").asText(""));
                loginMs.add(elapsed);
            } catch (IOException | InterruptedException failed) {
                errors.add("login " + playerId + ": " + failed.getMessage());
            }
        }

        // -- phase 2: enqueue
        void enqueueAll() throws Exception {
            eachPlayer(this::enqueue);
        }

        private void enqueue(String playerId) {
            long started = System.nanoTime();
            try {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("gameMode", mode);
                body.put("region", region);
                body.put("maxLatencyMs", 80);
                Http.Response response = Http.exchange("POST", API + "/v1/matchmaking/tickets",
                        tokens.get(playerId), body);
                double elapsed = elapsedMillis(started);
                if (response.status() == 503) {
                    // Shed by the admission cap (W17): the answer is meant to be fast, and the latency
                    // of the refusal is the number that says whether the player is better or worse off.
                    shed.add(response.body());
                    shedMs.add(elapsed);
                    return;
                }
                if (response.status() == 429) {
                    rateLimited.incrementAndGet();
                    return;
                }
                if (response.status() != 201) {
                    errors.add("enqueue " + playerId + ": " + response.status() + " " + Json.write(response.body()));
                    return;
                }
                tickets.put(playerId, response.body().path("ticketId").asText(""));
                enqueueMs.add(elapsed);
            } catch (IOException | InterruptedException failed) {
                errors.add("enqueue " + playerId + ": " + failed.getMessage());
            }
        }

        private void eachPlayer(PlayerTask task) throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(concurrency);
            try {
                for (String player : players) {
                    pool.submit(() -> {
                        try {
                            task.run(player);
                        } catch (Exception failed) {
                            errors.add(player + ": " + failed.getMessage());
                        }
                    });
                }
                pool.shutdown();
                if (!pool.awaitTermination(20, TimeUnit.MINUTES)) {
                    throw new IllegalStateException("the player pool did not finish in 20 minutes");
                }
            } finally {
                pool.shutdownNow();
            }
        }

        @FunctionalInterface
        private interface PlayerTask {
            void run(String playerId) throws Exception;
        }

        // -- the standing-in client: confirm a match as soon as the engine announces it
        //
        // Event-driven on purpose. Polling the pending keys instead cost a spawn (and a keyspace scan)
        // per cycle and could not keep up with a drain of tens of matches per second: matches hit
        // their 30 s window, were dissolved and their players requeued, and the churn hid the number
        // the run existed to measure. The generator was the ceiling, not the engine.
        void startConfirmer() throws IOException {
            publisher = new Confirmer(confirmRetries);
            subscriber = new Subscriber(this::onNotification);
        }

        private void onNotification(String frame) {
            JsonNode routed;
            try {
                routed = Json.parse(frame);
            } catch (IOException notJson) {
                return;
            }
            JsonNode envelope = routed.get("envelope");
            if (envelope == null || !"MATCH_PENDING_CONFIRMATION".equals(envelope.path("type").asText(""))) {
                return;
            }
            JsonNode payload = envelope.get("payload");
            String matchId = payload == null ? "" : payload.path("matchId").asText("");
            String playerId = routed.path("targetPlayerId").asText("");
            if (matchId.isEmpty() || playerId.isEmpty()) {
                return;
            }
            pendingReceived++;
            if (!playerIds.contains(playerId)) {
                return;
            }
            confirmedMatches.add(matchId);
            // A confirmation can be repeated on purpose (--confirm-retries): a client that did not get
            // an answer retries, and the engine must still announce the match once.
            publisher.confirm(matchId, playerId);
            confirmationsSent = publisher.sent();
        }

        void stopConfirmer() {
            if (subscriber != null) {
                subscriber.close();
            }
            if (publisher != null) {
                publisher.close();
            }
        }

        int notificationsReceived() {
            return subscriber == null ? 0 : subscriber.received;
        }

        // -- phase 3: watch the queue drain, sampling what it costs
        /**
         * Samples the queue until it is drained (see {@code floor}), reporting the peak and the cost.
         *
         * <p>{@code floor} is the depth that counts as drained. In a 1v1 queue an odd number of
         * accepted arrivals leaves exactly one player with nobody to pair with, and waiting for a zero
         * that cannot happen would turn a 30 s drain into a 400 s watchdog timeout — and turn the
         * honest drain rate into a meaningless average over the wait.
         */
        Drain watchDrain(double deadlineS, int floor) throws Exception {
            long started = System.nanoTime();
            long peakDepth = 0;
            Double drainS = null;
            double[] previous = null;
            // The peak rate, not the window average: the drain slows as the queue empties (a tick
            // pairs up to a batch, and a shallow queue gives it nothing to pair), so the average over
            // the whole window describes the tail, not the ceiling.
            peakPlayersPerSecond = 0;
            while (elapsedSeconds(started) < deadlineS) {
                long depth = redis.zcard(QUEUE_ZCARD);
                double now = elapsedSeconds(started);
                peakDepth = Math.max(peakDepth, depth);
                if (previous != null && depth < previous[0] && now - previous[1] > 0) {
                    peakPlayersPerSecond = Math.max(peakPlayersPerSecond,
                            (previous[0] - depth) / (now - previous[1]));
                }
                previous = new double[] {depth, now};
                Map<String, List<Scrape.Sample>> engine = scrapeOrEmpty(ENGINE_METRICS);
                samples.add(new Sample(now, depth, headWaitS(),
                        nullableTotal(engine, "process_cpu_usage", Map.of()),
                        nullableTotal(engine, "system_cpu_usage", Map.of()),
                        total(engine, "jvm_memory_used_bytes", Map.of("area", "heap")) / 1024 / 1024,
                        redis.opsPerSecond(),
                        total(engine, "matchmaking_matches_formed_total", Map.of()),
                        total(engine, "matchmaking_matches_confirmed_total", Map.of()),
                        total(engine, "matchmaking_matches_dissolved_total", Map.of("reason", "timeout")),
                        total(engine, "matchmaking_outbox_backlog", Map.of("status", "PENDING"))));
                leftover = depth;
                if (depth <= floor && total(engine, "matchmaking_matches_formed_total", Map.of()) > 0) {
                    drainS = elapsedSeconds(started);
                    break;
                }
                Thread.sleep(1_000);
            }
            return new Drain(peakDepth, drainS);
        }

        /** How long the oldest member of the queue has been waiting, read from the ZSET score. */
        private double headWaitS() throws IOException {
            List<String> parts = redis.zrangeHead(QUEUE_ZCARD);
            if (parts.size() < 2) {
                return 0.0;
            }
            try {
                return Math.max(0.0, decimals(System.currentTimeMillis() / 1000.0
                        - Double.parseDouble(parts.get(1)) / 1000.0, 1));
            } catch (NumberFormatException notANumber) {
                return 0.0;
            }
        }

        // -- phase 4: let the confirmations settle
        void settle(double deadlineS) throws Exception {
            long started = System.nanoTime();
            while (elapsedSeconds(started) < deadlineS) {
                if (redis.keys("match-pending:*").isEmpty()) {
                    return;
                }
                Thread.sleep(500);
            }
        }
    }

    // ------------------------------------------------------------------ the client's two sides

    /**
     * The client's push side: one long-lived connection subscribed to {@code player_notifications},
     * counting every frame and handing the confirmable ones to the run.
     */
    private static final class Subscriber implements Closeable {

        private final Resp resp;
        private final java.util.function.Consumer<String> onMessage;
        private final Thread reader;
        private volatile int received;

        Subscriber(java.util.function.Consumer<String> onMessage) throws IOException {
            this.onMessage = onMessage;
            this.resp = Resp.connect(REDIS_PORT);
            this.resp.command("SUBSCRIBE", "player_notifications");
            this.reader = new Thread(this::pump, "load-notifications");
            this.reader.setDaemon(true);
            this.reader.start();
        }

        private void pump() {
            try {
                while (true) {
                    Object frame = resp.read();
                    // The subscribe ack arrives first; only `message` arrays are frames.
                    if (!(frame instanceof List<?> message) || message.size() < 3) {
                        continue;
                    }
                    if (!"message".equals(message.get(0)) || !(message.get(2) instanceof String payload)) {
                        continue;
                    }
                    received++;
                    onMessage.accept(payload);
                }
            } catch (IOException | RuntimeException closed) {
                // The run is over, or Redis went away: either way the count stops here.
            }
        }

        @Override
        public void close() {
            resp.close();
            try {
                reader.join(1_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** The client's answer side: one connection, one synchronous PUBLISH at a time. */
    private static final class Confirmer implements Closeable {

        private final Resp resp;
        private final int retries;
        private int sent;

        Confirmer(int retries) throws IOException {
            this.retries = retries;
            this.resp = Resp.connect(REDIS_PORT);
        }

        void confirm(String matchId, String playerId) {
            String payload = "{\"matchId\":\"" + matchId + "\",\"playerId\":\"" + playerId
                    + "\",\"action\":\"CONFIRM\"}";
            for (int attempt = 0; attempt < retries; attempt++) {
                try {
                    synchronized (this) {
                        resp.command("PUBLISH", "match_confirmations", payload);
                        resp.read();
                        sent++;
                    }
                } catch (IOException failed) {
                    return;
                }
            }
        }

        int sent() {
            return sent;
        }

        @Override
        public void close() {
            resp.close();
        }
    }

    // ------------------------------------------------------------------ Redis, over a socket

    /**
     * The generator's own Redis client, over a socket — never {@code docker exec redis-cli}: the
     * exec-based subscriber lost notifications and made the measurement a measurement of Docker
     * (see the class comment).
     */
    private static final class Redis implements Closeable {

        private final Resp resp;

        private Redis(Resp resp) {
            this.resp = resp;
        }

        static Redis connect() throws IOException {
            return new Redis(Resp.connect(REDIS_PORT));
        }

        void flushAll() throws IOException {
            resp.flushAll();
        }

        long zcard(String key) throws IOException {
            return resp.zcard(key);
        }

        /** {@code ZRANGE key 0 0 WITHSCORES}: the member and its score, or an empty list. */
        List<String> zrangeHead(String key) throws IOException {
            resp.command("ZRANGE", key, "0", "0", "WITHSCORES");
            Object reply = resp.read();
            List<String> parts = new ArrayList<>();
            if (reply instanceof List<?> items) {
                for (Object item : items) {
                    if (item instanceof String text) {
                        parts.add(text);
                    }
                }
            }
            return parts;
        }

        List<String> keys(String pattern) throws IOException {
            return resp.keys(pattern);
        }

        /** {@code INFO stats} -> {@code instantaneous_ops_per_sec}, or null when it is not there. */
        Double opsPerSecond() throws IOException {
            String info = info("stats");
            for (String line : info.split("\n")) {
                if (line.startsWith("instantaneous_ops_per_sec:")) {
                    try {
                        return Double.parseDouble(line.split(":")[1].trim());
                    } catch (NumberFormatException notANumber) {
                        return null;
                    }
                }
            }
            return null;
        }

        /** {@code INFO memory} -> the used memory in MiB, from the human-readable field. */
        Double memoryMb() throws IOException {
            String info = info("memory");
            for (String line : info.split("\n")) {
                if (line.startsWith("used_memory_human:")) {
                    String raw = line.split(":")[1].trim()
                            .replace("M", "").replace("K", "e-3").replace("B", "");
                    try {
                        return decimals(Double.parseDouble(raw), 2);
                    } catch (NumberFormatException notANumber) {
                        return null;
                    }
                }
            }
            return null;
        }

        private String info(String section) throws IOException {
            resp.command("INFO", section);
            Object reply = resp.read();
            return reply instanceof String text ? text : "";
        }

        @Override
        public void close() {
            resp.close();
        }
    }

    // ------------------------------------------------------------------ the numbers

    /**
     * {@code count} player ids that all share one skill, so the pairing policy can always pair them.
     *
     * <p>The skill is the engine's deterministic function of the player id, so ids are scanned and
     * bucketed: the biggest bucket wins, which keeps the scan bounded whatever the spread. The
     * function is the product's own ({@link DeterministicSkill}), not a second copy — a harness with
     * its own copy pairs nobody and blames the engine.
     */
    private static List<String> playersWithOneSkill(int count, int[] skillOut) {
        int scan = Math.max(20_000, 300 * count);
        Map<Integer, List<String>> buckets = new LinkedHashMap<>();
        for (int i = 1; i <= scan; i++) {
            String playerId = "load-" + i;
            buckets.computeIfAbsent(DeterministicSkill.of(playerId), ignored -> new ArrayList<>()).add(playerId);
        }
        int bestSkill = 0;
        List<String> ids = List.of();
        for (Map.Entry<Integer, List<String>> bucket : buckets.entrySet()) {
            if (bucket.getValue().size() > ids.size()) {
                bestSkill = bucket.getKey();
                ids = bucket.getValue();
            }
        }
        if (ids.size() < count) {
            throw new IllegalStateException("only " + ids.size() + " ids share skill " + bestSkill
                    + " out of " + scan + " scanned");
        }
        skillOut[0] = bestSkill;
        return new ArrayList<>(ids.subList(0, count));
    }

    private static Map<String, List<Scrape.Sample>> scrapeOrEmpty(String url) {
        try {
            return Scrape.samples(url);
        } catch (Exception unreachable) {
            // A missing scrape must not kill the run: the run is the measurement, the scrape is a
            // cost observation on top of it.
            System.out.println("  ! could not scrape " + url + ": " + unreachable.getMessage());
            return Map.of();
        }
    }

    /** Sums every series of a metric matching the label filter; a missing metric is 0. */
    private static double total(Map<String, List<Scrape.Sample>> metrics, String name, Map<String, String> filter) {
        double value = 0;
        for (Scrape.Sample sample : metrics.getOrDefault(name, List.of())) {
            boolean matches = filter.entrySet().stream()
                    .allMatch(entry -> entry.getValue().equals(sample.labels().get(entry.getKey())));
            if (matches) {
                value += sample.value();
            }
        }
        return value;
    }

    /** The same sum, but {@code null} when the series is absent (an absent CPU gauge is not 0 %). */
    private static Double nullableTotal(Map<String, List<Scrape.Sample>> metrics, String name,
            Map<String, String> filter) {
        if (!metrics.containsKey(name)) {
            return null;
        }
        return total(metrics, name, filter);
    }

    private static double delta(Map<String, List<Scrape.Sample>> before, Map<String, List<Scrape.Sample>> after,
            String name, Map<String, String> filter) {
        return total(after, name, filter) - total(before, name, filter);
    }

    private static double delta(Map<String, List<Scrape.Sample>> before, Map<String, List<Scrape.Sample>> after,
            String name) {
        return delta(before, after, name, Map.of());
    }

    /**
     * The quantile of what happened <b>during</b> a run, not of everything since boot.
     *
     * <p>The scrape is cumulative, so the plain quantile mixes this run with every earlier one — and
     * the tail of {@code matchmaking_match_wait_seconds} is exactly where a previous run's dissolved
     * matches sit (a requeued ticket waits its 30 s window again). Subtracting the buckets first makes
     * the number describe the run, which is what an objective is calibrated on.
     */
    private static Double histogramQuantileDelta(Map<String, List<Scrape.Sample>> before,
            Map<String, List<Scrape.Sample>> after, String name, double q) {
        Map<Double, Double> beforeBuckets = bucketCounts(before, name);
        Map<Double, Double> afterBuckets = bucketCounts(after, name);
        List<double[]> buckets = new ArrayList<>();
        for (Map.Entry<Double, Double> bucket : afterBuckets.entrySet()) {
            buckets.add(new double[] {bucket.getKey(),
                    bucket.getValue() - beforeBuckets.getOrDefault(bucket.getKey(), 0.0)});
        }
        buckets.sort((left, right) -> Double.compare(left[0], right[0]));
        return quantileFromBuckets(buckets, q);
    }

    /** {@code le -> cumulative count} of one histogram, {@code +Inf} included as infinity. */
    private static Map<Double, Double> bucketCounts(Map<String, List<Scrape.Sample>> metrics, String name) {
        Map<Double, Double> counts = new LinkedHashMap<>();
        for (Scrape.Sample sample : metrics.getOrDefault(name + "_bucket", List.of())) {
            String le = sample.labels().get("le");
            if (le == null) {
                continue;
            }
            double bound = "+Inf".equals(le) ? Double.POSITIVE_INFINITY : Double.parseDouble(le);
            counts.put(bound, sample.value());
        }
        return counts;
    }

    /** The Prometheus algorithm: interpolate inside the bucket the rank falls in. */
    private static Double quantileFromBuckets(List<double[]> buckets, double q) {
        if (buckets.isEmpty()) {
            return null;
        }
        double totalCount = buckets.get(buckets.size() - 1)[1];
        if (totalCount <= 0) {
            return null;
        }
        double rank = q * totalCount;
        double previousLe = 0;
        double previousCount = 0;
        for (double[] bucket : buckets) {
            double le = bucket[0];
            double count = bucket[1];
            if (count >= rank) {
                if (Double.isInfinite(le) || count == previousCount) {
                    return Double.isInfinite(le) ? previousLe : le;
                }
                return previousLe + (le - previousLe) * (rank - previousCount) / (count - previousCount);
            }
            previousLe = le;
            previousCount = count;
        }
        return previousLe;
    }

    /** The nearest-rank percentile of a sample, the way the Python computed it. */
    private static Double percentile(List<Double> values, double q) {
        if (values.isEmpty()) {
            return null;
        }
        List<Double> ordered = new ArrayList<>(values);
        Collections.sort(ordered);
        int index = Math.min(ordered.size() - 1,
                Math.max(0, (int) Math.ceil(q * ordered.size()) - 1));
        return ordered.get(index);
    }

    private static double max(List<Double> values) {
        return Collections.max(values);
    }

    /**
     * How fast {@code field} moved across the sampling window (works on a partial drain too).
     *
     * <p>Depth goes down as the queue drains; a counter goes up as the work happens. The direction is
     * the caller's to state, or a partial window reports a negative rate.
     */
    private static Double observedRate(List<Sample> samples, String field, boolean decreasing) {
        if (samples.size() < 2) {
            return null;
        }
        Sample first = samples.get(0);
        Sample last = samples.get(samples.size() - 1);
        double elapsed = last.t() - first.t();
        if (elapsed <= 0) {
            return null;
        }
        double start = field(first, field);
        double end = field(last, field);
        if (Double.isNaN(start) || Double.isNaN(end)) {
            return null;
        }
        return (decreasing ? start - end : end - start) / elapsed;
    }

    private static double field(Sample sample, String name) {
        return switch (name) {
            case "depth" -> sample.depth();
            case "formed" -> sample.formed();
            case "confirmed" -> sample.confirmed();
            default -> Double.NaN;
        };
    }

    private static double doubleOf(Args args, String name, double fallback) {
        String value = args.optional(name, null);
        return value == null ? fallback : Double.parseDouble(value);
    }

    private static long number(String text) {
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException notANumber) {
            return 0;
        }
    }

    private static double elapsedSeconds(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }

    private static double elapsedMillis(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000.0;
    }

    /** A nullable number, rounded — {@code null} stays {@code null} instead of becoming a zero. */
    private static Double rounded(Double value, int decimalPlaces) {
        return value == null ? null : decimals(value, decimalPlaces);
    }

    /**
     * Python's {@code round(value, decimals)}: half <b>even</b>, so 0.625 becomes 0.62 and not 0.63.
     *
     * <p>A step-based version ({@code Math.round(value * 100) / 100}) rounds half up and would make
     * a result file differ from the one it is compared against on a value nobody cares about but
     * everybody diffs.
     */
    private static double decimals(double value, int decimalPlaces) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return value;
        }
        return BigDecimal.valueOf(value)
                .setScale(decimalPlaces, java.math.RoundingMode.HALF_EVEN)
                .doubleValue();
    }

    private static String oneDecimal(double value) {
        return String.format(Locale.ROOT, "%.1f", value);
    }

    private static String zeroDecimals(double value) {
        return String.format(Locale.ROOT, "%.0f", value);
    }

    private static String twoDecimals(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }
}
