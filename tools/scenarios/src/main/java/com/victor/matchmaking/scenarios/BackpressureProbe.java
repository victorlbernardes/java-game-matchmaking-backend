package com.victor.matchmaking.scenarios;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The W17 client: does the API stop admitting when the queue is full, and does the engine stop
 * asking Postgres once it is behind (ADR 0022/0023)?
 *
 * <p>Two commands, two questions:
 *
 * <ul>
 *   <li>{@code fill} drives real arrivals until the API answers {@code 503} with the capacity in
 *       the body — the honest way to see a cap is to reach it — and reports what it was told;
 *   <li>{@code db} drives a burst of confirmations against a deliberately slow Postgres (the
 *       scenario installs a sleeping trigger) and measures what the database was asked to hold:
 *       the sessions it ran at once, the engine's gauge, and whether every match landed.
 * </ul>
 *
 * <p>Both print one JSON summary on stdout, because the scenario asserts on the summary; progress
 * goes to stderr, where the shell prefixes it.
 */
final class BackpressureProbe {

    private static final String DEFAULT_API = "http://localhost:8080";
    private static final String DEFAULT_AUTH = "http://localhost:8081";
    private static final String DEFAULT_ENGINE_METRICS = "http://localhost:9091/metrics";
    private static final int DEFAULT_REDIS_PORT = 6379;

    private BackpressureProbe() {
    }

    static int dispatch(String[] argv) throws Exception {
        if (argv.length == 0) {
            throw new IllegalArgumentException("backpressure <fill|db> ...");
        }
        String command = argv[0];
        Args args = Args.of(java.util.Arrays.copyOfRange(argv, 1, argv.length));
        return switch (command) {
            case "fill" -> fill(args);
            case "db" -> database(args);
            default -> throw new IllegalArgumentException("unknown backpressure command: " + command);
        };
    }

    /**
     * {@code fill}: arrivals until the cap refuses, and what the refusal said.
     *
     * <p>The idempotency key is part of it: the same arrival sent again must answer with the ticket
     * the player already has, even with the queue full — a refusal of a replay would lose a ticket
     * that exists.
     */
    private static int fill(Args args) throws Exception {
        String api = args.optional("api", System.getenv().getOrDefault("API", DEFAULT_API));
        String auth = args.optional("auth", System.getenv().getOrDefault("AUTH", DEFAULT_AUTH));
        int players = args.intOf("players", 1100);
        int concurrency = args.intOf("concurrency", 32);
        String label = args.optional("label", "cap");

        String base = label + "-" + Instant.now().getEpochSecond();
        List<String> ids = new ArrayList<>(players);
        for (int i = 0; i < players; i++) {
            ids.add(base + "-" + i);
        }

        System.err.println("  " + players + " players: logging in");
        Map<String, String> tokens = new ConcurrentHashMap<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(concurrency)) {
            List<Future<?>> logins = new ArrayList<>(players);
            for (String playerId : ids) {
                logins.add(pool.submit(() -> {
                    Http.Response login = Http.exchange("POST", auth + "/v1/auth/login", null,
                            Map.of("playerId", playerId, "displayName", playerId));
                    if (login.status() != 200 && login.status() != 201) {
                        throw new IllegalStateException("login of " + playerId + " answered " + login.status());
                    }
                    tokens.put(playerId, login.body().path("token").asText());
                    return null;
                }));
            }
            for (Future<?> login : logins) {
                login.get();
            }

            System.err.println("  " + players + " players: enqueueing");
            long started = System.nanoTime();
            List<Http.Response> results = new ArrayList<>(players);
            List<Future<Http.Response>> arrivals = new ArrayList<>(players);
            for (String playerId : ids) {
                arrivals.add(pool.submit(() -> enqueue(api, tokens.get(playerId), playerId, base + "-idem-" + playerId)));
            }
            for (Future<Http.Response> arrival : arrivals) {
                results.add(arrival.get());
            }
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;

            int accepted = 0;
            int shed = 0;
            int other = 0;
            Map<String, Integer> statuses = new LinkedHashMap<>();
            List<Map<String, Object>> otherExamples = new ArrayList<>();
            Http.Response firstShed = null;
            int firstAcceptedIndex = -1;
            for (int i = 0; i < results.size(); i++) {
                Http.Response response = results.get(i);
                statuses.merge(Integer.toString(response.status()), 1, Integer::sum);
                if (response.status() == 200 || response.status() == 201) {
                    accepted++;
                    if (firstAcceptedIndex < 0) {
                        firstAcceptedIndex = i;
                    }
                } else if (response.status() == 503) {
                    shed++;
                    if (firstShed == null) {
                        firstShed = response;
                    }
                } else {
                    other++;
                    if (otherExamples.size() < 3) {
                        otherExamples.add(Map.of("status", response.status(),
                                "body", abbreviate(response.body().toString())));
                    }
                }
            }

            JsonNode shedBody = firstShed == null ? Json.object() : firstShed.body();
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("label", base);
            summary.put("players", players);
            summary.put("elapsed_seconds", round(elapsed, 2));
            summary.put("accepted", accepted);
            summary.put("shed", shed);
            summary.put("other", other);
            summary.put("statuses", statuses);
            summary.put("other_examples", otherExamples);
            summary.put("capacity", shedBody.path("capacity").isMissingNode() ? null : shedBody.path("capacity").asInt());
            summary.put("queue", shedBody.path("queue").asText(null));
            summary.put("depth", shedBody.path("depth").isMissingNode() ? null : shedBody.path("depth").asInt());
            summary.put("retry_after", firstShed == null ? "" : firstShed.header("Retry-After"));
            Map<String, Object> shedSummary = new LinkedHashMap<>();
            for (String field : List.of("error", "queue", "depth", "capacity", "retryAfterSeconds")) {
                JsonNode value = shedBody.get(field);
                if (value != null && !value.isNull()) {
                    shedSummary.put(field, value.isTextual() ? value.asText() : value.asInt());
                }
            }
            summary.put("shed_body", shedSummary);

            if (firstAcceptedIndex >= 0) {
                String playerId = ids.get(firstAcceptedIndex);
                Http.Response replay = enqueue(api, tokens.get(playerId), playerId, base + "-idem-" + playerId);
                summary.put("replay_status", replay.status());
                summary.put("replay_ticket", replay.body().path("ticketId").asText(""));
            }

            System.out.println(Json.write(summary));
            if (shed == 0) {
                System.err.println("  nothing was shed: the queue cap was never reached");
                return 1;
            }
            return 0;
        }
    }

    /**
     * {@code db}: a burst of confirmations while Postgres is slow, measuring what it was asked to
     * hold.
     *
     * <p>The ground truth is the database's own view ({@code pg_stat_activity}), not the gate's
     * promise: the probe counts the sessions actually running the insert and compares them with the
     * configured limit and with the engine's gauge. The probe excludes itself, because its own
     * statement contains the text it searches for.
     */
    private static int database(Args args) throws Exception {
        String api = args.optional("api", System.getenv().getOrDefault("API", DEFAULT_API));
        String auth = args.optional("auth", System.getenv().getOrDefault("AUTH", DEFAULT_AUTH));
        String metrics = args.optional("metrics",
                System.getenv().getOrDefault("ENGINE_METRICS", DEFAULT_ENGINE_METRICS));
        int redisPort = args.intOf("redis-port", DEFAULT_REDIS_PORT);
        int players = args.intOf("players", 24);
        int minMatches = args.intOf("min-matches", 12);
        double timeoutSeconds = Numbers.parse(args.optional("timeout", "120"));
        String label = args.optional("label", "w17db");

        Pairing.Group group = Pairing.playersWithOneSkill(players, label + "-");
        List<String> ids = group.players();
        String base = label + "-" + Instant.now().getEpochSecond();
        System.err.println("  " + players + " players share skill " + group.skill());

        Map<String, String> tokens = new LinkedHashMap<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            List<Future<String>> logins = new ArrayList<>(ids.size());
            for (String playerId : ids) {
                logins.add(pool.submit(() -> {
                    Http.Response login = Http.exchange("POST", auth + "/v1/auth/login", null,
                            Map.of("playerId", playerId, "displayName", playerId));
                    String token = login.body().path("token").asText("");
                    if (login.status() != 200 && login.status() != 201 || token.isEmpty()) {
                        throw new IllegalStateException("login of " + playerId + " answered " + login.status());
                    }
                    return token;
                }));
            }
            for (int i = 0; i < logins.size(); i++) {
                tokens.put(ids.get(i), logins.get(i).get());
            }

            List<Future<Http.Response>> arrivals = new ArrayList<>(ids.size());
            for (String playerId : ids) {
                arrivals.add(pool.submit(() -> Http.exchange("POST", api + "/v1/matchmaking/tickets",
                        tokens.get(playerId), Map.of("gameMode", "1v1", "region", "NA", "maxLatencyMs", 80))));
            }
            for (int i = 0; i < arrivals.size(); i++) {
                Http.Response arrival = arrivals.get(i).get();
                if (arrival.status() != 200 && arrival.status() != 201) {
                    throw new IllegalStateException("enqueue of " + ids.get(i) + " answered " + arrival.status()
                            + ": " + arrival.body());
                }
            }
        }
        System.err.println("  " + players + " players enqueued");

        Instant deadline = Instant.now().plusMillis((long) (timeoutSeconds * 1000));
        Map<String, List<String>> matches = Map.of();
        try (Resp redis = Resp.connect(redisPort)) {
            while (Instant.now().isBefore(deadline) && matches.size() < minMatches) {
                matches = Pending.forPlayers(redis, ids);
                if (matches.size() < minMatches) {
                    Thread.sleep(200);
                }
            }
        }
        System.err.println("  the engine formed " + matches.size() + " matches of the " + players / 2 + " possible");
        if (matches.size() < minMatches) {
            System.out.println(Json.write(Map.of("error", "not enough matches",
                    "matches", matches.size(), "wanted", minMatches)));
            return 1;
        }

        Sampler sampler = new Sampler(metrics);
        sampler.start();

        int sent = 0;
        long started = System.nanoTime();
        try (Resp.Confirmer confirmer = new Resp.Confirmer(redisPort)) {
            for (Map.Entry<String, List<String>> match : matches.entrySet()) {
                for (String playerId : match.getValue()) {
                    confirmer.confirm(match.getKey(), playerId);
                    sent++;
                }
            }
        }
        System.err.println("  " + sent + " confirmations published in "
                + String.format(java.util.Locale.ROOT, "%.2f", (System.nanoTime() - started) / 1_000_000_000.0) + "s");

        int written = 0;
        try (Postgres postgres = Postgres.connect()) {
            List<String> matchIds = new ArrayList<>(matches.keySet());
            while (Instant.now().isBefore(deadline) && written < matches.size()) {
                written = rowsPresent(postgres, matchIds);
                if (written < matches.size()) {
                    Thread.sleep(200);
                }
            }
            double burstSeconds = (System.nanoTime() - started) / 1_000_000_000.0;
            Thread.sleep(500);
            sampler.halt();

            double deferredAfter = deferred(metrics);
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("label", base);
            summary.put("players", players);
            summary.put("matches", matches.size());
            summary.put("confirmations", sent);
            summary.put("rows_written", written);
            summary.put("max_active_inserts", sampler.maxActiveInserts.get());
            summary.put("max_db_writes_in_flight", sampler.maxInFlight.get());
            summary.put("samples", sampler.samples.get());
            summary.put("deferred_before", (int) sampler.deferredBefore);
            summary.put("deferred_after", (int) deferredAfter);
            summary.put("deferred_delta", (int) (deferredAfter - sampler.deferredBefore));
            summary.put("burst_seconds", round(burstSeconds, 2));
            cleanUp(postgres, matchIds);
            summary.put("rows_after_cleanup", rowsPresent(postgres, matchIds));
            System.out.println(Json.write(summary));
            return written == matches.size() ? 0 : 1;
        }
    }

    /** Watches the database and the engine's gauge while the burst is being written. */
    private static final class Sampler extends Thread {

        private final String metrics;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicInteger maxActiveInserts = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private final AtomicInteger samples = new AtomicInteger();
        private double deferredBefore;

        Sampler(String metrics) throws Exception {
            super("probe-db-sampler");
            setDaemon(true);
            this.metrics = metrics;
            this.deferredBefore = deferred(metrics);
        }

        @Override
        public void run() {
            try (Postgres postgres = Postgres.connect()) {
                while (running.get()) {
                    try {
                        maxActiveInserts.accumulateAndGet(activeInserts(postgres), Math::max);
                        Map<String, List<Scrape.Sample>> scrape = Scrape.samples(metrics);
                        double inFlight = 0;
                        for (Scrape.Sample sample : scrape.getOrDefault("matchmaking_db_writes_in_flight", List.of())) {
                            inFlight += sample.value();
                        }
                        maxInFlight.accumulateAndGet((int) inFlight, Math::max);
                        samples.incrementAndGet();
                    } catch (Exception ignored) {
                        // Sampling must never break the run: a failed sample is a missing sample.
                    }
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } catch (Exception noDatabase) {
                // The run reports the absence of samples as 0, exactly like a sampler that never ran.
            }
        }

        void halt() throws InterruptedException {
            running.set(false);
            join(5_000);
        }
    }

    /**
     * Sessions Postgres is running right now whose statement is the match insert.
     *
     * <p>It excludes itself: the probe's own statement contains the text it searches for, and
     * counting itself would make the measurement read one higher than reality.
     */
    private static int activeInserts(Postgres postgres) throws Exception {
        return postgres.count("SELECT count(*) FROM pg_stat_activity "
                + "WHERE pid <> pg_backend_pid() AND state = 'active' "
                + "AND query ILIKE '%insert into matches%'");
    }

    private static int rowsPresent(Postgres postgres, List<String> matchIds) throws Exception {
        if (matchIds.isEmpty()) {
            return 0;
        }
        return postgres.count("SELECT count(*) FROM matches WHERE id IN (" + quoted(matchIds) + ")");
    }

    /**
     * Removes this run's rows: the test data must not outlive the test.
     *
     * <p>Order matters: {@code match_players} and {@code match_allocations} reference the match,
     * so deleting the match first would be refused; the outbox is keyed by the match id.
     */
    private static void cleanUp(Postgres postgres, List<String> matchIds) throws Exception {
        if (matchIds.isEmpty()) {
            return;
        }
        String listed = quoted(matchIds);
        postgres.execute("DELETE FROM match_players WHERE match_id IN (" + listed + ")");
        postgres.execute("DELETE FROM match_allocations WHERE match_id IN (" + listed + ")");
        postgres.execute("DELETE FROM outbox_events WHERE partition_key IN (" + listed + ")");
        postgres.execute("DELETE FROM matches WHERE id IN (" + listed + ")");
    }

    private static String quoted(List<String> values) {
        StringBuilder listed = new StringBuilder();
        for (String value : values) {
            if (!listed.isEmpty()) {
                listed.append(',');
            }
            listed.append('\'').append(value.replace("'", "")).append('\'');
        }
        return listed.toString();
    }

    private static double deferred(String metrics) throws Exception {
        double total = 0;
        for (Scrape.Sample sample : Scrape.samples(metrics).getOrDefault("matchmaking_db_write_deferred_total", List.of())) {
            total += sample.value();
        }
        return total;
    }

    private static Http.Response enqueue(String api, String token, String playerId, String idempotencyKey)
            throws Exception {
        return Http.exchange("POST", api + "/v1/matchmaking/tickets", token,
                Map.of("gameMode", "1v1", "region", "NA", "maxLatencyMs", 80),
                Map.of("Idempotency-Key", idempotencyKey));
    }

    private static String abbreviate(String value) {
        return value.length() <= 200 ? value : value.substring(0, 200);
    }

    private static double round(double value, int decimals) {
        double factor = Math.pow(10, decimals);
        return Math.round(value * factor) / factor;
    }
}
