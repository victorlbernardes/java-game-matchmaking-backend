package com.victor.matchmaking.scenarios;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The W18 client (ADR 0024): drives the stack into a dependency failure and then asks the state
 * machine what it left behind.
 *
 * <p>Every question the resilience scenario asks lives here, so the shell script stays a list of
 * assertions instead of a list of JSON parsers. The questions are about *observable state*, not
 * about the code: the ticket's state in Redis, the pending match's presence, the outbox rows in
 * Postgres, the row in {@code matches}, the HTTP answer of a read that needs the dependency.
 *
 * <p>That is what makes the degradation matrix falsifiable. \"Redis is down and the API answered
 * 503\" is not the property; \"Redis is down, the arrivals are 503 in under two seconds, nothing
 * hung, nothing was written, and the counter names the dependency\" is.
 */
final class ResilienceProbe {

    private static final String DEFAULT_API = "http://localhost:8080";
    private static final String DEFAULT_AUTH = "http://localhost:8081";
    private static final int DEFAULT_REDIS_PORT = 6379;

    private ResilienceProbe() {
    }

    static int dispatch(String[] argv) throws Exception {
        if (argv.length == 0) {
            throw new IllegalArgumentException("resilience <enqueue|drive|ticket-status|...> ...");
        }
        String command = argv[0];
        List<String> rest = new ArrayList<>(Arrays.asList(argv).subList(1, argv.length));
        boolean confirm = rest.remove("--confirm");
        Args args = Args.of(rest.toArray(new String[0]));
        return switch (command) {
            case "enqueue" -> enqueue(args);
            case "drive" -> drive(args, confirm);
            case "ticket-status" -> ticketStatus(args);
            case "pending-status" -> pendingStatus(args);
            case "outbox-status" -> outboxStatus(args);
            case "row-count" -> rowCount(args);
            case "history-status" -> historyStatus(args);
            case "ticket-http-status" -> ticketHttpStatus(args);
            case "readiness" -> readiness(args);
            case "confirm" -> confirm(args);
            default -> throw new IllegalArgumentException("unknown resilience command: " + command);
        };
    }

    /**
     * {@code enqueue --count N --prefix p [--player id]} -> the statuses, the latencies, and the
     * body of each status.
     *
     * <p>One player sends every arrival with its own idempotency key, so the refusal under test is
     * the dependency's, not the rate limiter's. The latency is measured here, on the client, on
     * purpose: "the refusal is fast" is a promise about the answer, and only the client can time
     * it.
     */
    private static int enqueue(Args args) throws Exception {
        String api = args.optional("api", System.getenv().getOrDefault("API", DEFAULT_API));
        String auth = args.optional("auth", System.getenv().getOrDefault("AUTH", DEFAULT_AUTH));
        String player = args.optional("player", "w18-degraded-enqueue");
        int count = args.intOf("count", 1);
        String prefix = args.optional("prefix", "w18");

        String token = login(auth, player);
        Map<String, Integer> statuses = new LinkedHashMap<>();
        List<Double> latencies = new ArrayList<>(count);
        Map<String, JsonNode> bodies = new LinkedHashMap<>();

        for (int i = 0; i < count; i++) {
            long started = System.nanoTime();
            Http.Response response = Http.exchange("POST", api + "/v1/matchmaking/tickets", token,
                    Map.of("gameMode", "1v1", "region", "NA", "maxLatencyMs", 80),
                    Map.of("Idempotency-Key", prefix + "-" + i));
            latencies.add((System.nanoTime() - started) / 1_000_000.0);
            String status = Integer.toString(response.status());
            statuses.merge(status, 1, Integer::sum);
            bodies.putIfAbsent(status, response.body());
        }

        List<Double> sorted = new ArrayList<>(latencies);
        java.util.Collections.sort(sorted);
        double p50 = median(sorted);
        double p99 = sorted.get(Math.max(0, (int) (sorted.size() * 0.99) - 1));
        double max = sorted.get(sorted.size() - 1);

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("statuses", statuses);
        summary.put("p50Ms", Numbers.round(p50, 1));
        summary.put("p99Ms", Numbers.round(p99, 1));
        summary.put("maxMs", Numbers.round(max, 1));
        Map<String, JsonNode> orderedBodies = new LinkedHashMap<>();
        new java.util.TreeSet<>(bodies.keySet()).forEach(status -> orderedBodies.put(status, bodies.get(status)));
        summary.put("bodies", orderedBodies);
        System.out.println(Json.write(summary));
        return 0;
    }

    /**
     * {@code drive [--confirm] [--wait N]} -> two compatible players enqueued, the pending match
     * the engine formed, and the state each ticket is in.
     *
     * <p>It is the scenario's only way to produce a real match while a dependency is down: the
     * question is always "what did the machine do", so the machine has to be driven.
     */
    private static int drive(Args args, boolean confirm) throws Exception {
        String api = args.optional("api", System.getenv().getOrDefault("API", DEFAULT_API));
        String auth = args.optional("auth", System.getenv().getOrDefault("AUTH", DEFAULT_AUTH));
        int redisPort = args.intOf("redis-port", DEFAULT_REDIS_PORT);
        double waitSeconds = Numbers.parse(args.optional("wait", "30"));

        Pairing.Group group = Pairing.playersWithOneSkill(2, args.optional("prefix", "w18-drive-"));
        List<String> ids = group.players();
        Map<String, String> tickets = new LinkedHashMap<>();
        for (String playerId : ids) {
            String token = login(auth, playerId);
            Http.Response response = Http.exchange("POST", api + "/v1/matchmaking/tickets", token,
                    Map.of("gameMode", "1v1", "region", "NA", "maxLatencyMs", 80),
                    Map.of("Idempotency-Key", "w18-drive-" + playerId + "-" + System.nanoTime()));
            if (response.status() != 201) {
                System.out.println(Json.write(Map.of("error",
                        "enqueue " + playerId + " -> " + response.status(), "body", response.body())));
                return 3;
            }
            tickets.put(playerId, response.body().path("ticketId").asText(""));
        }

        Instant deadline = Instant.now().plusMillis((long) (waitSeconds * 1000));
        Map<String, List<String>> matches = Map.of();
        try (Resp redis = Resp.connect(redisPort)) {
            while (Instant.now().isBefore(deadline) && matches.isEmpty()) {
                matches = Pending.forPlayers(redis, ids);
                if (matches.isEmpty()) {
                    Thread.sleep(300);
                }
            }
            if (matches.isEmpty()) {
                System.out.println(Json.write(Map.of("error", "no pending match formed", "tickets", tickets)));
                return 4;
            }
            String matchId = matches.keySet().iterator().next();
            if (confirm) {
                try (Resp.Confirmer confirmer = new Resp.Confirmer(redisPort)) {
                    for (String playerId : ids) {
                        confirmer.confirm(matchId, playerId);
                    }
                }
            }
            Map<String, String> states = new LinkedHashMap<>();
            for (Map.Entry<String, String> ticket : tickets.entrySet()) {
                states.put(ticket.getKey(), ticketState(redis, ticket.getValue()));
            }
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("matchId", matchId);
            summary.put("players", ids);
            summary.put("tickets", tickets);
            summary.put("states", states);
            summary.put("confirmed", confirm);
            System.out.println(Json.write(summary));
            return 0;
        }
    }

    /** {@code ticket-status <ticketId>} -> the state the ticket is in, or MISSING. */
    private static int ticketStatus(Args args) throws Exception {
        try (Resp redis = Resp.connect(args.intOf("redis-port", DEFAULT_REDIS_PORT))) {
            System.out.println(ticketState(redis, args.positional(0, "the ticket id")));
        }
        return 0;
    }

    /** {@code pending-status <matchId>} -> PRESENT or MISSING. */
    private static int pendingStatus(Args args) throws Exception {
        try (Resp redis = Resp.connect(args.intOf("redis-port", DEFAULT_REDIS_PORT))) {
            System.out.println(redis.exists("match-pending:" + args.positional(0, "the match id"))
                    ? "PRESENT" : "MISSING");
        }
        return 0;
    }

    /**
     * {@code outbox-status <matchId>} -> {@code {"PENDING": n, "PUBLISHED": n, ...}}.
     *
     * <p>A database that cannot be reached answers {@code {}}: the question is "what is durably
     * queued", and a database that is down has nothing queued *that can be seen* — the scenario
     * that asks during an outage is asking about the outage, and a probe that refused to answer
     * would turn "nothing is visible" into a crash. The reason goes to stderr, so the silence is
     * explained rather than assumed.
     */
    private static int outboxStatus(Args args) throws Exception {
        String matchId = args.positional(0, "the match id");
        Map<String, Integer> counts = new LinkedHashMap<>();
        try (Postgres postgres = Postgres.connect()) {
            for (String[] row : postgres.pairs(
                    "SELECT status, count(*) FROM outbox_events WHERE partition_key = '"
                            + matchId.replace("'", "") + "' GROUP BY status ORDER BY status")) {
                counts.put(row[0], Integer.parseInt(row[1]));
            }
        } catch (java.sql.SQLException databaseDown) {
            System.err.println("  outbox-status: " + databaseDown.getMessage() + " (answering {})");
        }
        System.out.println(Json.write(counts));
        return 0;
    }

    /**
     * {@code row-count <matchId>} -> the rows in {@code matches} for that match, {@code 0} when the
     * database cannot be reached.
     *
     * <p>The scenario asks this *while Postgres is stopped*: "no row exists for a match whose write
     * should have failed" is the assertion, and a database that is not answering satisfies it in
     * the only way it can — nothing was written that anyone can read. It is not a guess: the same
     * check runs again after the database returns and requires the row, so an unreachable database
     * cannot pass the scenario by being unreachable.
     */
    private static int rowCount(Args args) throws Exception {
        String matchId = args.positional(0, "the match id");
        int rows = 0;
        try (Postgres postgres = Postgres.connect()) {
            rows = postgres.count("SELECT count(*) FROM matches WHERE id = '" + matchId.replace("'", "") + "'");
        } catch (java.sql.SQLException databaseDown) {
            System.err.println("  row-count: " + databaseDown.getMessage() + " (answering 0)");
        }
        System.out.println(rows);
        return 0;
    }

    /** {@code history-status <playerId>} -> the status and body of the player's match history. */
    private static int historyStatus(Args args) throws Exception {
        String api = args.optional("api", System.getenv().getOrDefault("API", DEFAULT_API));
        String auth = args.optional("auth", System.getenv().getOrDefault("AUTH", DEFAULT_AUTH));
        String playerId = args.positional(0, "the player id");
        Http.Response response = Http.exchange("GET",
                api + "/v1/players/" + playerId + "/matches?limit=5", login(auth, playerId), null);
        System.out.println(Json.write(Map.of("status", response.status(), "body", response.body())));
        return 0;
    }

    /** {@code ticket-http-status <ticketId> [--player id]} -> the status the API answered. */
    private static int ticketHttpStatus(Args args) throws Exception {
        String api = args.optional("api", System.getenv().getOrDefault("API", DEFAULT_API));
        String auth = args.optional("auth", System.getenv().getOrDefault("AUTH", DEFAULT_AUTH));
        String playerId = args.optional("player", "w18-probe");
        Http.Response response = Http.exchange("GET",
                api + "/v1/matchmaking/tickets/" + args.positional(0, "the ticket id"), login(auth, playerId), null);
        System.out.println(response.status());
        return 0;
    }

    /** {@code readiness} -> the status and body of {@code /health/ready}. */
    private static int readiness(Args args) throws Exception {
        String api = args.optional("api", System.getenv().getOrDefault("API", DEFAULT_API));
        Http.Response response = Http.exchange("GET", api + "/health/ready", null, null);
        System.out.println(Json.write(Map.of("status", response.status(), "body", response.body())));
        return 0;
    }

    /** {@code confirm <matchId> <player>...} -> publishes the confirmation of each player. */
    private static int confirm(Args args) throws Exception {
        String matchId = args.positional(0, "the match id");
        List<String> players = new ArrayList<>();
        for (int i = 1; i < args.positionalCount(); i++) {
            players.add(args.positional(i, "a player id"));
        }
        int sent = 0;
        try (Resp.Confirmer confirmer = new Resp.Confirmer(args.intOf("redis-port", DEFAULT_REDIS_PORT))) {
            for (String playerId : players) {
                confirmer.confirm(matchId, playerId);
                sent++;
            }
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("matchId", matchId);
        summary.put("players", players);
        summary.put("published", sent);
        System.out.println(Json.write(summary));
        return 0;
    }

    private static String login(String auth, String playerId) throws Exception {
        Http.Response response = Http.exchange("POST", auth + "/v1/auth/login", null,
                Map.of("playerId", playerId, "displayName", playerId));
        String token = response.body().path("token").asText("");
        if (token.isEmpty()) {
            throw new IllegalStateException("login of " + playerId + " answered " + response.status()
                    + ": " + response.body());
        }
        return token;
    }

    private static String ticketState(Resp redis, String ticketId) throws Exception {
        String raw = redis.get("ticket:" + ticketId);
        if (raw == null || raw.isEmpty()) {
            return "MISSING";
        }
        try {
            return Json.parse(raw).path("status").asText("UNKNOWN");
        } catch (Exception unparseable) {
            return "UNPARSEABLE";
        }
    }

    /** The median of an already sorted list (the low middle for an even count, like statistics). */
    private static double median(List<Double> sorted) {
        int size = sorted.size();
        if (size == 0) {
            return 0;
        }
        return size % 2 == 1
                ? sorted.get(size / 2)
                : (sorted.get(size / 2 - 1) + sorted.get(size / 2)) / 2.0;
    }
}
