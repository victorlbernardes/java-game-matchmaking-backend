package com.victor.matchmaking.scenarios;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The observability helpers: which match the engine formed for a given pair, and the SLO ratios of
 * a window read from the scrape.
 *
 * <p>Both were a Python file because bash cannot do them: one has to read JSON out of Redis, the
 * other has to parse a Prometheus exposition correctly. The parse is where the subtleties live —
 * the {@code le="10"} bucket is a prefix of {@code le="100"}, so a substring match sums the wrong
 * buckets — and it is the reason the objective is asserted against a number instead of against a
 * grep.
 */
final class ObservabilityProbe {

    /** The SL-1 boundary: the edge the rule's {@code le} bucket counts. */
    private static final double LATENCY_BUCKET_LE = 10.0;

    private ObservabilityProbe() {
    }

    static int dispatch(String[] argv) throws Exception {
        if (argv.length == 0) {
            throw new IllegalArgumentException("observability <pending-match|slo> ...");
        }
        String command = argv[0];
        Args args = Args.of(java.util.Arrays.copyOfRange(argv, 1, argv.length));
        switch (command) {
            case "pending-match" -> {
                return pendingMatch(args);
            }
            case "slo" -> {
                return slo(args);
            }
            case "prometheus-up" -> {
                return prometheusUp(args);
            }
            case "prometheus-query" -> {
                return prometheusQuery(args);
            }
            case "grafana-health" -> {
                return grafanaHealth(args);
            }
            case "grafana-dashboard" -> {
                return grafanaDashboard(args);
            }
            case "grafana-value" -> {
                return grafanaValue(args);
            }
            case "prometheus-series" -> {
                return prometheusSeries(args);
            }
            case "rules" -> {
                return rules(args);
            }
            case "alert-states" -> {
                return alertStates(args);
            }
            case "alert-query" -> {
                return alertQuery(args);
            }
            default -> throw new IllegalArgumentException("unknown observability command: " + command);
        }
    }

    /**
     * {@code pending-match <playerA> <playerB> [--redis-port N]} -> the match id, nothing when no
     * pending match holds both, and exit 1 in that case.
     *
     * <p>Only the match with *our* two players answers: a pending match left over from another run
     * must not be picked up and confirmed by this one.
     */
    private static int pendingMatch(Args args) throws Exception {
        String playerA = args.positional(0, "player A");
        String playerB = args.positional(1, "player B");
        int redisPort = args.intOf("redis-port", 6379);
        try (Resp redis = Resp.connect(redisPort)) {
            for (String key : redis.keys("match-pending:*")) {
                String raw = redis.get(key);
                if (raw == null || raw.isEmpty()) {
                    continue;
                }
                JsonNode pending = Json.parse(raw);
                boolean hasA = false;
                boolean hasB = false;
                for (JsonNode player : pending.path("players")) {
                    String playerId = player.path("playerId").asText("");
                    hasA |= playerId.equals(playerA);
                    hasB |= playerId.equals(playerB);
                }
                if (hasA && hasB) {
                    System.out.println(pending.path("matchId").asText(""));
                    return 0;
                }
            }
        }
        return 1;
    }

    /** {@code slo read <url>} and {@code slo window <before.json> <after.json>}. */
    private static int slo(Args args) throws Exception {
        String command = args.positional(0, "read|window");
        if ("read".equals(command)) {
            System.out.println(Json.write(read(args.positional(1, "the scrape URL"))));
            return 0;
        }
        if ("window".equals(command)) {
            JsonNode before = Json.parse(java.nio.file.Files.readString(java.nio.file.Path.of(args.positional(1, "before"))));
            JsonNode after = Json.parse(java.nio.file.Files.readString(java.nio.file.Path.of(args.positional(2, "after"))));
            for (Map.Entry<String, Double> entry : window(before, after).entrySet()) {
                System.out.println(entry.getKey() + "="
                        + (entry.getValue() == null ? "n/a" : Json.pythonDouble(entry.getValue())));
            }
            return 0;
        }
        throw new IllegalArgumentException("unknown slo command: " + command);
    }

    /**
     * {@code prometheus-up <base>} -> {@code job=value} for every target, sorted, or
     * {@code NO TARGETS}.
     *
     * <p>It asks Prometheus, not the services: a scrape that the services answer and Prometheus
     * does not collect is the failure this check exists for.
     */
    private static int prometheusUp(Args args) throws Exception {
        JsonNode result = Json.parse(Http.text(args.positional(0, "the Prometheus base URL") + "/api/v1/query?query=up"))
                .path("data").path("result");
        List<String> targets = new java.util.ArrayList<>();
        for (JsonNode item : result) {
            targets.add(item.path("metric").path("job").asText("?") + "=" + item.path("value").path(1).asText(""));
        }
        java.util.Collections.sort(targets);
        System.out.println(targets.isEmpty() ? "NO TARGETS" : String.join(",", targets));
        return 0;
    }

    /**
     * {@code prometheus-series <base> <expr>} -> how many series the expression answers with.
     *
     * <p>Zero and "no such series" are different answers, and a rule that answers nothing would
     * pass every "is it quiet?" check while measuring nothing.
     */
    private static int prometheusSeries(Args args) throws Exception {
        String url = args.positional(0, "the Prometheus base URL") + "/api/v1/query?query="
                + java.net.URLEncoder.encode(args.positional(1, "the expression"), java.nio.charset.StandardCharsets.UTF_8);
        JsonNode result = Json.parse(Http.text(url)).path("data").path("result");
        System.out.println(result.isArray() ? result.size() : 0);
        return 0;
    }

    /**
     * {@code rules <file>} -> the {@code key=value} summary of the loaded rule groups.
     *
     * <p>Section 1 of the scenario asserts on these lines: two groups, ten recording rules, eight
     * alerts, no unhealthy rule, no rule that was never evaluated, and each alert carrying its
     * {@code severity} and {@code slo} labels. A rule set loaded from the wrong path answers
     * {@code groups=0} and looks perfectly healthy, which is the reason the file is in the answer.
     */
    private static int rules(Args args) throws Exception {
        JsonNode groups = Json.parse(java.nio.file.Files.readString(
                java.nio.file.Path.of(args.positional(0, "the rules JSON")))).path("data").path("groups");
        if (!groups.isArray() || groups.isEmpty()) {
            System.out.println("groups=0");
            return 0;
        }
        List<String> files = new java.util.ArrayList<>();
        List<String> names = new java.util.ArrayList<>();
        List<String> unhealthy = new java.util.ArrayList<>();
        List<String> unevaluated = new java.util.ArrayList<>();
        List<String> withoutLabels = new java.util.ArrayList<>();
        List<String> fastBurnFor = new java.util.ArrayList<>();
        int recording = 0;
        int alerting = 0;
        for (JsonNode group : groups) {
            files.add(group.path("file").asText(""));
            names.add(group.path("name").asText(""));
            String lastEvaluation = group.path("lastEvaluation").asText("");
            if (lastEvaluation.isEmpty() || lastEvaluation.startsWith("0001-")) {
                unevaluated.add(group.path("name").asText(""));
            }
            for (JsonNode rule : group.path("rules")) {
                String type = rule.path("type").asText("");
                String name = rule.path("name").asText("");
                if ("recording".equals(type)) {
                    recording++;
                } else if ("alerting".equals(type)) {
                    alerting++;
                    JsonNode labels = rule.path("labels");
                    if (labels.path("severity").asText("").isEmpty() || labels.path("slo").asText("").isEmpty()) {
                        withoutLabels.add(name);
                    }
                    if (name.contains("FastBurn")) {
                        fastBurnFor.add(name + ":" + rule.path("duration").asLong(0) + "s");
                    }
                }
                String health = rule.path("health").asText("");
                if (!health.isEmpty() && !"ok".equals(health)) {
                    unhealthy.add(name);
                }
            }
        }
        java.util.Collections.sort(files);
        java.util.Collections.sort(names);
        java.util.Collections.sort(fastBurnFor);
        System.out.println("groups=" + groups.size());
        System.out.println("files=" + String.join(",", new java.util.TreeSet<>(files)));
        System.out.println("groupnames=" + String.join(",", new java.util.TreeSet<>(names)));
        System.out.println("recording=" + recording);
        System.out.println("alerting=" + alerting);
        System.out.println("unhealthy=" + (unhealthy.isEmpty() ? "none" : String.join(",", unhealthy)));
        System.out.println("unevaluated=" + (unevaluated.isEmpty() ? "none" : String.join(",", unevaluated)));
        System.out.println("withoutlabels=" + (withoutLabels.isEmpty() ? "none" : String.join(",", withoutLabels)));
        System.out.println("fastburnfor=" + String.join(",", fastBurnFor));
        return 0;
    }

    /** {@code alert-states <file>} -> {@code name=state,...} for every loaded alert, or NONE. */
    private static int alertStates(Args args) throws Exception {
        List<String> states = new java.util.ArrayList<>();
        for (JsonNode group : loadedGroups(args.positional(0, "the rules JSON"))) {
            for (JsonNode rule : group.path("rules")) {
                states.add(rule.path("name").asText("?") + "=" + rule.path("state").asText("?"));
            }
        }
        System.out.println(states.isEmpty() ? "NONE" : String.join(",", states));
        return 0;
    }

    /**
     * {@code alert-query <file> <name>} -> the expression the loaded alert holds.
     *
     * <p>The scenario evaluates a firing alert's own query against Prometheus: an alert that
     * fires while its condition holds nothing is a phantom, and only the alert's real expression
     * can tell the two apart.
     */
    private static int alertQuery(Args args) throws Exception {
        String name = args.positional(1, "the alert name");
        for (JsonNode group : loadedGroups(args.positional(0, "the rules JSON"))) {
            for (JsonNode rule : group.path("rules")) {
                if (name.equals(rule.path("name").asText(""))) {
                    System.out.println(rule.path("query").asText("").replace("\n", " "));
                    return 0;
                }
            }
        }
        System.out.println("");
        return 0;
    }

    private static JsonNode loadedGroups(String file) throws Exception {
        return Json.parse(java.nio.file.Files.readString(java.nio.file.Path.of(file))).path("data").path("groups");
    }

    /** {@code prometheus-query <base> <expr>} -> the first value of the result, or NO SERIES. */
    private static int prometheusQuery(Args args) throws Exception {
        String url = args.positional(0, "the Prometheus base URL") + "/api/v1/query?query="
                + java.net.URLEncoder.encode(args.positional(1, "the expression"), java.nio.charset.StandardCharsets.UTF_8);
        JsonNode result = Json.parse(Http.text(url)).path("data").path("result");
        System.out.println(result.isEmpty() ? "NO SERIES" : result.get(0).path("value").path(1).asText("NO SERIES"));
        return 0;
    }

    /** {@code grafana-health <base>} -> the database field of Grafana's health, {@code ?} when absent. */
    private static int grafanaHealth(Args args) throws Exception {
        JsonNode health = Json.parse(Http.text(args.positional(0, "the Grafana base URL") + "/api/health"));
        String database = health.path("database").asText("?");
        System.out.println(database.isEmpty() ? "?" : database);
        return 0;
    }

    /** {@code grafana-dashboard <base> <query>} -> the title of the first dashboard found. */
    private static int grafanaDashboard(Args args) throws Exception {
        JsonNode found = Json.parse(Http.text(args.positional(0, "the Grafana base URL") + "/api/search?query="
                + java.net.URLEncoder.encode(args.positional(1, "the query"), java.nio.charset.StandardCharsets.UTF_8)));
        System.out.println(found.isArray() && !found.isEmpty() ? found.get(0).path("title").asText("NONE") : "NONE");
        return 0;
    }

    /**
     * {@code grafana-value <base> <expr>} -> the last value a panel would read, asked the way a
     * panel asks for it (through Grafana's datasource proxy).
     *
     * <p>A dashboard that exists but cannot query is a picture of nothing, so the check follows
     * the whole path: Grafana decides, the datasource answers, the frame carries the number.
     */
    private static int grafanaValue(Args args) throws Exception {
        String base = args.positional(0, "the Grafana base URL");
        String expr = args.positional(1, "the expression");
        String body = Json.write(Map.of(
                "queries", List.of(Map.of(
                        "refId", "A",
                        "datasource", Map.of("type", "prometheus", "uid", "prometheus"),
                        "expr", expr,
                        "instant", true,
                        "format", "table")),
                "from", "now-5m",
                "to", "now"));
        Http.Response response = Http.exchange("POST", base + "/api/ds/query", null, body);
        JsonNode frames = response.body().path("results").path("A").path("frames");
        String value = "NO VALUE";
        for (JsonNode frame : frames) {
            JsonNode values = frame.path("data").path("values");
            if (values.isArray() && !values.isEmpty()) {
                JsonNode lastSeries = values.get(values.size() - 1);
                if (lastSeries.isArray() && !lastSeries.isEmpty()) {
                    value = lastSeries.get(lastSeries.size() - 1).asText("NO VALUE");
                }
            }
        }
        System.out.println(value);
        return 0;
    }

    /** Every series of one exposition, summed by what it measures. */
    private static Map<String, Double> read(String url) throws Exception {
        Map<String, List<Scrape.Sample>> samples = Scrape.samples(url);
        Map<String, Double> counters = new LinkedHashMap<>();
        counters.put("wait_within_10_s", 0.0);
        counters.put("wait_total", 0.0);
        counters.put("formed", 0.0);
        counters.put("confirmed", 0.0);
        counters.put("dissolved_timeout", 0.0);
        counters.put("queue_depth", 0.0);
        counters.put("oldest_wait_s", 0.0);
        counters.put("outbox_pending", 0.0);
        counters.put("outbox_parked", 0.0);
        counters.put("outbox_oldest_pending_s", 0.0);

        for (Scrape.Sample sample : samples.getOrDefault("matchmaking_match_wait_seconds_bucket", List.of())) {
            if (bucketEdge(sample.labels().get("le")) == LATENCY_BUCKET_LE) {
                counters.merge("wait_within_10_s", sample.value(), Double::sum);
            }
        }
        sum(samples, "matchmaking_match_wait_seconds_count", "", counters, "wait_total");
        sum(samples, "matchmaking_matches_formed_total", "", counters, "formed");
        sum(samples, "matchmaking_matches_confirmed_total", "", counters, "confirmed");
        sum(samples, "matchmaking_matches_dissolved_total", "timeout", counters, "dissolved_timeout");
        sum(samples, "matchmaking_queue_depth", "", counters, "queue_depth");
        sum(samples, "matchmaking_outbox_backlog", "PENDING", counters, "outbox_pending");
        sum(samples, "matchmaking_outbox_backlog", "PARKED", counters, "outbox_parked");
        counters.put("oldest_wait_s", maximum(samples, "matchmaking_queue_oldest_wait_seconds"));
        counters.put("outbox_oldest_pending_s", maximum(samples, "matchmaking_outbox_oldest_pending_seconds"));
        return counters;
    }

    /** The SLO ratios of one window, from counters that only move forward. */
    private static Map<String, Double> window(JsonNode before, JsonNode after) {
        double matched = after.path("wait_total").asDouble() - before.path("wait_total").asDouble();
        double within = after.path("wait_within_10_s").asDouble() - before.path("wait_within_10_s").asDouble();
        double formed = after.path("formed").asDouble() - before.path("formed").asDouble();
        double confirmed = after.path("confirmed").asDouble() - before.path("confirmed").asDouble();

        Map<String, Double> ratios = new LinkedHashMap<>();
        ratios.put("latency_within_10_ratio", matched == 0 ? null : within / matched);
        ratios.put("confirm_ratio", formed == 0 ? null : confirmed / formed);
        ratios.put("matched_tickets", matched);
        ratios.put("matched_within_10_s", within);
        ratios.put("formed", formed);
        ratios.put("confirmed", confirmed);
        ratios.put("dissolved_timeout",
                after.path("dissolved_timeout").asDouble() - before.path("dissolved_timeout").asDouble());
        return ratios;
    }

    /**
     * The {@code le} label as a number, or {@code null} when it is not one (the {@code +Inf}
     * bucket). Compared as a number on purpose: Micrometer renders {@code le="10.0"} while the
     * objective is written {@code 10}.
     */
    private static Double bucketEdge(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }

    private static void sum(Map<String, List<Scrape.Sample>> samples, String name, String reason,
            Map<String, Double> counters, String counter) {
        for (Scrape.Sample sample : samples.getOrDefault(name, List.of())) {
            if (reason.isEmpty() || reason.equals(sample.labels().get("reason"))
                    || reason.equals(sample.labels().get("status"))) {
                counters.merge(counter, sample.value(), Double::sum);
            }
        }
    }

    private static double maximum(Map<String, List<Scrape.Sample>> samples, String name) {
        double maximum = 0;
        for (Scrape.Sample sample : samples.getOrDefault(name, List.of())) {
            maximum = Math.max(maximum, sample.value());
        }
        return maximum;
    }
}
