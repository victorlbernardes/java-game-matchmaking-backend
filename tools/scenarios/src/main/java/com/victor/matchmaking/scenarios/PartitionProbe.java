package com.victor.matchmaking.scenarios;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import com.victor.matchmaking.domain.player.DeterministicSkill;
import com.victor.matchmaking.redis.QueuePartitions;

/**
 * The W20 client: where the arrivals land, what the scrape says about it, whether two partitions
 * can still pair, and what the cap does when the queue is spread over several keys.
 *
 * <p>It answers four questions, and the difference from the Python it replaces is the point of the
 * migration: the partition index comes from {@link QueuePartitions} and the skill from
 * {@link DeterministicSkill} — the product's own functions — instead of a second implementation
 * written by hand. That second implementation could only ever agree or disagree silently; this one
 * cannot disagree at all. The Redis reads are over RESP through {@link Resp} rather than
 * {@code docker exec redis-cli}, so the probe needs no container name.
 */
final class PartitionProbe {

    private static final String DEFAULT_AUTH = "http://localhost:8081";

    private final Map<String, String> tokens = new ConcurrentHashMap<>();

    private PartitionProbe() {
    }

    static int dispatch(String[] argv) throws Exception {
        if (argv.length == 0) {
            throw new IllegalArgumentException("partitions <dist|metrics|cross|cap> ...");
        }
        String command = argv[0];
        Args args = Args.of(java.util.Arrays.copyOfRange(argv, 1, argv.length));
        PartitionProbe probe = new PartitionProbe();
        switch (command) {
            case "dist" -> probe.dist(args);
            case "metrics" -> probe.metrics(args);
            case "cross" -> probe.cross(args);
            case "cap" -> probe.cap(args);
            default -> throw new IllegalArgumentException("unknown partitions command: " + command);
        }
        return 0;
    }

    /** {@code dist}: enqueue N players and report how many landed in each partition key. */
    private void dist(Args args) throws Exception {
        String api = args.optional("api", "http://localhost:8094");
        String auth = args.optional("auth", DEFAULT_AUTH);
        String mode = args.optional("mode", "1v1");
        String region = args.optional("region", "NA");
        int players = args.intOf("players", 2000);
        int partitions = args.intOf("partitions", 8);
        String prefix = args.optional("prefix", "w20-dist-");
        int redisPort = args.intOf("redis-port", 6379);

        int admitted = 0;
        int refused = 0;
        // Virtual threads: the arrivals are I/O-bound calls, and 2000 of them should be in flight
        // 16 at a time (the same shape the Python used), not one after the other.
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            List<Future<Integer>> results = new ArrayList<>(players);
            for (int i = 0; i < players; i++) {
                String playerId = prefix + i;
                results.add(pool.submit(() -> enqueue(api, auth, playerId, mode, region, 80).status()));
            }
            for (Future<Integer> result : results) {
                if (result.get() == 201) {
                    admitted++;
                } else {
                    refused++;
                }
            }
        }

        Map<String, Integer> counts = new LinkedHashMap<>();
        int total = 0;
        try (Resp redis = Resp.connect(redisPort)) {
            for (String queueKey : redis.keys("queue:" + mode + ":" + region + "*")) {
                int depth = (int) redis.zcard(queueKey);
                counts.put(queueKey, depth);
                total += depth;
            }
        }
        int deepest = counts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        int keysWithMembers = (int) counts.values().stream().filter(depth -> depth > 0).count();
        double mean = partitions > 0 ? (double) total / partitions : 0;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("admitted", admitted);
        out.put("refused", refused);
        out.put("counts", counts);
        out.put("sum", total);
        out.put("keys_with_members", keysWithMembers);
        out.put("partitions", partitions);
        out.put("mean", mean);
        out.put("deepest", deepest);
        out.put("hotspot", mean == 0 ? 0.0 : deepest / mean);
        out.put("max_share_pct", total == 0 ? 0.0 : 100.0 * deepest / total);
        System.out.println(Json.write(out));
    }

    /** {@code metrics}: the per-partition gauges the engine publishes, and the hotspot they imply. */
    private void metrics(Args args) throws Exception {
        String metrics = args.optional("metrics", "http://localhost:9194/metrics");
        String mode = args.optional("mode", "1v1");
        String region = args.optional("region", "NA");
        Map<String, List<Scrape.Sample>> samples = Scrape.samples(metrics);

        Map<Integer, Double> partitions = new LinkedHashMap<>();
        for (Scrape.Sample sample : samples.getOrDefault("matchmaking_queue_partition_depth", List.of())) {
            if (mode.equals(sample.labels().get("mode")) && region.equals(sample.labels().get("region"))) {
                partitions.put(Integer.parseInt(sample.labels().getOrDefault("partition", "-1")), sample.value());
            }
        }
        double depth = series(samples, "matchmaking_queue_depth", mode, region);
        double hotspot = series(samples, "matchmaking_queue_hotspot_ratio", mode, region);
        double mean = partitions.isEmpty() ? 0
                : partitions.values().stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double deepest = partitions.values().stream().mapToDouble(Double::doubleValue).max().orElse(0);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("partition_depth", partitions);
        out.put("depth", depth);
        out.put("hotspot", hotspot);
        out.put("mean", mean);
        out.put("deepest", deepest);
        out.put("expected_hotspot", mean == 0 ? 0.0 : deepest / mean);
        System.out.println(Json.write(out));
    }

    /** {@code cross}: two compatible players that hash to different partitions must still pair. */
    private void cross(Args args) throws Exception {
        String api = args.optional("api", "http://localhost:8094");
        String auth = args.optional("auth", DEFAULT_AUTH);
        String mode = args.optional("mode", "1v1");
        String region = args.optional("region", "NA");
        int partitions = args.intOf("partitions", 8);
        double timeoutSeconds = Numbers.parse(args.optional("timeout", "10"));
        int redisPort = args.intOf("redis-port", 6379);

        // The pair is chosen, not sampled: same skill (so the engine considers them compatible) and
        // different partitions (so the match can only form if the heads are merged).
        Map<Integer, String[]> firstBySkill = new LinkedHashMap<>();
        String playerA = null;
        String playerB = null;
        int partitionA = -1;
        int partitionB = -1;
        for (int i = 0; i < 50_000 && playerA == null; i++) {
            String player = "w20-cross-" + i;
            int partition = QueuePartitions.partitionOf(player, partitions);
            String[] earlier = firstBySkill.get(DeterministicSkill.of(player));
            if (earlier != null && Integer.parseInt(earlier[1]) != partition) {
                playerA = earlier[0];
                partitionA = Integer.parseInt(earlier[1]);
                playerB = player;
                partitionB = partition;
            } else if (earlier == null) {
                firstBySkill.put(DeterministicSkill.of(player), new String[] {player, Integer.toString(partition)});
            }
        }
        if (playerA == null) {
            throw new IllegalStateException("could not find two compatible players in different partitions");
        }

        Http.Response first = enqueue(api, auth, playerA, mode, region, 80);
        Http.Response second = enqueue(api, auth, playerB, mode, region, 80);
        if (first.status() != 201 || second.status() != 201) {
            throw new IllegalStateException("enqueue failed: " + first.status() + " " + first.body()
                    + " / " + second.status() + " " + second.body());
        }

        String matchId = "";
        List<String> roster = List.of();
        double waited = 0;
        Instant deadline = Instant.now().plus(Duration.ofMillis((long) (timeoutSeconds * 1000)));
        try (Resp redis = Resp.connect(redisPort)) {
            while (Instant.now().isBefore(deadline) && matchId.isEmpty()) {
                for (String key : redis.keys("match-pending:*")) {
                    String raw = redis.get(key);
                    JsonNode pending = Json.parse(raw == null ? "{}" : raw);
                    List<String> ids = new ArrayList<>();
                    for (JsonNode player : pending.path("players")) {
                        ids.add(player.path("playerId").asText());
                    }
                    if (ids.contains(playerA) && ids.contains(playerB)) {
                        matchId = pending.path("matchId").asText("");
                        roster = ids;
                        break;
                    }
                }
                if (matchId.isEmpty()) {
                    Thread.sleep(200);
                    waited += 0.2;
                }
            }

            // The chosen keys are read back from Redis: a check that the hash here and the server
            // agree, instead of a check that the probe's own hash is self-consistent.
            String keyA = queueKey(mode, region, partitionA, partitions);
            String keyB = queueKey(mode, region, partitionB, partitions);
            Map<String, String> ticketStates = new LinkedHashMap<>();
            for (JsonNode body : List.of(first.body(), second.body())) {
                String ticketId = body.path("ticketId").asText("");
                String raw = ticketId.isEmpty() ? null : redis.get("ticket:" + ticketId);
                ticketStates.put(ticketId, raw == null ? "" : Json.parse(raw).path("status").asText(""));
            }

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("player_a", playerA);
            out.put("player_b", playerB);
            out.put("partition_a", partitionA);
            out.put("partition_b", partitionB);
            out.put("skill", DeterministicSkill.of(playerA));
            out.put("match_id", matchId);
            out.put("players", roster);
            out.put("waited_s", Math.round(waited * 100) / 100.0);
            out.put("key_a_depth", redis.zcard(keyA));
            out.put("key_b_depth", redis.zcard(keyB));
            out.put("ticket_states", ticketStates);
            System.out.println(Json.write(out));
        }
    }

    /** {@code cap}: enqueue past the configured depth and count what was admitted. */
    private void cap(Args args) throws Exception {
        String api = args.optional("api", "http://localhost:8094");
        String auth = args.optional("auth", DEFAULT_AUTH);
        String mode = args.optional("mode", "1v1");
        String region = args.optional("region", "NA");
        int players = args.intOf("players", 200);
        int redisPort = args.intOf("redis-port", 6379);

        int admitted = 0;
        int refused = 0;
        for (int i = 0; i < players; i++) {
            if (enqueue(api, auth, "w20-cap-" + i, mode, region, 80).status() == 201) {
                admitted++;
            } else {
                refused++;
            }
        }
        long depth = 0;
        try (Resp redis = Resp.connect(redisPort)) {
            for (String queueKey : redis.keys("queue:" + mode + ":" + region + "*")) {
                depth += redis.zcard(queueKey);
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("admitted", admitted);
        out.put("refused", refused);
        out.put("depth", depth);
        System.out.println(Json.write(out));
    }

    /** The queue key of a partition: the historical single key when there is only one. */
    private static String queueKey(String mode, String region, int partition, int partitions) {
        return partitions > 1
                ? "queue:" + mode + ":" + region + ":" + partition
                : "queue:" + mode + ":" + region;
    }

    private static double series(Map<String, List<Scrape.Sample>> samples, String name, String mode, String region) {
        for (Scrape.Sample sample : samples.getOrDefault(name, List.of())) {
            if (mode.equals(sample.labels().get("mode")) && region.equals(sample.labels().get("region"))) {
                return sample.value();
            }
        }
        return 0;
    }

    private Http.Response enqueue(String api, String auth, String playerId, String mode, String region, int latency)
            throws Exception {
        Http.Response login = Http.exchange("POST", auth + "/v1/auth/login", null, Map.of("playerId", playerId));
        if (login.status() != 200) {
            throw new IllegalStateException("login failed for " + playerId + ": " + login.status() + " " + login.body());
        }
        String token = login.body().path("token").asText();
        return Http.exchange("POST", api + "/v1/matchmaking/tickets", token,
                Map.of("gameMode", mode, "region", region, "maxLatencyMs", latency));
    }
}
