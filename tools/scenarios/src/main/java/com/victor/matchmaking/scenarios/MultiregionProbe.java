package com.victor.matchmaking.scenarios;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The W21 client (ADR 0039): probe a region, enqueue, answer the confirmation, report what it
 * measured.
 *
 * <p>Every number the sprint claims is produced here, from the client's side, against the real
 * services:
 *
 * <ul>
 *   <li>the RTT the <b>server</b> measured for this player in that region ({@code GET ping} +
 *       {@code POST pong});
 *   <li>the ticket's wait, from enqueue to the ticket being {@code CONFIRMED};
 *   <li>the region the match was formed in, as the ticket reports it.
 * </ul>
 *
 * <p>It talks to a region's Redis over a plain socket for the push ({@code player_notifications})
 * and the answer ({@code match_confirmations}), the same way a game client does — and for the same
 * reason the W16 load harness stopped using {@code docker exec redis-cli}: a client whose own
 * plumbing drops frames cannot measure a delivery objective.
 */
final class MultiregionProbe {

    private static final int WAIT_FOR_MATCH_SECONDS = 25;
    private static final int WAIT_FOR_CONFIRM_SECONDS = 25;

    private MultiregionProbe() {
    }

    static int dispatch(String[] argv) throws Exception {
        if (argv.length == 0) {
            throw new IllegalArgumentException("multiregion <players|measure|directory|ticket|play> ...");
        }
        String command = argv[0];
        List<String> rest = new ArrayList<>(java.util.Arrays.asList(argv).subList(1, argv.length));
        boolean noProbe = rest.remove("--no-probe");
        Args args = Args.of(rest.toArray(new String[0]));
        switch (command) {
            case "players" -> {
                Pairing.Group group = Pairing.playersWithOneSkill(
                        args.intOf("count", 2), args.optional("prefix", "mr-"));
                Map<String, Object> answer = new LinkedHashMap<>();
                answer.put("skill", group.skill());
                answer.put("players", group.players());
                System.out.println(Json.write(answer));
                return 0;
            }
            case "directory" -> {
                Http.Response response = Http.exchange("GET",
                        args.required("api") + "/v1/regions", args.required("token"), null);
                System.out.println(Json.write(Map.of("status", response.status(), "body", response.body())));
                return 0;
            }
            case "measure" -> {
                System.out.println(Json.write(measure(args.required("api"), args.required("token"), args.required("region"),
                        args.intOf("attempts", 1))));
                return 0;
            }
            case "ticket" -> {
                System.out.println(Json.write(ticket(args.required("api"), args.required("token"), args.required("region"),
                        args.intOf("budget", 150))));
                return 0;
            }
            case "play" -> {
                System.out.println(Json.write(play(args.required("api"), args.required("token"), args.required("player"),
                        args.required("region"), args.intOf("budget", 150),
                        args.intOf("redis-port", 6379), !noProbe)));
                return 0;
            }
            default -> throw new IllegalArgumentException("unknown multiregion command: " + command);
        }
    }

    /**
     * Two requests, timed by the server: the probe the placement is later decided on.
     *
     * <p>{@code attempts} repeats the probe and reports the best sample, because a single round
     * trip on a machine that is also running four JVMs measures the machine as much as the path.
     * The server stores the minimum too, so the client and the profile agree on which sample counts.
     */
    private static Map<String, Object> measure(String api, String token, String region, int attempts)
            throws Exception {
        // The samples stay JSON nodes on purpose: the server sends whole milliseconds, and a
        // script that compares them arithmetically ("the far region costs this much more") cannot
        // read 14.0 where the answer said 14.
        List<JsonNode> samples = new ArrayList<>();
        Map<String, Object> last = new LinkedHashMap<>();
        for (int attempt = 0; attempt < Math.max(1, attempts); attempt++) {
            Http.Response ping = Http.exchange("GET", api + "/v1/regions/" + region + "/ping", token, null);
            if (ping.status() != 201) {
                Map<String, Object> failed = new LinkedHashMap<>();
                failed.put("pingStatus", ping.status());
                failed.put("pongStatus", null);
                failed.put("rttMs", null);
                failed.put("samples", samples);
                failed.put("body", ping.body());
                return failed;
            }
            Http.Response pong = Http.exchange("POST", api + "/v1/regions/" + region + "/pong", token,
                    Map.of("nonce", ping.body().path("nonce").asText("")));
            last = new LinkedHashMap<>();
            last.put("pingStatus", ping.status());
            last.put("pongStatus", pong.status());
            last.put("body", pong.body());
            last.put("nonce", ping.body().path("nonce").asText(""));
            JsonNode rtt = pong.body().get("rttMs");
            if (rtt != null && !rtt.isNull()) {
                samples.add(rtt);
            }
        }
        JsonNode best = null;
        for (JsonNode sample : samples) {
            if (best == null || sample.asDouble() < best.asDouble()) {
                best = sample;
            }
        }
        last.put("samples", samples);
        last.put("rttMs", best);
        return last;
    }

    private static Map<String, Object> ticket(String api, String token, String region, int budget)
            throws Exception {
        Http.Response response = Http.exchange("POST", api + "/v1/matchmaking/tickets", token,
                Map.of("gameMode", "1v1", "region", region, "maxLatencyMs", budget));
        Map<String, Object> answer = new LinkedHashMap<>();
        answer.put("status", response.status());
        answer.put("body", response.body());
        return answer;
    }

    /** Probe (optionally), enqueue, answer the confirmation, and report the timings. */
    private static Map<String, Object> play(String api, String token, String player, String region,
            int budget, int redisPort, boolean doProbe) throws Exception {
        long started = System.nanoTime();
        // The probe's own measurement, as the server rendered it (14, not 14.0).
        JsonNode measuredRtt = null;
        if (doProbe) {
            // Three attempts, and the minimum, for the same reason the standalone probe uses three
            // (ADR 0039): the probe is two HTTP requests, so it includes the client's turnaround —
            // and this client is a JVM whose first requests pay class loading and the JIT. Measured
            // on a busy machine, the first sample is ~500 ms and the next two are ~15 ms; the
            // server keeps the minimum too, so both sides count the same sample. A single attempt
            // would report the harness's warm-up as the network's distance and refuse a player who
            // is standing next to the region.
            Map<String, Object> probed = measure(api, token, region, 3);
            JsonNode measured = (JsonNode) probed.get("rttMs");
            if (measured == null) {
                Map<String, Object> refused = new LinkedHashMap<>();
                refused.put("player", player);
                refused.put("probeStatus", probed.get("pingStatus"));
                refused.put("probe", probed.get("body"));
                refused.put("status", probed.get("pingStatus"));
                refused.put("body", probed.get("body"));
                return refused;
            }
            measuredRtt = measured;
        }

        // Subscribe before enqueuing: a match can be announced before this process gets to wait.
        try (Resp.Subscriber subscriber = new Resp.Subscriber(redisPort)) {
            Http.Response arrival = Http.exchange("POST", api + "/v1/matchmaking/tickets", token,
                    Map.of("gameMode", "1v1", "region", region, "maxLatencyMs", budget));
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("player", player);
            result.put("status", arrival.status());
            result.put("body", arrival.body());
            result.put("rttMs", measuredRtt);
            if (arrival.status() != 201) {
                return result;
            }
            String ticketId = arrival.body().path("ticketId").asText("");
            result.put("ticketId", ticketId);
            result.put("ticketRegion", arrival.body().path("region").asText(""));

            String matchId = null;
            Instant deadline = Instant.now().plusSeconds(WAIT_FOR_MATCH_SECONDS);
            while (Instant.now().isBefore(deadline) && matchId == null) {
                matchId = subscriber.matchFor(player);
                if (matchId == null) {
                    Thread.sleep(10);
                }
            }
            result.put("matchId", matchId);
            result.put("enqueuedMs", millisSince(started));
            if (matchId == null) {
                return result;
            }

            try (Resp.Confirmer confirmer = new Resp.Confirmer(redisPort)) {
                confirmer.confirm(matchId, player);
            }

            Instant confirmedAt = null;
            deadline = Instant.now().plusSeconds(WAIT_FOR_CONFIRM_SECONDS);
            while (Instant.now().isBefore(deadline) && confirmedAt == null) {
                Http.Response current = Http.exchange("GET",
                        api + "/v1/matchmaking/tickets/" + ticketId, token, null);
                if (current.status() == 200 && "CONFIRMED".equals(current.body().path("status").asText(""))) {
                    confirmedAt = Instant.now();
                } else {
                    Thread.sleep(20);
                }
            }
            result.put("confirmed", confirmedAt != null);
            result.put("waitMs", confirmedAt == null ? null : millisSince(started));
            return result;
        }
    }

    private static double millisSince(long started) {
        return Numbers.round((System.nanoTime() - started) / 1_000_000.0, 1);
    }
}
