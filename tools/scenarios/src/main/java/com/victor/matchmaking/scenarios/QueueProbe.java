package com.victor.matchmaking.scenarios;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The W22/W23 client: put a player in the queue, answer the confirmation, report what happened.
 *
 * <p>The two moments every earlier client ran together are kept apart on purpose — the match being
 * <b>formed</b> and the match being <b>confirmed</b> — because capacity sits between them: the engine
 * decides whether forming is worth doing and the orchestrator decides where the confirmed match runs.
 * A client that confirms immediately cannot observe the wait, and a scenario that cannot separate
 * them cannot test either.
 *
 * <p>Each command prints one JSON object and exits 0; a failed HTTP call is not an exception, it is
 * the answer ({@code status} says what happened).
 */
final class QueueProbe {

    static final int DEFAULT_WAIT_SECONDS = 20;

    private QueueProbe() {
    }

    static int dispatch(String[] argv) throws Exception {
        if (argv.length == 0) {
            throw new IllegalArgumentException("queue <players|enqueue|play|confirm|ticket> ...");
        }
        String command = argv[0];
        Args args = Args.of(Arrays.copyOfRange(argv, 1, argv.length));
        switch (command) {
            case "players" -> {
                Pairing.Group group = Pairing.playersWithOneSkill(
                        args.intOf("count", 2), args.optional("prefix", "gs-"));
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("skill", group.skill());
                out.put("players", group.players());
                System.out.println(Json.write(out));
            }
            case "enqueue" -> System.out.println(Json.write(enqueue(
                    args.required("api"), args.required("token"), args.required("player"),
                    args.optional("region", "NA"), args.requiredInt("redis-port"),
                    args.intOf("wait", DEFAULT_WAIT_SECONDS))));
            case "play" -> System.out.println(Json.write(play(
                    args.required("api"), args.required("token"), args.required("player"),
                    args.optional("region", "NA"), args.requiredInt("redis-port"),
                    args.intOf("wait", DEFAULT_WAIT_SECONDS))));
            case "confirm" -> System.out.println(Json.write(confirm(
                    args.requiredInt("redis-port"), args.required("match-id"), args.required("player"))));
            case "ticket" -> System.out.println(Json.write(ticket(
                    args.required("api"), args.required("token"), args.required("ticket-id"))));
            default -> throw new IllegalArgumentException("unknown queue command: " + command);
        }
        return 0;
    }

    /** Enqueue and wait for the announcement, without answering it. */
    static Map<String, Object> enqueue(String api, String token, String player, String region,
            int redisPort, int waitSeconds) throws Exception {
        try (Resp.Subscriber subscriber = new Resp.Subscriber(redisPort)) {
            long start = System.nanoTime();
            Http.Response response = Http.exchange("POST", api + "/v1/matchmaking/tickets", token,
                    Map.of("gameMode", "1v1", "region", region, "maxLatencyMs", 150));
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("player", player);
            result.put("status", response.status());
            result.put("body", response.body());
            if (response.status() != 201) {
                return result;
            }
            String ticketId = Json.pythonText(response.body().get("ticketId"));
            result.put("ticketId", ticketId);
            result.put("queuedStatus", Json.pythonText(response.body().get("status")));

            String matchId = null;
            long deadline = System.nanoTime() + waitSeconds * 1_000_000_000L;
            while (System.nanoTime() < deadline) {
                matchId = subscriber.matchFor(player);
                if (matchId != null) {
                    break;
                }
                Thread.sleep(10);
            }
            result.put("matchId", matchId);
            result.put("matched", matchId != null);
            result.put("waitedMs", millisSince(start));
            return result;
        }
    }

    /** Enqueue, answer the confirmation the moment it arrives, and report the wait. */
    static Map<String, Object> play(String api, String token, String player, String region,
            int redisPort, int waitSeconds) throws Exception {
        long start = System.nanoTime();
        Map<String, Object> queued = enqueue(api, token, player, region, redisPort, waitSeconds);
        String matchId = (String) queued.get("matchId");
        if (matchId == null) {
            return queued;
        }
        String ticketId = (String) queued.get("ticketId");
        try (Resp.Confirmer confirmer = new Resp.Confirmer(redisPort)) {
            confirmer.confirm(matchId, player);
        }
        long confirmedAt = 0L;
        long deadline = System.nanoTime() + waitSeconds * 1_000_000_000L;
        while (System.nanoTime() < deadline) {
            Http.Response current = Http.exchange(
                    "GET", api + "/v1/matchmaking/tickets/" + ticketId, token, null);
            if (current.status() == 200 && "CONFIRMED".equals(Json.pythonText(current.body().get("status")))) {
                confirmedAt = System.nanoTime();
                break;
            }
            Thread.sleep(20);
        }
        queued.put("confirmed", confirmedAt != 0L);
        queued.put("waitMs", confirmedAt == 0L ? null : millisSince(start, confirmedAt));
        return queued;
    }

    static Map<String, Object> ticket(String api, String token, String ticketId) throws Exception {
        Http.Response response = Http.exchange(
                "GET", api + "/v1/matchmaking/tickets/" + ticketId, token, null);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", response.status());
        result.put("ticketStatus", Json.pythonText(response.body().get("status")));
        result.put("body", response.body());
        return result;
    }

    static Map<String, Object> confirm(int redisPort, String matchId, String player) throws Exception {
        try (Resp.Confirmer confirmer = new Resp.Confirmer(redisPort)) {
            confirmer.confirm(matchId, player);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("matchId", matchId);
        result.put("player", player);
        result.put("published", true);
        return result;
    }

    /** Milliseconds since a mark, rounded to one decimal (the shape the scripts print). */
    private static double millisSince(long startNanos) {
        return millisSince(startNanos, System.nanoTime());
    }

    private static double millisSince(long startNanos, long endNanos) {
        return Math.round((endNanos - startNanos) / 1_000_000.0 * 10) / 10.0;
    }

    /** Unwraps the parsed body of a response the probe already returned as a map entry. */
    static JsonNode bodyOf(Map<String, Object> probeResult) {
        Object body = probeResult.get("body");
        return body instanceof JsonNode node ? node : Json.object();
    }
}
