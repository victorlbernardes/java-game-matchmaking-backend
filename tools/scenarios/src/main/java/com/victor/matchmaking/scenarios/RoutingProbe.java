package com.victor.matchmaking.scenarios;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * W19 (ADR 0026): a player's notification is delivered by whichever instance holds the socket.
 *
 * <p>Two API instances share one Redis. The probe is the player's client — a real WebSocket — so
 * the property under test is the one the player experiences:
 *
 * <ul>
 *   <li>phase 1 (routing): A connects to A and B to B; the engine forms a match; each notification
 *       arrives on the instance holding that player's socket, and the other instance counts the
 *       notification it had no socket for;
 *   <li>phase 2 (recovery): C connects to A, D never connects at all; the match forms, only C is
 *       notified, then A is killed hard (no close handlers). C reconnects on B, both send a
 *       heartbeat, and both must be re-presented the pending confirmation — D for the first time.
 * </ul>
 *
 * <p>Before W19, phase 2 had no answer: D's notification was published once to an instance with no
 * socket for D, the heartbeat answered PONG, and the match dissolved at its deadline with every
 * process that was still alive looking healthy.
 */
final class RoutingProbe {

    private static final int NOTIFY_TIMEOUT_S = 20;
    private static final int REPRESENT_TIMEOUT_S = 5;

    private static final List<String> PASSED = new ArrayList<>();
    private static final List<String> FAILED = new ArrayList<>();
    private static final Map<String, Object> NUMBERS = new LinkedHashMap<>();

    private static String apiA;
    private static String apiB;
    private static String auth;
    private static String metricsA;
    private static String metricsB;
    private static String engineMetrics;
    private static int redisPort;

    private RoutingProbe() {
    }

    static int run(String[] argv) throws Exception {
        apiA = env("API_A", "http://localhost:8092");
        apiB = env("API_B", "http://localhost:8093");
        auth = env("AUTH", "http://localhost:8081");
        metricsA = env("METRICS_A", "http://localhost:9192/metrics");
        metricsB = env("METRICS_B", "http://localhost:9193/metrics");
        engineMetrics = env("ENGINE_METRICS", "http://localhost:9091/metrics");
        redisPort = Integer.parseInt(env("REDIS_PORT", "6379"));
        long apiAPid = Long.parseLong(env("API_A_PID", "0"));

        System.out.println("=== W19: routing a notification to whichever instance holds the socket ===");
        Http.Response healthA = Http.exchange("GET", apiA + "/health", null, null);
        Http.Response healthB = Http.exchange("GET", apiB + "/health", null, null);
        String nameA = healthA.body().path("instance").asText("");
        String nameB = healthB.body().path("instance").asText("");
        check("both instances answer /health", healthA.status() == 200 && healthB.status() == 200,
                healthA.status() + "/" + healthB.status());
        check("the instances are distinguishable", !nameA.isEmpty() && !nameB.isEmpty() && !nameA.equals(nameB),
                nameA + " vs " + nameB);
        if (nameA.isEmpty() || nameB.isEmpty() || nameA.equals(nameB)) {
            return finish();
        }

        Pairing.Group group = Pairing.playersWithOneSkill(4, "routing-");
        List<String> ids = group.players();
        String playerA = ids.get(0);
        String playerB = ids.get(1);
        String playerC = ids.get(2);
        String playerD = ids.get(3);
        System.out.println("  phase 1 players: " + playerA + " / " + playerB + " (skill " + group.skill() + ")");
        System.out.println("  phase 2 players: " + playerC + " / " + playerD + " (skill " + group.skill() + ")");

        Double confirmedBefore = number(metric(engineMetrics, "matchmaking_matches_confirmed_total", Map.of()));
        Double dissolvedBefore = number(metric(engineMetrics, "matchmaking_matches_dissolved_total", Map.of()));

        phaseRouting(nameA, nameB, playerA, playerB);
        phaseRecovery(nameA, playerC, playerD, apiAPid);

        Double confirmedAfter = number(metric(engineMetrics, "matchmaking_matches_confirmed_total", Map.of()));
        Double dissolvedAfter = number(metric(engineMetrics, "matchmaking_matches_dissolved_total", Map.of()));
        check("the engine confirmed both matches", confirmedAfter - confirmedBefore >= 2,
                "confirmed " + confirmedBefore + " -> " + confirmedAfter);
        check("nothing dissolved: no match outlived its notification", dissolvedAfter.equals(dissolvedBefore),
                "dissolved " + dissolvedBefore + " -> " + dissolvedAfter);
        return finish();
    }

    /** Phase 1: the notification lands on the instance that holds the socket. */
    private static void phaseRouting(String nameA, String nameB, String playerA, String playerB) throws Exception {
        System.out.println("\n--- phase 1: each notification lands on the instance holding the socket ---");
        try (Ws clientA = Ws.connect(wsUrl(apiA, playerA));
                Ws clientB = Ws.connect(wsUrl(apiB, playerB));
                Resp redis = Resp.connect(redisPort)) {

            String presenceA = waitPresence(redis, playerA, nameA);
            String presenceB = waitPresence(redis, playerB, nameB);
            check("presence names the instance holding the socket", presenceA != null && presenceB != null,
                    playerA + "@" + safe(() -> redis.get("presence:" + playerA)) + " "
                            + playerB + "@" + safe(() -> redis.get("presence:" + playerB)));

            Http.Response enqueueA = enqueue(apiA, playerA);
            Http.Response enqueueB = enqueue(apiB, playerB);
            check("both players enqueue", enqueueA.status() == 201 && enqueueB.status() == 201,
                    enqueueA.status() + "/" + enqueueB.status());

            JsonNode notifiedA = awaitFrame(clientA, "MATCH_PENDING_CONFIRMATION", NOTIFY_TIMEOUT_S);
            JsonNode notifiedB = awaitFrame(clientB, "MATCH_PENDING_CONFIRMATION", NOTIFY_TIMEOUT_S);
            String matchId = matchIdOf(notifiedA);
            check("both players were notified", notifiedA != null && notifiedB != null, "matchId=" + matchId);
            check("the notification names the same match", !matchId.isEmpty()
                    && matchId.equals(matchIdOf(notifiedB)));

            Double deliveredA = metric(metricsA, "matchmaking_ws_notifications_total", Map.of("result", "delivered"));
            Double missedA = metric(metricsA, "matchmaking_ws_notifications_total", Map.of("result", "no_local_connection"));
            Double deliveredB = metric(metricsB, "matchmaking_ws_notifications_total", Map.of("result", "delivered"));
            Double missedB = metric(metricsB, "matchmaking_ws_notifications_total", Map.of("result", "no_local_connection"));
            check("each instance delivered the notification for its own socket",
                    isPositive(deliveredA) && isPositive(deliveredB), "A=" + deliveredA + " B=" + deliveredB);
            check("each instance counted the notification it had no socket for",
                    isPositive(missedA) && isPositive(missedB), "A=" + missedA + " B=" + missedB);
            NUMBERS.put("delivered_a", deliveredA);
            NUMBERS.put("missed_a", missedA);
            NUMBERS.put("delivered_b", deliveredB);
            NUMBERS.put("missed_b", missedB);

            // The match ends through each player's own instance: routing works both ways.
            clientA.send(Map.of("type", "MATCH_CONFIRM", "payload", Map.of("matchId", matchId)));
            clientB.send(Map.of("type", "MATCH_CONFIRM", "payload", Map.of("matchId", matchId)));
            boolean confirmed = waitUntil(
                    () -> safe(() -> redis.exists("match-confirmed-claim:" + matchId)), 10) != null;
            check("both confirmations are accepted and the match resolves", confirmed,
                    "match-pending exists=" + safe(() -> redis.exists("match-pending:" + matchId)));
        }
    }

    /** Phase 2: the instance dies and one player's notification was never delivered. */
    private static void phaseRecovery(String nameA, String playerC, String playerD, long apiAPid) throws Exception {
        System.out.println("\n--- phase 2: the instance dies and the notification was never delivered ---");
        try (Ws clientC = Ws.connect(wsUrl(apiA, playerC));
                Resp redis = Resp.connect(redisPort)) {

            Http.Response enqueueC = enqueue(apiB, playerC);
            Http.Response enqueueD = enqueue(apiB, playerD);
            check("both players enqueue (D without ever connecting)",
                    enqueueC.status() == 201 && enqueueD.status() == 201,
                    enqueueC.status() + "/" + enqueueD.status());

            String matchId = waitForMatch(redis, List.of(playerC, playerD), NOTIFY_TIMEOUT_S);
            check("the engine formed the match while D had no socket", matchId != null, String.valueOf(matchId));
            if (matchId == null) {
                return;
            }
            JsonNode notificationC = awaitFrame(clientC, "MATCH_PENDING_CONFIRMATION", 3);
            check("C, connected on A, was notified", notificationC != null);

            // The instance holding C's socket dies. SIGKILL: no close handler, no last goodbye.
            check("the scenario knows which process to kill", apiAPid > 0, "pid=" + apiAPid);
            if (apiAPid > 0) {
                ProcessHandle.of(apiAPid).ifPresent(ProcessHandle::destroyForcibly);
            }
            check("instance A is gone", waitUntil(() -> !answers(apiA + "/health"), 15) != null);

            // C reconnects on B and asks with a heartbeat.
            try (Ws resumedC = Ws.connect(wsUrl(apiB, playerC))) {
                resumedC.heartbeat();
                JsonNode pongC = awaitFrame(resumedC, "PONG", REPRESENT_TIMEOUT_S);
                long startedAt = System.nanoTime();
                JsonNode representedC = awaitFrame(resumedC, "MATCH_PENDING_CONFIRMATION", REPRESENT_TIMEOUT_S);
                check("C's heartbeat is answered", pongC != null);
                check("C's heartbeat re-presents the pending confirmation", representedC != null);

                // D connects for the first time, on B, and asks the same question.
                try (Ws clientD = Ws.connect(wsUrl(apiB, playerD))) {
                    clientD.heartbeat();
                    JsonNode pongD = awaitFrame(clientD, "PONG", REPRESENT_TIMEOUT_S);
                    JsonNode representedD = awaitFrame(clientD, "MATCH_PENDING_CONFIRMATION", REPRESENT_TIMEOUT_S);
                    check("D's heartbeat is answered", pongD != null);
                    check("D, who never received the notification, is told what it missed", representedD != null,
                            "matchId=" + matchIdOf(representedD));
                    String deliveredMatch = matchIdOf(representedD);
                    check("the re-presented match is the pending one", matchId.equals(deliveredMatch));
                    NUMBERS.put("first_heartbeat_to_representation_s",
                            Json.pythonDouble(Math.round((System.nanoTime() - startedAt) / 1_000_000.0) / 1000.0));

                    check("presence follows C to the surviving instance",
                            waitUntil(() -> "api-b".equals(safe(() -> redis.get("presence:" + playerC))), 3) != null,
                            playerC + "@" + safe(() -> redis.get("presence:" + playerC)));

                    // Both answer with the id the heartbeat gave them: D had no other way to know it.
                    resumedC.send(Map.of("type", "MATCH_CONFIRM", "payload", Map.of("matchId", deliveredMatch)));
                    clientD.send(Map.of("type", "MATCH_CONFIRM", "payload", Map.of("matchId", deliveredMatch)));
                    boolean confirmed = waitUntil(
                            () -> safe(() -> redis.exists("match-confirmed-claim:" + matchId)), 10) != null;
                    check("the match is confirmed by the two reconnected clients", confirmed);
                    check("the pending match key is gone",
                            waitUntil(() -> safe(() -> !redis.exists("match-pending:" + matchId)), 10) != null);
                    check("the reverse index is cleared once the match resolves",
                            waitUntil(() -> safe(() -> !redis.exists("player-pending:" + playerC)
                                    && !redis.exists("player-pending:" + playerD)), 10) != null);

                    // Nothing is owed any more: the same heartbeat must not re-present a resolved match.
                    clientD.heartbeat();
                    JsonNode pongAfter = awaitFrame(clientD, "PONG", REPRESENT_TIMEOUT_S);
                    JsonNode stale = awaitFrame(clientD, "MATCH_PENDING_CONFIRMATION", 1.0);
                    check("a resolved match is not re-presented", pongAfter != null && stale == null,
                            "frames=" + clientD.types());
                }
            }
        }
    }

    private static int finish() {
        System.out.println();
        System.out.println("W19 routing: " + PASSED.size() + " passed, " + FAILED.size() + " failed");
        for (Map.Entry<String, Object> number : NUMBERS.entrySet()) {
            System.out.println("  " + number.getKey() + ": " + number.getValue());
        }
        for (String failed : FAILED) {
            System.out.println("  FAILED: " + failed);
        }
        return FAILED.isEmpty() ? 0 : 1;
    }

    private static boolean check(String name, boolean condition, String detail) {
        (condition ? PASSED : FAILED).add(name);
        System.out.println("  [" + (condition ? "PASS" : "FAIL") + "] " + name
                + (detail == null || detail.isEmpty() ? "" : "  -> " + detail));
        return condition;
    }

    private static boolean check(String name, boolean condition) {
        return check(name, condition, "");
    }

    // ---------------------------------------------------------------------------------------------
    // The client's own clients: HTTP, WebSocket, Redis.
    // ---------------------------------------------------------------------------------------------

    private static String wsUrl(String base, String playerId) throws Exception {
        return base.replace("http", "ws") + "/v1/ws?token=" + token(playerId);
    }

    private static String token(String playerId) throws Exception {
        Http.Response login = Http.exchange("POST", auth + "/v1/auth/login", null,
                Map.of("playerId", playerId, "displayName", playerId));
        if (login.status() != 200) {
            throw new IllegalStateException("login failed for " + playerId + ": " + login.status() + " " + login.body());
        }
        return login.body().path("token").asText("");
    }

    private static Http.Response enqueue(String base, String playerId) throws Exception {
        return Http.exchange("POST", base + "/v1/matchmaking/tickets", token(playerId),
                Map.of("gameMode", "1v1", "region", "NA", "maxLatencyMs", 80));
    }

    /** Waits until the presence key names that instance, returning it (or {@code null}). */
    private static String waitPresence(Resp redis, String playerId, String instance) throws InterruptedException {
        return waitUntil(() -> {
            String presence = safe(() -> redis.get("presence:" + playerId));
            return presence != null && presence.equals(instance) ? presence : null;
        }, 3);
    }

    private static String waitForMatch(Resp redis, List<String> players, int timeoutSeconds) throws Exception {
        return waitUntil(() -> {
            Map<String, List<String>> matches = Pending.forPlayers(redis, players);
            return matches.isEmpty() ? null : matches.keySet().iterator().next();
        }, timeoutSeconds);
    }

    private static boolean answers(String url) {
        try {
            Http.Response response = Http.exchange("GET", url, null, null);
            return response.status() >= 200 && response.status() < 300;
        } catch (Exception unreachable) {
            return false;
        }
    }

    /** A metric as the scrape renders it, summed over the series that carry these labels. */
    private static Double metric(String url, String name, Map<String, String> labels) throws Exception {
        double total = 0;
        boolean found = false;
        Map<String, List<Scrape.Sample>> scraped;
        try {
            scraped = Scrape.samples(url);
        } catch (Exception noScrape) {
            // A scrape that is not answering reads as "no such series", which is what the check
            // then reports: the instance is not exposing the counter.
            return null;
        }
        for (Scrape.Sample sample : scraped.getOrDefault(name, List.of())) {
            boolean matches = labels.entrySet().stream()
                    .allMatch(label -> label.getValue().equals(sample.labels().get(label.getKey())));
            if (!matches) {
                continue;
            }
            total += sample.value();
            found = true;
        }
        return found ? total : null;
    }

    private static boolean isPositive(Double value) {
        return value != null && value > 0;
    }

    private static double number(Double value) {
        return value == null ? 0 : value;
    }

    private static String matchIdOf(JsonNode frame) {
        if (frame == null || frame.path("payload").path("matchId").isMissingNode()) {
            return "";
        }
        return frame.path("payload").path("matchId").asText("");
    }

    private static JsonNode awaitFrame(Ws socket, String type, double timeoutSeconds) throws InterruptedException {
        Instant deadline = Instant.now().plusMillis((long) (timeoutSeconds * 1000));
        while (Instant.now().isBefore(deadline)) {
            JsonNode found = socket.lastOfType(type);
            if (found != null) {
                return found;
            }
            Thread.sleep(5);
        }
        return null;
    }

    /** A call that may fail: a poll of Redis whose dependency is allowed to be down mid-scenario. */
    @FunctionalInterface
    private interface Throwing<T> {
        T get() throws Exception;
    }

    /**
     * Waits for a predicate to answer something truthy, returning it (or {@code null} on timeout).
     *
     * <p>A predicate that throws counts as "not yet": the polls ask Redis and Postgres, and the
     * scenario is *about* those dependencies going away, so a failure mid-poll is a state to
     * retry, not a reason to abort the run.
     */
    private static <T> T waitUntil(Throwing<T> predicate, int timeoutSeconds) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(timeoutSeconds);
        while (Instant.now().isBefore(deadline)) {
            T value = safe(predicate);
            if (value != null && !Boolean.FALSE.equals(value)) {
                return value;
            }
            Thread.sleep(100);
        }
        return null;
    }

    private static <T> T safe(Throwing<T> call) {
        try {
            return call.get();
        } catch (Exception failed) {
            return null;
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    /**
     * The scenario's WebSocket client: one connection, every text frame kept, so a check can ask
     * "did this arrive" long after it did.
     */
    private static final class Ws implements AutoCloseable {

        private final Collector collector;
        private final WebSocket socket;

        private Ws(WebSocket socket, Collector collector) {
            this.socket = socket;
            this.collector = collector;
        }

        static Ws connect(String url) throws Exception {
            Collector collector = new Collector();
            WebSocket socket = HttpClient.newHttpClient().newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .buildAsync(URI.create(url), collector)
                    .get(15, TimeUnit.SECONDS);
            return new Ws(socket, collector);
        }

        void send(Object frame) throws Exception {
            socket.sendText(Json.write(frame), true).get(5, TimeUnit.SECONDS);
        }

        /** Sends a heartbeat with an empty frame list, so the answer is unambiguous. */
        void heartbeat() throws Exception {
            collector.frames.clear();
            send(Map.of("type", "PING", "payload", Map.of()));
        }

        JsonNode lastOfType(String type) {
            JsonNode found = null;
            synchronized (collector.frames) {
                for (JsonNode frame : collector.frames) {
                    if (type.equals(frame.path("type").asText(""))) {
                        found = frame;
                    }
                }
            }
            return found;
        }

        List<String> types() {
            List<String> types = new ArrayList<>();
            synchronized (collector.frames) {
                for (JsonNode frame : collector.frames) {
                    types.add(frame.path("type").asText("?"));
                }
            }
            return types;
        }

        @Override
        public void close() {
            try {
                socket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(2, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException alreadyGone) {
                // The server may be the one that went away (phase 2 kills it): closing is best effort.
            }
        }

        /** Keeps every text frame the server pushes. */
        private static final class Collector implements WebSocket.Listener {

            private final List<JsonNode> frames = Collections.synchronizedList(new ArrayList<>());
            private final StringBuilder partial = new StringBuilder();

            @Override
            public void onOpen(WebSocket webSocket) {
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                partial.append(data);
                if (last) {
                    try {
                        frames.add(Json.parse(partial.toString()));
                    } catch (Exception unparseable) {
                        frames.add(Json.object().put("type", "unparseable").put("raw", partial.toString()));
                    }
                    partial.setLength(0);
                }
                webSocket.request(1);
                return null;
            }
        }
    }
}
