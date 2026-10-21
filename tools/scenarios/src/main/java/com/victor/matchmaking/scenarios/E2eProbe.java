package com.victor.matchmaking.scenarios;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.victor.matchmaking.domain.player.DeterministicSkill;

/**
 * The end-to-end validation (W9–W13) of the matchmaking backend, against the running services and
 * the real Redis/Postgres/Kafka.
 *
 * <p>It is the scenario a person runs first: eleven scenarios, each one a promise the product makes
 * — a pair forms, a five-versus-five needs ten, a party is never split, a cancelled member takes the
 * party with it, the skill window widens with the wait, a confirmation that never comes is swept,
 * the confirmed match reaches the log as a typed event and the projection exactly once.
 *
 * <p>It was a Python script that asked a container ({@code docker exec redis-cli}) about Redis and
 * another ({@code docker exec psql}) about Postgres. Both are now addressed directly — the RESP
 * client the other scenarios already use, and a JDBC connection with the same defaults the services
 * read — so the scenario measures the system instead of the plumbing in front of it, and it does
 * not depend on a container name to know what a ticket says.
 *
 * <p>The shell it does keep is the Kafka verifier: decoding a record through the Schema Registry is
 * its own tool ({@code requests/kafka/verify_match_confirmed.sh}), and this scenario drives it as a
 * client would, parsing {@code EVENT_JSON=} out of its output.
 */
final class E2eProbe {

    private static final String DEFAULT_API = "http://localhost:8080";
    private static final String DEFAULT_AUTH = "http://localhost:8081";
    private static final String KAFKA_VERIFIER = "requests/kafka/verify_match_confirmed.sh";
    private static final int DEFAULT_REDIS_PORT = 6379;

    private final String api;
    private final String auth;
    private final int redisPort;
    private final String repositoryRoot;
    private final List<String> passed = new ArrayList<>();
    private final List<String> failed = new ArrayList<>();
    private final Map<String, String> tokens = new LinkedHashMap<>();
    private Resp redis;
    private Postgres postgres;

    private E2eProbe(String api, String auth, int redisPort, String repositoryRoot) {
        this.api = api;
        this.auth = auth;
        this.redisPort = redisPort;
        this.repositoryRoot = repositoryRoot;
    }

    static int run(String[] argv) throws Exception {
        Args args = Args.of(argv);
        E2eProbe e2e = new E2eProbe(
                args.optional("api", DEFAULT_API),
                args.optional("auth", DEFAULT_AUTH),
                args.intOf("redis-port", DEFAULT_REDIS_PORT),
                args.optional("repo", new File(".").getAbsolutePath()));
        e2e.redis = Resp.connect(e2e.redisPort);
        boolean sweep = args.positionalCount() == 1 && "sweep".equals(args.positional(0, ""));
        e2e.postgres = sweep ? null : Postgres.connect();
        try {
            if (sweep) {
                return e2e.confirmationSweep();
            }
            System.out.println("E2E validation of the matchmaking backend");
            e2e.health();
            e2e.oneVersusOne();
            e2e.fiveVersusFive();
            e2e.partyFills();
            e2e.partyNotFillable();
            e2e.cancelMember();
            e2e.removeMember();
            e2e.dissolveParty();
            e2e.widening();
            e2e.confirmationTimeout();
            e2e.matchConfirmed();
            e2e.matchProjected();
        } finally {
            if (e2e.postgres != null) {
                e2e.postgres.close();
            }
            e2e.redis.close();
        }
        return e2e.summary();
    }

    // ------------------------------------------------------------------ scenarios

    /**
     * The confirmation-timeout timeline, printed instead of asserted: the diagnostic a person runs
     * when a match dissolves at its deadline and the question is *when* each piece moved.
     *
     * <p>It enqueues two compatible players that never confirm and samples every four seconds, so
     * the pending match, its deadline and both tickets' versions can be read against the clock.
     * It was {@code requests/e2e/check_confirmation_sweep.py}; the numbers are the same, the reads
     * are direct instead of one {@code docker exec} per sample.
     */
    private int confirmationSweep() throws Exception {
        redis.flushAll();
        String ticketA = enqueue("p1", "1v1", "NA");
        String ticketB = enqueue("p37", "1v1", "NA");
        System.out.println("tickets: " + ticketA + " " + ticketB);
        long started = System.nanoTime();
        for (int sample = 0; sample < 11; sample++) {
            Thread.sleep(4_000);
            Map<String, List<String>> pending = Pending.all(redis);
            String expiresAt = "-";
            if (!pending.isEmpty()) {
                String raw = redis.get("match-pending:" + pending.keySet().iterator().next());
                if (raw != null && !raw.isEmpty()) {
                    expiresAt = Json.parse(raw).path("expiresAt").asText("-");
                }
            }
            JsonNode a = Json.parse(redis.get("ticket:" + ticketA));
            JsonNode b = Json.parse(redis.get("ticket:" + ticketB));
            System.out.printf(java.util.Locale.ROOT,
                    "t=%5.1fs pending=%d expiresAt=%s a=(v%d,%s) b=(v%d,%s)%n",
                    (System.nanoTime() - started) / 1_000_000_000.0, pending.size(), expiresAt,
                    a.path("version").asInt(0), a.path("status").asText(""),
                    b.path("version").asInt(0), b.path("status").asText(""));
        }
        return 0;
    }

    private void health() throws Exception {
        scenario("S0 health + auth");
        Http.Response response = Http.exchange("GET", api + "/health", null, null);
        check("GET /health -> 200 UP",
                response.status() == 200 && "UP".equals(response.body().path("status").asText("")),
                response.status() + " " + Json.write(response.body()));
        check("auth login returns a token", !token("p2").isEmpty());
    }

    private void oneVersusOne() throws Exception {
        scenario("S1 1v1 pairing (queue head, skill window 2)");
        List<String> pair = Pairing.playersWithOneSkill(2, "p").players();
        String a = pair.get(0);
        String b = pair.get(1);
        String ticketA = enqueue(a);
        enqueue(b);
        Map<String, List<String>> pending = pendingFor(pair, 8);
        List<List<String>> rosters = new ArrayList<>(pending.values());
        check("a match-pending exists", !pending.isEmpty(), pending.toString());
        check("the pending match has exactly 2 players", rosters.size() == 1 && rosters.get(0).size() == 2,
                pending.toString());
        check("the pair is exactly the two queued players",
                rosters.size() == 1 && Set.copyOf(rosters.get(0)).equals(Set.copyOf(pair)), pending.toString());
        check("ticket a is CONFIRMING", "CONFIRMING".equals(ticketStatus(ticketA)), ticketStatus(ticketA));
        check("queue is drained (zcard 0)", redis.zcard("queue:1v1:NA") == 0,
                String.valueOf(redis.zcard("queue:1v1:NA")));
    }

    private void fiveVersusFive() throws Exception {
        scenario("S2 5v5 pairing requires exactly 10 (W9)");
        List<String> players = Pairing.playersWithOneSkill(10, "p").players();
        List<String> tickets = new ArrayList<>();
        for (String player : players) {
            tickets.add(enqueue(player, "5v5", "NA"));
        }
        Map<String, List<String>> pending = pendingFor(players, 8);
        List<List<String>> rosters = new ArrayList<>(pending.values());
        check("one pending match formed", rosters.size() == 1, pending.toString());
        check("the pending match has 10 players", rosters.size() == 1 && rosters.get(0).size() == 10,
                pending.toString());
        check("all ten queued players are in it",
                rosters.size() == 1 && Set.copyOf(rosters.get(0)).equals(Set.copyOf(players)));
        boolean allConfirming = true;
        for (String ticket : tickets) {
            allConfirming &= "CONFIRMING".equals(ticketStatus(ticket));
        }
        check("all tickets CONFIRMING", allConfirming);
    }

    private void partyFills() throws Exception {
        scenario("S3 party fill: 3-member party + 7 solos -> one 10-player match (W11)");
        Pairing.Group group = Pairing.playersWithOneSkill(13, "p");
        List<String> pool = group.players();
        List<String> party = pool.subList(0, 3);
        List<String> solos = pool.subList(3, 10);
        String leader = party.get(0);

        Http.Response created = Http.exchange("POST", api + "/v1/parties", token(leader), null);
        check("POST /v1/parties -> 201", created.status() == 201, Json.write(created.body()));
        String partyId = created.body().path("partyId").asText("");
        for (String member : party.subList(1, party.size())) {
            Http.Response added = Http.exchange("POST", api + "/v1/parties/" + partyId + "/members",
                    token(member), null);
            check("add member " + member + " -> 200",
                    added.status() == 200 && contains(added.body().path("members"), member),
                    Json.write(added.body()));
        }

        Http.Response loaded = Http.exchange("GET", api + "/v1/parties/" + partyId, token(leader), null);
        check("GET party shows 3 members + aggregatedSkill",
                loaded.status() == 200 && loaded.body().path("members").size() == 3
                        && loaded.body().path("aggregatedSkill").asInt(-1) == group.skill(),
                Json.write(loaded.body()));

        String firstTicket = enqueue(leader, "5v5", "NA", partyId, party);
        JsonNode partyTicket = Json.parse(redis.get("ticket:" + firstTicket));
        check("party ticket carries the partyId", partyId.equals(partyTicket.path("partyId").asText("")),
                partyTicket.path("partyId").asText(""));

        for (String solo : solos) {
            enqueue(solo, "5v5", "NA");
        }

        List<String> everyone = new ArrayList<>(party);
        everyone.addAll(solos);
        Map<String, List<String>> pending = pendingFor(everyone, 8);
        List<List<String>> rosters = new ArrayList<>(pending.values());
        check("exactly one pending match", rosters.size() == 1, pending.toString());
        check("it has 10 players", rosters.size() == 1 && rosters.get(0).size() == 10, pending.toString());
        check("the 3 party members are together (party never split)",
                rosters.size() == 1 && rosters.get(0).containsAll(party), pending.toString());
        check("the queue is drained", redis.zcard("queue:5v5:NA") == 0,
                String.valueOf(redis.zcard("queue:5v5:NA")));
    }

    private void partyNotFillable() throws Exception {
        scenario("S4 party not fillable: 2 parties of 3 + 4 solos -> no match (party stays queued)");
        List<String> pool = Pairing.playersWithOneSkill(10, "p").players();
        List<String> partyA = pool.subList(0, 3);
        List<String> partyB = pool.subList(3, 6);
        List<String> solos = pool.subList(6, 10);

        String partyAId = Http.exchange("POST", api + "/v1/parties", token(partyA.get(0)), null)
                .body().path("partyId").asText("");
        for (String member : partyA.subList(1, partyA.size())) {
            Http.exchange("POST", api + "/v1/parties/" + partyAId + "/members", token(member), null);
        }
        String partyBId = Http.exchange("POST", api + "/v1/parties", token(partyB.get(0)), null)
                .body().path("partyId").asText("");
        for (String member : partyB.subList(1, partyB.size())) {
            Http.exchange("POST", api + "/v1/parties/" + partyBId + "/members", token(member), null);
        }

        enqueue(partyA.get(0), "5v5", "SA", partyAId, partyA);
        enqueue(partyB.get(0), "5v5", "SA", partyBId, partyB);
        for (String solo : solos) {
            enqueue(solo, "5v5", "SA");
        }
        Thread.sleep(3_000);
        Map<String, List<String>> pending = Pending.all(redis);
        check("no pending match was created", pending.isEmpty(), pending.toString());
        check("all 10 tickets stay SEARCHING", redis.zcard("queue:5v5:SA") == 10,
                String.valueOf(redis.zcard("queue:5v5:SA")));
    }

    private void cancelMember() throws Exception {
        scenario("S5 atomic cancel: cancelling one member cancels the whole party (W11)");
        List<String> pool = Pairing.playersWithOneSkill(3, "p").players();
        String partyId = Http.exchange("POST", api + "/v1/parties", token(pool.get(0)), null)
                .body().path("partyId").asText("");
        for (String member : pool.subList(1, pool.size())) {
            Http.exchange("POST", api + "/v1/parties/" + partyId + "/members", token(member), null);
        }
        enqueue(pool.get(0), "5v5", "EU", partyId, pool);
        List<String> partyTickets = partyTickets(partyId);
        check("party enqueue created 3 ticketed members", partyTickets.size() == 3,
                String.valueOf(partyTickets.size()));

        JsonNode victim = Json.parse(redis.get(partyTickets.get(0)));
        Http.Response cancelled = Http.exchange("DELETE",
                api + "/v1/matchmaking/tickets/" + victim.path("ticketId").asText(""),
                token(victim.path("playerId").asText("")), null);
        check("DELETE member ticket -> 200", cancelled.status() == 200,
                cancelled.status() + " " + Json.write(cancelled.body()));
        List<String> partyStatuses = statusList(partyTickets);
        check("all 3 party tickets are CANCELLED",
                partyStatuses.equals(List.of("CANCELLED", "CANCELLED", "CANCELLED")), partyStatuses.toString());
        check("the party is out of the queue", redis.zcard("queue:5v5:EU") == 0,
                String.valueOf(redis.zcard("queue:5v5:EU")));
    }

    private void removeMember() throws Exception {
        scenario("S6 removing a member via the party API cancels the whole party");
        List<String> pool = Pairing.playersWithOneSkill(3, "p").players();
        String partyId = Http.exchange("POST", api + "/v1/parties", token(pool.get(0)), null)
                .body().path("partyId").asText("");
        for (String member : pool.subList(1, pool.size())) {
            Http.exchange("POST", api + "/v1/parties/" + partyId + "/members", token(member), null);
        }
        enqueue(pool.get(0), "5v5", "EU", partyId, pool);
        List<String> partyTickets = partyTickets(partyId);

        Http.Response removed = Http.exchange("DELETE",
                api + "/v1/parties/" + partyId + "/members/" + pool.get(1), token(pool.get(0)), null);
        check("DELETE member -> 200", removed.status() == 200,
                removed.status() + " " + Json.write(removed.body()));
        List<String> partyStatuses = statusList(partyTickets);
        check("all 3 party tickets are CANCELLED",
                partyStatuses.equals(List.of("CANCELLED", "CANCELLED", "CANCELLED")), partyStatuses.toString());
        check("the party stays active with the remaining members",
                removed.body().path("active").asBoolean(false)
                        && !contains(removed.body().path("members"), pool.get(1)),
                Json.write(removed.body()));
    }

    private void dissolveParty() throws Exception {
        scenario("S7 dissolving a party cancels every member ticket");
        List<String> pool = Pairing.playersWithOneSkill(3, "p").players();
        String partyId = Http.exchange("POST", api + "/v1/parties", token(pool.get(0)), null)
                .body().path("partyId").asText("");
        for (String member : pool.subList(1, pool.size())) {
            Http.exchange("POST", api + "/v1/parties/" + partyId + "/members", token(member), null);
        }
        enqueue(pool.get(0), "5v5", "EU", partyId, pool);
        List<String> partyTickets = partyTickets(partyId);

        Http.Response dissolved = Http.exchange("DELETE", api + "/v1/parties/" + partyId,
                token(pool.get(0)), null);
        check("DELETE party -> 200", dissolved.status() == 200,
                dissolved.status() + " " + Json.write(dissolved.body()));
        check("party dissolved (active=false)", !dissolved.body().path("active").asBoolean(true),
                dissolved.body().path("active").toString());
        List<String> partyStatuses = statusList(partyTickets);
        check("all 3 party tickets are CANCELLED",
                partyStatuses.equals(List.of("CANCELLED", "CANCELLED", "CANCELLED")), partyStatuses.toString());
    }

    /**
     * W10: the skill gap waits instead of never pairing.
     *
     * <p>The two ids are pinned ({@code p1} = 87, {@code p4} = 90), not chosen by bucket: the check
     * is about a gap of 3, which only those two names guarantee. The first four seconds must find
     * nothing (half-range 2), and the wait must widen the range until the pair forms.
     */
    private void widening() throws Exception {
        scenario("S8 widening (W10): skill gap 3 not paired at t=0, paired after waiting");
        String a = "p1";
        String b = "p4";
        enqueue(a, "1v1", "NA");
        enqueue(b, "1v1", "NA");
        Thread.sleep(4_000);
        Map<String, List<String>> immediately = Pending.all(redis);
        check("not paired immediately (half-range 2)", immediately.isEmpty(), immediately.toString());

        Map<String, List<String>> widened = waitFor(
                () -> {
                    Map<String, List<String>> now = Pending.all(redis);
                    return now.isEmpty() ? null : now;
                }, 45, 1_000);
        check("paired after the head waited (half-range widened)", widened != null,
                String.valueOf(widened));
        if (widened != null) {
            check("the widened match contains both players",
                    Set.copyOf(widened.values().iterator().next()).equals(Set.of(a, b)), widened.toString());
        }
    }

    private void confirmationTimeout() throws Exception {
        scenario("S9 confirmation timeout: pending match is swept and tickets requeue");
        List<String> pair = Pairing.playersWithOneSkill(2, "p").players();
        String ticketA = enqueue(pair.get(0), "1v1", "NA");
        String ticketB = enqueue(pair.get(1), "1v1", "NA");
        Map<String, List<String>> pending = pendingFor(pair, 8);
        check("pending match created", !pending.isEmpty(), pending.toString());
        Thread.sleep(36_000);
        int versionA = version(ticketA);
        int versionB = version(ticketB);
        check("tickets were swept and re-queued/re-paired (version >= 3)",
                versionA >= 3 && versionB >= 3, versionA + ", " + versionB);
    }

    private void matchConfirmed() throws Exception {
        scenario("S10 confirmed match (W12): MATCH_CONFIRMED lands on the log as a typed Protobuf event");
        List<String> pair = Pairing.playersWithOneSkill(2, "p").players();
        String a = pair.get(0);
        String b = pair.get(1);
        String ticketA = enqueue(a, "1v1", "NA");
        String ticketB = enqueue(b, "1v1", "NA");
        Map<String, List<String>> pending = pendingFor(pair, 8);
        check("a pending match was created", !pending.isEmpty(), pending.toString());
        if (pending.isEmpty()) {
            return;
        }
        String matchId = pending.keySet().iterator().next();

        // The engine's input contract is the match_confirmations channel; the API translates a WS
        // MATCH_CONFIRM into exactly this message (see WsHub).
        long confirmedAt = System.currentTimeMillis();
        try (Resp.Confirmer confirmer = new Resp.Confirmer(redisPort)) {
            confirmer.confirm(matchId, a);
            confirmer.confirm(matchId, b);
        }

        waitFor(() -> !redis.exists("match-pending:" + matchId) ? Boolean.TRUE : null, 10, 200);
        check("both tickets reached CONFIRMED",
                "CONFIRMED".equals(ticketStatus(ticketA)) && "CONFIRMED".equals(ticketStatus(ticketB)),
                ticketStatus(ticketA) + ", " + ticketStatus(ticketB));

        // The match row must be durable: the engine publishes only after the insert, so the event
        // is never ahead of the source of truth.
        Http.Response history = Http.exchange("GET", api + "/v1/players/" + a + "/matches", token(a), null);
        check("the match is readable through the history API",
                history.status() == 200 && Json.write(history.body()).contains(matchId),
                history.status() + " " + abbreviate(Json.write(history.body())));

        // The durable event, decoded by the standalone verifier (single source of truth for reading
        // the log through the Schema Registry).
        Verified verified = verifyMatchConfirmed(matchId);
        check("MATCH_CONFIRMED decodes from the log with the registered schema", verified.rc() == 0);
        if (verified.rc() != 0 || verified.event() == null) {
            return;
        }
        JsonNode event = verified.event();
        check("the event carries the confirmed match id", matchId.equals(event.path("matchId").asText("")),
                event.path("matchId").asText(""));

        JsonNode meta = event.path("meta");
        check("the envelope carries type, contract version and a fresh event id",
                "MATCH_CONFIRMED".equals(meta.path("eventType").asText(""))
                        && meta.path("schemaVersion").asInt(0) == 1
                        && !meta.path("eventId").asText("").isEmpty()
                        && !meta.path("occurredAt").asText("").isEmpty(),
                Json.write(meta));
        check("correlationId groups the events of one match",
                matchId.equals(meta.path("correlationId").asText("")), meta.path("correlationId").asText(""));

        Map<String, Integer> teams = new LinkedHashMap<>();
        Map<String, Integer> skills = new LinkedHashMap<>();
        for (JsonNode player : event.path("players")) {
            String playerId = player.path("playerId").asText("");
            teams.put(playerId, player.path("team").asInt(0));
            skills.put(playerId, player.path("skillAtMatch").asInt(0));
        }
        check("the roster carries both players, one team each (1|2)",
                teams.keySet().equals(Set.of(a, b))
                        && !teams.getOrDefault(a, 0).equals(teams.getOrDefault(b, 0))
                        && teams.values().stream().allMatch(team -> team == 1 || team == 2),
                teams.toString());
        check("the skill snapshot is the skill the engine matched with",
                skills.getOrDefault(a, -1) == DeterministicSkill.of(a)
                        && skills.getOrDefault(b, -1) == DeterministicSkill.of(b),
                skills.toString());

        long latencyMs = verified.createTimeMs() - confirmedAt;
        check("the event reached the log within 5s of the confirmation",
                latencyMs >= 0 && latencyMs <= 5_000, latencyMs + " ms");
        System.out.println("  (measured: CONFIRM published -> MATCH_CONFIRMED on the log = " + latencyMs + " ms)");
    }

    /** W13: the confirmed match reaches the orchestrator's projection exactly once. */
    private void matchProjected() throws Exception {
        scenario("S11 idempotent consumer (W13): one projection row per confirmed match");
        List<String> pair = Pairing.playersWithOneSkill(2, "p").players();
        String a = pair.get(0);
        String b = pair.get(1);
        enqueue(a, "1v1", "NA");
        enqueue(b, "1v1", "NA");
        Map<String, List<String>> pending = pendingFor(pair, 8);
        if (pending.isEmpty()) {
            check("a pending match exists to confirm", false, "(engine not pairing)");
            return;
        }
        String matchId = pending.keySet().iterator().next();
        try (Resp.Confirmer confirmer = new Resp.Confirmer(redisPort)) {
            confirmer.confirm(matchId, a);
            confirmer.confirm(matchId, b);
        }
        waitFor(() -> !redis.exists("match-pending:" + matchId) ? Boolean.TRUE : null, 10, 200);

        Verified verified = verifyMatchConfirmed(matchId);
        check("the event is on the log, so the consumer has something to read", verified.rc() == 0);
        if (verified.rc() != 0 || verified.event() == null) {
            return;
        }
        String eventId = verified.event().path("meta").path("eventId").asText("");

        // The consumer is a separate process: it has to poll, project and commit. The check is on
        // the database, because that is where the effect is — the log line only says what the
        // consumer intended.
        String allocatons = "SELECT count(*) FROM match_allocations WHERE match_id = '" + matchId + "'";
        check("the orchestrator projected the confirmed match",
                waitFor(() -> projectionCount(allocatons) == 1 ? Boolean.TRUE : null, 20, 500) != null,
                "rows=" + projectionCount(allocatons) + " (start the orchestrator: see requests/README.md)");

        String row = postgres.scalar("SELECT game_mode || '|' || region || '|' || player_count || '|' || status "
                + "FROM match_allocations WHERE match_id = '" + matchId + "'");
        check("the projection carries mode, region, roster size and its own state",
                "1v1|NA|2|PENDING_ALLOCATION".equals(row), row);

        String inbox = eventId.isEmpty() ? "0" : postgres.scalar(
                "SELECT count(*) FROM processed_events WHERE event_id = '" + eventId + "' "
                        + "AND consumer_group = 'match-orchestrator'");
        check("the event is recorded in the inbox of the consumer group", "1".equals(inbox), "rows=" + inbox);

        // A consumer that projected the same match twice would show up here, and this is the shape
        // the replay check (requests/kafka/verify_idempotent_consumer.sh) proves for the whole topic.
        Thread.sleep(2_000);
        check("still exactly one row after the consumer committed and moved on",
                projectionCount(allocatons) == 1, "rows=" + projectionCount(allocatons));
    }

    // ------------------------------------------------------------------ helpers

    private record Verified(int rc, JsonNode event, long createTimeMs) {
    }

    /** The Kafka verifier's answer, parsed out of its {@code KEY=value} header lines. */
    private Verified verifyMatchConfirmed(String matchId) throws Exception {
        ProcessBuilder builder = new ProcessBuilder("bash", repositoryRoot + "/" + KAFKA_VERIFIER, matchId);
        builder.directory(new File(repositoryRoot));
        Process process = builder.start();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        process.waitFor();
        for (String line : stdout.split("\n")) {
            System.out.println("   " + line);
        }
        JsonNode event = null;
        long createTimeMs = 0;
        for (String line : stdout.split("\n")) {
            if (line.startsWith("EVENT_JSON=")) {
                event = Json.parse(line.substring("EVENT_JSON=".length()));
            } else if (line.startsWith("EVENT_CREATE_TIME_MS=")) {
                createTimeMs = Long.parseLong(line.substring("EVENT_CREATE_TIME_MS=".length()).trim());
            }
        }
        return new Verified(process.exitValue(), event, createTimeMs);
    }

    private int projectionCount(String sql) {
        try {
            return postgres.count(sql);
        } catch (Exception unreachable) {
            return 0;
        }
    }

    private void scenario(String title) throws Exception {
        System.out.println();
        System.out.println("=== " + title + " ===");
        redis.flushAll();
    }

    private void check(String name, boolean condition) {
        check(name, condition, "");
    }

    private void check(String name, boolean condition, String detail) {
        (condition ? passed : failed).add(name);
        System.out.println("  [" + (condition ? "PASS" : "FAIL") + "] " + name
                + (detail == null || detail.isEmpty() ? "" : "  -> " + detail));
    }

    private int summary() {
        System.out.println();
        System.out.println("===== RESULT: " + passed.size() + " passed, " + failed.size() + " failed =====");
        failed.forEach(failure -> System.out.println("  FAILED: " + failure));
        return failed.isEmpty() ? 0 : 1;
    }

    private String token(String playerId) throws Exception {
        String cached = tokens.get(playerId);
        if (cached != null) {
            return cached;
        }
        Http.Response login = Http.exchange("POST", auth + "/v1/auth/login", null,
                Map.of("playerId", playerId));
        if (login.status() != 200) {
            throw new IllegalStateException("login failed for " + playerId + ": " + login.status()
                    + " " + Json.write(login.body()));
        }
        String token = login.body().path("token").asText("");
        tokens.put(playerId, token);
        return token;
    }

    private String enqueue(String player) throws Exception {
        return enqueue(player, "1v1", "NA");
    }

    private String enqueue(String player, String mode, String region) throws Exception {
        return enqueue(player, mode, region, null, null);
    }

    private String enqueue(String player, String mode, String region, String partyId, List<String> members)
            throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("gameMode", mode);
        body.put("region", region);
        body.put("maxLatencyMs", 80);
        if (partyId != null) {
            body.put("partyId", partyId);
            body.put("members", members);
        }
        Http.Response response = Http.exchange("POST", api + "/v1/matchmaking/tickets", token(player), body);
        if (response.status() != 201) {
            throw new IllegalStateException("enqueue failed for " + player + ": " + response.status()
                    + " " + Json.write(response.body()));
        }
        return response.body().path("ticketId").asText("");
    }

    private String ticketStatus(String ticketId) throws Exception {
        String raw = redis.get("ticket:" + ticketId);
        return raw == null || raw.isEmpty() ? "" : Json.parse(raw).path("status").asText("");
    }

    private int version(String ticketId) throws Exception {
        String raw = redis.get("ticket:" + ticketId);
        return raw == null || raw.isEmpty() ? 0 : Json.parse(raw).path("version").asInt(0);
    }

    /** The tickets of one party, read out of Redis: the party's own roster lives on each of them. */
    private List<String> partyTickets(String partyId) throws Exception {
        List<String> found = new ArrayList<>();
        for (String key : redis.keys("ticket:*")) {
            String raw = redis.get(key);
            if (raw != null && !raw.isEmpty() && partyId.equals(Json.parse(raw).path("partyId").asText(""))) {
                found.add(key);
            }
        }
        return found;
    }

    /** One read of every ticket's status: the check and the detail must describe the same instant. */
    private List<String> statusList(List<String> tickets) throws Exception {
        List<String> found = new ArrayList<>();
        for (String ticket : tickets) {
            String raw = redis.get(ticket);
            found.add(raw == null || raw.isEmpty() ? "" : Json.parse(raw).path("status").asText(""));
        }
        return found;
    }

    private Map<String, List<String>> pendingFor(List<String> players, int seconds) throws Exception {
        Map<String, List<String>> found = waitFor(() -> {
            Map<String, List<String>> now = Pending.forPlayers(redis, players);
            return now.isEmpty() ? null : now;
        }, seconds, 500);
        return found == null ? Map.of() : found;
    }

    private interface Poll<T> {
        T get() throws Exception;
    }

    private <T> T waitFor(Poll<T> poll, int seconds, long intervalMs) throws Exception {
        long deadline = System.currentTimeMillis() + seconds * 1_000L;
        while (System.currentTimeMillis() < deadline) {
            T value = poll.get();
            if (value != null) {
                return value;
            }
            Thread.sleep(intervalMs);
        }
        return null;
    }

    private static boolean contains(JsonNode array, String value) {
        for (JsonNode element : array) {
            if (value.equals(element.asText(""))) {
                return true;
            }
        }
        return false;
    }

    private static String abbreviate(String text) {
        return text.length() <= 140 ? text : text.substring(0, 140);
    }
}
