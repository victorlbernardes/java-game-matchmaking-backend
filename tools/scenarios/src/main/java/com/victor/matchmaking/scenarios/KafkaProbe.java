package com.victor.matchmaking.scenarios;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Everything the Kafka and contract scripts need, without a second language: the timing around a
 * round trip, the read that decodes the log through the Schema Registry, the parsing of a dead
 * letter dump, and the two checks of the contract gate (a compatibility verdict and a mutation the
 * evolution check applies to a copy of the contract).
 *
 * <p>The scripts keep the orchestration — creating the topic, moving the consumer group, starting
 * and stopping the orchestrator — and call here for the parts that are logic. Two of these were
 * Python heredocs written into a temporary directory at run time ({@code compat.py},
 * {@code mutate.py}); the others were inline {@code python3 -} blocks. Writing code into a temp
 * directory to then run it is the arrangement this migration removes: the code is now compiled,
 * reachable from a test, and fails at build time instead of at three minutes into a scenario.
 */
final class KafkaProbe {

    private static final Pattern OFFSET_LINE = Pattern.compile("^Offset:(\\d+)\t");
    private static final Pattern CREATE_TIME = Pattern.compile("CreateTime:(\\d+)");
    private static final Pattern RECORD_OFFSET = Pattern.compile("Offset:(\\d+)");
    private static final Pattern DLQ_STAGE = Pattern.compile("dlq\\.stage:(\\w+)");
    private static final int MAX_READS = 200;

    private KafkaProbe() {
    }

    static int dispatch(String[] argv) throws Exception {
        if (argv.length == 0) {
            throw new IllegalArgumentException(
                    "kafka <stamp|send|report|epoch|verify-log|dlq-offset|dlq-checks|compat|mutate> ...");
        }
        Args args = Args.of(Arrays.copyOfRange(argv, 1, argv.length));
        return switch (argv[0]) {
            case "stamp" -> stamp();
            case "send" -> send(args.requiredInt("count"), Numbers.parse(args.optional("spacing-ms", "5")),
                    args.required("fifo"));
            case "report" -> report(args.required("path"), args.requiredInt("expected"));
            case "epoch" -> {
                System.out.println(System.currentTimeMillis());
                yield 0;
            }
            case "verify-log" -> verifyLog(args);
            case "dlq-offset" -> dlqOffset(args);
            case "dlq-checks" -> dlqChecks(args);
            case "compat" -> compat(args);
            case "mutate" -> mutate(args);
            default -> throw new IllegalArgumentException("unknown kafka command: " + argv[0]);
        };
    }

    // ------------------------------------------------------------------ the round trip

    /** Every line of stdin, prefixed with the millisecond it was read. Flushed per line: the shell tails it. */
    private static int stamp() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
        String line;
        while ((line = reader.readLine()) != null) {
            out.println(System.currentTimeMillis() + "\t" + line.trim());
            out.flush();
        }
        return 0;
    }

    /** Writes {@code index:millis} into the producer's FIFO, spaced out, keeping the producer JVM warm. */
    private static int send(int count, double spacingMs, String fifo) throws Exception {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(fifo), StandardCharsets.UTF_8))) {
            for (int index = 0; index < count; index++) {
                writer.write(index + ":" + System.currentTimeMillis() + "\n");
                writer.flush();
                Thread.sleep((long) spacingMs);
            }
        }
        return 0;
    }

    /**
     * The verdict on the round trip: everything sent was consumed, in order, and how long it took.
     *
     * <p>Two numbers, deliberately apart. The broker's own is the difference between the producer's
     * write and the record's {@code CreateTime}; the other includes the console consumer's text
     * output, which is a property of the CLI, not of Kafka.
     */
    private static int report(String path, int expected) throws IOException {
        record Row(int index, long sentAt, long createdAt, long receivedAt) {
        }
        List<Row> rows = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(path), StandardCharsets.UTF_8)) {
            String[] parts = line.strip().split("\t");
            if (parts.length != 3 || parts[0].isEmpty() || !parts[0].chars().allMatch(Character::isDigit)) {
                continue;
            }
            long receivedAt = Long.parseLong(parts[0]);
            int colon = parts[1].indexOf(':');
            if (colon < 0) {
                continue;
            }
            long createdAt = Long.parseLong(parts[1].substring(colon + 1).trim());
            String value = parts[2];
            if (value.startsWith("warmup:") || !value.contains(":")) {
                continue;
            }
            String[] fields = value.split(":", 2);
            rows.add(new Row(Integer.parseInt(fields[0].trim()), Long.parseLong(fields[1].trim()),
                    createdAt, receivedAt));
        }

        if (rows.size() != expected) {
            System.out.println("  FAIL  consumed " + rows.size() + "/" + expected + " messages");
            return 1;
        }
        List<Integer> indices = new ArrayList<>();
        for (Row row : rows) {
            indices.add(row.index());
        }
        List<Integer> sorted = new ArrayList<>(indices);
        Collections.sort(sorted);
        if (!indices.equals(sorted)) {
            System.out.println("  FAIL  messages arrived out of order on a single partition");
            return 1;
        }

        long[] broker = new long[rows.size()];
        long[] cli = new long[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            broker[i] = rows.get(i).createdAt() - rows.get(i).sentAt();
            cli[i] = rows.get(i).receivedAt() - rows.get(i).sentAt();
        }
        long[] brokerStats = stats(broker);
        long[] cliStats = stats(cli);
        System.out.println("  PASS  consumed " + rows.size() + "/" + expected
                + " messages, in order, none lost");
        System.out.println("        broker accept (write -> record CreateTime): min=" + brokerStats[0]
                + "ms p50=" + brokerStats[1] + "ms max=" + brokerStats[2] + "ms");
        System.out.println("        consumer CLI floor (write -> CLI stdout):    min=" + cliStats[0]
                + "ms p50=" + cliStats[1] + "ms max=" + cliStats[2] + "ms"
                + "  [kafka-console-consumer output granularity, not a broker property]");
        return 0;
    }

    /** min, median and max of a sample — the median by position, as the Python did. */
    private static long[] stats(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return new long[] {sorted[0], sorted[sorted.length / 2], sorted[sorted.length - 1]};
    }

    // ------------------------------------------------------------------ the log

    /**
     * Read the confirmed-match log through the Schema Registry and assert what it says.
     *
     * <p>A chunk at a time, on purpose: {@code kafka-protobuf-console-consumer} stops at the first
     * record it cannot decode, unlike the orchestrator, which dead-letters it and carries on. The
     * reader therefore reads from an offset, steps over the record it could not read (counting it)
     * and resumes after it — so one poison record on the topic cannot hide every record behind it
     * (the accounting check is what proves every offset was seen: decoded + undecodable = scanned).
     *
     * <p>The scan starts near the end ({@code --scan-records}, default 2000): the shared test topic
     * holds tens of thousands of records from the load runs, and each chunk is a {@code docker exec}
     * that starts a JVM (~8 s). {@code --scan-records 0} reads the whole log, which is what the dead
     * letter accounting wants.
     */
    private static int verifyLog(Args args) throws Exception {
        String matchId = args.optional("match-id", "");
        String subject = args.required("subject");
        String versions = args.optional("versions", "");
        String kafka = args.required("kafka-container");
        String registry = args.required("registry-container");
        String topic = args.required("topic");
        String partition = args.required("partition");
        String broker = args.required("broker");
        String registryBroker = args.required("registry-broker");
        String registryUrl = args.required("registry-url");
        int maxMessages = args.intOf("max-messages", 500);
        int quietWindowMs = args.intOf("quiet-window-ms", 4000);
        int scanRecords = args.intOf("scan-records", 2000);

        List<String> failures = new ArrayList<>();
        long end = logEndOffset(kafka, broker, topic, partition);
        int scanFrom = scanRecords > 0 ? (int) Math.max(0, end - scanRecords) : 0;

        List<Record> records = new ArrayList<>();
        long undecodable = 0;
        long offset = scanFrom;
        int reads = 0;
        while (offset < end && reads < MAX_READS) {
            reads++;
            List<Record> chunk = readFrom(registry, registryBroker, topic, partition, offset, maxMessages,
                    quietWindowMs, registryUrl);
            if (!chunk.isEmpty()) {
                records.addAll(chunk);
                offset = chunk.get(chunk.size() - 1).offset() + 1;
            } else {
                undecodable++;
                offset++;
            }
        }

        check(failures, "subject " + subject + " is registered", !versions.isBlank(),
                versions.isBlank() ? "(missing)" : versions.strip());
        check(failures, "records decode as MatchConfirmed", !records.isEmpty(),
                records.size() + " record(s)");
        if (undecodable > 0) {
            System.out.println("NOTE: " + undecodable + " record(s) on the topic do not decode - stepped over "
                    + "(the dead letter queue covers them: docs/adr/0019-dlq-retry.md)");
        }
        long scanned = end - scanFrom;
        check(failures, "every record in the scanned window was decoded or accounted for",
                records.size() + undecodable == scanned,
                "decoded=" + records.size() + " undecodable=" + undecodable + " scanned=" + scanned
                        + " log_end=" + end);
        if (scanFrom > 0) {
            System.out.println("NOTE: scanned offsets " + scanFrom + ".." + end + "; the " + scanFrom
                    + " earlier record(s) were not examined (SCAN_RECORDS=" + scanRecords
                    + "; SCAN_RECORDS=0 scans the whole log)");
        }
        System.out.println("DECODED_RECORDS=" + records.size());
        System.out.println("UNDECODABLE_RECORDS=" + undecodable);
        System.out.println("SCANNED_FROM=" + scanFrom);
        System.out.println("LOG_END_OFFSET=" + end);

        List<Record> selected = records;
        if (!matchId.isEmpty()) {
            selected = new ArrayList<>();
            for (Record record : records) {
                if (matchId.equals(record.event().path("matchId").asText(""))) {
                    selected.add(record);
                }
            }
            List<String> last = new ArrayList<>();
            for (Record record : records.subList(Math.max(0, records.size() - 5), records.size())) {
                last.add(record.event().path("matchId").asText(""));
            }
            check(failures, "record for matchId=" + matchId + " is on the log", !selected.isEmpty(),
                    "found=" + last + " (of " + records.size() + ")");
        }

        for (Record record : selected) {
            JsonNode event = record.event();
            String eventMatchId = event.path("matchId").asText("?");
            JsonNode meta = event.path("meta");
            JsonNode players = event.path("players");
            check(failures, eventMatchId + ": envelope carries type, contract version and event id",
                    "MATCH_CONFIRMED".equals(meta.path("eventType").asText(""))
                            && meta.path("schemaVersion").asInt(0) == 1
                            && !meta.path("eventId").asText("").isEmpty()
                            && !meta.path("occurredAt").asText("").isEmpty(),
                    Json.write(meta));
            ArrayNode roster = Json.object().arrayNode();
            boolean rosterOk = players.isArray() && !players.isEmpty();
            for (JsonNode player : players) {
                ObjectNode entry = Json.object();
                entry.put("playerId", player.path("playerId").asText(""));
                entry.put("team", player.path("team").asInt(0));
                entry.put("skillAtMatch", player.path("skillAtMatch").asInt(0));
                roster.add(entry);
                int team = player.path("team").asInt(0);
                int skill = player.path("skillAtMatch").asInt(0);
                rosterOk &= !player.path("playerId").asText("").isEmpty() && (team == 1 || team == 2)
                        && skill >= 0 && skill <= 100;
            }
            check(failures, eventMatchId + ": roster carries player, team (1|2) and skill snapshot",
                    rosterOk, Json.write(roster));
            System.out.println("EVENT_JSON=" + Json.write(event));
            System.out.println("EVENT_CREATE_TIME_MS=" + record.createTimeMs());
        }

        System.out.println("RESULT: " + failures.size() + " failed");
        return failures.isEmpty() ? 0 : 1;
    }

    private record Record(long createTimeMs, long offset, JsonNode event) {
    }

    /** The partition's end offset, from the broker: the record count to account for. */
    private static long logEndOffset(String kafka, String broker, String topic, String partition) throws Exception {
        String out = docker(kafka, "kafka-get-offsets", "--bootstrap-server", broker, "--topic", topic);
        for (String line : out.split("\n")) {
            String[] parts = line.strip().split(":");
            if (parts.length == 3 && partition.equals(parts[1])) {
                return Long.parseLong(parts[2].trim());
            }
        }
        return 0;
    }

    private static List<Record> readFrom(String registry, String broker, String topic, String partition,
            long offset, int maxMessages, int quietWindowMs, String registryUrl) throws Exception {
        String out = docker(registry, "kafka-protobuf-console-consumer",
                "--bootstrap-server", broker, "--topic", topic,
                "--partition", partition, "--offset", String.valueOf(offset),
                "--max-messages", String.valueOf(maxMessages), "--timeout-ms", String.valueOf(quietWindowMs),
                "--property", "schema.registry.url=" + registryUrl,
                "--property", "print.offset=true", "--property", "print.timestamp=true");
        List<Record> found = new ArrayList<>();
        for (String line : out.split("\n")) {
            // Decoded records start with the broker's CreateTime; log lines do not.
            if (!line.startsWith("CreateTime:")) {
                continue;
            }
            Matcher createTime = CREATE_TIME.matcher(line);
            Matcher recordOffset = RECORD_OFFSET.matcher(line);
            int brace = line.indexOf('{');
            if (!createTime.find() || !recordOffset.find() || brace < 0) {
                continue;
            }
            try {
                found.add(new Record(Long.parseLong(createTime.group(1)), Long.parseLong(recordOffset.group(1)),
                        Json.parse(line.substring(brace))));
            } catch (IOException notJson) {
                // A record the client cannot read at all is not a decoded record.
            }
        }
        return found;
    }

    private static void check(List<String> failures, String name, boolean condition, String detail) {
        System.out.println("  [" + (condition ? "PASS" : "FAIL") + "] " + name
                + (detail == null || detail.isEmpty() ? "" : "  -> " + detail));
        if (!condition) {
            failures.add(name);
        }
    }

    private static String docker(String... command) throws Exception {
        List<String> argv = new ArrayList<>();
        argv.add("docker");
        argv.add("exec");
        argv.addAll(Arrays.asList(command));
        Process process = new ProcessBuilder(argv).start();
        // stderr is drained on its own thread: the console consumer logs a page of configuration
        // per run, and a pipe nobody reads fills up and stops the process it belongs to.
        Thread drain = new Thread(() -> {
            try {
                process.getErrorStream().readAllBytes();
            } catch (IOException closed) {
                // The process ended; there is nothing left to drain.
            }
        }, "docker-stderr");
        drain.setDaemon(true);
        drain.start();
        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        process.waitFor();
        return stdout;
    }

    // ------------------------------------------------------------------ dead letters

    /**
     * One dead letter, as the dump leaves it: an {@code Offset:} header, the record's headers and
     * the payload as bytes.
     *
     * <p>Parsed at the byte level because the payload is bytes — a protobuf wire payload starts with
     * {@code 0x00} and is not valid UTF-8, and a header value (an error message) can contain
     * anything, including tabs and newlines. A record is therefore not a line: it starts at the line
     * that begins with {@code Offset:<n>\t} and runs until the next one.
     */
    private record Letter(long offset, String headers, byte[] payload) {
    }

    private static List<Letter> deadLetters(byte[] dump) {
        List<Long> offsets = new ArrayList<>();
        List<byte[]> bodies = new ArrayList<>();
        ByteArrayOutputStream current = null;
        int index = 0;
        while (index < dump.length) {
            int lineEnd = index;
            while (lineEnd < dump.length && dump[lineEnd] != '\n') {
                lineEnd++;
            }
            byte[] line = Arrays.copyOfRange(dump, index, lineEnd);
            index = lineEnd + 1;
            String head = new String(line, 0, Math.min(line.length, 32), StandardCharsets.ISO_8859_1);
            Matcher match = OFFSET_LINE.matcher(head);
            if (match.find() && match.start() == 0) {
                if (current != null) {
                    bodies.add(current.toByteArray());
                }
                offsets.add(Long.parseLong(match.group(1)));
                int bodyStart = 0;
                int tabs = 0;
                while (bodyStart < line.length && tabs < 1) {
                    if (line[bodyStart] == '\t') {
                        tabs++;
                    }
                    bodyStart++;
                }
                current = new ByteArrayOutputStream();
                current.write(line, bodyStart, line.length - bodyStart);
            } else if (head.startsWith("Processed a total")) {
                continue;
            } else if (current != null) {
                current.write('\n');
                current.write(line, 0, line.length);
            }
        }
        if (current != null) {
            bodies.add(current.toByteArray());
        }

        List<Letter> letters = new ArrayList<>();
        for (int i = 0; i < bodies.size(); i++) {
            byte[] body = bodies.get(i);
            int tab = 0;
            while (tab < body.length && body[tab] != '\t') {
                tab++;
            }
            String headers = new String(body, 0, tab, StandardCharsets.UTF_8);
            byte[] payload = tab < body.length ? Arrays.copyOfRange(body, tab + 1, body.length) : new byte[0];
            letters.add(new Letter(offsets.get(i), headers, payload));
        }
        return letters;
    }

    /** The offset of the newest dead letter of a stage: after a rewind the same record can appear twice. */
    private static int dlqOffset(Args args) throws IOException {
        byte[] dump = Files.readAllBytes(Path.of(args.required("path")));
        String stage = args.required("stage");
        long found = -1;
        for (Letter letter : deadLetters(dump)) {
            if (letter.headers().contains("dlq.stage:" + stage)) {
                found = letter.offset();
            }
        }
        System.out.println(found < 0 ? "" : String.valueOf(found));
        return 0;
    }

    /**
     * The assertions on a dead letter: where it came from, who gave up, how many attempts, what
     * failed, and whether the payload survived the trip verbatim.
     *
     * <p>The assertions are about the <b>newest</b> letter of the stage, not about all of them: a
     * topic accumulates across runs (a record cannot be deleted from a Kafka log), so "the topic
     * holds the letter this run produced" is the question, and answering it with a letter from a
     * previous run is a scenario answering about itself.
     */
    private static int dlqChecks(Args args) throws IOException {
        byte[] dump = Files.readAllBytes(Path.of(args.required("path")));
        String stage = args.required("stage");
        String wantOffset = args.required("offset");
        String wantEventId = args.required("event-id");
        String wantPayload = args.required("payload");
        String topic = args.required("topic");
        String group = args.required("group");
        String partition = args.required("partition");

        List<Letter> letters = new ArrayList<>();
        for (Letter letter : deadLetters(dump)) {
            letters.add(letter);
        }
        List<String> out = new ArrayList<>();
        checkLine(out, "the dead letter topic received a record", !letters.isEmpty(),
                letters.size() + " record(s)");

        Set<String> stages = new TreeSet<>();
        for (Letter letter : letters) {
            Matcher match = DLQ_STAGE.matcher(letter.headers());
            stages.add(match.find() ? match.group(1) : "?");
        }
        Letter newest = null;
        for (Letter letter : letters) {
            if (letter.headers().contains("dlq.stage:" + stage)) {
                newest = letter;
            }
        }
        checkLine(out, "a dead letter carries stage=" + stage, newest != null,
                "stages on the topic: " + String.join(",", stages));

        if (newest != null) {
            String headers = newest.headers();
            List<String> origin = new ArrayList<>();
            for (String header : headers.split(",\\s*")) {
                if (header.startsWith("dlq.original")) {
                    origin.add(header);
                }
            }
            checkLine(out, "it records where the record came from (offset " + wantOffset + ")",
                    headers.contains("dlq.original-offset:" + wantOffset)
                            && headers.contains("dlq.original-partition:" + partition)
                            && headers.contains("dlq.original-topic:" + topic),
                    String.join(" ", origin));
            checkLine(out, "it records which consumer group gave up on it",
                    headers.contains("dlq.consumer-group:" + group), "");
            List<String> attempts = new ArrayList<>();
            for (String header : headers.split(",\\s*")) {
                if (header.startsWith("dlq.attempts")) {
                    attempts.add(header);
                }
            }
            checkLine(out, "it records how many attempts were made (3)", headers.contains("dlq.attempts:3"),
                    String.join(" ", attempts));
            List<String> errors = new ArrayList<>();
            for (String header : headers.split(",\\s*")) {
                if (header.startsWith("dlq.error")) {
                    errors.add(header);
                }
            }
            checkLine(out, "it records the failure, class and message",
                    headers.contains("dlq.error-class:") && headers.contains("dlq.error-message:"),
                    String.join(" ", errors));
            if ("-".equals(wantEventId)) {
                checkLine(out, "it does not invent an event id for a payload that never decoded",
                        !headers.contains("dlq.event-id:"), "");
            } else {
                checkLine(out, "it records the event id of the payload that failed (" + wantEventId + ")",
                        headers.contains("dlq.event-id:" + wantEventId), "");
            }
            byte[] payload = newest.payload();
            if ("-".equals(wantPayload)) {
                checkLine(out, "it carries the original payload", payload.length > 0,
                        payload.length + " byte(s)");
            } else {
                byte[] expected = wantPayload.getBytes(StandardCharsets.UTF_8);
                String got = new String(payload, 0, Math.min(payload.length, 80), StandardCharsets.ISO_8859_1);
                checkLine(out, "it carries the original payload verbatim", Arrays.equals(payload, expected),
                        "got " + got + (payload.length > 80 ? "..." : ""));
            }
        }

        out.forEach(System.out::println);
        return 0;
    }

    private static void checkLine(List<String> out, String name, boolean condition, String detail) {
        out.add((condition ? "CHECK_PASS:" : "CHECK_FAIL:") + name
                + (detail == null || detail.isEmpty() ? "" : "  -> " + detail));
    }

    // ------------------------------------------------------------------ the contract

    /**
     * Ask the registry whether the mutated contract is compatible with the registered one.
     *
     * <p>The reference version of the imported types is resolved from the registry, not assumed: an
     * import that has moved on would otherwise make the check compare against the wrong schema.
     */
    private static int compat(Args args) throws Exception {
        String registryUrl = args.required("registry");
        String subject = args.required("subject");
        String schemaPath = args.required("schema");
        String typesProto = args.required("types-proto");

        Http.Response latest = Http.exchange("GET",
                registryUrl + "/subjects/" + encode(typesProto) + "/versions/latest", null, null);
        int typesVersion = latest.body().path("version").asInt(0);

        ObjectNode body = Json.object();
        body.put("schemaType", "PROTOBUF");
        body.put("schema", Files.readString(Path.of(schemaPath), StandardCharsets.UTF_8));
        ArrayNode references = body.putArray("references");
        ObjectNode reference = references.addObject();
        reference.put("name", typesProto);
        reference.put("subject", typesProto);
        reference.put("version", typesVersion);

        Http.Response verdict = Http.exchange("POST",
                registryUrl + "/compatibility/subjects/" + encode(subject) + "/versions/latest", null, body);
        if (verdict.status() >= 400) {
            String raw = verdict.body().has("raw") ? verdict.body().get("raw").asText() : Json.write(verdict.body());
            System.out.println("HTTP " + verdict.status() + ": "
                    + raw.substring(0, Math.min(200, raw.length())));
        } else {
            System.out.println(Json.write(verdict.body()));
        }
        return 0;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Apply one of the mutations the evolution check compares: the two a correct checker must refuse
     * (renumbering, a field's kind) and the two it must accept (an added field, a removal done with
     * {@code reserved}), plus the enum case and the plain removal the checker must also refuse.
     *
     * <p>Each mutation is a single exact replacement, and a mutation whose anchor text has moved fails
     * loudly: a control that silently stops applying would leave the checker untested and the run
     * green.
     */
    private static int mutate(Args args) throws IOException {
        String mutation = args.required("case");
        Path eventProto = Path.of(args.required("event-proto"));
        Path typesProto = Path.of(args.required("types-proto"));
        switch (mutation) {
            case "additive" -> edit(eventProto, "\n}\n\nmessage MatchPlayerState {",
                    "\n  string correlation_note = 6;\n}\n\nmessage MatchPlayerState {");
            case "renumbered" -> edit(eventProto, "repeated MatchPlayerState players = 5;",
                    "repeated MatchPlayerState players = 6;");
            case "removed" -> edit(eventProto, "  repeated MatchPlayerState players = 5;\n", "");
            case "reserved" -> edit(eventProto, "  repeated MatchPlayerState players = 5;\n",
                    "  reserved 5;\n  reserved \"players\";\n");
            case "kind" -> edit(eventProto, "int32 skill_at_match = 3;", "sint32 skill_at_match = 3;");
            case "enum" -> edit(typesProto, "GAME_MODE_5V5 = 2;", "GAME_MODE_5V5 = 6;");
            default -> {
                System.err.println("unknown case: " + mutation);
                return 1;
            }
        }
        return 0;
    }

    private static void edit(Path path, String anchor, String replacement) throws IOException {
        String text = Files.readString(path, StandardCharsets.UTF_8);
        if (!text.contains(anchor)) {
            System.err.println("the mutation no longer matches " + path + ": " + anchor);
            System.exit(1);
        }
        int at = text.indexOf(anchor);
        Files.writeString(path, text.substring(0, at) + replacement + text.substring(at + anchor.length()),
                StandardCharsets.UTF_8);
    }
}
