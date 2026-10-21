package com.victor.matchmaking.scenarios;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The three small programs {@code requests/kafka/check_kafka.sh} needs around a Kafka round trip.
 *
 * <p>They exist because the measurement has to stay outside the thing being measured: a Java
 * producer and consumer inside the broker's container would be measuring their own JVM, and a
 * fresh JVM per batch would add its boot time to every message. The script keeps the broker JVMs
 * warm through a FIFO and these programs do the timing around them:
 *
 * <ul>
 *   <li>{@code stamp} — reads the console consumer's stdout and writes each line back with the
 *       millisecond it arrived on this side of the pipe;
 *   <li>{@code send} — writes {@code index:millis} records into the producer's stdin, one every
 *       {@code spacing} ms, so the write time is known before the broker sees it;
 *   <li>{@code report} — pairs the two and prints the write-to-CreateTime and write-to-stdout
 *       percentiles, failing when a message was lost or arrived out of order.
 * </ul>
 *
 * <p>They were three Python heredocs written into a temporary directory. The parsing is the same;
 * the difference is that the harness can be tested and type-checked like the rest of it.
 */
final class KafkaProbe {

    private KafkaProbe() {
    }

    static int dispatch(String[] argv) throws Exception {
        if (argv.length == 0) {
            throw new IllegalArgumentException("kafka <stamp|send|report|epoch> ...");
        }
        Args args = Args.of(java.util.Arrays.copyOfRange(argv, 1, argv.length));
        return switch (argv[0]) {
            case "stamp" -> stamp();
            case "send" -> send(args.requiredInt("count"), Numbers.parse(args.optional("spacing-ms", "5")),
                    args.required("fifo"));
            case "report" -> report(args.required("path"), args.requiredInt("expected"));
            case "epoch" -> {
                System.out.println(System.currentTimeMillis());
                yield 0;
            }
            default -> throw new IllegalArgumentException("unknown kafka command: " + argv[0]);
        };
    }

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
     * output, which is a property of the CLI, not of Kafka. Mixing them is what makes a measured
     * "Kafka latency" that is really a JVM printing to a pipe.
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
            // The warm-up message carries no index, and neither does anything the producer wrapped.
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
        java.util.Arrays.sort(sorted);
        return new long[] {sorted[0], sorted[sorted.length / 2], sorted[sorted.length - 1]};
    }
}
