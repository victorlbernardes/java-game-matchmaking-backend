package com.victor.matchmaking.scenarios;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reading one series out of a Prometheus scrape: the text the shell used to pipe through a
 * one-line Python. Same rule as before — the first line that starts with the series prefix answers,
 * and the value is its last whitespace-separated token — so every metric prefix in the scripts keeps
 * matching Micrometer's exposition, including its trailing comma inside the braces.
 */
final class Scrape {

    private Scrape() {
    }

    /** {@code metric <url> <series prefix>} -> the value, or nothing when the series is absent. */
    static int metric(String[] argv) throws Exception {
        Args args = Args.of(argv);
        String url = args.positional(0, "the scrape url");
        String prefix = args.positional(1, "the series prefix");
        System.out.println(value(url, prefix));
        return 0;
    }

    static String value(String url, String prefix) throws Exception {
        for (String line : Http.text(url).split("\n")) {
            if (line.startsWith(prefix) && !line.isBlank()) {
                String[] tokens = line.trim().split("\\s+");
                return tokens[tokens.length - 1];
            }
        }
        return "";
    }

    /**
     * {@code metric-sum <url> <series> [label-portion]} -> the sum of every sample of the series,
     * or {@code ABSENT} when the series is not exposed at all.
     *
     * <p>ABSENT and 0 are different answers: "the counter has not moved" and "this build does not
     * expose the counter" are different defects, and a check that cannot tell them apart passes
     * for the wrong reason.
     */
    static int sum(String[] argv) throws Exception {
        Args args = Args.of(argv);
        String url = args.positional(0, "the scrape URL");
        String name = args.positional(1, "the series name");
        String labelFilter = args.positionalCount() > 2 ? args.positional(2, "the label portion") : "";
        double total = 0;
        boolean seen = false;
        for (String line : Http.text(url).split("\n")) {
            if (line.isBlank() || line.startsWith("#") || !line.startsWith(name)) {
                continue;
            }
            int lastSpace = line.lastIndexOf(' ');
            String head = lastSpace < 0 ? line : line.substring(0, lastSpace);
            if (!labelFilter.isEmpty() && !head.contains(labelFilter)) {
                continue;
            }
            try {
                total += Double.parseDouble(line.substring(lastSpace + 1).trim());
                seen = true;
            } catch (NumberFormatException | StringIndexOutOfBoundsException notASample) {
                // A header-ish line that starts with the name is not a sample.
            }
        }
        System.out.println(seen ? Json.pythonDouble(total) : "ABSENT");
        return 0;
    }

    /** One exposition sample: its labels and its value. */
    record Sample(Map<String, String> labels, double value) {
    }

    /**
     * The whole exposition, grouped by series name.
     *
     * <p>A scrape line is {@code name{label="value",...} value}; Micrometer writes a trailing comma
     * inside the braces, and the closing brace is not always at the end of the labels, so the parse
     * is deliberately the same shape the Python probe used.
     */
    static Map<String, List<Sample>> samples(String url) throws Exception {
        Map<String, List<Sample>> byName = new LinkedHashMap<>();
        for (String line : Http.text(url).split("\n")) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String name = line.split("\\{")[0].split("\\s+")[0];
            Map<String, String> labels = new LinkedHashMap<>();
            int open = line.indexOf('{');
            int close = line.indexOf('}');
            if (open >= 0 && close > open) {
                for (String pair : line.substring(open + 1, close).split(",")) {
                    int equals = pair.indexOf('=');
                    if (equals < 0) {
                        continue;
                    }
                    String value = pair.substring(equals + 1).trim();
                    if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
                        value = value.substring(1, value.length() - 1);
                    }
                    labels.put(pair.substring(0, equals).trim(), value);
                }
            }
            String[] tokens = line.trim().split("\\s+");
            try {
                double value = Double.parseDouble(tokens[tokens.length - 1]);
                byName.computeIfAbsent(name, ignored -> new ArrayList<>()).add(new Sample(labels, value));
            } catch (NumberFormatException notASample) {
                // A series whose value cannot be read is not a measurement; the caller sees it as
                // an absent series instead of a zero.
            }
        }
        return byName;
    }
}
