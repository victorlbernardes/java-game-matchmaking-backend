package com.victor.matchmaking.scenarios;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * JSON at the harness's boundary: the HTTP API and Redis speak JSON, and the shell reads single
 * fields with {@code mm.sh json <file> <field>}.
 *
 * <p>{@link #pythonText(JsonNode)} exists because the request scripts were written against Python's
 * {@code print()} of a parsed value: a boolean is {@code True}/{@code False} there and
 * {@code true}/{@code false} here, and the scripts compare strings. Keeping the rendering means the
 * migration changes no assertion.
 */
final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {
    }

    static JsonNode parse(String text) throws JsonProcessingException {
        return MAPPER.readTree(text == null || text.isBlank() ? "{}" : text);
    }

    static JsonNode read(String source) throws IOException {
        if ("-".equals(source)) {
            return parse(new String(System.in.readAllBytes(), StandardCharsets.UTF_8));
        }
        Path path = Path.of(source);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("no such file: " + source);
        }
        return parse(Files.readString(path));
    }

    static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException impossibleForMaps) {
            throw new IllegalStateException(impossibleForMaps);
        }
    }

    static ObjectNode object() {
        return MAPPER.createObjectNode();
    }

    /**
     * The node at a dotted path, or {@code null}.
     *
     * <p>A numeric step indexes an array ({@code players.0}, {@code regions.1}), because that is how
     * the scripts ask for "the first of these" — the same syntax the Python helper accepted.
     */
    static JsonNode at(JsonNode root, String dotted) {
        JsonNode current = root;
        for (String step : dotted.split("\\.")) {
            if (current == null || current.isMissingNode() || current.isNull()) {
                return null;
            }
            if (current.isArray() && step.matches("\\d+")) {
                int index = Integer.parseInt(step);
                current = index < current.size() ? current.get(index) : null;
            } else {
                current = current.get(step);
            }
        }
        return current;
    }

    /** A single value as the request scripts printed it, or {@code ""} when absent. */
    static String pythonText(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return "";
        }
        if (node.isBoolean()) {
            return node.booleanValue() ? "True" : "False";
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        if (node.isIntegralNumber()) {
            return Long.toString(node.longValue());
        }
        if (node.isFloatingPointNumber()) {
            return pythonDouble(node.doubleValue());
        }
        return node.toString();
    }

    /** {@code json <file|-> <dotted.field>} -> the value, {@code ""} when null or absent. */
    static int field(String[] argv) throws IOException {
        Args args = Args.of(argv);
        String source = args.positional(0, "the JSON source (- for stdin)");
        String path = args.positional(1, "the field");
        System.out.println(pythonText(at(read(source), path)));
        return 0;
    }

    /**
     * {@code json-array <file|-> <dotted.field> [<subfield>]} -> the values joined by a space.
     *
     * <p>With a subfield, each element is an object (the {@code match-pending} roster, whose players
     * are objects with a {@code playerId}); without one, each element is a scalar (the id list).
     */
    static int array(String[] argv) throws IOException {
        Args args = Args.of(argv);
        String source = args.positional(0, "the JSON source (- for stdin)");
        String path = args.positional(1, "the array field");
        String subfield = args.positionalCount() > 2 ? args.positional(2, "the element field") : null;
        JsonNode node = at(read(source), path);
        StringBuilder out = new StringBuilder();
        if (node != null && node.isArray()) {
            for (JsonNode element : node) {
                String value = subfield == null ? pythonText(element) : pythonText(element.get(subfield));
                if (value.isEmpty()) {
                    continue;
                }
                if (!out.isEmpty()) {
                    out.append(' ');
                }
                out.append(value);
            }
        }
        System.out.println(out);
        return 0;
    }

    /** {@code json-length <file|-> <dotted.field>} -> the size of an array (or of an object). */
    static int length(String[] argv) throws IOException {
        Args args = Args.of(argv);
        JsonNode node = at(read(args.positional(0, "the JSON source (- for stdin)")),
                args.positional(1, "the field"));
        System.out.println(node != null && (node.isArray() || node.isObject()) ? node.size() : 0);
        return 0;
    }

    /**
     * {@code json-element <file|-> <array field> <key> <value> <field>} -> the field of the first
     * element whose {@code key} equals {@code value} ("" when no element matches).
     *
     * <p>A single condition on purpose: this answers "what does *that* one say", which is the
     * difference between checking that some server is stale and checking that the server that went
     * stale is the one under test.
     */
    static int element(String[] argv) throws IOException {
        Args args = Args.of(argv);
        JsonNode node = at(read(args.positional(0, "the JSON source (- for stdin)")),
                args.positional(1, "the array field"));
        String key = args.positional(2, "the element key");
        String expected = args.positional(3, "the expected value");
        String field = args.positional(4, "the field to print");
        if (node != null && node.isArray()) {
            for (JsonNode candidate : node) {
                if (expected.equals(pythonText(candidate.get(key)))) {
                    System.out.println(pythonText(candidate.get(field)));
                    return 0;
                }
            }
        }
        System.out.println();
        return 0;
    }

    /**
     * {@code json-first-key <file|-> <dotted.field>} -> the first key of an object ("" when empty).
     *
     * <p>The status histogram is keyed by the status code, so "which status did the run get" is
     * its first key — a question about the answer, not about its order.
     */
    static int firstKey(String[] argv) throws IOException {
        Args args = Args.of(argv);
        JsonNode node = at(read(args.positional(0, "the JSON source (- for stdin)")),
                args.positional(1, "the object field"));
        System.out.println(node != null && node.isObject() && !node.isEmpty()
                ? node.fieldNames().next() : "");
        return 0;
    }

    /** {@code json-values <file|-> <dotted.field>} -> an object's values joined by a space. */
    static int values(String[] argv) throws IOException {
        Args args = Args.of(argv);
        JsonNode node = at(read(args.positional(0, "the JSON source (- for stdin)")),
                args.positional(1, "the object field"));
        StringBuilder joined = new StringBuilder();
        if (node != null && node.isObject()) {
            node.forEach(value -> {
                if (!joined.isEmpty()) {
                    joined.append(' ');
                }
                joined.append(pythonText(value));
            });
        }
        System.out.println(joined);
        return 0;
    }

    /** {@code json-int <file|-> <dotted.field>} -> the value truncated to an integer ("0" absent). */
    static int integer(String[] argv) throws IOException {
        Args args = Args.of(argv);
        JsonNode node = at(read(args.positional(0, "the JSON source (- for stdin)")),
                args.positional(1, "the field"));
        System.out.println(node == null || !node.isNumber() ? "0" : Long.toString(node.asLong()));
        return 0;
    }

    /** {@code json-sum <file|-> <dotted.field>} -> the sum of an object's numeric values. */
    static int sum(String[] argv) throws IOException {
        Args args = Args.of(argv);
        JsonNode node = at(read(args.positional(0, "the JSON source (- for stdin)")),
                args.positional(1, "the field"));
        double total = 0;
        if (node != null && node.isObject()) {
            for (JsonNode value : node) {
                if (value.isNumber()) {
                    total += value.doubleValue();
                }
            }
        }
        System.out.println(pythonDouble(total));
        return 0;
    }

    /** Reads all of a stream as UTF-8 (the HTTP and RESP clients both hand back bytes). */
    static String text(InputStream stream) throws IOException {
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }

    /** A JSON number rendered the way {@code str(float)} does in Python ({@code 12.0}, not {@code 12}). */
    static String pythonDouble(double value) {
        // BigDecimal keeps 1792232380054 from turning into 1.792232380054E12, which is what
        // Double.toString would print and what one scenario note used to quote.
        BigDecimal decimal = BigDecimal.valueOf(value).stripTrailingZeros();
        if (decimal.scale() <= 0) {
            return decimal.toPlainString() + ".0";
        }
        return decimal.toPlainString();
    }
}
