package com.victor.matchmaking.scenarios;

import java.time.Instant;
import java.util.Locale;

/**
 * The numeric helpers the request scripts used to get from one-line Python: comparisons (by exit
 * code, so {@code if mm.sh gt "$a" "$b"; then} keeps working) and the two clocks.
 */
final class Numbers {

    private Numbers() {
    }

    /** {@code gt A B} -> exit 0 when A > B. */
    static int gt(String[] argv) {
        Args args = Args.of(argv);
        return parse(args.positional(0, "A")) > parse(args.positional(1, "B")) ? 0 : 1;
    }

    /** {@code eq A B} -> exit 0 when A == B. */
    static int eq(String[] argv) {
        Args args = Args.of(argv);
        return parse(args.positional(0, "A")) == parse(args.positional(1, "B")) ? 0 : 1;
    }

    /** {@code approx A B [TOL]} -> exit 0 when |A - B| <= TOL (default 1.0). */
    static int approx(String[] argv) {
        Args args = Args.of(argv);
        double tolerance = args.positionalCount() > 2 ? parse(args.positional(2, "TOL")) : 1.0;
        return Math.abs(parse(args.positional(0, "A")) - parse(args.positional(1, "B"))) <= tolerance ? 0 : 1;
    }

    /** {@code between X LO HI} -> exit 0 when LO < X < HI. */
    static int between(String[] argv) {
        Args args = Args.of(argv);
        double value = parse(args.positional(0, "X"));
        return value > parse(args.positional(1, "LO")) && value < parse(args.positional(2, "HI")) ? 0 : 1;
    }

    /** {@code within X LO HI} -> exit 0 when LO <= X <= HI (a TTL, an inclusive budget). */
    static int within(String[] argv) {
        Args args = Args.of(argv);
        double value = parse(args.positional(0, "X"));
        return value >= parse(args.positional(1, "LO")) && value <= parse(args.positional(2, "HI")) ? 0 : 1;
    }

    /** {@code num X} -> the value rendered like Python's {@code print(float(X))}. */
    static int num(String[] argv) {
        Args args = Args.of(argv);
        System.out.println(Json.pythonDouble(parse(args.positional(0, "X"))));
        return 0;
    }

    /** {@code ceil A} -> the smallest integer that is not below A. */
    static int ceil(String[] argv) {
        Args args = Args.of(argv);
        System.out.println((long) Math.ceil(parse(args.positional(0, "A"))));
        return 0;
    }

    /**
     * {@code sum V...} -> the sum of the values, where {@code ABSENT} and an empty value count as
     * zero.
     *
     * <p>A scrape answers ABSENT for a counter that never moved; adding it as zero is what a
     * scenario wants ("the backlog adds up"), while a check that has to tell them apart asks
     * {@code metric-sum} directly.
     */
    static int sum(String[] argv) {
        double total = 0;
        for (String value : argv) {
            if (value == null || value.isEmpty() || "ABSENT".equals(value)) {
                continue;
            }
            total += parse(value);
        }
        System.out.println(Json.pythonDouble(total));
        return 0;
    }

    /** {@code sub A B [decimals]} -> A - B, rounded to 6 decimals by default. */
    static int sub(String[] argv) {
        Args args = Args.of(argv);
        double difference = parse(args.positional(0, "A")) - parse(args.positional(1, "B"));
        int decimals = args.positionalCount() > 2 ? Integer.parseInt(args.positional(2, "decimals")) : 6;
        System.out.println(Json.pythonDouble(round(difference, decimals)));
        return 0;
    }

    /** {@code le A B} -> exit 0 when A <= B. */
    static int le(String[] argv) {
        Args args = Args.of(argv);
        return parse(args.positional(0, "A")) <= parse(args.positional(1, "B")) ? 0 : 1;
    }

    /** {@code ge A B} -> exit 0 when A >= B. */
    static int ge(String[] argv) {
        Args args = Args.of(argv);
        return parse(args.positional(0, "A")) >= parse(args.positional(1, "B")) ? 0 : 1;
    }

    /** {@code mul A B [DECIMALS]} -> A * B, rounded (2 decimals by default). */
    static int mul(String[] argv) {
        Args args = Args.of(argv);
        int decimals = args.positionalCount() > 2 ? Integer.parseInt(args.positional(2, "DECIMALS")) : 2;
        System.out.printf(Locale.ROOT, "%." + decimals + "f%n",
                parse(args.positional(0, "A")) * parse(args.positional(1, "B")));
        return 0;
    }

    /** {@code ratio TOTAL COUNT [DECIMALS]} -> TOTAL / COUNT, or "0" when COUNT is zero. */
    static int ratio(String[] argv) {
        Args args = Args.of(argv);
        double total = parse(args.positional(0, "TOTAL"));
        double count = parse(args.positional(1, "COUNT"));
        int decimals = args.positionalCount() > 2 ? Integer.parseInt(args.positional(2, "DECIMALS")) : 3;
        if (count == 0) {
            System.out.println("0");
            return 0;
        }
        System.out.printf(Locale.ROOT, "%." + decimals + "f%n", total / count);
        return 0;
    }

    /** {@code now-millis} -> epoch milliseconds (the queue's score unit). */
    static int nowMillis(String[] argv) {
        System.out.println(System.currentTimeMillis());
        return 0;
    }

    /** {@code epoch-millis <iso-8601>} -> the instant in epoch milliseconds (a ticket's queuedAt). */
    static int epochMillis(String[] argv) {
        Args args = Args.of(argv);
        System.out.println(Instant.parse(args.positional(0, "the timestamp")).toEpochMilli());
        return 0;
    }

    /** {@code now} -> epoch seconds with millisecond precision (a start mark for {@code elapsed}). */
    static int now(String[] argv) {
        // Locale.ROOT: a machine set to pt-BR would print 1792585451,487 and the shell would read two
        // arguments — the failure this harness must never have, since it only shows up on some machines.
        System.out.printf(Locale.ROOT, "%.3f%n", Instant.now().toEpochMilli() / 1000.0);
        return 0;
    }

    /** {@code elapsed START} -> seconds since a {@code now} mark, two decimals. */
    static int elapsed(String[] argv) {
        Args args = Args.of(argv);
        double start = parse(args.positional(0, "START"));
        System.out.printf(Locale.ROOT, "%.2f%n", Instant.now().toEpochMilli() / 1000.0 - start);
        return 0;
    }

    static double round(double value, int decimals) {
        double factor = Math.pow(10, decimals);
        return Math.round(value * factor) / factor;
    }

    static double parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("a number was expected, got '" + value + "'");
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException("a number was expected, got '" + value + "'");
        }
    }
}
