package com.victor.matchmaking.scenarios;

import java.util.Arrays;

/**
 * The harness entry point: one command per thing a request script needs, dispatched by name.
 *
 * <p>It replaces the Python the scripts used to call, so the vocabulary is the scripts' own:
 * comparisons answer with an exit code, everything else prints one line (or one JSON object) on
 * stdout. Failures exit non-zero and say why on stderr — a misconfigured scenario must fail loudly,
 * not answer with an empty string that a check then reads as a verdict.
 */
public final class ScenarioCli {

    private ScenarioCli() {
    }

    public static void main(String[] argv) {
        if (argv.length == 0) {
            usage();
            System.exit(2);
        }
        int exit;
        try {
            exit = run(argv);
        } catch (IllegalArgumentException badUsage) {
            System.err.println(badUsage.getMessage());
            System.err.println();
            usage();
            exit = 2;
        } catch (Exception failure) {
            System.err.println(failure.getClass().getSimpleName() + ": " + failure.getMessage());
            exit = 1;
        }
        System.exit(exit);
    }

    private static int run(String[] argv) throws Exception {
        String command = argv[0];
        String[] rest = Arrays.copyOfRange(argv, 1, argv.length);
        return switch (command) {
            // Scrapes and JSON: the two one-liners every script used.
            case "metric" -> Scrape.metric(rest);
            case "json" -> Json.field(rest);
            case "json-array" -> Json.array(rest);
            case "json-length" -> Json.length(rest);
            case "json-sum" -> Json.sum(rest);
            case "json-int" -> Json.integer(rest);
            case "json-first-key" -> Json.firstKey(rest);
            case "json-values" -> Json.values(rest);
            case "metric-sum" -> Scrape.sum(rest);
            case "json-element" -> Json.element(rest);
            case "jwt" -> Jwt.claim(rest);
            // Numbers: comparisons by exit code, and the clocks.
            case "gt" -> Numbers.gt(rest);
            case "ge" -> Numbers.ge(rest);
            case "le" -> Numbers.le(rest);
            case "mul" -> Numbers.mul(rest);
            case "ceil" -> Numbers.ceil(rest);
            case "sum" -> Numbers.sum(rest);
            case "sub" -> Numbers.sub(rest);
            case "now-millis" -> Numbers.nowMillis(rest);
            case "eq" -> Numbers.eq(rest);
            case "approx" -> Numbers.approx(rest);
            case "between" -> Numbers.between(rest);
            case "within" -> Numbers.within(rest);
            case "num" -> Numbers.num(rest);
            case "now" -> Numbers.now(rest);
            case "elapsed" -> Numbers.elapsed(rest);
            case "ratio" -> Numbers.ratio(rest);
            case "epoch-millis" -> Numbers.epochMillis(rest);
            // Clients.
            case "queue" -> QueueProbe.dispatch(rest);
            case "partitions" -> PartitionProbe.dispatch(rest);
            case "backpressure" -> BackpressureProbe.dispatch(rest);
            case "observability" -> ObservabilityProbe.dispatch(rest);
            case "pairing" -> Pairing.dispatch(rest);
            case "resilience" -> ResilienceProbe.dispatch(rest);
            case "routing" -> RoutingProbe.run(rest);
            case "e2e" -> E2eProbe.run(rest);
            case "kafka" -> KafkaProbe.dispatch(rest);
            case "multiregion" -> MultiregionProbe.dispatch(rest);
            case "delay-proxy" -> DelayProxy.run(rest);
            case "help", "-h", "--help" -> {
                usage();
                yield 0;
            }
            default -> throw new IllegalArgumentException("unknown command: " + command);
        };
    }

    private static void usage() {
        System.err.println("""
                usage: mm.sh <command> [args]

                  metric <url> <series prefix>        the value of a scraped series (nothing when absent)
                  json <file|-> <dotted.field>        one field, Python-rendered ("" when null)
                  json-array <file|-> <field> [sub]   the values joined by a space
                  json-length <file|-> <field>        the size of an array
                  json-sum <file|-> <field>           the sum of an object's values
                  json-int <file|-> <field>           the value truncated to an integer
                  json-first-key <file|-> <field>     the first key of an object
                  json-values <file|-> <field>        an object's values, joined by a space
                  metric-sum <url> <series> [label]   the sum of a series, or ABSENT
                  json-element <file|-> <arr> <k> <v> <f>  the f of the element whose k is v
                  jwt <token> <claim>                 a claim of a token, unverified
                  gt|ge|le|eq <a> <b>                 exit 0 when true
                  mul <a> <b> [decimals]              a * b, rounded
                  ceil <a>                            the smallest integer >= a
                  sum <v>...                          the values added (ABSENT and empty are 0)
                  sub <a> <b> [decimals]              a - b, rounded
                  now-millis                          epoch milliseconds
                  approx <a> <b> [tol]                exit 0 when |a-b| <= tol (default 1.0)
                  between|within <x> <lo> <hi>        exit 0 when inside the range
                  num <x>                             a float like Python's print(float(x))
                  ratio <total> <count> [decimals]    total / count, "0" when count is zero
                  now / elapsed <start>               epoch seconds / seconds since a mark
                  epoch-millis <iso-8601>             an instant in epoch milliseconds
                  queue <players|enqueue|play|confirm|ticket> [flags]
                                                      the player client (W22/W23 capacity and lifecycle)
                  partitions <dist|metrics|cross|cap> [flags]
                                                      the W20 client (queue distribution, scrape, cap)
                  backpressure <fill|db> [flags]      the W17 client (queue cap, bounded DB writes)
                  observability <pending-match|slo> [flags]
                                                      the W15/W16 helpers (pending match, SLO ratios)
                  pairing <pair|three> [--prefix X]   pairable ids (and a stranger, for the reaper)
                  resilience <enqueue|drive|ticket-status|...> [flags]
                                                      the W18 client (dependency failures)
                  routing                             the W19 client (two instances, WebSockets)
                  multiregion <players|measure|directory|ticket|play> [flags]
                                                      the W21 client (probe, place, play a match)
                  delay-proxy --listen P --upstream P [--delay-ms N]
                                                      a one-way delay in front of a region
                  e2e [sweep] [--api URL] [--auth URL] [--redis-port P]
                                                      the whole end-to-end validation (52 checks),
                                                      or the confirmation-timeout timeline
                  kafka <stamp|send|report|epoch> ...  the timing around a Kafka round trip""");
    }
}
