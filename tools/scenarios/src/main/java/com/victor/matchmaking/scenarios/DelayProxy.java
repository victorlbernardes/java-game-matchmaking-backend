package com.victor.matchmaking.scenarios;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * A one-way delay in front of an endpoint, so "distant" is a real number and not a config value
 * (W21, ADR 0039).
 *
 * <p>The sprint proves that a player is placed in a region only when the distance to it fits the
 * player's own ceiling, and that failing over costs that distance. On one machine every region
 * answers in a millisecond, so the distance has to be produced: this proxy sits in front of one
 * region's API, waits {@code --delay-ms} before forwarding the request and the same again before
 * returning the answer. A round trip through it therefore costs {@code 2 × delay}, which is exactly
 * what the server measures when the client probes that region — the number in the scenario is the
 * proxy's, but the measurement is the production one (two requests, timed by the server).
 *
 * <p>HTTP only. The game socket is out of scope for that sprint: what is being decided is *where*
 * the player goes, not how the socket behaves once it is there.
 *
 * <p>It was a Python {@code ThreadingHTTPServer}; it is now the JDK's own server, so the scenario
 * has one language and one HTTP stack — the same client that drives the scenario also forwards for
 * it, which is the shortest possible path between "what a player sees" and "what the harness runs".
 */
final class DelayProxy {

    /** Headers that describe one hop, not the message: forwarding them would lie about the next. */
    private static final Set<String> HOP_BY_HOP = Set.of(
            "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te",
            "trailers", "transfer-encoding", "upgrade");

    private DelayProxy() {
    }

    static int run(String[] argv) throws Exception {
        Args args = Args.of(argv);
        int listen = args.intOf("listen", 0);
        int upstream = args.intOf("upstream", 0);
        if (listen == 0 || upstream == 0) {
            throw new IllegalArgumentException("delay-proxy --listen <port> --upstream <port> [--delay-ms N]");
        }
        String upstreamHost = args.optional("upstream-host", "localhost");
        long delayMs = (long) Numbers.parse(args.optional("delay-ms", "40"));
        URI upstreamBase = URI.create("http://" + upstreamHost + ":" + upstream);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", listen), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/", exchange -> forward(exchange, client, upstreamBase, delayMs));
        server.start();
        System.out.println("{\"listening\":" + listen + ",\"upstream\":" + upstream
                + ",\"delay_ms_each_way\":" + delayMs + "}");
        System.out.flush();

        CountDownLatch forever = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0);
            forever.countDown();
        }));
        forever.await();
        return 0;
    }

    private static void forward(HttpExchange exchange, HttpClient client, URI upstreamBase, long delayMs) {
        try {
            byte[] requestBody = exchange.getRequestBody().readAllBytes();
            HttpRequest.Builder request = HttpRequest.newBuilder(
                            upstreamBase.resolve(exchange.getRequestURI().toString()))
                    .timeout(Duration.ofSeconds(15))
                    .method(exchange.getRequestMethod(),
                            requestBody.length == 0
                                    ? HttpRequest.BodyPublishers.noBody()
                                    : HttpRequest.BodyPublishers.ofByteArray(requestBody));
            copyHeaders(exchange, request);

            // The delay on the way in: the request reaches the region after the cable.
            Thread.sleep(delayMs);
            HttpResponse<byte[]> response;
            try {
                response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
            } catch (IOException upstreamDown) {
                Thread.sleep(delayMs);
                respond(exchange, 502, ("{\"error\":\"upstream unavailable\",\"reason\":\""
                        + upstreamDown.getMessage().replace("\"", "'") + "\"}").getBytes(),
                        Map.of("content-type", "application/json"));
                return;
            }
            Thread.sleep(delayMs);
            respond(exchange, response.statusCode(), response.body(), response.headers().map().entrySet().stream()
                    .filter(entry -> !HOP_BY_HOP.contains(entry.getKey().toLowerCase(Locale.ROOT))
                            && !"content-length".equalsIgnoreCase(entry.getKey()))
                    .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,
                            entry -> entry.getValue().get(0), (first, second) -> first)));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Exception failed) {
            try {
                respond(exchange, 502, ("{\"error\":\"proxy failure\",\"reason\":\""
                        + failed.getClass().getSimpleName() + "\"}").getBytes(), Map.of());
            } catch (IOException closed) {
                // The client went away: there is nobody left to tell.
            }
        }
    }

    private static void copyHeaders(HttpExchange exchange, HttpRequest.Builder request) {
        exchange.getRequestHeaders().forEach((name, values) -> {
            String lower = name.toLowerCase(Locale.ROOT);
            if (HOP_BY_HOP.contains(lower) || "host".equals(lower) || "content-length".equals(lower)) {
                return;
            }
            values.forEach(value -> request.header(name, value));
        });
    }

    private static void respond(HttpExchange exchange, int status, byte[] body, Map<String, String> headers)
            throws IOException {
        headers.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
