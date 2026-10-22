package com.victor.matchmaking.scenarios;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * The HTTP client of the harness: the JDK's, so a scenario needs no dependency to talk to the API.
 *
 * <p>Its two contracts come straight from the Python it replaces, because the checks depend on
 * them: a non-2xx answer is a normal result (the status and the body are returned, not thrown), and
 * a body that is not JSON is wrapped as {@code {"raw": "..."}} instead of failing.
 */
final class Http {

    record Response(int status, JsonNode body, Map<String, String> headers) {

        /** A header by name, case-insensitively; {@code ""} when the answer did not carry it. */
        String header(String name) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    return entry.getValue();
                }
            }
            return "";
        }
    }

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .build();

    private Http() {
    }

    static Response exchange(String method, String url, String token, Object body)
            throws IOException, InterruptedException {
        return exchange(method, url, token, body, Map.of());
    }

    /** The same call with extra request headers (the scenario's own {@code Idempotency-Key}). */
    static Response exchange(String method, String url, String token, Object body, Map<String, String> headers)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url)).timeout(REQUEST_TIMEOUT);
        headers.forEach(builder::header);
        if (token != null && !token.isEmpty()) {
            builder.header("authorization", "Bearer " + token);
        }
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("content-type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(Json.write(body), StandardCharsets.UTF_8));
        }
        HttpResponse<String> response = CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        Map<String, String> received = new LinkedHashMap<>();
        response.headers().map().forEach((name, values) -> {
            if (!values.isEmpty()) {
                received.put(name, values.get(0));
            }
        });
        return new Response(response.statusCode(), parseOrWrap(response.body()), received);
    }

    /** The raw body of a GET: a scrape is lines of text, not JSON. */
    static String text(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return CLIENT.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }

    private static JsonNode parseOrWrap(String body) {
        try {
            return Json.parse(body);
        } catch (JsonProcessingException notJson) {
            return Json.object().put("raw", body);
        }
    }
}
