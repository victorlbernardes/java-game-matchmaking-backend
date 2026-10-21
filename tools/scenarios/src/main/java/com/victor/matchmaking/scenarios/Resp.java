package com.victor.matchmaking.scenarios;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Redis over a plain socket, the way a game client is allowed to talk to it: RESP, no driver.
 *
 * <p>The harness had this in Python and keeps the shape — a subscriber for the push
 * ({@code player_notifications}) and a publisher for the answer ({@code match_confirmations}) — because
 * the W16 load measurement learned the hard way that a client whose own plumbing drops frames cannot
 * measure a delivery: {@code docker exec redis-cli SUBSCRIBE} lost 24 of 1024 notifications and every
 * lost match dissolved at its deadline.
 */
final class Resp implements Closeable {

    private static final String NOTIFICATIONS = "player_notifications";
    private static final String CONFIRMATIONS = "match_confirmations";

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    private Resp(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = socket.getOutputStream();
    }

    static Resp connect(int port) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("localhost", port), 5_000);
        socket.setTcpNoDelay(true);
        return new Resp(socket);
    }

    void command(String... parts) throws IOException {
        out.write(encode(parts));
        out.flush();
    }

    /** A single-key read: {@code GET}. */
    String get(String key) throws IOException {
        command("GET", key);
        Object reply = read();
        return reply instanceof String value ? value : null;
    }

    /** The cardinality of a sorted set: {@code ZCARD}. */
    /** {@code EXISTS key} -> whether the key is there (an empty payload is not "missing"). */
    boolean exists(String key) throws IOException {
        command("EXISTS", key);
        Object reply = read();
        return reply instanceof String value && !"0".equals(value);
    }

    long zcard(String key) throws IOException {
        command("ZCARD", key);
        Object reply = read();
        return reply instanceof String value ? Long.parseLong(value) : 0L;
    }

    /** The keys matching a pattern: {@code KEYS}. A probe's read, not a production one. */
    List<String> keys(String pattern) throws IOException {
        command("KEYS", pattern);
        Object reply = read();
        List<String> found = new ArrayList<>();
        if (reply instanceof List<?> items) {
            for (Object item : items) {
                if (item instanceof String key) {
                    found.add(key);
                }
            }
        }
        return found;
    }

    /**
     * Drops every key: {@code FLUSHALL}. The scenario's own boundary between scenarios — each one
     * starts from an empty queue, or a leftover ticket is paired by the next one.
     */
    void flushAll() throws IOException {
        command("FLUSHALL");
        read();
    }

    /** Removes keys: {@code DEL}. */
    long del(String... keys) throws IOException {
        String[] parts = new String[keys.length + 1];
        parts[0] = "DEL";
        System.arraycopy(keys, 0, parts, 1, keys.length);
        command(parts);
        Object reply = read();
        return reply instanceof String value ? Long.parseLong(value) : 0L;
    }

    /** One RESP frame: a list (array), a string (bulk or status), or {@code null} for a nil bulk. */
    Object read() throws IOException {
        int prefix = in.read();
        if (prefix < 0) {
            throw new EOFException("redis closed the connection");
        }
        String line = readLine();
        return switch ((char) prefix) {
            case '*' -> {
                int count = Integer.parseInt(line);
                List<Object> items = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    items.add(read());
                }
                yield items;
            }
            case '$' -> {
                int size = Integer.parseInt(line);
                if (size < 0) {
                    yield null;
                }
                byte[] payload = in.readNBytes(size + 2);
                yield new String(payload, 0, size, StandardCharsets.UTF_8);
            }
            default -> line;
        };
    }

    private String readLine() throws IOException {
        StringBuilder line = new StringBuilder();
        int previous = -1;
        int current;
        while ((current = in.read()) >= 0) {
            if (previous == '\r' && current == '\n') {
                return line.substring(0, line.length() - 1);
            }
            line.append((char) current);
            previous = current;
        }
        throw new EOFException("redis closed the connection mid-line");
    }

    private static byte[] encode(String... parts) {
        StringBuilder out = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) {
            byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
            out.append('$').append(bytes.length).append("\r\n").append(part).append("\r\n");
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing a client is best effort: the probe is about to exit anyway.
        }
    }

    /**
     * The client's push side: one connection subscribed to the notifications channel, remembering
     * which match each player was told about.
     */
    static final class Subscriber implements Closeable {

        private final Resp resp;
        private final Map<String, String> matches = new ConcurrentHashMap<>();
        private final Thread reader;
        private volatile boolean running = true;

        Subscriber(int port) throws IOException {
            this.resp = Resp.connect(port);
            this.resp.command("SUBSCRIBE", NOTIFICATIONS);
            this.reader = new Thread(this::pump, "probe-notifications");
            this.reader.setDaemon(true);
            this.reader.start();
        }

        private void pump() {
            try {
                while (running) {
                    onFrame(resp.read());
                }
            } catch (IOException | RuntimeException closed) {
                // The socket was closed by close(), or Redis went away: the probe reports the
                // absence of the match, which is the truthful result either way.
            }
        }

        private void onFrame(Object frame) {
            if (!(frame instanceof List<?> message) || message.size() < 3) {
                return;
            }
            if (!"message".equals(message.get(0)) || !(message.get(2) instanceof String payload)) {
                return;
            }
            try {
                JsonNode routed = Json.parse(payload);
                JsonNode envelope = routed.get("envelope");
                if (envelope == null || !"MATCH_PENDING_CONFIRMATION".equals(text(envelope, "type"))) {
                    return;
                }
                String player = text(routed, "targetPlayerId");
                String matchId = envelope.get("payload") == null ? null : text(envelope.get("payload"), "matchId");
                if (player != null && !player.isEmpty() && matchId != null && !matchId.isEmpty()) {
                    matches.put(player, matchId);
                }
            } catch (IOException notJson) {
                // A frame the client cannot read is not a notification; ignore it, as the Python did.
            }
        }

        private static String text(JsonNode node, String field) {
            JsonNode value = node.get(field);
            return value == null || value.isNull() ? null : value.asText();
        }

        String matchFor(String player) {
            return matches.get(player);
        }

        @Override
        public void close() {
            running = false;
            resp.close();
            try {
                reader.join(1_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** The client's answer side: one connection, one PUBLISH at a time (a lockstep reply read). */
    static final class Confirmer implements Closeable {

        private final Resp resp;

        Confirmer(int port) throws IOException {
            this.resp = Resp.connect(port);
            this.resp.socket.setSoTimeout(10_000);
        }

        void confirm(String matchId, String playerId) throws IOException {
            String payload = Json.write(Map.of(
                    "matchId", matchId,
                    "playerId", playerId,
                    "action", "CONFIRM"));
            resp.command("PUBLISH", CONFIRMATIONS, payload);
            resp.read();
        }

        @Override
        public void close() {
            resp.close();
        }
    }
}
