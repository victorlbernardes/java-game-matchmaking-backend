package com.victor.matchmaking.redis;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;

import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/** Redis-backed hot state for tickets: ticket JSON + per-queue sorted set + idempotency index. */
public final class RedisTicketRepository implements TicketRepository {

    private static final int TICKET_ID_LENGTH = 8;
    private static final int TICKET_TTL_SECONDS = 900;

    private final Redis client;
    private final AtomicReference<String> createScriptSha = new AtomicReference<>();

    public RedisTicketRepository(Redis client) {
        this.client = client;
    }

    @Override
    public Future<String> createIdempotent(String idempotencyKey, MatchmakingTicket ticket) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return save(null, ticket).map(v -> ticket.id());
        }
        String sha = createScriptSha.get();
        if (sha != null) {
            return evalCreateScript(sha, idempotencyKey, ticket);
        }
        return loadCreateScript().flatMap(loadedSha -> evalCreateScript(loadedSha, idempotencyKey, ticket));
    }

    private Future<String> loadCreateScript() {
        try {
            String script = new String(getClass().getResourceAsStream("/create.lua").readAllBytes(),
                    StandardCharsets.UTF_8);
            return client.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                    .map(resp -> {
                        String sha = resp != null ? resp.toString() : null;
                        if (sha != null) {
                            createScriptSha.set(sha);
                        }
                        return sha;
                    });
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    private Future<String> evalCreateScript(String sha, String idempotencyKey, MatchmakingTicket ticket) {
        String queuedAt = String.valueOf(ticket.queuedAt().toEpochMilli());
        return client.send(Request.cmd(Command.EVALSHA).arg(sha)
                        .arg("1").arg(idempotencyKey(idempotencyKey))
                        .arg(TicketCodec.toJson(ticket).encode())
                        .arg(ticket.id())
                        .arg(queueKey(ticket))
                        .arg(queuedAt)
                        .arg(String.valueOf(TICKET_TTL_SECONDS))
                        .arg(String.valueOf(TICKET_TTL_SECONDS)))
                .map(resp -> resp != null ? resp.toString() : null);
    }

    @Override
    public Future<MatchmakingTicket> findById(String ticketId) {
        return client.send(Request.cmd(Command.GET).arg(ticketKey(ticketId)))
                .map(response -> response != null ? TicketCodec.fromJson(new JsonObject(response.toString())) : null);
    }

    @Override
    public Future<MatchmakingTicket> findByIdempotencyKey(String key) {
        return client.send(Request.cmd(Command.GET).arg(idempotencyKey(key)))
                .flatMap(response -> response != null
                        ? findById(response.toString())
                        : Future.succeededFuture(null));
    }

    @Override
    public Future<Void> save(String idempotencyKey, MatchmakingTicket ticket) {
        Future<Response> stored = client.send(Request.cmd(Command.SET)
                .arg(ticketKey(ticket.id()))
                .arg(TicketCodec.toJson(ticket).encode())
                .arg("EX").arg(TICKET_TTL_SECONDS));
        Future<Response> queue = ticket.status() == TicketState.SEARCHING
                ? client.send(Request.cmd(Command.ZADD)
                        .arg(queueKey(ticket))
                        .arg(String.valueOf(ticket.queuedAt().toEpochMilli()))
                        .arg(ticket.id()))
                : client.send(Request.cmd(Command.ZREM).arg(queueKey(ticket)).arg(ticket.id()));
        Future<Response> idem = (idempotencyKey != null && !idempotencyKey.isBlank())
                ? client.send(Request.cmd(Command.SET)
                        .arg(idempotencyKey(idempotencyKey))
                        .arg(ticket.id())
                        .arg("EX").arg(TICKET_TTL_SECONDS))
                : Future.succeededFuture();
        return Future.all(stored, queue, idem).mapEmpty();
    }

    @Override
    public Future<List<String>> createPartyTickets(List<MatchmakingTicket> tickets) {
        if (tickets.isEmpty()) {
            return Future.succeededFuture(List.of());
        }
        Future<List<String>> combined = Future.succeededFuture(ticketIds(tickets));
        for (MatchmakingTicket ticket : tickets) {
            combined = combined.flatMap(kept -> save(null, ticket)
                    .map(v -> {
                        List<String> next = new ArrayList<>(kept);
                        next.add(ticket.id());
                        return next;
                    }));
        }
        return combined.map(kept -> List.copyOf(kept));
    }

    private static List<String> ticketIds(List<MatchmakingTicket> tickets) {
        return tickets.stream()
                .map(MatchmakingTicket::id)
                .toList();
    }

    @Override
    public String newTicketId() {
        return "ticket-" + UUID.randomUUID().toString().substring(0, TICKET_ID_LENGTH);
    }

    @Override
    public Instant now() {
        return Instant.now();
    }

    private static String ticketKey(String ticketId) {
        return RedisKeys.ticketKey(ticketId);
    }

    private static String idempotencyKey(String key) {
        return RedisKeys.idempotencyKey(key);
    }

    private static String queueKey(MatchmakingTicket ticket) {
        return RedisKeys.queueKey(ticket.gameMode().label(), ticket.region().name());
    }
}
