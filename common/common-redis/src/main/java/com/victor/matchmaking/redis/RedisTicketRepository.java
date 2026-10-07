package com.victor.matchmaking.redis;

import java.time.Instant;
import java.util.UUID;

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

    public RedisTicketRepository(Redis client) {
        this.client = client;
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
