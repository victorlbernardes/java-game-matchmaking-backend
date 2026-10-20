package com.victor.matchmaking.redis;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
    /** 0 = no admission limit: a queue may grow as deep as arrivals make it. */
    public static final int UNBOUNDED_QUEUE = 0;

    private final Redis client;
    private final int queueMaxDepth;
    private final int partitions;
    private final RedisScript createScript;

    public RedisTicketRepository(Redis client) {
        this(client, UNBOUNDED_QUEUE);
    }

    /**
     * @param queueMaxDepth tickets a single queue admits before it refuses new arrivals
     *                      ({@link #UNBOUNDED_QUEUE} to disable)
     */
    public RedisTicketRepository(Redis client, int queueMaxDepth) {
        this(client, queueMaxDepth, QueuePartitions.DEFAULT_PARTITIONS);
    }

    /**
     * @param partitions how many keys a (mode, region) queue is spread over (W20, ADR 0028);
     *                   {@code 1} is the historical single key. Every service that writes or reads
     *                   the queue must use the same value — an arrival written to partition 3 is
     *                   invisible to an engine that reads one key.
     */
    public RedisTicketRepository(Redis client, int queueMaxDepth, int partitions) {
        this.client = client;
        this.queueMaxDepth = queueMaxDepth;
        this.partitions = Math.max(1, partitions);
        this.createScript = new RedisScript(client, "/create.lua");
    }

    @Override
    public Future<String> createIdempotent(String idempotencyKey, MatchmakingTicket ticket) {
        // Both paths go through the script, with an empty key when the caller sent none: the
        // admission check is about the queue, not about the caller having asked for idempotency,
        // and a cap that only applies to callers who sent a header is a cap that can be walked
        // around (measured in W17).
        String idemKey = idempotencyKey == null || idempotencyKey.isBlank()
                ? ""
                : idempotencyKey(idempotencyKey);
        return evalCreateScript(idemKey, ticket);
    }

    /**
     * @param idempotencyKey the Redis key to record the ticket under, or the empty string when the
     *                       caller sent no {@code Idempotency-Key}: the script checks the queue
     *                       either way
     */
    private Future<String> evalCreateScript(String idempotencyKey, MatchmakingTicket ticket) {
        String queuedAt = String.valueOf(ticket.queuedAt().toEpochMilli());
        // The SHA is the script's business, not the repository's: RedisScript reloads it when the
        // server forgets it (W18, ADR 0024), which is what makes a Redis restart survivable.
        return createScript.eval(sha -> Request.cmd(Command.EVALSHA).arg(sha)
                        .arg("1").arg(idempotencyKey)
                        .arg(TicketCodec.toJson(ticket).encode())
                        .arg(ticket.id())
                        .arg(queueKey(ticket))
                        .arg(queuedAt)
                        .arg(String.valueOf(TICKET_TTL_SECONDS))
                        .arg(String.valueOf(TICKET_TTL_SECONDS))
                        .arg(String.valueOf(capacityPerPartition())))
                .map(resp -> {
                    // {status, payload}: the status is a word, the payload is an id or a depth.
                    // A ticket id is not a good place to carry state - the reason exists (Regra 11).
                    String status = resp.get(0).toString();
                    String payload = resp.get(1).toString();
                    if ("FULL".equals(status)) {
                        throw new QueueFullException(queueKey(ticket), Long.parseLong(payload), capacityPerPartition());
                    }
                    return payload;
                });
    }

    /**
     * The cap divided by the partitions, floored: the sum of the partitions' caps is then
     * {@code <= queueMaxDepth}, so partitioning cannot raise the total the queue holds. With one
     * partition this is the historical cap exactly; with {@code C < P} it degenerates to one per
     * partition (the smallest cap that still refuses something).
     */
    private int capacityPerPartition() {
        if (queueMaxDepth <= 0) {
            return UNBOUNDED_QUEUE;
        }
        return Math.max(1, queueMaxDepth / partitions);
    }

    @Override
    public Future<Long> queueDepth(String gameModeLabel, String regionName) {
        List<Future<Response>> reads = new ArrayList<>();
        for (String key : RedisKeys.queueKeys(gameModeLabel, regionName, partitions)) {
            reads.add(client.send(Request.cmd(Command.ZCARD).arg(key)));
        }
        return Future.all(reads).map(ignored -> {
            long total = 0;
            for (int i = 0; i < reads.size(); i++) {
                Response response = reads.get(i).result();
                total += response != null ? response.toLong() : 0L;
            }
            return total;
        });
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
        MatchmakingTicket first = tickets.get(0);
        // Party admission is a bounded approximation, not the exact cap the solo path has: the
        // party enqueue is already several writes (one ticket per member, then the party link), so
        // a read here can only observe the depth a moment earlier. The cap still holds solo
        // arrivals exactly, and a party can overshoot by at most its own size - a bounded drift,
        // not the unbounded growth the cap exists to stop (ADR 0022).
        return admitParty(first, tickets.size()).flatMap(admitted -> {
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
        });
    }

    private Future<Long> admitParty(MatchmakingTicket first, int partySize) {
        if (queueMaxDepth <= 0) {
            return Future.succeededFuture(0L);
        }
        return queueDepth(first.gameMode().label(), first.region().name())
                .flatMap(depth -> depth + partySize > queueMaxDepth
                        ? Future.failedFuture(new QueueFullException(queueKey(first), depth, queueMaxDepth))
                        : Future.succeededFuture(depth));
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

    private String queueKey(MatchmakingTicket ticket) {
        String enqueueKey = ticket.partyId() != null && !ticket.partyId().isBlank()
                ? ticket.partyId()
                : ticket.playerId();
        int partition = QueuePartitions.partitionOf(enqueueKey, partitions);
        return RedisKeys.queueKey(ticket.gameMode().label(), ticket.region().name(), partition, partitions);
    }
}
