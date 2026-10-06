package com.victor.matchmaking.engine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.domain.ticket.TicketStateMachine;
import com.victor.matchmaking.engine.application.PairingPolicy;
import com.victor.matchmaking.engine.application.PendingMatch;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.RedisPendingMatchStore;
import com.victor.matchmaking.redis.Envelope;
import com.victor.matchmaking.redis.message.Payloads;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.RedisPlayerNotifier;
import com.victor.matchmaking.redis.RedisPublisher;
import com.victor.matchmaking.redis.RedisSubscriber;
import com.victor.matchmaking.redis.RedisTicketRepository;
import com.victor.matchmaking.redis.TicketRepository;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/** 1v1 matchmaking worker: pairs queue heads, tracks confirmations, dissolves on decline/timeout. */
public final class MatchmakingEngineApplication {

    private static final long PAIRING_INTERVAL_MS = 500;
    private static final long SWEEP_INTERVAL_MS = 1000;
    private static final long CONFIRMATION_TIMEOUT_SECONDS = 30;
    private static final int TICKET_ID_LENGTH = 8;

    private MatchmakingEngineApplication() {
    }

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        String redisUri = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
        io.vertx.redis.client.Redis redis = io.vertx.redis.client.Redis.createClient(vertx,
                new RedisOptions().setConnectionString(redisUri));
        TicketRepository tickets = new RedisTicketRepository(redis);
        PendingMatchStore pending = new RedisPendingMatchStore(redis);
        RedisPublisher publisher = new RedisPublisher(redis);
        RedisPlayerNotifier notifier = new RedisPlayerNotifier(publisher);
        com.victor.matchmaking.db.DatabaseConfig dbConfig = com.victor.matchmaking.db.DatabaseConfig.fromEnv();
        com.victor.matchmaking.db.DatabaseMigrator.migrate(dbConfig);
        com.victor.matchmaking.db.MatchRepository matches =
                com.victor.matchmaking.db.MatchRepository.connect(vertx, dbConfig);

        RedisSubscriber.subscribe(vertx, redisUri, RedisKeys.CHANNEL_MATCH_CONFIRMATIONS,
                msg -> handleConfirmation(msg, redis, tickets, pending, notifier, matches));

        vertx.setPeriodic(PAIRING_INTERVAL_MS, id -> scanAndPair(vertx, redis, tickets, pending, notifier));
        vertx.setPeriodic(SWEEP_INTERVAL_MS, id -> sweepExpired(redis, tickets, pending, notifier));

        System.out.println("matchmaking-engine running");
    }

    private static void scanAndPair(Vertx vertx, io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier) {
        redis.send(Request.cmd(Command.SCAN).arg("0").arg("MATCH").arg("queue:*")).onSuccess(scan -> {
            Response keysResp = scan.get(1);
            if (keysResp != null) {
                for (int i = 0; i < keysResp.size(); i++) {
                    tryPairFromQueue(keysResp.get(i).toString(), redis, tickets, pending, notifier);
                }
            }
        }).onFailure(Throwable::printStackTrace);
    }

    private static void tryPairFromQueue(String queueKey, io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier) {
        redis.send(Request.cmd(Command.ZRANGE).arg(queueKey).arg("0").arg("1")).onSuccess(range -> {
            if (range == null || range.size() < 2) {
                return;
            }
            String firstId = range.get(0).toString();
            String secondId = range.get(1).toString();
            Future<MatchmakingTicket> first = tickets.findById(firstId);
            Future<MatchmakingTicket> second = tickets.findById(secondId);
            Future.all(first, second).onSuccess(cf -> {
                MatchmakingTicket a = first.result();
                MatchmakingTicket b = second.result();
                if (a == null || b == null || a.status() != TicketState.SEARCHING || b.status() != TicketState.SEARCHING
                        || !PairingPolicy.canPair(a, b)) {
                    return;
                }
                pair(a, b, tickets, pending, notifier);
            }).onFailure(Throwable::printStackTrace);
        }).onFailure(Throwable::printStackTrace);
    }

    private static void pair(MatchmakingTicket a, MatchmakingTicket b, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier) {
        MatchmakingTicket aConfirming = transition(a, TicketState.CONFIRMING);
        MatchmakingTicket bConfirming = transition(b, TicketState.CONFIRMING);
        Future<Void> saveA = tickets.save(null, aConfirming);
        Future<Void> saveB = tickets.save(null, bConfirming);
        Future.all(saveA, saveB).onSuccess(v -> {
            String matchId = "match-" + UUID.randomUUID().toString().substring(0, TICKET_ID_LENGTH);
            PendingMatch pm = new PendingMatch(matchId,
                    List.of(new PendingMatch.PendingPlayer(a.playerId(), a.id()),
                            new PendingMatch.PendingPlayer(b.playerId(), b.id())),
                    List.of(), Instant.now().plusSeconds(CONFIRMATION_TIMEOUT_SECONDS));
            pending.save(pm).onSuccess(ignored -> {
                Payloads.MatchPending pendingPayload = new Payloads.MatchPending(matchId, pm.playerIds());
                notifier.notifyPlayer(a.playerId(), Envelope.of("MATCH_PENDING_CONFIRMATION", pendingPayload), Payloads.MatchPending::toJson)
                        .onFailure(Throwable::printStackTrace);
                notifier.notifyPlayer(b.playerId(), Envelope.of("MATCH_PENDING_CONFIRMATION", pendingPayload), Payloads.MatchPending::toJson)
                        .onFailure(Throwable::printStackTrace);
            }).onFailure(Throwable::printStackTrace);
        }).onFailure(Throwable::printStackTrace);
    }

    private static void handleConfirmation(JsonObject msg, io.vertx.redis.client.Redis redis,
            TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, com.victor.matchmaking.db.MatchRepository matches) {
        Payloads.MatchConfirmation command;
        try {
            command = Payloads.MatchConfirmation.fromJson(msg);
        } catch (Exception e) {
            return;
        }
        String matchId = command.matchId();
        String playerId = command.playerId();
        String action = command.action().name();
        pending.findById(matchId).onSuccess(pm -> {
            if (pm == null) {
                notifier.notifyPlayer(playerId, Envelope.of("ERROR", new Payloads.Error("match not found: " + matchId)),
                        Payloads.Error::toJson)
                        .onFailure(Throwable::printStackTrace);
                return;
            }
            if ("DECLINE".equals(action)) {
                dissolve(pm, "declined by " + playerId, tickets, pending, notifier, redis);
                return;
            }
            if ("CONFIRM".equals(action)) {
                if (!pm.playerIds().contains(playerId)) {
                    return;
                }
                String confirmedKey = "match-confirmed:" + matchId;
                redisCommands(redis, Request.cmd(Command.SADD).arg(confirmedKey).arg(playerId))
                        .flatMap(v -> redisCommands(redis, Request.cmd(Command.SCARD).arg(confirmedKey)))
                        .onSuccess(count -> {
                            if (count.toLong() != null && count.toLong() >= pm.players().size()) {
                                confirm(pm, tickets, pending, notifier, matches);
                            }
                        })
                        .onFailure(Throwable::printStackTrace);
            }
        }).onFailure(Throwable::printStackTrace);
    }

    private static void confirm(PendingMatch pm, TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, com.victor.matchmaking.db.MatchRepository matches) {
        // Load both tickets, then transition them and persist the durable match row.
        List<Future<MatchmakingTicket>> loaded = pm.players().stream()
                .map(p -> tickets.findById(p.ticketId()))
                .toList();
        Future.all(loaded).onSuccess(cf -> {
            List<MatchmakingTicket> found = loaded.stream().map(Future::result).toList();
            if (found.stream().anyMatch(t -> t == null || t.status() != TicketState.CONFIRMING)) {
                System.err.println("cannot confirm match " + pm.matchId() + ": tickets not in CONFIRMING");
                return;
            }
            MatchmakingTicket first = found.get(0);
            for (MatchmakingTicket t : found) {
                tickets.save(null, transition(t, TicketState.CONFIRMED))
                        .onFailure(Throwable::printStackTrace);
            }
            List<com.victor.matchmaking.domain.match.MatchPlayer> players = new ArrayList<>();
            for (int i = 0; i < pm.players().size(); i++) {
                players.add(new com.victor.matchmaking.domain.match.MatchPlayer(pm.matchId(),
                        pm.players().get(i).playerId(), i == 0 ? "A" : "B", found.get(i).skill()));
            }
            com.victor.matchmaking.domain.match.Match match = new com.victor.matchmaking.domain.match.Match(
                    pm.matchId(), first.gameMode(), first.region(),
                    com.victor.matchmaking.domain.match.MatchStatus.PENDING, Instant.now(), null, null);
            matches.save(match, players).onFailure(Throwable::printStackTrace);
            pending.delete(pm.matchId()).onFailure(Throwable::printStackTrace);
            for (PendingMatch.PendingPlayer player : pm.players()) {
                notifier.notifyPlayer(player.playerId(), Envelope.of("MATCH_CONFIRMED",
                        new Payloads.MatchConfirmed(pm.matchId())), Payloads.MatchConfirmed::toJson)
                        .onFailure(Throwable::printStackTrace);
            }
        }).onFailure(Throwable::printStackTrace);
    }

    private static void dissolve(PendingMatch pm, String reason, TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, io.vertx.redis.client.Redis redis) {
        for (PendingMatch.PendingPlayer player : pm.players()) {
            tickets.findById(player.ticketId()).onSuccess(ticket -> {
                if (ticket != null && ticket.status() == TicketState.CONFIRMING) {
                    tickets.save(null, transition(ticket, TicketState.SEARCHING))
                            .onFailure(Throwable::printStackTrace);
                }
            }).onFailure(Throwable::printStackTrace);
        }
        pending.delete(pm.matchId()).onFailure(Throwable::printStackTrace);
        redis.send(Request.cmd(Command.DEL).arg("match-confirmed:" + pm.matchId()))
                .onFailure(Throwable::printStackTrace);
        for (PendingMatch.PendingPlayer player : pm.players()) {
            notifier.notifyPlayer(player.playerId(), Envelope.of("MATCH_CANCELLED",
                    new Payloads.MatchCancelled(pm.matchId(), reason)), Payloads.MatchCancelled::toJson)
                    .onFailure(Throwable::printStackTrace);
        }
    }

    private static Future<Response> redisCommands(io.vertx.redis.client.Redis redis, Request cmd) {
        return redis.send(cmd);
    }

    private static MatchmakingTicket transition(MatchmakingTicket ticket, TicketState target) {
        return new MatchmakingTicket(ticket.id(), ticket.playerId(), ticket.gameMode(), ticket.region(),
                ticket.maxLatencyMs(), ticket.skill(), ticket.queuedAt(),
                TicketStateMachine.transition(ticket.status(), target), ticket.version() + 1);
    }

    private static void sweepExpired(io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier) {
        redis.send(Request.cmd(Command.SCAN).arg("0").arg("MATCH").arg("match-pending:*")).onSuccess(scan -> {
            Response keysResp = scan.get(1);
            if (keysResp != null) {
                for (int i = 0; i < keysResp.size(); i++) {
                    String key = keysResp.get(i).toString();
                    pending.findById(key.substring("match-pending:".length()))
                            .onSuccess(pm -> {
                                if (pm != null && Instant.now().isAfter(pm.expiresAt())) {
                                    dissolve(pm, "timeout", tickets, pending, notifier, redis);
                                }
                            })
                            .onFailure(Throwable::printStackTrace);
                }
            }
        }).onFailure(Throwable::printStackTrace);
    }
}
