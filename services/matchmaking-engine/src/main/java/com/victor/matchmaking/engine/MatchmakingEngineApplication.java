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

    private static final System.Logger LOG = System.getLogger("matchmaking.engine");
    private static final long PAIRING_INTERVAL_MS = 500;
    private static final long SWEEP_INTERVAL_MS = 1000;
    private static final long CONFIRMATION_TIMEOUT_SECONDS = 30;
    private static final int TICKET_ID_LENGTH = 8;

    private MatchmakingEngineApplication() {
    }

    public static void main(String[] args) {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %5$s%n");
        java.util.Locale.setDefault(java.util.Locale.ENGLISH);
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

        LOG.log(System.Logger.Level.INFO, "engine.started pairingIntervalMs={0} sweepIntervalMs={1} confirmationTimeoutS={2}",
                String.valueOf(PAIRING_INTERVAL_MS), String.valueOf(SWEEP_INTERVAL_MS),
                String.valueOf(CONFIRMATION_TIMEOUT_SECONDS));
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
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=scanQueues", err));
    }

    private static void tryPairFromQueue(String queueKey, io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier) {
        redis.send(Request.cmd(Command.ZRANGE).arg(queueKey).arg("0").arg("9")).onSuccess(range -> {
            if (range == null || range.size() < 2) {
                return;
            }
            List<Future<MatchmakingTicket>> loaded = new ArrayList<>();
            for (int i = 0; i < range.size(); i++) {
                loaded.add(tickets.findById(range.get(i).toString()));
            }
            Future.all(loaded).onSuccess(cf -> {
                List<MatchmakingTicket> candidates = loaded.stream().map(Future::result).toList();
                for (int i = 0; i < candidates.size(); i++) {
                    for (int j = i + 1; j < candidates.size(); j++) {
                        MatchmakingTicket a = candidates.get(i);
                        MatchmakingTicket b = candidates.get(j);
                        if (a != null && b != null && a.status() == TicketState.SEARCHING
                                && b.status() == TicketState.SEARCHING && PairingPolicy.canPair(a, b)) {
                            pair(a, b, tickets, pending, notifier);
                            return;
                        }
                    }
                }
            }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                    "engine.error stage=loadTickets queue={0}", err, queueKey));
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=readQueue queue={0}", err, queueKey));
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
                LOG.log(System.Logger.Level.INFO,
                        "pair.created matchId={0} players={1} gameMode={2} region={3} skillA={4} skillB={5}",
                        matchId, pm.playerIds(), a.gameMode().label(), a.region().name(),
                        String.valueOf(a.skill()), String.valueOf(b.skill()));
                notifier.notifyPlayer(a.playerId(), Envelope.of("MATCH_PENDING_CONFIRMATION", pendingPayload), Payloads.MatchPending::toJson)
                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=notifyPending playerId=" + a.playerId(), err));
                notifier.notifyPlayer(b.playerId(), Envelope.of("MATCH_PENDING_CONFIRMATION", pendingPayload), Payloads.MatchPending::toJson)
                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=notifyPending playerId=" + b.playerId(), err));
            }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                    "engine.error stage=savePendingMatch matchId=" + matchId, err));
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=saveConfirmingTickets", err));
    }

    private static void handleConfirmation(JsonObject msg, io.vertx.redis.client.Redis redis,
            TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, com.victor.matchmaking.db.MatchRepository matches) {
        Payloads.MatchConfirmation command;
        try {
            command = Payloads.MatchConfirmation.fromJson(msg);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "confirm.rejected error=unparseable message={0}", msg.encode());
            return;
        }
        String matchId = command.matchId();
        String playerId = command.playerId();
        String action = command.action().name();
        pending.findById(matchId).onSuccess(pm -> {
            if (pm == null) {
                LOG.log(System.Logger.Level.WARNING,
                        "confirm.rejected matchId={0} playerId={1} reason=match not found", matchId, playerId);
                notifier.notifyPlayer(playerId, Envelope.of("ERROR", new Payloads.Error("match not found: " + matchId)),
                        Payloads.Error::toJson)
                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=notifyError playerId=" + playerId, err));
                return;
            }
            if ("DECLINE".equals(action)) {
                dissolve(pm, "declined by " + playerId, tickets, pending, notifier, redis);
                return;
            }
            if ("CONFIRM".equals(action)) {
                if (!pm.playerIds().contains(playerId)) {
                    LOG.log(System.Logger.Level.WARNING,
                            "confirm.rejected matchId={0} playerId={1} reason=not a player of this match",
                            matchId, playerId);
                    return;
                }
                String confirmedKey = "match-confirmed:" + matchId;
                redisCommands(redis, Request.cmd(Command.SADD).arg(confirmedKey).arg(playerId))
                        .flatMap(v -> redisCommands(redis, Request.cmd(Command.SCARD).arg(confirmedKey)))
                        .onSuccess(count -> {
                            long confirmed = count.toLong() != null ? count.toLong() : 0L;
                            LOG.log(System.Logger.Level.INFO,
                                    "confirm.received matchId={0} playerId={1} count={2} required={3}",
                                    matchId, playerId, String.valueOf(confirmed),
                                    String.valueOf(pm.players().size()));
                            if (confirmed >= pm.players().size()) {
                                confirm(pm, tickets, pending, notifier, matches);
                            }
                        })
                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=countConfirmations matchId=" + matchId, err));
            }
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=loadPendingMatch matchId=" + matchId, err));
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
                LOG.log(System.Logger.Level.WARNING,
                        "match.confirmRejected matchId={0} reason=tickets not in CONFIRMING", pm.matchId());
                return;
            }
            MatchmakingTicket first = found.get(0);
            for (MatchmakingTicket t : found) {
                tickets.save(null, transition(t, TicketState.CONFIRMED))
                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=saveConfirmedTicket ticketId=" + t.id(), err));
            }
            List<com.victor.matchmaking.domain.match.MatchPlayer> players = new ArrayList<>();
            for (int i = 0; i < pm.players().size(); i++) {
                players.add(new com.victor.matchmaking.domain.match.MatchPlayer(pm.matchId(),
                        pm.players().get(i).playerId(), i == 0 ? "A" : "B", found.get(i).skill()));
            }
            com.victor.matchmaking.domain.match.Match match = new com.victor.matchmaking.domain.match.Match(
                    pm.matchId(), first.gameMode(), first.region(),
                    com.victor.matchmaking.domain.match.MatchStatus.PENDING, Instant.now(), null, null);
            matches.save(match, players)
                    .onSuccess(v -> LOG.log(System.Logger.Level.INFO,
                            "match.confirmed matchId={0} players={1} persisted=true",
                            pm.matchId(), pm.playerIds()))
                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                            "engine.error stage=persistMatch matchId=" + pm.matchId(), err));
            pending.delete(pm.matchId()).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                    "engine.error stage=deletePendingMatch matchId=" + pm.matchId(), err));
            for (PendingMatch.PendingPlayer player : pm.players()) {
                notifier.notifyPlayer(player.playerId(), Envelope.of("MATCH_CONFIRMED",
                        new Payloads.MatchConfirmed(pm.matchId())), Payloads.MatchConfirmed::toJson)
                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=notifyConfirmed playerId=" + player.playerId(), err));
            }
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=loadTicketsForConfirm", err));
    }

    private static void dissolve(PendingMatch pm, String reason, TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, io.vertx.redis.client.Redis redis) {
        LOG.log(System.Logger.Level.INFO, "match.dissolved matchId={0} reason={1}", pm.matchId(), reason);
        for (PendingMatch.PendingPlayer player : pm.players()) {
            tickets.findById(player.ticketId()).onSuccess(ticket -> {
                if (ticket != null && ticket.status() == TicketState.CONFIRMING) {
                    tickets.save(null, transition(ticket, TicketState.SEARCHING))
                            .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                    "engine.error stage=requeueTicket ticketId=" + ticket.id(), err));
                }
            }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                    "engine.error stage=loadTicketForDissolve matchId=" + pm.matchId(), err));
        }
        pending.delete(pm.matchId()).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=deletePendingMatch matchId=" + pm.matchId(), err));
        redis.send(Request.cmd(Command.DEL).arg("match-confirmed:" + pm.matchId()))
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                        "engine.error stage=deleteConfirmedSet matchId=" + pm.matchId(), err));
        for (PendingMatch.PendingPlayer player : pm.players()) {
            notifier.notifyPlayer(player.playerId(), Envelope.of("MATCH_CANCELLED",
                    new Payloads.MatchCancelled(pm.matchId(), reason)), Payloads.MatchCancelled::toJson)
                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                            "engine.error stage=notifyCancelled playerId=" + player.playerId(), err));
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
                            .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                    "engine.error stage=loadExpiredPending key=" + key, err));
                }
            }
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=sweepExpired", err));
    }
}
