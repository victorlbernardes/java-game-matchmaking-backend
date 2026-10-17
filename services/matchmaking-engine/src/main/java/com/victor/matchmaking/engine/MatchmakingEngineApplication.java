package com.victor.matchmaking.engine;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.domain.ticket.TicketStateMachine;
import com.victor.matchmaking.engine.application.FillPolicy;
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

/** Matchmaking worker: pairs queue heads, tracks confirmations, dissolves on decline/timeout. */
public final class MatchmakingEngineApplication {

    private static final System.Logger LOG = System.getLogger("matchmaking.engine");
    private static final long PAIRING_INTERVAL_MS = 500;
    private static final long SWEEP_INTERVAL_MS = 1000;
    static final long CONFIRMATION_TIMEOUT_SECONDS = 30;
    /**
     * The pending-match Redis key must outlive its confirmation deadline,
     * otherwise it expires before the sweeper can read it and the tickets stay
     * in CONFIRMING forever. TTL = deadline + this grace (W8 two-phase confirm).
     */
    static final long PENDING_KEY_GRACE_SECONDS = 60;
    static final long PENDING_KEY_TTL_SECONDS = CONFIRMATION_TIMEOUT_SECONDS + PENDING_KEY_GRACE_SECONDS;
    private static final int TICKET_ID_LENGTH = 8;
    private static volatile String PAIR_SCRIPT_SHA = "";

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

        // Load the atomic pairing script before starting the pairing loop
        loadPairScript(redis).onSuccess(sha -> {
            PAIR_SCRIPT_SHA = sha;
            vertx.setPeriodic(PAIRING_INTERVAL_MS, id -> scanAndPair(vertx, redis, tickets, pending, notifier));
            vertx.setPeriodic(SWEEP_INTERVAL_MS, id -> sweepExpired(redis, tickets, pending, notifier));

            LOG.log(System.Logger.Level.INFO, "engine.started pairingIntervalMs={0} sweepIntervalMs={1} confirmationTimeoutS={2} pairScript={3}",
                    String.valueOf(PAIRING_INTERVAL_MS), String.valueOf(SWEEP_INTERVAL_MS),
                    String.valueOf(CONFIRMATION_TIMEOUT_SECONDS), sha);
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=loadPairScript", err));
    }

    private static Future<String> loadPairScript(io.vertx.redis.client.Redis redis) {
        String script;
        try {
            script = new String(MatchmakingEngineApplication.class.getResourceAsStream("/pair.lua").readAllBytes());
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
        return redis.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                .map(resp -> resp != null ? resp.toString() : null);
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
        GameMode gameMode = gameModeOfQueueKey(queueKey);
        if (gameMode == null) {
            return;
        }
        int playerCount = gameMode.playersPerMatch();
        redis.send(Request.cmd(Command.ZRANGE).arg(queueKey).arg("0").arg(playerCount - 1)).onSuccess(range -> {
            if (range == null || range.size() < playerCount) {
                return;
            }
            List<Future<MatchmakingTicket>> loaded = new ArrayList<>();
            for (int i = 0; i < range.size(); i++) {
                loaded.add(tickets.findById(range.get(i).toString()));
            }
            Future.all(loaded).onSuccess(cf -> {
                List<MatchmakingTicket> candidates = loaded.stream().map(Future::result).toList();
                // The queue head (oldest ticket) is the widening reference: its
                // queuedAt tells PairingPolicy how long the player has been waiting.
                List<MatchmakingTicket> roster = buildRoster(candidates, gameMode, candidates.get(0).queuedAt());
                if (roster != null) {
                    pair(roster, queueKey, redis, tickets, pending, notifier);
                }
            }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                    "engine.error stage=loadTickets queue={0}", err, queueKey));
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=readQueue queue={0}", err, queueKey));
    }

    /**
     * Resolves the roster for one pairing sweep (ADR-0013): partial parties are
     * completed with the closest solo candidates via the fill policy and parties
     * are always paired as a unit; the remaining solos pair on their own.
     *
     * @return the roster to pair, or null when no valid roster can be formed
     */
    static List<MatchmakingTicket> buildRoster(List<MatchmakingTicket> candidates, GameMode gameMode, Instant queuedAt) {
        List<MatchmakingTicket> solos = new ArrayList<>();
        Map<String, List<MatchmakingTicket>> partiesById = new LinkedHashMap<>();
        for (MatchmakingTicket candidate : candidates) {
            if (candidate.partyId() == null) {
                solos.add(candidate);
            } else {
                partiesById.computeIfAbsent(candidate.partyId(), key -> new ArrayList<>()).add(candidate);
            }
        }
        for (List<MatchmakingTicket> party : partiesById.values()) {
            List<MatchmakingTicket> combined = FillPolicy.fill(party, solos);
            if (combined == null) {
                // The party cannot be completed; it stays queued, never split.
                continue;
            }
            if (PairingPolicy.canPair(combined, queuedAt)) {
                return combined;
            }
        }
        if (solos.size() == gameMode.playersPerMatch() && PairingPolicy.canPair(solos, queuedAt)) {
            return solos;
        }
        return null;
    }

    /** Extracts the game mode encoded in a queue key such as "queue:5v5:NA". */
    private static GameMode gameModeOfQueueKey(String queueKey) {
        String[] segments = queueKey.split(":", 3);
        if (segments.length < 3) {
            return null;
        }
        for (GameMode gameMode : GameMode.values()) {
            if (gameMode.label().equals(segments[1])) {
                return gameMode;
            }
        }
        return null;
    }

    /** Pairs exactly the given candidates into one pending match. */
    private static void pair(List<MatchmakingTicket> candidates, String queueKey,
            io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier) {
        String matchId = "match-" + UUID.randomUUID().toString().substring(0, TICKET_ID_LENGTH);
        List<PendingMatch.PendingPlayer> roster = new ArrayList<>();
        for (MatchmakingTicket candidate : candidates) {
            roster.add(new PendingMatch.PendingPlayer(candidate.playerId(), candidate.id()));
        }
        PendingMatch pm = new PendingMatch(matchId, roster, List.of(),
                Instant.now().plusSeconds(CONFIRMATION_TIMEOUT_SECONDS));

        // Atomic pairing: ZREM from queue + SET pending match in one Lua script.
        // If another worker won the race, the script returns nil and we skip.
        redis.send(Request.cmd(Command.EVALSHA).arg(PAIR_SCRIPT_SHA)
                .arg("1").arg(queueKey)
                .arg(pm.matchId()).arg(pm.toJson().encode())
                .arg(String.valueOf(PENDING_KEY_TTL_SECONDS))
                .arg(String.valueOf(roster.size())))
                .onSuccess(result -> {
                    if (result == null) {
                        // Another worker already removed these tickets from the queue
                        return;
                    }
                    // Paired! Transition to CONFIRMING and notify every player.
                    for (int i = 0; i < candidates.size(); i++) {
                        MatchmakingTicket candidate = candidates.get(i);
                        tickets.save(null, transition(candidate, TicketState.CONFIRMING))
                                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                        "engine.error stage=saveConfirmingTicket ticketId=" + candidate.id(), err));
                    }
                    Payloads.MatchPending pendingPayload = new Payloads.MatchPending(matchId, roster.stream()
                            .map(PendingMatch.PendingPlayer::playerId).toList());
                    for (PendingMatch.PendingPlayer player : roster) {
                        notifier.notifyPlayer(player.playerId(), Envelope.of("MATCH_PENDING_CONFIRMATION", pendingPayload), Payloads.MatchPending::toJson)
                                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                        "engine.error stage=notifyPending playerId=" + player.playerId(), err));
                    }
                })
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                        "engine.error stage=pairScript matchId=" + matchId, err));
    }

    /**
     * Assigns every player a team label. Teams are sized after {@code teamSize};
     * the current match model always splits into two teams ("A"/"B").
     */
    private static List<String> teamAssignments(List<MatchmakingTicket> found, int playerCount) {
        int teamSize = found.get(0).gameMode().teamSize();
        List<String> assignments = new ArrayList<>();
        for (int i = 0; i < playerCount; i++) {
            assignments.add(i / teamSize == 0 ? "A" : "B");
        }
        return assignments;
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
            List<String> assignments = teamAssignments(found, pm.players().size());
            for (int i = 0; i < pm.players().size(); i++) {
                players.add(new com.victor.matchmaking.domain.match.MatchPlayer(pm.matchId(),
                        pm.players().get(i).playerId(), assignments.get(i), found.get(i).skill()));
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
                TicketStateMachine.transition(ticket.status(), target), ticket.version() + 1,
                ticket.partyId());
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
