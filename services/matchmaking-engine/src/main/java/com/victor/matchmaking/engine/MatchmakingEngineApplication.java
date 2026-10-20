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
import com.victor.matchmaking.engine.application.OutboxQueue;
import com.victor.matchmaking.engine.application.OutboxRecord;
import com.victor.matchmaking.engine.application.OutboxRelay;
import com.victor.matchmaking.engine.application.PairingPolicy;
import com.victor.matchmaking.engine.application.PendingMatch;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.engine.infrastructure.KafkaEventPublisher;
import com.victor.matchmaking.engine.infrastructure.OutboxMetricsPoller;
import com.victor.matchmaking.engine.infrastructure.PostgresOutboxQueue;
import com.victor.matchmaking.engine.infrastructure.QueueMetricsPoller;
import com.victor.matchmaking.engine.infrastructure.RedisPendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.WriteGate;
import com.victor.matchmaking.redis.Envelope;
import com.victor.matchmaking.redis.message.Payloads;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.RedisScanner;
import com.victor.matchmaking.redis.RedisPlayerNotifier;
import com.victor.matchmaking.redis.RedisPublisher;
import com.victor.matchmaking.redis.RedisScript;
import com.victor.matchmaking.redis.RedisSubscriber;
import com.victor.matchmaking.redis.RedisTicketRepository;
import com.victor.matchmaking.redis.TicketRepository;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/** Matchmaking worker: pairs queue heads, tracks confirmations, dissolves on decline/timeout. */
public final class MatchmakingEngineApplication {

    private static final System.Logger LOG = System.getLogger("matchmaking.engine");
    private static final long PAIRING_INTERVAL_MS = 500;
    private static final long SWEEP_INTERVAL_MS = 1000;
    /**
     * How often the outbox relay looks for events to publish (W14). Short enough that the
     * added lag is well under the refresh of the WebSocket push, long enough not to poll
     * a table 10 times a second for nothing.
     */
    private static final long OUTBOX_INTERVAL_MS = 250;
    /**
     * How often the derived KPIs (queue depth and head wait in Redis, outbox backlog in
     * Postgres) are read (W15). The scrape reads the last poll, so this is the bound on how
     * stale {@code matchmaking_queue_*} and {@code matchmaking_outbox_backlog} can be; every
     * two seconds is fresh enough for a dashboard and cheap enough for the event loop.
     */
    static final long KPI_POLL_INTERVAL_MS = 2000;
    /**
     * How many queue members one reaping pass inspects once an expired ticket is found (W16,
     * ADR 0025). Bounded on purpose: a queue full of ghosts is cleared over a few ticks of the
     * 500 ms loop instead of reading a whole queue in one tick.
     */
    static final long REAP_BATCH = 64;
    /**
     * How many matches one queue may pair in a single 500 ms tick (W16, ADR 0021). Pairing a
     * single match per tick put the ceiling at 2 matches/s per mode/region no matter the
     * hardware — measured under load, a queue of a thousand players drained at 4 players/s
     * while the engine sat idle. The budget lets a queue drain at Redis speed while keeping a
     * burst from monopolizing the event loop that also runs the sweep, the outbox relay and
     * the KPI pollers.
     */
    static final int PAIR_BATCH_PER_TICK = 200;
    /** Its own port: the engine has no public HTTP surface and must not grow one for metrics. */
    private static final int ENGINE_METRICS_PORT = 9091;
    static final long CONFIRMATION_TIMEOUT_SECONDS = 30;
    /**
     * The pending-match Redis key must outlive its confirmation deadline,
     * otherwise it expires before the sweeper can read it and the tickets stay
     * in CONFIRMING forever. TTL = deadline + this grace (W8 two-phase confirm).
     */
    static final long PENDING_KEY_GRACE_SECONDS = 60;
    static final long PENDING_KEY_TTL_SECONDS = CONFIRMATION_TIMEOUT_SECONDS + PENDING_KEY_GRACE_SECONDS;
    /**
     * How long a match's confirmation keys are kept: the set of players who confirmed and the
     * claim saying the match is spoken for. The set is deleted by the confirmation, the claim
     * expires, and the TTL is the guarantee for the match whose players never all confirm
     * (W16: the set had no TTL at all, so every confirmed match left one behind - 198 of them
     * after a single load run).
     */
    static final long MATCH_CONFIRMED_TTL_SECONDS = 300;
    private static final int TICKET_ID_LENGTH = 8;
    /**
     * How many writes the engine may have in flight against Postgres (W17, ADR 0022). A bound,
     * not a queue size: the pairing is the source of the confirmations and one tick can form
     * hundreds of matches, so without this a slow database receives every transaction at once
     * and the engine's own throughput becomes the outage. Sized small on purpose: the point is
     * to find out the database is behind while it still is behind, and the number is measured
     * (the {@code matchmaking_db_writes_in_flight} gauge) rather than assumed.
     */
    static final int DB_MAX_IN_FLIGHT = 8;
    /**
     * How long a confirmation that could not write waits before trying again. Short against the
     * database's latency (tens of milliseconds when healthy) and long against a hot loop that
     * would turn a saturated database into a busy-wait.
     */
    static final long DB_WRITE_RETRY_MS = 100;
    /**
     * How many times a deferred confirmation tries before giving up. Bounded so a database that
     * never comes back cannot leave timers alive forever; the window is longer than the
     * pending-match key's life, so the retry outlives the state it is racing.
     */
    static final int DB_WRITE_MAX_ATTEMPTS = 600;
    /**
     * Package-private with the pairing entry points below: the stale-member test drives one
     * pairing tick directly, which needs the script the app loads at startup.
     */
    /**
     * The pairing script. A {@link RedisScript} rather than a bare SHA (W18, ADR 0024): Redis
     * drops its script cache on a restart, a failover or a {@code SCRIPT FLUSH}, and a caller
     * that only remembers the SHA stops matching until it is restarted itself. Measured in W18:
     * a Redis restart left the engine looping on {@code NOSCRIPT} with the queue untouched.
     *
     * <p>Package-private and volatile because the tests drive one pairing tick by hand: they
     * install a script of their own against the same Redis.
     */
    static volatile RedisScript PAIR_SCRIPT;

    /** The pairing script against this client; package-private for the by-hand pairing tests. */
    static RedisScript pairScript(io.vertx.redis.client.Redis redis) {
        return new RedisScript(redis, "/pair.lua");
    }

    private MatchmakingEngineApplication() {
    }

    public static void main(String[] args) {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %5$s%n");
        java.util.Locale.setDefault(java.util.Locale.ENGLISH);
        // The registry before the Vertx instance (see MatchmakingMetrics): the engine's own
        // meters and Vert.x's (event loop, JVM) end up in one scrape (W15, ADR 0020).
        com.victor.matchmaking.metrics.MatchmakingMetrics metrics =
                new com.victor.matchmaking.metrics.MatchmakingMetrics();
        Vertx vertx = Vertx.vertx(metrics.vertxOptions());
        EngineKpis kpis = new EngineKpis(metrics.registry());
        com.victor.matchmaking.metrics.MetricsEndpoint.start(vertx, metrics.registry(),
                        com.victor.matchmaking.metrics.MetricsEndpoint.portFromEnv(ENGINE_METRICS_PORT))
                .onSuccess(s -> LOG.log(System.Logger.Level.INFO, "metrics.started port={0}",
                        String.valueOf(s.actualPort())))
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=metricsStart", err));
        String redisUri = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
        io.vertx.redis.client.Redis redis = io.vertx.redis.client.Redis.createClient(vertx,
                com.victor.matchmaking.redis.RedisClients.options(redisUri));
        TicketRepository tickets = new RedisTicketRepository(redis);
        PendingMatchStore pending = new RedisPendingMatchStore(redis);
        RedisPublisher publisher = new RedisPublisher(redis);
        RedisPlayerNotifier notifier = new RedisPlayerNotifier(publisher);
        com.victor.matchmaking.db.DatabaseConfig dbConfig = com.victor.matchmaking.db.DatabaseConfig.fromEnv();
        com.victor.matchmaking.db.DatabaseMigrator.migrate(dbConfig);
        PostgresOutboxQueue outbox = PostgresOutboxQueue.connect(vertx, dbConfig);

        // Durable event log (Kafka): Redis carries the live push to the API, Kafka the
        // event every other consumer (orchestrator, analytics) reads. See ADR 0014/0016.
        // Since W14 the engine does not publish on the confirmation path: the event is a
        // row written in the same transaction as the match, and this relay publishes it
        // (ADR 0018), so a broker outage delays the event instead of losing it.
        KafkaEventPublisher kafkaPublisher = KafkaEventPublisher.fromEnv();
        OutboxRelay relay = new OutboxRelay(outbox, kafkaPublisher, java.time.Clock.systemUTC(), relayId());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaPublisher.close();
            outbox.close();
        }, "engine-outbox-close"));

        // The database's concurrency bound (W17, ADR 0022): the pairing checks it before forming
        // a match and the confirmation waits for a slot instead of issuing its transaction. The
        // gauge is read from the gate itself, so the exposition carries the number the engine is
        // actually deciding on.
        WriteGate dbGate = WriteGate.of(DB_MAX_IN_FLIGHT);
        metrics.registry().gauge(com.victor.matchmaking.metrics.MetricNames.DB_WRITES_IN_FLIGHT, dbGate,
                WriteGate::inFlight);

        RedisSubscriber.subscribe(vertx, redisUri, RedisKeys.CHANNEL_MATCH_CONFIRMATIONS,
                msg -> handleConfirmation(msg, redis, tickets, pending, notifier, outbox, kpis, dbGate, vertx));

        // Load the atomic pairing script before starting the pairing loop. The SHA is cached
        // inside the script (which reloads it when Redis forgets it): the engine no longer has to
        // be restarted because Redis was.
        PAIR_SCRIPT = pairScript(redis);
        PAIR_SCRIPT.load().onSuccess(sha -> {
            vertx.setPeriodic(PAIRING_INTERVAL_MS, id -> pairAcrossQueues(vertx, redis, tickets, pending,
                    notifier, kpis, dbGate));
            vertx.setPeriodic(SWEEP_INTERVAL_MS, id -> sweepExpired(redis, tickets, pending, notifier, kpis));
            vertx.setPeriodic(OUTBOX_INTERVAL_MS, id -> relay.drainOnce()
                    .onSuccess(kpis::relayDrained)
                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=outboxDrain", err)));

            // The derived KPIs: the queues live in Redis, the outbox in Postgres, and both are
            // read asynchronously into gauges (never from the scrape thread).
            QueueMetricsPoller queueKpis = new QueueMetricsPoller(redis, kpis, java.time.Clock.systemUTC());
            OutboxMetricsPoller outboxKpis = new OutboxMetricsPoller(outbox, kpis);
            vertx.setPeriodic(KPI_POLL_INTERVAL_MS, id -> queueKpis.pollOnce()
                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=queueKpis", err)));
            vertx.setPeriodic(KPI_POLL_INTERVAL_MS, id -> outboxKpis.pollOnce()
                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=outboxKpis", err)));

            LOG.log(System.Logger.Level.INFO, "engine.started pairingIntervalMs={0} sweepIntervalMs={1} confirmationTimeoutS={2} pairScript={3}",
                    String.valueOf(PAIRING_INTERVAL_MS), String.valueOf(SWEEP_INTERVAL_MS),
                    String.valueOf(CONFIRMATION_TIMEOUT_SECONDS), sha);
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=loadPairScript", err));
    }

    /**
     * Every queue the engine can pair from, from the closed set of game modes and regions
     * ({@link RedisKeys#allQueueKeys()}).
     *
     * <p>Why this replaced {@code SCAN}: the pairing used to ask Redis for one {@code SCAN} page
     * and ignore the cursor, so with a few hundred keys the answer was an empty page and the
     * engine matched nothing at all while players waited (measured in W16: 404 keys, no key
     * returned, the queue on page 6 of 39). Building the names is exact, and an empty queue
     * costs one trivial {@code ZRANGE}.
     */
    static List<String> queueKeys() {
        return RedisKeys.allQueueKeys();
    }

    private static void pairAcrossQueues(Vertx vertx, io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis, WriteGate dbGate) {
        if (dbGate.saturated()) {
            // The database is at its write limit: forming more matches would only create more
            // confirmations it cannot take. The queue waits, and the boundary that can say no to
            // a player (the admission cap) is what absorbs the arrival rate (W17, ADR 0022).
            LOG.log(System.Logger.Level.WARNING,
                    "engine.backpressure pairing paused: database writes in flight={0} limit={1}",
                    String.valueOf(dbGate.inFlight()), String.valueOf(dbGate.limit()));
            return;
        }
        for (String queueKey : queueKeys()) {
            tryPairFromQueue(queueKey, redis, tickets, pending, notifier, kpis, dbGate);
        }
    }

    static void tryPairFromQueue(String queueKey, io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis) {
        tryPairFromQueue(queueKey, redis, tickets, pending, notifier, kpis, WriteGate.unbounded());
    }

    static void tryPairFromQueue(String queueKey, io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis, WriteGate dbGate) {
        pairBatch(queueKey, redis, tickets, pending, notifier, kpis, PAIR_BATCH_PER_TICK, true, dbGate);
    }

    /**
     * Pairs from one queue until the queue stops giving (no roster fits the head, or another
     * worker took it) or this tick's budget is spent.
     *
     * @param budget       matches still allowed in this tick; keeps a burst from monopolizing
     *                     the event loop while letting a queue drain at Redis speed
     * @param mayLookAgain false on the pass that follows a reap, so a queue made only of
     *                     ghosts cannot make this tick loop
     * @param dbGate       refuses to form a match while the database is at its write limit
     */
    private static void pairBatch(String queueKey, io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis, int budget,
            boolean mayLookAgain, WriteGate dbGate) {
        if (budget <= 0) {
            return;
        }
        if (dbGate.saturated()) {
            return;
        }
        GameMode gameMode = gameModeOfQueueKey(queueKey);
        if (gameMode == null) {
            return;
        }
        int playerCount = gameMode.playersPerMatch();
        readHead(queueKey, playerCount, redis, tickets).onSuccess(head -> {
            if (!head.deadMembers().isEmpty()) {
                // A ticket expires on its TTL, but a ZSET member cannot: it stays in the
                // queue as a ghost, and read as the head it blocks every compatible player
                // behind it - the whole mode/region stops pairing (measured in W15). Reap
                // the head batch and look once more in the same tick, so the players behind
                // the ghost get their turn now instead of waiting for a tick of their own.
                reapStaleMembers(queueKey, redis, tickets).onComplete(reaped -> {
                    if (mayLookAgain) {
                        pairBatch(queueKey, redis, tickets, pending, notifier, kpis, budget, false, dbGate);
                    }
                });
                return;
            }
            if (head.candidates().size() < playerCount) {
                return;
            }
            // The queue head (oldest ticket) is the widening reference: its
            // queuedAt tells PairingPolicy how long the player has been waiting.
            List<MatchmakingTicket> roster = buildRoster(head.candidates(), gameMode,
                    head.candidates().get(0).queuedAt());
            if (roster == null) {
                return;
            }
            // Chain the next attempt on this one's completion, so the queue is read after the
            // tickets are really off it; a nil from the script means somebody else paired them,
            // and then this tick is done with the queue.
            pair(roster, queueKey, redis, tickets, pending, notifier, kpis).onComplete(paired -> {
                if (paired.succeeded() && Boolean.TRUE.equals(paired.result())) {
                    pairBatch(queueKey, redis, tickets, pending, notifier, kpis, budget - 1, mayLookAgain, dbGate);
                }
            });
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=readQueue queue={0}", err, queueKey));
    }

    /**
     * The head of a queue: the tickets that are still alive, in queue order, and the members
     * whose ticket is gone.
     */
    private record QueueHead(List<MatchmakingTicket> candidates, List<String> deadMembers) {
    }

    /** Reads up to {@code playerCount} members of a queue and resolves their tickets. */
    private static Future<QueueHead> readHead(String queueKey, int playerCount,
            io.vertx.redis.client.Redis redis, TicketRepository tickets) {
        return redis.send(Request.cmd(Command.ZRANGE).arg(queueKey).arg("0").arg(String.valueOf(playerCount - 1)))
                .compose(range -> resolve(queueKey, range, tickets));
    }

    /**
     * Inspects the head batch of a queue and takes out the members whose ticket no longer
     * exists. Bounded on purpose: the head batch, not the whole queue, and only when a ghost
     * was seen - a queue of thousands of ghosts clears in a few ticks of the 500 ms loop.
     */
    private static Future<Void> reapStaleMembers(String queueKey, io.vertx.redis.client.Redis redis,
            TicketRepository tickets) {
        return redis.send(Request.cmd(Command.ZRANGE).arg(queueKey).arg("0")
                        .arg(String.valueOf(REAP_BATCH - 1)))
                .compose(range -> resolve(queueKey, range, tickets))
                .compose(head -> {
                    if (head.deadMembers().isEmpty()) {
                        return Future.succeededFuture();
                    }
                    Request zrem = Request.cmd(Command.ZREM).arg(queueKey);
                    for (String member : head.deadMembers()) {
                        zrem.arg(member);
                    }
                    return redis.send(zrem).onSuccess(removed -> LOG.log(System.Logger.Level.INFO,
                                    "engine.reapedStaleMembers queue={0} inspected={1} removed={2}",
                                    queueKey, String.valueOf(head.candidates().size() + head.deadMembers().size()),
                                    String.valueOf(removed)))
                            .mapEmpty();
                });
    }

    /** Splits the members of a {@code ZRANGE} reply into live tickets and dead members. */
    private static Future<QueueHead> resolve(String queueKey, Response range, TicketRepository tickets) {
        if (range == null || range.size() == 0) {
            return Future.succeededFuture(new QueueHead(List.of(), List.of()));
        }
        List<String> members = new ArrayList<>();
        List<Future<MatchmakingTicket>> loaded = new ArrayList<>();
        for (int i = 0; i < range.size(); i++) {
            members.add(range.get(i).toString());
            loaded.add(tickets.findById(members.get(i)));
        }
        return Future.all(loaded).map(done -> {
            List<MatchmakingTicket> candidates = new ArrayList<>();
            List<String> deadMembers = new ArrayList<>();
            for (int i = 0; i < members.size(); i++) {
                MatchmakingTicket ticket = loaded.get(i).result();
                if (ticket == null) {
                    deadMembers.add(members.get(i));
                } else {
                    candidates.add(ticket);
                }
            }
            return new QueueHead(candidates, deadMembers);
        });
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

    /**
     * Pairs exactly the given candidates into one pending match.
     *
     * @return true when this worker won the atomic race and the match exists, false when the
     *         Lua script returned nil because another worker had already taken those tickets
     */
    static Future<Boolean> pair(List<MatchmakingTicket> candidates, String queueKey,
            io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis) {
        String matchId = "match-" + UUID.randomUUID().toString().substring(0, TICKET_ID_LENGTH);
        List<PendingMatch.PendingPlayer> roster = new ArrayList<>();
        for (MatchmakingTicket candidate : candidates) {
            roster.add(new PendingMatch.PendingPlayer(candidate.playerId(), candidate.id()));
        }
        PendingMatch pm = new PendingMatch(matchId, roster, List.of(),
                Instant.now().plusSeconds(CONFIRMATION_TIMEOUT_SECONDS));

        // Atomic pairing: the roster goes off the queue and the pending match is set in one Lua
        // script. The members are named, not read from the queue inside the script: a caller
        // whose read overlapped another tick's pairing must take nothing, or it removes the
        // wrong members and stores a roster nobody paired (W16). A nil answer means another
        // worker won the race and we skip.
        //
        // The request is built from the SHA inside the lambda, not held in a field: after a Redis
        // restart the script reloads with a new SHA, and a request built from the old one is
        // exactly the NOSCRIPT this class recovers from (W18, ADR 0024).
        return PAIR_SCRIPT.eval(sha -> {
            Request pairRequest = Request.cmd(Command.EVALSHA).arg(sha)
                    .arg("1").arg(queueKey)
                    .arg(pm.matchId()).arg(pm.toJson().encode())
                    .arg(String.valueOf(PENDING_KEY_TTL_SECONDS))
                    .arg(String.valueOf(roster.size()));
            for (MatchmakingTicket candidate : candidates) {
                pairRequest.arg(candidate.id());
            }
            return pairRequest;
        })
                .compose(result -> {
                    if (result == null) {
                        // Another worker already removed these tickets from the queue
                        return Future.succeededFuture(Boolean.FALSE);
                    }
                    // The roster is off the queue: this is the moment the wait ended, and the
                    // only place it can be measured per player (W15). Recorded before the
                    // notifications, so a slow notifier cannot hide a fast pairing.
                    kpis.matchFormed(candidates, Instant.now());
                    // Paired! The tickets must be CONFIRMING before anyone is told: the
                    // notification is what opens the players' confirmation window, and a client
                    // that answers within milliseconds used to beat these (unawaited) writes -
                    // its confirmation was refused as "tickets not in CONFIRMING" and the match
                    // then died on its timeout (measured in W16: a whole load run dissolving 141
                    // matches, every one of them a client that was too fast for the engine).
                    List<Future<Void>> confirming = new ArrayList<>();
                    // The reverse index a reconnecting client needs (W19, ADR 0026): player →
                    // match, written alongside the tickets that are now CONFIRMING. A beat later
                    // and a heartbeat in between would find the tickets confirming but no match
                    // to re-present; the index is part of "the match exists now", not a follow-up.
                    confirming.add(pending.indexPlayers(pm)
                            .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                    "engine.error stage=indexPlayers matchId=" + matchId, err)));
                    for (MatchmakingTicket candidate : candidates) {
                        confirming.add(tickets.save(null, transition(candidate, TicketState.CONFIRMING))
                                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                        "engine.error stage=saveConfirmingTicket ticketId=" + candidate.id(), err)));
                    }
                    return Future.all(confirming).map(ignored -> {
                        Payloads.MatchPending pendingPayload = new Payloads.MatchPending(matchId,
                                roster.stream().map(PendingMatch.PendingPlayer::playerId).toList());
                        for (PendingMatch.PendingPlayer player : roster) {
                            notifier.notifyPlayer(player.playerId(), Envelope.of("MATCH_PENDING_CONFIRMATION", pendingPayload), Payloads.MatchPending::toJson)
                                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                            "engine.error stage=notifyPending playerId=" + player.playerId(), err));
                        }
                        return Boolean.TRUE;
                    });
                })
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                        "engine.error stage=pairScript matchId=" + matchId, err))
                .otherwise(false);
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

    /** The event's own clock, so the relay's lag is measured against the fact, not against the write. */
    private static Instant occurredAtOf(com.victor.matchmaking.events.MatchConfirmed event) {
        return Instant.ofEpochSecond(
                event.getMeta().getOccurredAt().getSeconds(), event.getMeta().getOccurredAt().getNanos());
    }

    /** Identifies this relay on the rows it claims, for whoever has to read a stuck queue later. */
    private static String relayId() {
        return "engine-" + ProcessHandle.current().pid();
    }

    static void handleConfirmation(JsonObject msg, io.vertx.redis.client.Redis redis,
            TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, OutboxQueue outbox, EngineKpis kpis) {
        handleConfirmation(msg, redis, tickets, pending, notifier, outbox, kpis, WriteGate.unbounded(), null);
    }

    static void handleConfirmation(JsonObject msg, io.vertx.redis.client.Redis redis,
            TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, OutboxQueue outbox, EngineKpis kpis, WriteGate dbGate,
            Vertx vertx) {
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
                dissolve(pm, com.victor.matchmaking.metrics.MetricNames.REASON_DECLINED,
                        "declined by " + playerId, tickets, pending, notifier, redis, kpis);
                return;
            }
            if ("CONFIRM".equals(action)) {
                if (!pm.playerIds().contains(playerId)) {
                    LOG.log(System.Logger.Level.WARNING,
                            "confirm.rejected matchId={0} playerId={1} reason=not a player of this match",
                            matchId, playerId);
                    return;
                }
                String confirmedKey = RedisKeys.matchConfirmedKey(matchId);
                String claimKey = RedisKeys.matchConfirmedClaimKey(matchId);
                redisCommands(redis, Request.cmd(Command.SADD).arg(confirmedKey).arg(playerId))
                        .flatMap(v -> redisCommands(redis, Request.cmd(Command.EXPIRE)
                                .arg(confirmedKey).arg(String.valueOf(MATCH_CONFIRMED_TTL_SECONDS))))
                        .flatMap(v -> redisCommands(redis, Request.cmd(Command.SCARD).arg(confirmedKey)))
                        .flatMap(count -> {
                            long confirmed = count.toLong() != null ? count.toLong() : 0L;
                            LOG.log(System.Logger.Level.INFO,
                                    "confirm.received matchId={0} playerId={1} count={2} required={3}",
                                    matchId, playerId, String.valueOf(confirmed),
                                    String.valueOf(pm.players().size()));
                            if (confirmed < pm.players().size()) {
                                return Future.succeededFuture(Boolean.FALSE);
                            }
                            // SET NX is the whole decision: the first caller to create the claim
                            // confirms, and every later one is refused - a retry, a duplicate
                            // delivery, a message that arrives while the match is still being
                            // deleted. Counting alone does not do it: a retry after the count was
                            // already complete rebuilds a complete set and would confirm the same
                            // match again (measured in W16: one match, four events).
                            return redisCommands(redis, Request.cmd(Command.SET).arg(claimKey).arg("1")
                                            .arg("NX").arg("EX")
                                            .arg(String.valueOf(MATCH_CONFIRMED_TTL_SECONDS)))
                                    .map(set -> {
                                        boolean won = set != null;
                                        if (!won) {
                                            LOG.log(System.Logger.Level.INFO,
                                                    "confirm.duplicate matchId={0} playerId={1} ignored=true",
                                                    matchId, playerId);
                                        }
                                        return won;
                                    });
                        })
                        .onSuccess(isConfirmer -> {
                            if (Boolean.TRUE.equals(isConfirmer)) {
                                // The count has done its job; the claim key is what says the match
                                // is spoken for, and it expires on its own.
                                redisCommands(redis, Request.cmd(Command.DEL).arg(confirmedKey))
                                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                                "engine.error stage=deleteConfirmedSet matchId=" + matchId, err));
                                confirm(pm, tickets, pending, notifier, outbox, kpis, dbGate, vertx, 0);
                            }
                        })
                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=countConfirmations matchId=" + matchId, err));
            }
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=loadPendingMatch matchId=" + matchId, err));
    }

    private static void confirm(PendingMatch pm, TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, OutboxQueue outbox, EngineKpis kpis, WriteGate dbGate,
            Vertx vertx, int attempt) {
        // The database's concurrency bound comes first, before any side effect: a confirmation
        // that cannot write yet must not leave the match half-done (tickets CONFIRMED, row
        // missing). The claim key keeps the match alive for the sweep, and the pending match is
        // what the retry keys off, so nothing is written until there is a slot (W17, ADR 0022).
        if (!dbGate.tryEnter()) {
            kpis.dbWriteDeferred();
            if (vertx == null) {
                LOG.log(System.Logger.Level.ERROR,
                        "engine.backpressure confirmations must be retried with a scheduler matchId={0}",
                        pm.matchId());
                return;
            }
            if (attempt >= DB_WRITE_MAX_ATTEMPTS) {
                LOG.log(System.Logger.Level.ERROR,
                        "engine.backpressure gave up persisting a confirmed match matchId={0} attempts={1} inFlight={2}",
                        pm.matchId(), String.valueOf(attempt), String.valueOf(dbGate.inFlight()));
                return;
            }
            LOG.log(System.Logger.Level.WARNING,
                    "engine.backpressure confirmation deferred matchId={0} attempt={1} inFlight={2} limit={3}",
                    pm.matchId(), String.valueOf(attempt), String.valueOf(dbGate.inFlight()),
                    String.valueOf(dbGate.limit()));
            vertx.setTimer(DB_WRITE_RETRY_MS, id -> pending.findById(pm.matchId()).onSuccess(latest -> {
                if (latest != null) {
                    confirm(latest, tickets, pending, notifier, outbox, kpis, dbGate, vertx, attempt + 1);
                }
            }));
            return;
        }
        // One release point for the slot, whatever the outcome: a permit that is not given back
        // shrinks the database's concurrency for the life of the process.
        //
        // A write that FAILS is retried like a write that had to wait, and for the same reason
        // (W18, ADR 0024): a database that is unreachable for a moment must not cost the players
        // their match while the sweep's deadline is still minutes away. The tickets are still
        // CONFIRMING (the durable fact is written before the hot state moves), so a retry that
        // succeeds finishes the same match, and a retry that runs out of attempts leaves a state
        // the sweep can requeue rather than hot state nobody can repair.
        persistConfirmedMatch(pm, tickets, pending, notifier, outbox, kpis)
                .onFailure(err -> {
                    LOG.log(System.Logger.Level.ERROR,
                            "engine.backpressure confirmation write failed matchId={0} attempt={1} inFlight={2}",
                            pm.matchId(), String.valueOf(attempt), String.valueOf(dbGate.inFlight()));
                    if (vertx == null || attempt >= DB_WRITE_MAX_ATTEMPTS) {
                        return;
                    }
                    vertx.setTimer(DB_WRITE_RETRY_MS, id -> pending.findById(pm.matchId()).onSuccess(latest -> {
                        if (latest != null) {
                            confirm(latest, tickets, pending, notifier, outbox, kpis, dbGate, vertx,
                                    attempt + 1);
                        }
                    }));
                })
                .onComplete(done -> dbGate.leave());
    }

    /**
     * Writes the match with its event and only then moves the players' tickets to CONFIRMED.
     *
     * <p>Ordering matters and is the point of the shape (W18, ADR 0024). The durable fact goes
     * first: the row, the roster and the event are one transaction (W14), and until that
     * transaction lands the players are still {@code CONFIRMING}. A write that fails therefore
     * leaves a state the sweep already knows how to recover — the pending match dissolves on its
     * deadline and the tickets go back to {@code SEARCHING} — instead of hot state that nobody
     * can repair (measured before this: the tickets went {@code CONFIRMED} while the row was
     * never written, and the sweep could not requeue what was no longer {@code CONFIRMING}).
     *
     * <p>The notification comes last, for the same reason: it is what tells a player "you are in
     * a match", and sending it before the row is durable tells him something the database does
     * not know yet.
     */
    private static Future<Void> persistConfirmedMatch(PendingMatch pm, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, OutboxQueue outbox, EngineKpis kpis) {
        List<Future<MatchmakingTicket>> loaded = pm.players().stream()
                .map(p -> tickets.findById(p.ticketId()))
                .toList();
        return Future.all(loaded).compose(cf -> {
            List<MatchmakingTicket> found = loaded.stream().map(Future::result).toList();
            if (found.stream().anyMatch(t -> t == null || t.status() != TicketState.CONFIRMING)) {
                LOG.log(System.Logger.Level.WARNING,
                        "match.confirmRejected matchId={0} reason=tickets not in CONFIRMING", pm.matchId());
                return Future.succeededFuture();
            }
            MatchmakingTicket first = found.get(0);
            List<com.victor.matchmaking.domain.match.MatchPlayer> players = new ArrayList<>();
            List<String> assignments = teamAssignments(found, pm.players().size());
            for (int i = 0; i < pm.players().size(); i++) {
                players.add(new com.victor.matchmaking.domain.match.MatchPlayer(pm.matchId(),
                        pm.players().get(i).playerId(), assignments.get(i), found.get(i).skill()));
            }
            com.victor.matchmaking.domain.match.Match match = new com.victor.matchmaking.domain.match.Match(
                    pm.matchId(), first.gameMode(), first.region(),
                    com.victor.matchmaking.domain.match.MatchStatus.PENDING, Instant.now(), null, null);
            // The event is built here, at the moment the fact happens, and stored as bytes
            // next to the match: the same transaction writes both, so a crash can no longer
            // leave a match nobody was told about (W14, ADR 0018). Publishing is the
            // relay's job, and the event id is what tells a consumer this event is this
            // event (ADR 0038).
            com.victor.matchmaking.events.MatchConfirmed event =
                    com.victor.matchmaking.events.MatchConfirmedMapper.toEvent(match, players, Instant.now(), pm.matchId());
            OutboxRecord record = new OutboxRecord(
                    event.getMeta().getEventId(),
                    com.victor.matchmaking.events.EventTopics.MATCH_CONFIRMED,
                    match.id(),
                    event.getMeta().getEventType(),
                    event.toByteArray(),
                    occurredAtOf(event),
                    Instant.now(),
                    0);
            return outbox.writeMatchWithEvent(match, players, record)
                    .onSuccess(ignored -> {
                        // The match is a fact now: counted here, not when the event reaches the
                        // broker (that is the outbox KPI's job). Same moment, different question.
                        kpis.matchConfirmed(first.gameMode(), first.region());
                        LOG.log(System.Logger.Level.INFO,
                                "match.confirmed matchId={0} players={1} persisted=true eventId={2} outbox=PENDING",
                                pm.matchId(), pm.playerIds(), record.eventId());
                        // The hot state follows the durable fact, never the other way around
                        // (W18, ADR 0024). These writes used to run before the row existed: with
                        // the database unavailable the tickets became CONFIRMED for a match that
                        // was never written, and the timeout sweep could no longer return them to
                        // the queue (it only requeues what is still CONFIRMING) - measured: 0 rows,
                        // 2 tickets CONFIRMED for good. A failure here leaves the tickets
                        // CONFIRMING and the pending match in place, which is a state the sweep
                        // already knows how to recover.
                        for (MatchmakingTicket t : found) {
                            tickets.save(null, transition(t, TicketState.CONFIRMED))
                                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                            "engine.error stage=saveConfirmedTicket ticketId=" + t.id(), err));
                        }
                        pending.delete(pm.matchId()).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=deletePendingMatch matchId=" + pm.matchId(), err));
                        pending.clearPlayerIndex(pm).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "engine.error stage=clearPlayerIndex matchId=" + pm.matchId(), err));
                        for (PendingMatch.PendingPlayer player : pm.players()) {
                            notifier.notifyPlayer(player.playerId(), Envelope.of("MATCH_CONFIRMED",
                                    new Payloads.MatchConfirmed(pm.matchId())), Payloads.MatchConfirmed::toJson)
                                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                            "engine.error stage=notifyConfirmed playerId=" + player.playerId(), err));
                        }
                    })
                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                            "engine.error stage=persistMatchWithEvent matchId=" + pm.matchId(), err));
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=loadTicketsForConfirm", err));
    }

    private static void dissolve(PendingMatch pm, String reasonTag, String detail, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, io.vertx.redis.client.Redis redis,
            EngineKpis kpis) {
        LOG.log(System.Logger.Level.INFO, "match.dissolved matchId={0} reason={1} detail={2}",
                pm.matchId(), reasonTag, detail);
        // The tag is a closed set (timeout, declined): a free-form reason would create one
        // series per player id. The detail still travels to the players and to the log.
        kpis.matchDissolved(reasonTag);
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
        pending.clearPlayerIndex(pm).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "engine.error stage=clearPlayerIndex matchId=" + pm.matchId(), err));
        redis.send(Request.cmd(Command.DEL).arg(RedisKeys.matchConfirmedKey(pm.matchId()))
                        .arg(RedisKeys.matchConfirmedClaimKey(pm.matchId())))
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                        "engine.error stage=deleteConfirmedSet matchId=" + pm.matchId(), err));
        for (PendingMatch.PendingPlayer player : pm.players()) {
            notifier.notifyPlayer(player.playerId(), Envelope.of("MATCH_CANCELLED",
                    new Payloads.MatchCancelled(pm.matchId(), detail)), Payloads.MatchCancelled::toJson)
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

    /**
     * Dissolves the pending matches whose confirmation window has passed. The pending matches are
     * the one set of keys the engine cannot name in advance, so they are found with a real
     * {@code SCAN} - every page of it ({@link RedisScanner}); the single-page version used to
     * miss them the same way it missed the queues (W16).
     *
     * <p>Package-private so a test can run one sweep by hand: it is also the recovery path when a
     * confirmation could not be written (W18, ADR 0024).
     */
    static void sweepExpired(io.vertx.redis.client.Redis redis, TicketRepository tickets,
            PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis) {
        RedisScanner.scanAll(redis, "match-pending:*")
                .onSuccess(keys -> {
                    for (String key : keys) {
                        pending.findById(key.substring("match-pending:".length()))
                                .onSuccess(pm -> {
                                    if (pm != null && Instant.now().isAfter(pm.expiresAt())) {
                                        // A claimed match is one every player already confirmed and
                                        // whose row is being written (possibly waiting for a database
                                        // write slot, W17). Dissolving it would cancel a match the
                                        // players accepted, so the sweep lets the claim own it.
                                        skipIfClaimed(redis, pm, tickets, pending, notifier, kpis);
                                    }
                                })
                                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                        "engine.error stage=loadExpiredPending key=" + key, err));
                    }
                })
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR, "engine.error stage=sweepExpired", err));
    }

    /** Dissolves an expired match unless its confirmation claim says it is spoken for. */
    private static void skipIfClaimed(io.vertx.redis.client.Redis redis, PendingMatch pm,
            TicketRepository tickets, PendingMatchStore pending, RedisPlayerNotifier notifier, EngineKpis kpis) {
        redis.send(Request.cmd(Command.EXISTS).arg(RedisKeys.matchConfirmedClaimKey(pm.matchId())))
                .onSuccess(exists -> {
                    if (exists != null && exists.toLong() == 1L) {
                        LOG.log(System.Logger.Level.INFO,
                                "engine.sweepSkipped matchId={0} reason=confirmed-awaiting-persist",
                                pm.matchId());
                        return;
                    }
                    dissolve(pm, com.victor.matchmaking.metrics.MetricNames.REASON_TIMEOUT,
                            "timeout", tickets, pending, notifier, redis, kpis);
                })
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                        "engine.error stage=sweepClaimCheck matchId=" + pm.matchId(), err));
    }
}
