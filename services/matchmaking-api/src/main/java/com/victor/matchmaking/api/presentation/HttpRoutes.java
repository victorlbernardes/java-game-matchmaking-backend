package com.victor.matchmaking.api.presentation;

import com.victor.matchmaking.api.application.AddMemberUseCase;
import com.victor.matchmaking.api.application.CancelTicketUseCase;
import com.victor.matchmaking.api.application.CreatePartyUseCase;
import com.victor.matchmaking.api.application.CreateTicketCommand;
import com.victor.matchmaking.api.application.CreateTicketUseCase;
import com.victor.matchmaking.api.application.DissolvePartyUseCase;
import com.victor.matchmaking.api.application.GetPartyUseCase;
import com.victor.matchmaking.api.application.GetTicketUseCase;
import com.victor.matchmaking.api.application.RemoveMemberUseCase;
import com.victor.matchmaking.api.application.PartyUseCases;
import com.victor.matchmaking.db.MatchRepository;
import com.victor.matchmaking.domain.player.SkillProvider;
import com.victor.matchmaking.redis.PartyStore;
import com.victor.matchmaking.redis.QueueFullException;
import com.victor.matchmaking.redis.RateLimiter;
import com.victor.matchmaking.redis.TicketRepository;
import com.victor.matchmaking.api.infrastructure.DeterministicSkillProvider;
import com.victor.matchmaking.api.infrastructure.InMemoryPartyStore;
import com.victor.matchmaking.api.infrastructure.InMemoryTicketRepository;
import com.victor.matchmaking.redis.TicketCodec;
import com.victor.matchmaking.redis.RedisPublisher;
import com.victor.matchmaking.metrics.MetricNames;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

/** HTTP routes for the matchmaking gateway. */
public final class HttpRoutes {

    private static final System.Logger LOG = System.getLogger("matchmaking.api.http");

    /** The bucket's scope: the limit is "ticket creations per player", not per connection. */
    private static final String RATE_LIMIT_SCOPE = "tickets";
    private static final String HEADER_RATE_LIMIT_LIMIT = "X-RateLimit-Limit";
    private static final String HEADER_RATE_LIMIT_REMAINING = "X-RateLimit-Remaining";
    private static final String HEADER_RETRY_AFTER = "Retry-After";
    /**
     * How long a shed arrival is asked to wait. A promise would have to be derived from the
     * drain rate and the queue depth, and it changes every tick; the honest answer is the one the
     * client can act on - come back later, and the queue will have moved (ADR 0022).
     */
    private static final int QUEUE_FULL_RETRY_AFTER_SECONDS = 5;
    /**
     * A dependency that is unreachable is usually unreachable for a second, not a minute: the
     * answer says "retry now", and the client that backs off politely retries once and succeeds
     * (W18, ADR 0024).
     */
    private static final int DEPENDENCY_RETRY_AFTER_SECONDS = 1;

    /**
     * What the clients say when their dependency is gone, lowercased. Every entry was observed in a
     * real outage (W18): a stopped Redis, a stopped Postgres whose pooled connection had already
     * been established when it went away, and the two script/queue limits. A new client means a new
     * wording — which is why a failure that matches nothing stays a 500 with the details in the log
     * rather than being guessed at.
     */
    private static final java.util.List<String> UNAVAILABLE_MESSAGE_FRAGMENTS = java.util.List.of(
            "noscript",                    // the script cache was lost (Redis restart)
            "refused",                     // "Connection refused: host:port" / "Connection to host:5432 refused"
            "connection has been closed",  // Vert.x pools hand back a connection the server closed
            "connection is closed",
            "connection reset",
            "broken pipe",
            "max wait queue size");        // the pool's waiting queue, bounded, is full

    private HttpRoutes() {
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth) {
        return createRouter(vertx, jwtAuth, new InMemoryTicketRepository());
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository) {
        return createRouter(vertx, jwtAuth, repository, new WsHub(null), null);
    }

    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, RedisPublisher publisher) {
        return createRouter(vertx, jwtAuth, repository, new WsHub(publisher), null);
    }

    /** Full injection used by main: caller keeps the WsHub to route Pub/Sub notifications. */
    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub) {
        return createRouter(vertx, jwtAuth, repository, wsHub, null);
    }

    /** Full injection used by main when history is backed by PostgreSQL. */
    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub,
            MatchRepository matchRepository) {
        return devRouter(vertx, jwtAuth, repository, wsHub, matchRepository);
    }

    /** Full injection with the real skill provider and party store (main wiring). */
    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub,
            MatchRepository matchRepository, SkillProvider skillProvider, PartyStore partyStore) {
        return createRouter(vertx, jwtAuth, repository, wsHub, matchRepository, skillProvider, partyStore,
                new SimpleMeterRegistry());
    }

    /**
     * Main wiring plus the registry the domain KPIs are counted on. The routes count what
     * crosses them (a ticket enqueued is an arrival, and only the boundary sees every
     * arrival, including the ones the queue never pairs); deeper numbers come from the
     * state itself in the engine.
     */
    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub,
            MatchRepository matchRepository, SkillProvider skillProvider, PartyStore partyStore,
            MeterRegistry registry) {
        return createRouter(vertx, jwtAuth, repository, wsHub, matchRepository, skillProvider, partyStore,
                registry, null);
    }

    /**
     * Main wiring plus the ingress rate limiter ({@code null} disables it, which is what tests and
     * the in-memory wiring use). The limiter is mandatory in the real service: it is the only
     * thing between one authenticated caller and a queue nobody can drain (ADR 0023).
     */
    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub,
            MatchRepository matchRepository, SkillProvider skillProvider, PartyStore partyStore,
            MeterRegistry registry, RateLimiter rateLimiter) {
        return createRouter(vertx, jwtAuth, repository, wsHub, matchRepository, skillProvider, partyStore,
                registry, rateLimiter, Readiness.none());
    }

    /**
     * Main wiring plus the readiness checks behind {@code GET /health/ready} (W18, ADR 0024).
     * {@link Readiness#none()} means "nothing to probe" (tests and the in-memory wiring), which is
     * an instance that is ready by definition.
     */
    public static Router createRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub,
            MatchRepository matchRepository, SkillProvider skillProvider, PartyStore partyStore,
            MeterRegistry registry, RateLimiter rateLimiter, Readiness readiness) {
        return buildRouter(vertx, jwtAuth,
                new CreateTicketUseCase(repository, skillProvider, partyStore),
                new GetTicketUseCase(repository),
                new CancelTicketUseCase(repository, partyStore), wsHub, matchRepository,
                new PartyUseCases(partyStore, repository, skillProvider), registry, rateLimiter, readiness);
    }

    /**
     * In-memory wiring for tests and local runs. Every use case shares the same
     * party store, so a party enqueued through one route is visible to the others
     * (a party enqueue must reach the same party the create route stored).
     */
    private static Router devRouter(Vertx vertx, JWTAuth jwtAuth, TicketRepository repository, WsHub wsHub,
            MatchRepository matchRepository) {
        PartyStore partyStore = new InMemoryPartyStore();
        SkillProvider skillProvider = new DeterministicSkillProvider();
        return buildRouter(vertx, jwtAuth,
                new CreateTicketUseCase(repository, skillProvider, partyStore),
                new GetTicketUseCase(repository),
                new CancelTicketUseCase(repository, partyStore), wsHub, matchRepository,
                new PartyUseCases(partyStore, repository, skillProvider), new SimpleMeterRegistry(), null,
                Readiness.none());
    }

    private static Router buildRouter(Vertx vertx, JWTAuth jwtAuth, CreateTicketUseCase createTicket,
            GetTicketUseCase getTicket, CancelTicketUseCase cancelTicket, WsHub wsHub,
            MatchRepository matchRepository, PartyUseCases partyUseCases, MeterRegistry registry,
            RateLimiter rateLimiter, Readiness readiness) {

        Router router = Router.router(vertx);
        router.route().handler(ctx -> {
            long start = System.nanoTime();
            ctx.response().endHandler(v -> LOG.log(System.Logger.Level.INFO,
                    "http {0} {1} -> {2} in {3}ms sub={4}",
                    ctx.request().method().name(), ctx.normalizedPath(),
                    String.valueOf(ctx.response().getStatusCode()),
                    String.valueOf((System.nanoTime() - start) / 1_000_000),
                    ctx.get("playerId") != null ? ctx.get("playerId") : "-"));
            ctx.next();
        });
        router.route().handler(BodyHandler.create());

        router.get("/health").handler(ctx -> ctx.response()
                .putHeader("content-type", "application/json")
                .end("{\"status\":\"UP\"}"));

        // Readiness, not liveness (W18, ADR 0024): whether the dependencies this instance needs
        // are answering. Measured in W18: with Redis stopped, /health said UP while every enqueue
        // failed - the process was fine and the service was not.
        router.get("/health/ready").handler(ctx -> readiness.probeAll().onSuccess(probed -> {
            boolean healthy = readiness.healthy(probed);
            JsonObject checks = new JsonObject();
            probed.forEach((name, up) -> checks.put(name, up ? "UP" : "DOWN"));
            respond(ctx, healthy ? 200 : 503,
                    new JsonObject().put("status", healthy ? "UP" : "DOWN").put("checks", checks));
        }));

        router.get("/v1/ws").handler(ctx -> {
            String token = bearerToken(ctx);
            if (token == null) {
                ctx.response().setStatusCode(401).putHeader("content-type", "application/json")
                        .end("{\"error\":\"missing token\"}");
                return;
            }
            jwtAuth.authenticate(new TokenCredentials(token))
                    .onSuccess(user -> {
                        ctx.put("playerId", user.subject());
                        ctx.request().toWebSocket()
                            .onSuccess(ws -> {
                                String playerId = user.subject();
                                wsHub.register(playerId, ws);
                                ws.textMessageHandler(text -> wsHub.handleClientMessage(playerId, ws, text));
                                ws.closeHandler(v -> wsHub.unregister(playerId, ws));
                                ws.exceptionHandler(err -> wsHub.unregister(playerId, ws));
                            })
                            .onFailure(err -> ctx.fail(400));
                    })
                    .onFailure(err -> ctx.response().setStatusCode(401).putHeader("content-type", "application/json")
                            .end(new io.vertx.core.json.JsonObject().put("error", "invalid token").encode()));
        });

        router.post("/v1/matchmaking/tickets").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> withRateLimit(ctx, user.subject(), rateLimiter, registry, () -> {
                    try {
                        CreateTicketCommand command = TicketRequestParser.parseCreate(
                                ctx.body() != null ? ctx.body().asJsonObject() : null);
                        createTicket.execute(user.subject(), command, ctx.request().getHeader("Idempotency-Key"))
                                .onSuccess(result -> {
                                    if (!result.created()) {
                                        respond(ctx, 200, TicketCodec.toJson(result.ticket()));
                                        return;
                                    }
                                    countEnqueuedTicket(registry, result.ticket());
                                    respond(ctx, 201, TicketCodec.toCreateResponse(result.ticket()));
                                })
                                .onFailure(err -> {
                                    if (err instanceof QueueFullException full) {
                                        shedQueueFull(ctx, registry, full);
                                        return;
                                    }
                                    if (isDependencyUnavailable(err)) {
                                        // The hot state is down: unavailable, not broken. A 500
                                        // here would tell the client to give up on a request that
                                        // works again the moment Redis is back (W18, ADR 0024).
                                        shedUnavailable(ctx, registry, MetricNames.TAG_DEPENDENCY_REDIS,
                                                "hot state unavailable", err);
                                        return;
                                    }
                                    failInternal(ctx, err);
                                });
                    } catch (IllegalArgumentException e) {
                        fail(ctx, 400, e.getMessage());
                    }
                })));

        router.get("/v1/matchmaking/tickets/:ticketId").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> getTicket.execute(ctx.pathParam("ticketId"), user.subject())
                        .onSuccess(ticket -> respond(ctx, 200, TicketCodec.toJson(ticket)))
                        .onFailure(err -> failWithDomainError(ctx, registry, err))));

        router.delete("/v1/matchmaking/tickets/:ticketId").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> cancelTicket.execute(ctx.pathParam("ticketId"), user.subject())
                        .onSuccess(ticket -> respond(ctx, 200, TicketCodec.toJson(ticket)))
                        .onFailure(err -> failWithDomainError(ctx, registry, err))));

        router.get("/v1/players/:playerId/matches").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> {
                    if (!ctx.pathParam("playerId").equals(user.subject())) {
                        fail(ctx, 403, "history belongs to another player");
                        return;
                    }
                    if (matchRepository == null) {
                        fail(ctx, 503, "history store unavailable");
                        return;
                    }
                    int limit;
                    try {
                        String limitParam = ctx.request().getParam("limit");
                        limit = limitParam == null || limitParam.isBlank() ? 20 : Integer.parseInt(limitParam);
                        if (limit < 1 || limit > 100) {
                            fail(ctx, 400, "limit must be between 1 and 100");
                            return;
                        }
                    } catch (NumberFormatException e) {
                        fail(ctx, 400, "limit must be a number");
                        return;
                    }
                    String cursorParam = ctx.request().getParam("cursor");
                    try {
                        // cursor is validated by the repository's codec
                        matchRepository.historyForPlayer(user.subject(), cursorParam, limit)
                                .onSuccess(page -> respond(ctx, 200, page.toJson()))
                                .onFailure(err -> {
                                    if (isDependencyUnavailable(err)) {
                                        shedUnavailable(ctx, registry, MetricNames.TAG_DEPENDENCY_POSTGRES,
                                                "history store unavailable", err);
                                        return;
                                    }
                                    failInternal(ctx, err);
                                });
                    } catch (IllegalArgumentException e) {
                        fail(ctx, 400, e.getMessage());
                    }
                }));

        // ---- Party routes (W11) ----
        router.get("/v1/parties/:partyId").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> partyUseCases.getPartyUseCase().execute(ctx.pathParam("partyId"), user.subject())
                        .onSuccess(view -> respond(ctx, 200, partyViewToJson(view)))
                        .onFailure(err -> failWithDomainError(ctx, registry, err))));

        router.post("/v1/parties").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> partyUseCases.createPartyUseCase().execute(user.subject())
                        .onSuccess(partyId -> respond(ctx, 201, partyCreatedToJson(partyId)))
                        .onFailure(err -> failWithDomainError(ctx, registry, err))));

        router.post("/v1/parties/:partyId/members").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> partyUseCases.addMemberUseCase().execute(ctx.pathParam("partyId"), user.subject())
                        .onSuccess(party -> respond(ctx, 200, partyToJson(party)))
                        .onFailure(err -> failWithDomainError(ctx, registry, err))));

        router.delete("/v1/parties/:partyId/members/:playerId").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> {
                    String targetPlayerId = ctx.pathParam("playerId");
                    // The leader rule is enforced by removeMemberUseCase itself.
                    partyUseCases.removeMemberUseCase()
                            .execute(ctx.pathParam("partyId"), user.subject(), targetPlayerId)
                            .onSuccess(party -> respond(ctx, 200, partyToJson(party)))
                            .onFailure(err -> failWithDomainError(ctx, registry, err));
                }));

        router.delete("/v1/parties/:partyId").handler(ctx ->
                authenticate(ctx, jwtAuth, user -> {
                    // The leader rule is enforced by dissolveUseCase itself.
                    partyUseCases.dissolveUseCase()
                            .execute(ctx.pathParam("partyId"), user.subject())
                            .onSuccess(party -> respond(ctx, 200, partyToJson(party)))
                            .onFailure(err -> failWithDomainError(ctx, registry, err));
                }));

        return router;
    }

    /**
     * Counts an arrival: a ticket that entered a queue. Tagged by mode and region so the KPI
     * answers "who is waiting where", and only on a real creation - an idempotent replay
     * returns 200 and must not inflate the arrival count.
     */
    private static void countEnqueuedTicket(MeterRegistry registry,
            com.victor.matchmaking.domain.ticket.MatchmakingTicket ticket) {
        registry.counter(MetricNames.TICKETS_ENQUEUED,
                MetricNames.TAG_MODE, ticket.gameMode().label(),
                MetricNames.TAG_REGION, ticket.region().name()).increment();
    }

    /**
     * Runs {@code admitted} only if the caller is under its ingress budget (ADR 0023).
     *
     * <p>The decision is taken before the body is parsed and before any domain write, so a
     * throttled caller costs one Redis round-trip and nothing else. The answer carries the
     * header a well-behaved client backs off on ({@code Retry-After}) and the ones that let it
     * pace itself ({@code X-RateLimit-*}).
     */
    private static void withRateLimit(RoutingContext ctx, String playerId, RateLimiter rateLimiter,
            MeterRegistry registry, Runnable admitted) {
        if (rateLimiter == null) {
            admitted.run();
            return;
        }
        rateLimiter.acquire(RATE_LIMIT_SCOPE, playerId)
                .onSuccess(decision -> {
                    ctx.response()
                            .putHeader(HEADER_RATE_LIMIT_LIMIT, String.valueOf(decision.limit()))
                            .putHeader(HEADER_RATE_LIMIT_REMAINING,
                                    String.valueOf(Math.max(0, decision.remaining())));
                    if (decision.allowed()) {
                        admitted.run();
                        return;
                    }
                    registry.counter(MetricNames.TICKETS_REJECTED,
                            MetricNames.TAG_REASON, MetricNames.REASON_RATE_LIMITED).increment();
                    ctx.response().putHeader(HEADER_RETRY_AFTER,
                            String.valueOf(Math.max(1, decision.retryAfterSeconds())));
                    fail(ctx, 429, "rate limit exceeded: at most " + decision.limit()
                            + " enqueue requests are admitted per burst");
                })
                .onFailure(err -> {
                    // Fail open, loudly. The limiter guards the queue, and the queue has its own
                    // gate (the admission cap); refusing every request because the limiter's own
                    // store is unreachable would turn a guard into the outage it exists to prevent.
                    LOG.log(System.Logger.Level.WARNING, "http.ratelimit.unavailable player={0} reason={1}",
                            playerId, err.getMessage());
                    admitted.run();
                });
    }

    /**
     * The queue is full: accept nothing more, say so in a way the client can act on (W17, ADR
     * 0022). 503 and not 429 - this is the server asking for a moment, not the caller
     * misbehaving - and not a 500 either: nothing failed, the system is protecting itself.
     */
    private static void shedQueueFull(RoutingContext ctx, MeterRegistry registry, QueueFullException full) {
        registry.counter(MetricNames.TICKETS_REJECTED,
                MetricNames.TAG_REASON, MetricNames.REASON_QUEUE_FULL).increment();
        ctx.response().putHeader(HEADER_RETRY_AFTER, String.valueOf(QUEUE_FULL_RETRY_AFTER_SECONDS));
        respond(ctx, 503, new io.vertx.core.json.JsonObject()
                .put("error", "queue full")
                .put("queue", full.queue())
                .put("depth", full.depth())
                .put("capacity", full.capacity())
                .put("retryAfterSeconds", QUEUE_FULL_RETRY_AFTER_SECONDS));
    }

    private static JsonObject partyViewToJson(GetPartyUseCase.PartyView view) {
        return new io.vertx.core.json.JsonObject()
                .put("partyId", view.party().id())
                .put("leaderId", view.party().leaderId())
                .put("members", new io.vertx.core.json.JsonArray(view.party().members()))
                .put("active", view.party().active())
                .put("aggregatedSkill", view.aggregatedSkill())
                .put("dispersion", view.dispersion());
    }

    private static JsonObject partyCreatedToJson(String partyId) {
        return new io.vertx.core.json.JsonObject().put("partyId", partyId);
    }

    private static JsonObject partyToJson(com.victor.matchmaking.domain.player.Party party) {
        return new io.vertx.core.json.JsonObject()
                .put("partyId", party.id())
                .put("leaderId", party.leaderId())
                .put("members", new io.vertx.core.json.JsonArray(party.members()))
                .put("active", party.active());
    }

    private static String bearerToken(RoutingContext ctx) {
        String header = ctx.request().getHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            return header.substring("Bearer ".length());
        }
        String queryToken = ctx.request().getParam("token");
        return queryToken != null && !queryToken.isBlank() ? queryToken : null;
    }

    private static void authenticate(RoutingContext ctx, JWTAuth jwtAuth,
            java.util.function.Consumer<io.vertx.ext.auth.User> onAuthorized) {
        String header = ctx.request().getHeader("Authorization");
        if (header == null || !header.startsWith("Bearer ")) {
            fail(ctx, 401, "missing Bearer token");
            return;
        }
        String token = header.substring("Bearer ".length());
        jwtAuth.authenticate(new TokenCredentials(token))
                .onSuccess(user -> {
                    ctx.put("playerId", user.subject());
                    onAuthorized.accept(user);
                })
                .onFailure(err -> fail(ctx, 401, "invalid token: " + err.getMessage()));
    }

    private static void failWithDomainError(RoutingContext ctx, MeterRegistry registry, Throwable err) {
        if (err instanceof GetTicketUseCase.TicketNotFoundException) {
            fail(ctx, 404, err.getMessage());
        } else if (err instanceof GetTicketUseCase.TicketForbiddenException) {
            fail(ctx, 403, err.getMessage());
        } else if (err instanceof CancelTicketUseCase.TicketConflictException) {
            fail(ctx, 409, err.getMessage());
        } else if (err instanceof AddMemberUseCase.AlreadyMemberException
                || err instanceof AddMemberUseCase.PartyFullException
                || err instanceof AddMemberUseCase.PartyNotActiveException) {
            fail(ctx, 409, err.getMessage());
        } else if (err instanceof GetPartyUseCase.PartyNotFoundException
                || err instanceof RemoveMemberUseCase.PartyNotFoundException
                || err instanceof CreateTicketUseCase.PartyNotFoundException
                || err instanceof DissolvePartyUseCase.PartyNotFoundException) {
            fail(ctx, 404, err.getMessage());
        } else if (err instanceof GetPartyUseCase.ForbiddenException
                || err instanceof RemoveMemberUseCase.ForbiddenException
                || err instanceof DissolvePartyUseCase.ForbiddenException) {
            fail(ctx, 403, err.getMessage());
        } else if (isDependencyUnavailable(err)) {
            // Every domain route reads or writes the hot state, so this is where a Redis outage
            // lands: the same 503 the enqueue path answers, not a 500 (W18, ADR 0024).
            shedUnavailable(ctx, registry, MetricNames.TAG_DEPENDENCY_REDIS, "hot state unavailable", err);
        } else {
            failInternal(ctx, err);
        }
    }

    /**
     * True when the failure is a dependency being unreachable rather than this service being wrong
     * (W18, ADR 0024). A dependency that is down is answered with 503 and a {@code Retry-After}; a
     * bug is answered with 500. Collapsing the two is how a client learns to retry a typo forever,
     * or to give up on a request that would have worked a second later.
     *
     * <p>Detected by type first (the Vert.x Redis client reports an unreachable server as a
     * {@link java.net.ConnectException}) and by the clients' own wording second, because a pool that
     * hands out a connection the server has since closed, a pool whose waiting queue is full and a
     * script cache that lost its script all speak through messages rather than types. The wording
     * list is deliberately short and about *transport*, never about SQL or business state: a query
     * that fails because a column is missing must stay a 500.
     */
    private static boolean isDependencyUnavailable(Throwable err) {
        for (Throwable cause = err; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
            if (cause instanceof java.net.ConnectException) {
                return true;
            }
            String message = cause.getMessage();
            if (message == null) {
                continue;
            }
            String normalized = message.toLowerCase(java.util.Locale.ENGLISH);
            for (String fragment : UNAVAILABLE_MESSAGE_FRAGMENTS) {
                if (normalized.contains(fragment)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * A dependency is unreachable: 503, a {@code Retry-After}, and a body that names what is
     * unavailable without saying where it lives (W18, ADR 0024). Measured before this existed: the
     * client was told {@code 500 {"error":"Connection refused: localhost/127.0.0.1:6379"}} - the
     * wrong status and a map of the internal network.
     */
    private static void shedUnavailable(RoutingContext ctx, MeterRegistry registry, String dependency,
            String error, Throwable cause) {
        registry.counter(MetricNames.DEPENDENCY_ERRORS, MetricNames.TAG_DEPENDENCY, dependency).increment();
        LOG.log(System.Logger.Level.WARNING, "http.dependency.unavailable dependency={0} path={1} reason={2}",
                dependency, ctx.normalizedPath(), String.valueOf(cause.getMessage()));
        ctx.response().putHeader(HEADER_RETRY_AFTER, String.valueOf(DEPENDENCY_RETRY_AFTER_SECONDS));
        respond(ctx, 503, new io.vertx.core.json.JsonObject()
                .put("error", error)
                .put("dependency", dependency)
                .put("retryAfterSeconds", DEPENDENCY_RETRY_AFTER_SECONDS));
    }

    /**
     * A 500 the client can read: the details go to the log, not to the body (W18, ADR 0024). A
     * driver exception's message names hosts, ports and table shapes - useful to an operator,
     * free reconnaissance to everyone else.
     */
    private static void failInternal(RoutingContext ctx, Throwable err) {
        LOG.log(System.Logger.Level.ERROR, "http.internal {0} {1} reason={2}",
                ctx.request().method().name(), ctx.normalizedPath(), String.valueOf(err.getMessage()));
        fail(ctx, 500, "internal error");
    }

    private static void respond(RoutingContext ctx, int statusCode, io.vertx.core.json.JsonObject body) {
        ctx.response().setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(body.encode());
    }

    private static void fail(RoutingContext ctx, int statusCode, String message) {
        if (statusCode >= 400) {
            LOG.log(System.Logger.Level.WARNING, "http.error {0} {1} -> {2} reason={3}",
                    ctx.request().method().name(), ctx.normalizedPath(),
                    statusCode, message);
        }
        ctx.response().setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(new io.vertx.core.json.JsonObject().put("error", message).encode());
    }
}
