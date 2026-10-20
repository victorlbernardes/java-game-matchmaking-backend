package com.victor.matchmaking.db;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.victor.matchmaking.domain.match.AllocationStatus;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

/**
 * The game server pool: which servers exist, how many matches each can still take, and the
 * allocation that takes a slot (W22, ADR 0031).
 *
 * <p>Two callers with very different jobs share this one table, on purpose:
 *
 * <ul>
 *   <li>the <b>orchestrator</b> writes it: a server registers and heartbeats, and a confirmed match
 *       takes a slot inside the transaction that also writes the projection (so a replay does not
 *       allocate twice and two confirmations cannot take the same last slot);</li>
 *   <li>the <b>engine</b> reads it, once a second, to decide whether it may form a match at all.
 *       It only reads: a matchmaking worker that could also allocate would be a second writer of a
 *       number whose whole value is having one.</li>
 * </ul>
 *
 * <p>Eligibility is a predicate, not a state: {@code last_seen > now() - SERVER_TTL} and
 * {@code NOT draining}, evaluated by the query that needs it. There is no sweep that has to run for
 * the pool to be correct — a server that stops heartbeating stops being picked within one TTL, and
 * {@link #list()} derives {@code STALE} for the operator instead of storing it.
 */
public final class GameServerRepository {

    /** How long a server stays eligible after its last heartbeat. */
    public static final String ENV_SERVER_TTL_SECONDS = "SERVER_TTL_SECONDS";
    public static final int DEFAULT_SERVER_TTL_SECONDS = 30;

    /** Where a server is in its lifecycle, derived at read time (never stored). */
    public enum ServerState {
        READY,
        STALE,
        DRAINING
    }

    /** One server as the pool holds it. {@link #free()} is the number the allocator decides on. */
    public record GameServer(String serverId, Region region, String endpoint, List<GameMode> gameModes,
            int capacity, int allocated, boolean draining, Instant lastSeen) {

        public int free() {
            return capacity - allocated;
        }

        public ServerState stateAt(Instant now, int ttlSeconds) {
            if (draining) {
                return ServerState.DRAINING;
            }
            return lastSeen.isAfter(now.minusSeconds(ttlSeconds)) ? ServerState.READY : ServerState.STALE;
        }
    }

    /** What a server reports when it registers or renews: its whole offer, not a delta. */
    public record Registration(String serverId, Region region, String endpoint, List<GameMode> gameModes,
            int capacity) {
    }

    /**
     * One region's capacity. {@code authoritative} false means "this region never registered a
     * server", which is <em>unknown</em>, not zero: a deployment with no pool behaves as it did
     * before W22 instead of refusing to match anybody (W22 plan, ADR 0031).
     */
    public record RegionCapacity(boolean authoritative, Map<GameMode, ModeCapacity> byMode) {

        public static final RegionCapacity UNKNOWN = new RegionCapacity(false, Map.of());

        /** What one mode can still host in this region: slots and how many servers offer them. */
        public record ModeCapacity(int free, int servers) {
        }

        public int free(GameMode mode) {
            ModeCapacity capacity = byMode.get(mode);
            return capacity == null ? 0 : capacity.free();
        }

        public int servers(GameMode mode) {
            ModeCapacity capacity = byMode.get(mode);
            return capacity == null ? 0 : capacity.servers();
        }
    }

    /** The server cannot shrink below the matches it is already running. */
    public static final class CapacityBelowAllocated extends RuntimeException {
        public CapacityBelowAllocated(String serverId, int capacity, int allocated) {
            super("capacity " + capacity + " is below the " + allocated + " matches already allocated on "
                    + serverId);
        }
    }

    private static final String UPSERT =
            "INSERT INTO game_servers (server_id, region, endpoint, game_modes, capacity, allocated, draining, "
                    + "last_seen) VALUES (?, ?, ?, string_to_array(?, ','), ?, 0, FALSE, now()) "
                    + "ON CONFLICT (server_id) DO UPDATE SET region = EXCLUDED.region, "
                    + "endpoint = EXCLUDED.endpoint, game_modes = EXCLUDED.game_modes, "
                    + "capacity = EXCLUDED.capacity, draining = FALSE, last_seen = now() "
                    // The WHERE is what refuses a shrink: the row is left untouched when the new
                    // capacity cannot hold what is already running, and the caller turns the
                    // zero row count into a conflict instead of over-committing silently.
                    + "WHERE game_servers.allocated <= EXCLUDED.capacity";

    private static final String SELECT_ONE =
            "SELECT server_id, region, endpoint, game_modes, capacity, allocated, draining, last_seen "
                    + "FROM game_servers WHERE server_id = ?";

    private static final String SELECT_ALL =
            "SELECT server_id, region, endpoint, game_modes, capacity, allocated, draining, last_seen "
                    + "FROM game_servers ORDER BY server_id";

    /** One row per (region, mode) a *ready* server accepts, with the slots it still has. */
    private static final String SNAPSHOT =
            "SELECT region, mode, sum(free_slots)::int AS free, count(*)::int AS servers "
                    + "FROM (SELECT g.region, m AS mode, g.capacity - g.allocated AS free_slots "
                    + "      FROM game_servers g, unnest(g.game_modes) AS m "
                    + "      WHERE NOT g.draining AND g.last_seen > now() - make_interval(secs => ?)) eligible "
                    + "GROUP BY region, mode";

    private static final String REGIONS_WITH_POOL = "SELECT DISTINCT region FROM game_servers";

    /**
     * The fullest server that still has room, locked so a second allocation cannot read the same
     * row. {@code SKIP LOCKED} is on purpose: two confirmations arriving together should take two
     * servers, not queue behind each other for one.
     */
    private static final String SELECT_VICTIM =
            "SELECT server_id FROM game_servers "
                    + "WHERE region = ? AND ? = ANY(game_modes) AND NOT draining "
                    + "AND last_seen > now() - make_interval(secs => ?) AND allocated < capacity "
                    // Bin-packing: fill the server that is already busy, so the others stay empty
                    // and an operator can drain a whole machine instead of every machine a little.
                    + "ORDER BY (capacity - allocated) ASC, allocated DESC, server_id ASC "
                    + "LIMIT 1 FOR UPDATE SKIP LOCKED";

    private static final String TAKE_SLOT =
            "UPDATE game_servers SET allocated = allocated + 1 WHERE server_id = ? AND allocated < capacity";

    private static final String RELEASE_ALLOCATION =
            "UPDATE match_allocations SET status = ? WHERE match_id = ? AND server_id = ? AND status = ?";

    private static final String GIVE_SLOT_BACK =
            "UPDATE game_servers SET allocated = allocated - 1 WHERE server_id = ? AND allocated > 0";

    private final Pool pool;
    private final int ttlSeconds;

    public GameServerRepository(Pool pool) {
        this(pool, ttlSecondsFromEnv());
    }

    public GameServerRepository(Pool pool, int ttlSeconds) {
        this.pool = pool;
        this.ttlSeconds = ttlSeconds;
    }

    public static GameServerRepository connect(Vertx vertx, DatabaseConfig config) {
        return new GameServerRepository(DatabasePool.connect(vertx, config));
    }

    public static int ttlSecondsFromEnv() {
        String raw = System.getenv(ENV_SERVER_TTL_SECONDS);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_SERVER_TTL_SECONDS;
        }
        int seconds = Integer.parseInt(raw.trim());
        if (seconds <= 0) {
            throw new IllegalArgumentException(ENV_SERVER_TTL_SECONDS + " must be positive, got " + seconds);
        }
        return seconds;
    }

    public int ttlSeconds() {
        return ttlSeconds;
    }

    /**
     * Registers a server or renews it. Idempotent by design: the heartbeat <em>is</em> the
     * registration, so an agent has nothing else to call and a missed renewal is not a state to
     * repair.
     *
     * <p>A heartbeat clears {@code draining} — it is the server saying "I am here and I host N" —
     * which is why draining is a decision that lasts until the next offer (ADR 0031, trade-offs).
     */
    public Future<GameServer> register(Registration registration) {
        String modes = String.join(",", registration.gameModes().stream().map(GameMode::label).toList());
        return pool.preparedQuery(UPSERT)
                .execute(Tuple.of(registration.serverId(), registration.region().name(),
                        registration.endpoint(), modes, registration.capacity()))
                .compose(upserted -> {
                    if (upserted.rowCount() == 0) {
                        // The upsert refused: the row exists and runs more matches than the new
                        // capacity could hold. Read it so the refusal carries both numbers.
                        return find(registration.serverId()).compose(existing -> Future.failedFuture(
                                new CapacityBelowAllocated(registration.serverId(), registration.capacity(),
                                        existing.map(GameServer::allocated).orElse(0))));
                    }
                    return find(registration.serverId()).map(found -> found.orElseThrow(
                            () -> new IllegalStateException("registered server disappeared: "
                                    + registration.serverId())));
                });
    }

    public Future<Optional<GameServer>> find(String serverId) {
        return pool.preparedQuery(SELECT_ONE).execute(Tuple.of(serverId)).map(rows -> {
            var iterator = rows.iterator();
            return iterator.hasNext() ? Optional.of(toServer(iterator.next())) : Optional.<GameServer>empty();
        });
    }

    public Future<List<GameServer>> list() {
        return pool.preparedQuery(SELECT_ALL).execute().map(rows -> {
            List<GameServer> servers = new ArrayList<>();
            rows.forEach(row -> servers.add(toServer(row)));
            return servers;
        });
    }

    /**
     * Stops the server from receiving new matches without touching what it is already running. The
     * next heartbeat undoes it (see {@link #register}).
     */
    public Future<Optional<GameServer>> drain(String serverId) {
        return pool.preparedQuery("UPDATE game_servers SET draining = TRUE WHERE server_id = ?")
                .execute(Tuple.of(serverId))
                .compose(drained -> drained.rowCount() == 0
                        ? Future.succeededFuture(Optional.<GameServer>empty())
                        : find(serverId));
    }

    /** Every (mode, region) pair a ready server has room for, plus which regions have a pool at all. */
    public Future<Map<Region, RegionCapacity>> snapshot() {
        return pool.preparedQuery(SNAPSHOT).execute(Tuple.of(ttlSeconds)).compose(rows -> {
            Map<Region, Map<GameMode, RegionCapacity.ModeCapacity>> byMode = new HashMap<>();
            rows.forEach(row -> byMode
                    .computeIfAbsent(Region.valueOf(row.getString("region")), ignored -> new EnumMap<>(GameMode.class))
                    .put(GameMode.fromLabel(row.getString("mode")),
                            new RegionCapacity.ModeCapacity(row.getInteger("free"), row.getInteger("servers"))));
            return pool.preparedQuery(REGIONS_WITH_POOL).execute().map(regions -> {
                Map<Region, RegionCapacity> snapshot = new EnumMap<>(Region.class);
                regions.forEach(row -> {
                    Region region = Region.valueOf(row.getString("region"));
                    // A region with a pool is authoritative even when nothing in it is ready: an
                    // empty (or entirely stale) pool means "this region can host nothing now",
                    // which is exactly what the engine must not paper over.
                    snapshot.put(region, new RegionCapacity(true,
                            byMode.getOrDefault(region, new EnumMap<>(GameMode.class))));
                });
                return snapshot;
            });
        });
    }

    /**
     * Takes a slot for a match, on a connection the caller already owns, so the allocation is part
     * of the same transaction as the event that asked for it (W13's inbox + W22's allocation).
     *
     * <p>Empty means "no server could take it" — the pool is full, draining, stale or absent. The
     * caller keeps the match in {@code PENDING_ALLOCATION}; it never retries here, because the
     * retry loop of a Kafka consumer is for failures, and "no capacity" is an answer.
     */
    public Future<Optional<String>> reserve(SqlConnection connection, GameMode gameMode, Region region) {
        return connection.preparedQuery(SELECT_VICTIM)
                .execute(Tuple.of(region.name(), gameMode.label(), ttlSeconds))
                .compose(rows -> {
                    var iterator = rows.iterator();
                    if (!iterator.hasNext()) {
                        return Future.succeededFuture(Optional.<String>empty());
                    }
                    String serverId = iterator.next().getString("server_id");
                    return connection.preparedQuery(TAKE_SLOT).execute(Tuple.of(serverId))
                            .map(taken -> taken.rowCount() == 1 ? Optional.of(serverId) : Optional.<String>empty());
                });
    }

    /**
     * Gives a slot back when the match ended, and nothing when it did not: the projection row is
     * flipped from {@code ALLOCATED} to {@code RELEASED} first, in the same transaction, so a
     * repeated release finds nothing to flip and cannot return a second slot.
     */
    public Future<Optional<GameServer>> release(String serverId, String matchId) {
        return pool.withTransaction(connection -> connection.preparedQuery(RELEASE_ALLOCATION)
                .execute(Tuple.of(AllocationStatus.RELEASED.name(), matchId, serverId,
                        AllocationStatus.ALLOCATED.name()))
                .compose(released -> released.rowCount() == 0
                        ? Future.succeededFuture(Optional.<GameServer>empty())
                        : connection.preparedQuery(GIVE_SLOT_BACK).execute(Tuple.of(serverId))
                                .compose(ignored -> findOn(connection, serverId))));
    }

    private Future<Optional<GameServer>> findOn(SqlConnection connection, String serverId) {
        return connection.preparedQuery(SELECT_ONE).execute(Tuple.of(serverId)).map(rows -> {
            var iterator = rows.iterator();
            return iterator.hasNext() ? Optional.of(toServer(iterator.next())) : Optional.<GameServer>empty();
        });
    }

    /** The row is the contract: array column and timestamps are decoded here, once. */
    private static GameServer toServer(Row row) {
        List<GameMode> modes = new ArrayList<>();
        for (String label : row.getArrayOfStrings("game_modes")) {
            modes.add(GameMode.fromLabel(label));
        }
        return new GameServer(row.getString("server_id"), Region.valueOf(row.getString("region")),
                row.getString("endpoint"), List.copyOf(modes), row.getInteger("capacity"),
                row.getInteger("allocated"), row.getBoolean("draining"),
                MatchRepository.instantOf(row.getValue("last_seen")));
    }
}
