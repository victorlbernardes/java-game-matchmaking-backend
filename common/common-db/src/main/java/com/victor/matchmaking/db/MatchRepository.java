package com.victor.matchmaking.db;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

/** Durable match facts: inserts on confirmation, serves cursor-paginated history. */
public final class MatchRepository {

    private final Pool pool;

    public MatchRepository(Pool pool) {
        this.pool = pool;
    }

    public static MatchRepository connect(Vertx vertx, DatabaseConfig config) {
        Pool pool = JDBCPool.pool(vertx, new JDBCConnectOptions()
                .setJdbcUrl(config.jdbcUrl())
                .setUser(config.user())
                .setPassword(config.password()), new io.vertx.sqlclient.PoolOptions());
        return new MatchRepository(pool);
    }

    /** Inserts a match and its players. Idempotent on match id (retry-safe for W7/W8). */
    public Future<Void> save(Match match, List<MatchPlayer> players) {
        Future<RowSet<Row>> insertMatch = pool.preparedQuery(
                        "INSERT INTO matches (id, game_mode, region, status, created_at, started_at, finished_at) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING")
                .execute(Tuple.of(
                        match.id(),
                        match.gameMode().label(),
                        match.region().name(),
                        match.status().name(),
                        timestampOf(match.createdAt()),
                        timestampOf(match.startedAt()),
                        timestampOf(match.finishedAt())));
        return insertMatch.flatMap(v -> Future.all(
                        players.stream()
                                .map(p -> pool.preparedQuery(
                                                "INSERT INTO match_players (match_id, player_id, team, skill_at_match) "
                                                        + "VALUES (?, ?, ?, ?) ON CONFLICT (match_id, player_id) DO NOTHING")
                                        .execute(Tuple.of(p.matchId(), p.playerId(), p.team(), p.skillAtMatch())))
                                .toList())
                .map(composite -> composite.list()))
                .mapEmpty();
    }

    /** Newest-first page of a player's matches; nextCursor is null on the last page. */
    public Future<MatchHistoryPage> historyForPlayer(String playerId, String cursor, int limit) {
        MatchHistoryCursor.Decoded decoded = MatchHistoryCursor.decode(cursor);
        String sql;
        Tuple params;
        if (decoded == null) {
            sql = "SELECT m.id, m.game_mode, m.region, m.status, m.created_at, m.started_at, m.finished_at, mp.team "
                    + "FROM match_players mp JOIN matches m ON m.id = mp.match_id "
                    + "WHERE mp.player_id = ? ORDER BY m.created_at DESC, m.id DESC LIMIT ?";
            params = Tuple.of(playerId, limit + 1);
        } else {
            sql = "SELECT m.id, m.game_mode, m.region, m.status, m.created_at, m.started_at, m.finished_at, mp.team "
                    + "FROM match_players mp JOIN matches m ON m.id = mp.match_id "
                    + "WHERE mp.player_id = ? AND (m.created_at, m.id) < (?, ?) "
                    + "ORDER BY m.created_at DESC, m.id DESC LIMIT ?";
            params = Tuple.of(playerId, Timestamp.from(Instant.ofEpochMilli(decoded.createdAtEpochMs())),
                    decoded.matchId(), limit + 1);
        }
        return pool.preparedQuery(sql).execute(params).map(rs -> toPage(rs, limit));
    }

    private static MatchHistoryPage toPage(RowSet<Row> rs, int limit) {
        List<Row> rows = new ArrayList<>();
        rs.forEach(rows::add);
        List<MatchHistoryItem> items = new ArrayList<>();
        List<Instant> createdAts = new ArrayList<>();
        for (Row row : rows) {
            createdAts.add(instantOf(row.getValue("created_at")));
            items.add(new MatchHistoryItem(
                    row.getString("id"),
                    row.getString("game_mode"),
                    row.getString("region"),
                    row.getString("status"),
                    instantOf(row.getValue("started_at")),
                    instantOf(row.getValue("finished_at")),
                    row.getString("team")));
        }
        List<MatchHistoryItem> page = items.size() > limit ? items.subList(0, limit) : items;
        String nextCursor = null;
        if (items.size() > limit && !page.isEmpty()) {
            nextCursor = MatchHistoryCursor.encode(createdAts.get(page.size() - 1), page.get(page.size() - 1).matchId());
        }
        return new MatchHistoryPage(List.copyOf(page), nextCursor);
    }

    private static Timestamp timestampOf(Instant instant) {
        return instant != null ? Timestamp.from(instant) : null;
    }

    private static Instant instantOf(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant();
        }
        if (value instanceof Date date) {
            return date.toInstant();
        }
        if (value instanceof java.time.OffsetDateTime odt) {
            return odt.toInstant();
        }
        throw new IllegalStateException("unsupported timestamp type: " + value.getClass());
    }
}
