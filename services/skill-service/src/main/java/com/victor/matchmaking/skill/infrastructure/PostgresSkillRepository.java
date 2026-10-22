package com.victor.matchmaking.skill.infrastructure;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.domain.skill.PlayerSkill;
import com.victor.matchmaking.skill.application.SkillUpdater;
import com.victor.matchmaking.skill.domain.SkillApplication;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

/**
 * PostgreSQL persistence for player skills (W24).
 *
 * <p>The upsert uses {@code ON CONFLICT} with a row lock ({@code SELECT ... FOR UPDATE})
 * to prevent lost updates when two matches of the same player finish concurrently.
 * The lock order is by player_id to avoid deadlocks.
 */
public final class PostgresSkillRepository implements SkillUpdater, AutoCloseable {

    private static final String SELECT_FOR_UPDATE =
            "SELECT player_id, rating, matches_played, provisional, updated_at FROM player_skills "
                    + "WHERE player_id IN ("
                    + String.join(",", java.util.Collections.nCopies(10, "?")) // placeholder, built dynamically
                    + ") ORDER BY player_id FOR UPDATE";

    private static final String UPSERT =
            "INSERT INTO player_skills (player_id, rating, matches_played, provisional, updated_at) "
                    + "VALUES (?, ?, ?, ?, ?) "
                    + "ON CONFLICT (player_id) DO UPDATE SET "
                    + "rating = EXCLUDED.rating, "
                    + "matches_played = EXCLUDED.matches_played, "
                    + "provisional = EXCLUDED.provisional, "
                    + "updated_at = EXCLUDED.updated_at";

    private static final String INSERT_EVENT =
            "INSERT INTO outbox_events (event_id, topic, partition_key, event_type, payload, occurred_at, "
                    + "status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)";

    private static final String RECORD_PROCESSED =
            "INSERT INTO processed_events (event_id, consumer_group, topic, partition_id, record_offset, processed_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING RETURNING event_id";

    private final Pool pool;
    private final String consumerGroup;
    private final com.victor.matchmaking.outbox.OutboxQueue outbox;

    public PostgresSkillRepository(Pool pool, String consumerGroup, com.victor.matchmaking.outbox.OutboxQueue outbox) {
        this.pool = pool;
        this.consumerGroup = consumerGroup;
        this.outbox = outbox;
    }

    public static PostgresSkillRepository connect(Vertx vertx, DatabaseConfig config, String consumerGroup,
            com.victor.matchmaking.outbox.OutboxQueue outbox) {
        Pool pool = DatabasePool.connect(vertx, config);
        return new PostgresSkillRepository(pool, consumerGroup, outbox);
    }

    @Override
    public Future<PlayerSkill> getOrCreate(String playerId) {
        return pool.preparedQuery(
                        "SELECT player_id, rating, matches_played, provisional, updated_at FROM player_skills WHERE player_id = ?")
                .execute(Tuple.of(playerId))
                .map(rows -> {
                    var iterator = rows.iterator();
                    if (!iterator.hasNext()) {
                        return new PlayerSkill(playerId, PlayerSkill.defaultRating(), 0, true, Instant.now());
                    }
                    return toSkill(iterator.next());
                });
    }

    @Override
    public Future<List<SkillApplication>> apply(com.victor.matchmaking.events.MatchFinished event, UUID eventId, com.victor.matchmaking.events.RawRecord record) {
        return pool.withTransaction(connection -> {
            // 1) Read current skills with row lock (ordered by player_id to avoid deadlocks).
            java.util.List<String> playerIds = event.getPlayersList().stream()
                    .map(com.victor.matchmaking.events.MatchPlayerResult::getPlayerId)
                    .distinct()
                    .sorted()
                    .toList();
            return lockSkills(connection, playerIds)
                    .compose(skillsMap -> {
                        // 2) Convert to domain types for the calculation.
                        var domainPlayers = event.getPlayersList().stream()
                                .map(p -> new com.victor.matchmaking.domain.skill.MatchResultPlayer(
                                        p.getPlayerId(), p.getTeam(), p.getSkillAtMatch()))
                                .toList();
                        var domainWinner = com.victor.matchmaking.domain.skill.MatchWinner.fromWireName(event.getWinner().name());
                        var newRatings = com.victor.matchmaking.domain.skill.SkillRating.updateAll(
                                domainPlayers, domainWinner, skillsMap);
                        // 3) Build applications and upsert.
                        java.util.List<SkillApplication> applications = new ArrayList<>();
                        int idx = 0;
                        for (var p : event.getPlayersList()) {
                            PlayerSkill current = skillsMap.get(p.getPlayerId());
                            double before = current != null ? current.rating() : PlayerSkill.defaultRating();
                            int matchesPlayed = (current != null ? current.matchesPlayed() : 0) + 1;
                            boolean provisional = matchesPlayed < PlayerSkill.calibrationMatches();
                            double after = newRatings.get(idx++);
                            applications.add(new SkillApplication(p.getPlayerId(), before, after,
                                    matchesPlayed, provisional));
                        }
                        return upsertSkills(connection, applications)
                                .compose(ignored -> recordProcessed(connection, eventId, record)
                                        .compose(ignored2 -> writeSkillUpdatedEvent(connection, event, applications)
                                                .map(ignored3 -> applications)));
                    });
        });
    }

    private Future<Map<String, PlayerSkill>> lockSkills(SqlConnection connection, List<String> playerIds) {
        if (playerIds.isEmpty()) {
            return Future.succeededFuture(new LinkedHashMap<>());
        }
        String placeholders = String.join(", ", java.util.Collections.nCopies(playerIds.size(), "?"));
        String sql = "SELECT player_id, rating, matches_played, provisional, updated_at FROM player_skills "
                + "WHERE player_id IN (" + placeholders + ") ORDER BY player_id FOR UPDATE";
        Tuple params = Tuple.tuple();
        for (String id : playerIds) {
            params.addString(id);
        }
        return connection.preparedQuery(sql).execute(params)
                .map(rows -> {
                    Map<String, PlayerSkill> map = new LinkedHashMap<>();
                    for (Row row : rows) {
                        PlayerSkill skill = toSkill(row);
                        map.put(skill.playerId(), skill);
                    }
                    return map;
                });
    }

    private Future<Void> upsertSkills(SqlConnection connection, List<SkillApplication> applications) {
        io.vertx.core.Future<Void>[] futures = applications.stream()
                .map(app -> connection.preparedQuery(UPSERT)
                        .execute(Tuple.of(app.playerId(), app.skillAfter(), app.matchesPlayed(),
                                app.provisional(), Timestamp.from(Instant.now())))
                        .map(v -> (Void) null))
                .toArray(io.vertx.core.Future[]::new);
        return Future.all(futures).mapEmpty();
    }

    private Future<Void> recordProcessed(SqlConnection connection,
            UUID eventId, com.victor.matchmaking.events.RawRecord record) {
        return connection.preparedQuery(
                        "INSERT INTO processed_events (event_id, consumer_group, topic, partition_id, record_offset, processed_at) "
                                + "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING")
                .execute(Tuple.of(eventId.toString(), consumerGroup,
                        record.topic(), record.partition(), record.offset(), Timestamp.from(Instant.now())))
                .mapEmpty();
    }

    private Future<Void> writeSkillUpdatedEvent(SqlConnection connection,
            com.victor.matchmaking.events.MatchFinished event, List<SkillApplication> applications) {
        // Build the SKILL_UPDATED protobuf.
        com.victor.matchmaking.events.SkillUpdated.Builder builder = com.victor.matchmaking.events.SkillUpdated.newBuilder()
                .setMeta(com.victor.matchmaking.events.MatchConfirmedMapper.metadata(
                        Instant.now(), event.getMatchId()))
                .setMatchId(event.getMatchId());
        for (var app : applications) {
            builder.addUpdates(com.victor.matchmaking.events.PlayerSkillUpdate.newBuilder()
                    .setPlayerId(app.playerId())
                    .setSkillBefore(app.skillBefore())
                    .setSkillAfter(app.skillAfter())
                    .setMatchesPlayed(app.matchesPlayed())
                    .setProvisional(app.provisional()));
        }
        com.victor.matchmaking.events.SkillUpdated skillUpdated = builder.build();

        com.victor.matchmaking.outbox.OutboxRecord record = new com.victor.matchmaking.outbox.OutboxRecord(
                skillUpdated.getMeta().getEventId(),
                com.victor.matchmaking.events.EventTopics.SKILL_UPDATED,
                event.getMatchId(),
                "SKILL_UPDATED",
                skillUpdated.toByteArray(),
                Instant.now(),
                Instant.now(),
                0);

        return outbox.writeWithEvent(conn -> Future.succeededFuture(null), record);
    }

    private static PlayerSkill toSkill(Row row) {
        return new PlayerSkill(
                row.getString("player_id"),
                row.getDouble("rating"),
                row.getInteger("matches_played"),
                row.getBoolean("provisional"),
                row.getLocalDateTime("updated_at").atZone(java.time.ZoneOffset.UTC).toInstant());
    }

    @Override
    public void close() {
        pool.close();
    }
}