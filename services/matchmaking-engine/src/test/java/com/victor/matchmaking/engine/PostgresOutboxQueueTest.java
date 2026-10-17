package com.victor.matchmaking.engine.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.db.DatabaseConfig;
import com.victor.matchmaking.db.DatabaseMigrator;
import com.victor.matchmaking.db.DatabasePool;
import com.victor.matchmaking.domain.match.Match;
import com.victor.matchmaking.domain.match.MatchPlayer;
import com.victor.matchmaking.domain.match.MatchStatus;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.engine.application.OutboxRecord;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

/**
 * The properties of the outbox that only the database can provide (ADR 0018): the match
 * and its event committed together, one relay per row at a time, and a row that comes
 * back when the relay that held it dies.
 *
 * <p>A fake would test the SQL we wrote instead of the SQL Postgres runs, and
 * {@code FOR UPDATE SKIP LOCKED} does not exist outside Postgres. Requires the database
 * (docker compose up -d postgres) and cleans up after itself.
 *
 * <p>The claim is a queue: it hands over the oldest pending rows of the whole table, not
 * the rows of one test. Assertions are therefore about *this* test's row (is it in the
 * claim, is it not), never about the length of a claim.
 */
class PostgresOutboxQueueTest {

    private static final String MATCH_ID_PREFIX = "match-w14-";
    private static final Instant OCCURRED_AT = Instant.parse("2026-10-17T12:00:00Z");
    /** Large enough to reach this test's rows whatever else the table holds. */
    private static final int WHOLE_QUEUE = 1000;

    private static Vertx vertx;
    private static PostgresOutboxQueue outbox;
    private static Pool probe;

    @BeforeAll
    static void connect() {
        DatabaseConfig config = DatabaseConfig.fromEnv();
        DatabaseMigrator.migrate(config);
        vertx = Vertx.vertx();
        outbox = PostgresOutboxQueue.connect(vertx, config);
        // Second pool, used only to read and to simulate a relay that died holding a row.
        probe = DatabasePool.connect(vertx, config);
    }

    @BeforeEach
    void forgetPreviousRows() {
        // The claim reads the whole table; a row left behind by the previous test would
        // show up in this one's claim.
        deleteOurRows();
    }

    @AfterAll
    static void cleanup() {
        deleteOurRows();
        outbox.close();
        probe.close();
        vertx.close();
    }

    @Test
    void theMatchAndItsEventAreOneWrite() {
        String matchId = matchId();
        OutboxRecord event = event(matchId);

        await(outbox.writeMatchWithEvent(match(matchId), roster(matchId), event));

        assertEquals(1, countMatches(matchId));
        assertEquals(2, countPlayers(matchId));
        assertEquals(PostgresOutboxQueue.PENDING, statusOf(event.eventId()));
    }

    @Test
    void anEventThatCannotBeWrittenTakesTheMatchWithIt() {
        String firstMatch = matchId();
        await(outbox.writeMatchWithEvent(match(firstMatch), roster(firstMatch), event(firstMatch)));
        String secondMatch = matchId();

        // The event id is the primary key, so this write fails. Without a transaction the
        // match would be committed and no event would ever announce it - the exact hole
        // the outbox closes.
        assertThrows(RuntimeException.class,
                () -> await(outbox.writeMatchWithEvent(match(secondMatch), roster(secondMatch),
                        event(secondMatch, OCCURRED_AT, eventIdOf(firstMatch)))));

        assertEquals(0, countMatches(secondMatch));
        assertEquals(0, countPlayers(secondMatch));
    }

    @Test
    void aClaimedRowIsNotHandedToASecondRelayWhileTheLeaseHolds() {
        String matchId = matchId();
        OutboxRecord event = event(matchId);
        await(outbox.writeMatchWithEvent(match(matchId), roster(matchId), event));

        assertTrue(claimedEventIds("relay-a").contains(event.eventId()));
        // The same row is still PENDING, but it is leased: a second relay skips it instead
        // of publishing it a second time.
        assertFalse(claimedEventIds("relay-b").contains(event.eventId()));
    }

    @Test
    void aLeaseOfADeadRelayExpiresAndTheRowComesBack() {
        String matchId = matchId();
        OutboxRecord event = event(matchId);
        await(outbox.writeMatchWithEvent(match(matchId), roster(matchId), event));
        await(outbox.claim("relay-that-died", WHOLE_QUEUE));

        // What a crashed relay leaves behind: the row is claimed and nothing will ever
        // mark it. The lease is the only thing that brings it back.
        await(probe.preparedQuery("UPDATE outbox_events SET claimed_at = now() - INTERVAL '60 seconds' "
                + "WHERE event_id = ?").execute(Tuple.of(UUID.fromString(event.eventId()))).mapEmpty());

        assertTrue(claimedEventIds("relay-awake").contains(event.eventId()));
    }

    @Test
    void aRowScheduledForLaterIsNotClaimedYet() {
        String matchId = matchId();
        OutboxRecord event = event(matchId);
        await(outbox.writeMatchWithEvent(match(matchId), roster(matchId), event));

        await(outbox.markFailed(event.eventId(), "broker is down", Instant.now().plusSeconds(30)));
        assertFalse(claimedEventIds("relay-a").contains(event.eventId()));

        // The backoff is in the row: once it has passed, the same row is claimable again
        // and the attempt is counted.
        await(probe.preparedQuery("UPDATE outbox_events SET next_attempt_at = now() - INTERVAL '1 second' "
                + "WHERE event_id = ?").execute(Tuple.of(UUID.fromString(event.eventId()))).mapEmpty());
        List<OutboxRecord> reclaimed = await(outbox.claim("relay-a", WHOLE_QUEUE));
        OutboxRecord back = reclaimed.stream()
                .filter(record -> record.eventId().equals(event.eventId())).findFirst().orElseThrow();
        assertEquals(1, back.attempts());
        assertEquals(PostgresOutboxQueue.PENDING, statusOf(event.eventId()));
    }

    @Test
    void aParkedRowLeavesTheQueueWithItsReason() {
        String matchId = matchId();
        OutboxRecord event = event(matchId);
        await(outbox.writeMatchWithEvent(match(matchId), roster(matchId), event));

        await(outbox.park(event.eventId(), "undecodable payload"));

        assertFalse(claimedEventIds("relay-a").contains(event.eventId()));
        assertEquals(PostgresOutboxQueue.PARKED, statusOf(event.eventId()));
        Row row = await(probe.preparedQuery("SELECT last_error FROM outbox_events WHERE event_id = ?")
                .execute(Tuple.of(UUID.fromString(event.eventId())))).iterator().next();
        assertEquals("undecodable payload", row.getString("last_error"));
    }

    @Test
    void publishesTheOldestRowsFirst() {
        String oldest = matchId();
        String middle = matchId();
        String newest = matchId();
        // Written out of order on purpose: insertion order is created_at, not the order
        // the rows happen to be written in the test.
        await(writeAt(middle, OCCURRED_AT.plusSeconds(2)));
        await(writeAt(newest, OCCURRED_AT.plusSeconds(3)));
        await(writeAt(oldest, OCCURRED_AT.plusSeconds(1)));

        List<String> mine = await(outbox.claim("relay-a", WHOLE_QUEUE)).stream()
                .map(OutboxRecord::partitionKey)
                .filter(key -> key.equals(oldest) || key.equals(middle) || key.equals(newest))
                .toList();

        assertEquals(List.of(oldest, middle, newest), mine);
    }

    @Test
    void aClaimBringsAtMostTheBatchSize() {
        await(writeAt(matchId(), OCCURRED_AT));
        await(writeAt(matchId(), OCCURRED_AT.plusSeconds(1)));

        assertEquals(1, await(outbox.claim("relay-a", 1)).size());
    }

    private Future<Void> writeAt(String matchId, Instant createdAt) {
        return outbox.writeMatchWithEvent(match(matchId), roster(matchId), event(matchId, createdAt));
    }

    /** The ids the claim handed over - membership, not length: the queue is shared. */
    private static List<String> claimedEventIds(String relayId) {
        return await(outbox.claim(relayId, WHOLE_QUEUE)).stream().map(OutboxRecord::eventId).toList();
    }

    private static void deleteOurRows() {
        await(probe.preparedQuery("DELETE FROM outbox_events WHERE partition_key LIKE ?")
                .execute(Tuple.of(MATCH_ID_PREFIX + "%")).mapEmpty());
        await(probe.preparedQuery("DELETE FROM match_players WHERE match_id LIKE ?")
                .execute(Tuple.of(MATCH_ID_PREFIX + "%")).mapEmpty());
        await(probe.preparedQuery("DELETE FROM matches WHERE id LIKE ?")
                .execute(Tuple.of(MATCH_ID_PREFIX + "%")).mapEmpty());
    }

    private static String matchId() {
        return MATCH_ID_PREFIX + UUID.randomUUID();
    }

    private static Match match(String matchId) {
        return new Match(matchId, GameMode.FIVE_V_FIVE, Region.SA, MatchStatus.PENDING, OCCURRED_AT, null, null);
    }

    private static List<MatchPlayer> roster(String matchId) {
        return List.of(new MatchPlayer(matchId, "player-1", "A", 55),
                new MatchPlayer(matchId, "player-2", "B", 61));
    }

    private static OutboxRecord event(String matchId) {
        return event(matchId, OCCURRED_AT);
    }

    private static OutboxRecord event(String matchId, Instant createdAt) {
        return event(matchId, createdAt, UUID.randomUUID().toString());
    }

    private static OutboxRecord event(String matchId, Instant createdAt, String eventId) {
        return new OutboxRecord(eventId, "matchmaking.events.match-confirmed", matchId, "MATCH_CONFIRMED",
                new byte[] {0x0A, 0x02, 0x08, 0x01}, createdAt, createdAt, 0);
    }

    private static String eventIdOf(String matchId) {
        return await(probe.preparedQuery("SELECT event_id FROM outbox_events WHERE partition_key = ?")
                .execute(Tuple.of(matchId))).iterator().next().getUUID("event_id").toString();
    }

    private static String statusOf(String eventId) {
        return await(probe.preparedQuery("SELECT status FROM outbox_events WHERE event_id = ?")
                .execute(Tuple.of(UUID.fromString(eventId)))).iterator().next().getString("status");
    }

    private static long countMatches(String matchId) {
        return await(probe.preparedQuery("SELECT count(*) AS total FROM matches WHERE id = ?")
                .execute(Tuple.of(matchId))).iterator().next().getLong("total");
    }

    private static long countPlayers(String matchId) {
        return await(probe.preparedQuery("SELECT count(*) AS total FROM match_players WHERE match_id = ?")
                .execute(Tuple.of(matchId))).iterator().next().getLong("total");
    }

    private static <T> T await(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }
}
