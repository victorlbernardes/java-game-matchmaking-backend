package com.victor.matchmaking.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * The database is a startup dependency, and that is not allowed to look like a slow boot (W18,
 * ADR 0024).
 *
 * <p>Measured in W18: with Postgres stopped, the API and the engine both sat in the boot-time
 * migration with an empty log and nothing listening — indistinguishable from a service that is
 * simply taking its time, and unactionable for whoever is watching a rollout. Two things make it
 * actionable: the driver gives up instead of waiting forever, and the failure says what happened.
 */
class DatabaseMigratorTest {

    @Test
    void addsTheDriversConnectTimeoutWhenTheUrlHasNone() {
        assertEquals("jdbc:postgresql://localhost:5432/matchmaking?connectTimeout=5",
                DatabaseMigrator.withConnectTimeout("jdbc:postgresql://localhost:5432/matchmaking"));
    }

    @Test
    void keepsAConfiguredConnectTimeout() {
        String configured = "jdbc:postgresql://localhost:5432/matchmaking?connectTimeout=30";
        assertEquals(configured, DatabaseMigrator.withConnectTimeout(configured),
                "whoever set it knows something this default does not");
    }

    @Test
    void appendsToAnExistingQueryString() {
        assertEquals("jdbc:postgresql://localhost:5432/matchmaking?sslmode=require&connectTimeout=5",
                DatabaseMigrator.withConnectTimeout(
                        "jdbc:postgresql://localhost:5432/matchmaking?sslmode=require"));
    }

    @Test
    void anUnreachableDatabaseFailsInsteadOfHanging() {
        long start = System.nanoTime();
        RuntimeException failure = assertThrows(RuntimeException.class, () -> DatabaseMigrator.migrate(
                new DatabaseConfig("jdbc:postgresql://localhost:1/matchmaking", "matchmaking", "matchmaking")));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs < TimeUnit.SECONDS.toMillis(30),
                "an unreachable source of truth must end the boot, not hang it (took " + elapsedMs + "ms)");
        assertTrue(failure.getMessage() != null, "the failure carries a reason for the operator");
    }
}
