package com.victor.matchmaking.db;

import org.flywaydb.core.Flyway;

/**
 * Runs schema migrations. Idempotent; safe for multiple services to run at boot.
 *
 * <p>This runs in {@code main} before the service binds its port, which makes the database a
 * **startup** dependency as well as a runtime one — and the W18 measurement showed the cost of
 * getting that wrong: with Postgres stopped, the API neither started nor failed, it sat in the
 * migration with an empty log and nothing listening, which from the outside is indistinguishable
 * from a slow boot. Two answers, both needed: the driver's own connect timeout (so a black-holed
 * host gives up instead of waiting forever) and a log line that says what happened and why the
 * service will not come up.
 */
public final class DatabaseMigrator {

    private static final System.Logger LOG = System.getLogger("matchmaking.db");

    /** A database unreachable at boot is a deployment problem, not a waiting game (W18). */
    private static final int CONNECT_TIMEOUT_SECONDS = 5;

    private DatabaseMigrator() {
    }

    public static void migrate(DatabaseConfig config) {
        try {
            Flyway.configure()
                    .dataSource(withConnectTimeout(config.jdbcUrl()), config.user(), config.password())
                    .locations("classpath:db/migration")
                    .load()
                    .migrate();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR,
                    "db.migrate.failed url={0} - the service cannot start without its source of truth",
                    config.jdbcUrl());
            throw e;
        }
    }

    /**
     * Adds the driver's connect timeout, which is the knob that decides whether an unreachable host
     * fails or hangs. Left alone when the URL already carries one: whoever configured it knows
     * something this default does not.
     */
    static String withConnectTimeout(String jdbcUrl) {
        if (jdbcUrl.contains("connectTimeout=")) {
            return jdbcUrl;
        }
        return jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") + "connectTimeout=" + CONNECT_TIMEOUT_SECONDS;
    }
}
