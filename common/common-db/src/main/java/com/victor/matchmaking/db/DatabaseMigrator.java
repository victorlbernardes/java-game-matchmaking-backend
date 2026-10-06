package com.victor.matchmaking.db;

import org.flywaydb.core.Flyway;

/** Runs schema migrations. Idempotent; safe for multiple services to run at boot. */
public final class DatabaseMigrator {

    private DatabaseMigrator() {
    }

    public static void migrate(DatabaseConfig config) {
        Flyway.configure()
                .dataSource(config.jdbcUrl(), config.user(), config.password())
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }
}
