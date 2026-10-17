package com.victor.matchmaking.db;

import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;

/**
 * Builds the shared JDBC pool. Every service talks to the same database, so how a
 * connection is built (and where the URL comes from) belongs in one place instead
 * of being repeated per service.
 */
public final class DatabasePool {

    private DatabasePool() {
    }

    public static Pool connect(Vertx vertx, DatabaseConfig config) {
        return JDBCPool.pool(vertx, new JDBCConnectOptions()
                .setJdbcUrl(config.jdbcUrl())
                .setUser(config.user())
                .setPassword(config.password()), new PoolOptions());
    }
}
