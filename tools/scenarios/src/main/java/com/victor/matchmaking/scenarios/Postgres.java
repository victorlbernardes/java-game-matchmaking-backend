package com.victor.matchmaking.scenarios;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The scenario's own connection to Postgres, for the checks whose question is about the database:
 * what it is executing right now, and what it kept.
 *
 * <p>The Python probe asked a container ({@code docker exec … psql}) for the same answers, which
 * tied the measurement to a container name and to Docker being reachable. The scenario is already
 * a client of the API and of Redis; the database is a third endpoint, and it is addressed the same
 * way — by URL, with the same defaults the services use.
 */
final class Postgres implements Closeable {

    private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/matchmaking";
    private static final String DEFAULT_USER = "matchmaking";
    private static final String DEFAULT_PASSWORD = "matchmaking";

    private final Connection connection;

    private Postgres(Connection connection) {
        this.connection = connection;
    }

    static Postgres connect() throws SQLException {
        String url = System.getenv().getOrDefault("DB_URL", DEFAULT_URL);
        String user = System.getenv().getOrDefault("DB_USER", DEFAULT_USER);
        String password = System.getenv().getOrDefault("DB_PASSWORD", DEFAULT_PASSWORD);
        return new Postgres(DriverManager.getConnection(url, user, password));
    }

    /** The first column of the first row as text; {@code ""} when the query answers nothing. */
    String scalar(String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
            if (rows.next()) {
                String value = rows.getString(1);
                return value == null ? "" : value;
            }
            return "";
        }
    }

    /** The first column of the first row as a count; {@code 0} when it is not a number. */
    int count(String sql) throws SQLException {
        String value = scalar(sql);
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException notACount) {
            return 0;
        }
    }

    /** Every row's first two columns, as pairs; {@code []} when the query answers nothing. */
    java.util.List<String[]> pairs(String sql) throws SQLException {
        java.util.List<String[]> rows = new java.util.ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                rows.add(new String[] {result.getString(1), result.getString(2)});
            }
        }
        return rows;
    }

    /** Runs a statement that answers no rows. */
    void execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException closed) {
            // A probe that is exiting: the process would close it anyway.
        }
    }
}
