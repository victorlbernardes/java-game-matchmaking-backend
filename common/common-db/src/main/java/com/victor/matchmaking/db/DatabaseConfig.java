package com.victor.matchmaking.db;

/** Database connection settings, sourced from the environment. */
public record DatabaseConfig(String jdbcUrl, String user, String password) {

    public static DatabaseConfig fromEnv() {
        return new DatabaseConfig(
                System.getenv().getOrDefault("DB_JDBC_URL", "jdbc:postgresql://localhost:5432/matchmaking"),
                System.getenv().getOrDefault("DB_USER", "matchmaking"),
                System.getenv().getOrDefault("DB_PASSWORD", "matchmaking"));
    }
}
