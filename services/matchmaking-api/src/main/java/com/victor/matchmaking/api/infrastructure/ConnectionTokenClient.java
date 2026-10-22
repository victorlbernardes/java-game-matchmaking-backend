package com.victor.matchmaking.api.infrastructure;

import java.time.Instant;

import com.victor.matchmaking.api.application.ConnectionTokenIssuer;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

/**
 * The API's call to the auth service: "sign a connection token for this player, in this match"
 * (W23, ADR 0033).
 *
 * <p>The API cannot sign it itself, and that is the point: the private key lives in one place, so a
 * bug in the gateway cannot mint a credential the game servers accept. What the API sends is the
 * player's own session token - the auth service verifies it and signs only for the subject it could
 * verify, which is what makes the endpoint safe to expose internally without a service secret.
 *
 * <p>A non-200 answer is a dependency failure, not a client error: the player did nothing wrong, and
 * telling him so with a {@code Retry-After} (503) is what lets him try again when the issuer is back.
 */
public final class ConnectionTokenClient implements ConnectionTokenIssuer {

    /** Where the auth service listens; the same host the JWKS is fetched from in a real deployment. */
    public static final String ENV_AUTH_BASE_URL = "AUTH_BASE_URL";
    public static final String DEFAULT_AUTH_BASE_URL = "http://localhost:8081";

    private static final String PATH = "/v1/auth/connection-tokens";

    private final WebClient client;
    private final String url;

    public ConnectionTokenClient(Vertx vertx, String authBaseUrl) {
        this.client = WebClient.create(vertx);
        this.url = (authBaseUrl.endsWith("/") ? authBaseUrl.substring(0, authBaseUrl.length() - 1) : authBaseUrl)
                + PATH;
    }

    public static String baseUrlFromEnv() {
        String configured = System.getenv(ENV_AUTH_BASE_URL);
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        // Falling back to the JWKS URL's origin keeps a deployment that configured one and not the
        // other working: they are the same service, and two knobs for it is one too many.
        String jwks = System.getenv("AUTH_JWKS_URL");
        if (jwks != null && jwks.contains("/.well-known/")) {
            return jwks.substring(0, jwks.indexOf("/.well-known/"));
        }
        return DEFAULT_AUTH_BASE_URL;
    }

    @Override
    public Future<IssuedConnectionToken> issue(String playerToken, String matchId, int team) {
        return client.postAbs(url)
                .putHeader("Authorization", "Bearer " + playerToken)
                .putHeader("content-type", "application/json")
                .sendJsonObject(new JsonObject().put("matchId", matchId).put("team", team))
                .compose(response -> {
                    if (response.statusCode() != 200 || response.bodyAsJsonObject() == null) {
                        return Future.failedFuture(new TokenIssuerUnavailable(
                                "auth-service answered " + response.statusCode()));
                    }
                    JsonObject body = response.bodyAsJsonObject();
                    String token = body.getString("connectionToken");
                    if (token == null || token.isBlank()) {
                        return Future.failedFuture(new TokenIssuerUnavailable("auth-service returned no token"));
                    }
                    return Future.succeededFuture(new IssuedConnectionToken(token,
                            Instant.parse(body.getString("expiresAt")),
                            body.getLong("expiresInSeconds")));
                });
    }

    /** The issuer is unreachable, refused, or answered something unusable: 503, not 500. */
    public static final class TokenIssuerUnavailable extends RuntimeException {
        public TokenIssuerUnavailable(String message) {
            super(message);
        }
    }
}
