package com.victor.matchmaking.api;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;

import com.victor.matchmaking.redis.RedisTicketRepository;
import com.victor.matchmaking.api.presentation.HttpRoutes;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;

/** Composition root: wires config, JWKS and HTTP server. */
public final class MatchmakingApiApplication {

    private static final int DEFAULT_PORT = 8080;

    private MatchmakingApiApplication() {
    }

    public static void main(String[] args) throws Exception {
        Vertx vertx = Vertx.vertx();
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", String.valueOf(DEFAULT_PORT)));
        String jwksUrl = System.getenv().getOrDefault("AUTH_JWKS_URL", "http://localhost:8081/.well-known/jwks.json");

        String redisUri = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(redisUri));
        com.victor.matchmaking.redis.TicketRepository repository =
                new RedisTicketRepository(redis);
        com.victor.matchmaking.redis.RedisPublisher publisher = new com.victor.matchmaking.redis.RedisPublisher(redis);
        com.victor.matchmaking.api.presentation.WsHub wsHub =
                new com.victor.matchmaking.api.presentation.WsHub(publisher);
        com.victor.matchmaking.db.DatabaseConfig dbConfig = com.victor.matchmaking.db.DatabaseConfig.fromEnv();
        com.victor.matchmaking.db.DatabaseMigrator.migrate(dbConfig);
        com.victor.matchmaking.db.MatchRepository matchRepository =
                com.victor.matchmaking.db.MatchRepository.connect(vertx, dbConfig);
        com.victor.matchmaking.redis.RedisSubscriber.subscribe(vertx, redisUri,
                com.victor.matchmaking.redis.RedisKeys.CHANNEL_PLAYER_NOTIFICATIONS,
                msg -> wsHub.deliver(com.victor.matchmaking.redis.RoutedEnvelope.fromJson(msg)));
        JWTAuth jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(fetchPublicJwk(jwksUrl)));
        Router router = HttpRoutes.createRouter(vertx, jwtAuth, repository, wsHub, matchRepository);

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router)
                .listen(port)
                .onSuccess(s -> System.out.println("matchmaking-api listening on :" + s.actualPort()))
                .onFailure(Throwable::printStackTrace);
    }

    static Router createRouter(Vertx vertx, JWTAuth jwtAuth, com.victor.matchmaking.redis.TicketRepository repository) {
        return HttpRoutes.createRouter(vertx, jwtAuth, repository);
    }

    static Router createRouter(Vertx vertx, JWTAuth jwtAuth) {
        return HttpRoutes.createRouter(vertx, jwtAuth);
    }

    private static JsonObject fetchPublicJwk(String jwksUrl) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder().uri(URI.create(jwksUrl)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("failed to fetch JWKS: HTTP " + response.statusCode());
            }
            JsonArray keys = new JsonObject(response.body()).getJsonArray("keys");
            if (keys == null || keys.isEmpty()) {
                throw new IllegalStateException("JWKS has no keys");
            }
            return keys.getJsonObject(0);
        }
    }
}
