package com.victor.matchmaking.auth;

import java.security.KeyPair;

import com.victor.matchmaking.auth.infrastructure.RsaKeyPairProvider;
import com.victor.matchmaking.auth.presentation.AuthRoutes;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;

/** Composition root: wires keys, JWTAuth and HTTP server. */
public final class AuthServiceApplication {

    private static final int DEFAULT_PORT = 8081;

    private AuthServiceApplication() {
    }

    public static void main(String[] args) throws Exception {
        Vertx vertx = Vertx.vertx();
        int port = Integer.parseInt(System.getenv().getOrDefault("AUTH_PORT", String.valueOf(DEFAULT_PORT)));

        RsaKeyPairProvider keys = RsaKeyPairProvider.generate();
        JWTAuth jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(keys.privateJwk()));
        Router router = createRouter(vertx, jwtAuth, keys.keyPair());

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router)
                .listen(port)
                .onSuccess(s -> System.out.println("auth-service listening on :" + s.actualPort()))
                .onFailure(Throwable::printStackTrace);
    }

    static Router createRouter(Vertx vertx, JWTAuth jwtAuth, KeyPair keyPair) {
        return AuthRoutes.createRouter(vertx, jwtAuth, keyPair);
    }
}
