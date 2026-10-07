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
    private static final System.Logger LOG = System.getLogger("matchmaking.auth");

    private AuthServiceApplication() {
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %5$s%n");
        java.util.Locale.setDefault(java.util.Locale.ENGLISH);
        Vertx vertx = Vertx.vertx();
        int port = Integer.parseInt(System.getenv().getOrDefault("AUTH_PORT", String.valueOf(DEFAULT_PORT)));

        RsaKeyPairProvider keys = RsaKeyPairProvider.generate();
        JWTAuth jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions().addJwk(keys.privateJwk()));
        Router router = createRouter(vertx, jwtAuth, keys.keyPair());

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router)
                .listen(port)
                .onSuccess(s -> LOG.log(System.Logger.Level.INFO, "auth.started port={0}", String.valueOf(s.actualPort())))
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR, "auth.start.failed port=" + port, err));
    }

    static Router createRouter(Vertx vertx, JWTAuth jwtAuth, KeyPair keyPair) {
        return AuthRoutes.createRouter(vertx, jwtAuth, keyPair);
    }
}
