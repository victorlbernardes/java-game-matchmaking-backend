package com.victor.matchmaking.domain;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Verifies common-domain stays framework-free: compiled classes must not reference
 * framework packages. Fails if someone adds io.vertx, org.springframework, etc.
 */
class DomainPurityTest {

    private static final List<String> FORBIDDEN_PACKAGES = List.of(
            "io/vertx", "org/springframework", "redis/clients", "jakarta/", "com/mysql", "org/postgresql");

    @Test
    void domainClassesOnlyReferenceJavaPackages() throws IOException {
        Class<?>[] domainTypes = {
                Player.class, MatchmakingTicket.class, Match.class, MatchPlayer.class, Party.class,
                TicketState.class, MatchStatus.class, GameMode.class, Region.class,
                TicketStateMachine.class, InvalidStateTransitionException.class
        };
        for (Class<?> type : domainTypes) {
            String resource = type.getName().replace('.', '/') + ".class";
            try (InputStream in = type.getClassLoader().getResourceAsStream(resource)) {
                byte[] bytes = in.readAllBytes();
                String content = new String(bytes, StandardCharsets.ISO_8859_1);
                for (String forbidden : FORBIDDEN_PACKAGES) {
                    assertFalse(content.contains(forbidden),
                            type.getSimpleName() + " must not reference " + forbidden.replace('/', '.'));
                }
            }
        }
    }
}
