package com.victor.matchmaking.scenarios;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Reading a JWT's claims without validating it.
 *
 * <p>Deliberately no signature check: the scenarios that use this are asking what the issuer *said*
 * (the {@code scope}, the {@code matchId}, the {@code team}, the {@code exp}), and the signature is
 * asserted by the tests that own it ({@code ConnectionTokenTest}), against the published JWKS. A
 * harness that verified signatures here would be a second verifier to keep in step for no added
 * coverage.
 */
final class Jwt {

    private Jwt() {
    }

    /** {@code jwt <token> <claim>} -> the claim, or nothing when absent. */
    static int claim(String[] argv) throws Exception {
        Args args = Args.of(argv);
        String token = args.positional(0, "the token");
        String claim = args.positional(1, "the claim");
        System.out.println(claims(token).get(claim) == null ? "" : Json.pythonText(claims(token).get(claim)));
        return 0;
    }

    static JsonNode claims(String token) throws Exception {
        String[] parts = token.split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException("not a JWT: " + token);
        }
        String payload = parts[1];
        // Base64url, unpadded: the decoder wants the padding, and the spec does not send it.
        payload = payload + "=".repeat((4 - payload.length() % 4) % 4);
        byte[] decoded = Base64.getUrlDecoder().decode(payload.getBytes(StandardCharsets.UTF_8));
        return Json.parse(new String(decoded, StandardCharsets.UTF_8));
    }
}
