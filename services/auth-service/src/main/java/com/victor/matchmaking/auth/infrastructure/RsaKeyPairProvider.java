package com.victor.matchmaking.auth.infrastructure;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;
import java.util.Base64;

import io.vertx.core.json.JsonObject;

/** Generates and exposes the RSA key pair used for token signing. */
public final class RsaKeyPairProvider {

    private static final String KEY_ID = "dev-key-1";

    private final KeyPair keyPair;

    private RsaKeyPairProvider(KeyPair keyPair) {
        this.keyPair = keyPair;
    }

    public static RsaKeyPairProvider generate() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        return new RsaKeyPairProvider(gen.generateKeyPair());
    }

    public KeyPair keyPair() {
        return keyPair;
    }

    public JsonObject privateJwk() {
        return toJwk((RSAPublicKey) keyPair.getPublic(), (RSAPrivateCrtKey) keyPair.getPrivate());
    }

    public JsonObject publicJwk() {
        return toJwk((RSAPublicKey) keyPair.getPublic(), null);
    }

    public String keyId() {
        return KEY_ID;
    }

    public static JsonObject publicJwkOf(KeyPair keyPair) {
        return toJwk((RSAPublicKey) keyPair.getPublic(), null);
    }

    private static JsonObject toJwk(RSAPublicKey publicKey, RSAPrivateCrtKey privateKey) {
        JsonObject jwk = new JsonObject()
                .put("kty", "RSA")
                .put("use", "sig")
                .put("alg", "RS256")
                .put("kid", KEY_ID)
                .put("n", b64url(publicKey.getModulus()))
                .put("e", b64url(publicKey.getPublicExponent()));
        if (privateKey != null) {
            jwk.put("d", b64url(privateKey.getPrivateExponent()))
                    .put("p", b64url(privateKey.getPrimeP()))
                    .put("q", b64url(privateKey.getPrimeQ()))
                    .put("dp", b64url(privateKey.getPrimeExponentP()))
                    .put("dq", b64url(privateKey.getPrimeExponentQ()))
                    .put("qi", b64url(privateKey.getCrtCoefficient()));
        }
        return jwk;
    }

    private static String b64url(BigInteger value) {
        byte[] bytes = value.toByteArray();
        if (bytes[0] == 0 && bytes.length > 1) {
            bytes = Arrays.copyOfRange(bytes, 1, bytes.length);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
