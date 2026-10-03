package com.victor.matchmaking.auth.presentation;

import io.vertx.core.json.JsonObject;

/** HTTP response body for POST /v1/auth/login. */
public record TokenResponse(String token, long expiresIn) {

    public JsonObject toJson() {
        return new JsonObject().put("token", token).put("expiresIn", expiresIn);
    }
}
