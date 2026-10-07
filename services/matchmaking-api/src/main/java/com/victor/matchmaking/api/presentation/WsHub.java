package com.victor.matchmaking.api.presentation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.victor.matchmaking.redis.Envelope;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.RedisPublisher;
import com.victor.matchmaking.redis.RoutedEnvelope;
import com.victor.matchmaking.redis.message.Payloads;

import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;

/** Local WebSocket registry: playerId → connection. Receives Pub/Sub notifications and forwards them. */
public final class WsHub {

    private static final System.Logger LOG = System.getLogger("matchmaking.api.ws");
    private static final String TYPE_MATCH_CONFIRM = "MATCH_CONFIRM";
    private static final String TYPE_MATCH_DECLINE = "MATCH_DECLINE";
    private static final String TYPE_PING = "PING";

    private final Map<String, ServerWebSocket> byPlayer = new ConcurrentHashMap<>();
    private final RedisPublisher publisher;

    public WsHub(RedisPublisher publisher) {
        this.publisher = publisher;
    }

    public void register(String playerId, ServerWebSocket ws) {
        LOG.log(System.Logger.Level.INFO, "ws.connected playerId={0}", playerId);
        ServerWebSocket previous = byPlayer.put(playerId, ws);
        if (previous != null && previous != ws) {
            previous.close();
        }
    }

    public void unregister(String playerId, ServerWebSocket ws) {
        if (byPlayer.remove(playerId, ws)) {
            LOG.log(System.Logger.Level.INFO, "ws.disconnected playerId={0}", playerId);
        }
    }

    /** Forward a Pub/Sub notification frame to the right local connection. */
    public void deliver(RoutedEnvelope routed) {
        ServerWebSocket ws = byPlayer.get(routed.targetPlayerId());
        if (ws != null) {
            String type = new JsonObject(routed.envelopeJson()).getString("type");
            LOG.log(System.Logger.Level.INFO, "ws.push playerId={0} type={1}",
                    routed.targetPlayerId(), type);
            ws.writeTextMessage(routed.envelopeJson());
        }
    }

    public void handleClientMessage(String playerId, ServerWebSocket ws, String text) {
        ClientFrame frame;
        try {
            frame = ClientFrame.parse(text);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "ws.frame playerId={0} error=invalid JSON", playerId);
            sendError(ws, "invalid JSON");
            return;
        }
        switch (frame.type() != null ? frame.type() : "") {
            case TYPE_PING -> ws.writeTextMessage(
                    Envelope.of("PONG", new Payloads.Empty()).toJson(Payloads.Empty::toJson).encode());
            case TYPE_MATCH_CONFIRM, TYPE_MATCH_DECLINE -> {
                if (frame.matchId() == null || frame.matchId().isBlank()) {
                    LOG.log(System.Logger.Level.WARNING,
                            "ws.frame playerId={0} type={1} error=matchId is required", playerId, frame.type());
                    sendError(ws, "matchId is required");
                    return;
                }
                if (publisher == null) {
                    sendError(ws, "confirmations unavailable in this environment");
                    return;
                }
                LOG.log(System.Logger.Level.INFO, "ws.frame playerId={0} type={1} matchId={2}",
                        playerId, frame.type(), frame.matchId());
                Payloads.MatchConfirmation command = new Payloads.MatchConfirmation(frame.matchId(), playerId,
                        TYPE_MATCH_CONFIRM.equals(frame.type())
                                ? Payloads.MatchConfirmation.Action.CONFIRM
                                : Payloads.MatchConfirmation.Action.DECLINE);
                publisher.publish(RedisKeys.CHANNEL_MATCH_CONFIRMATIONS, command.toJson().encode())
                        .onFailure(err -> sendError(ws, "publish failed: " + err.getMessage()));
            }
            default -> {
                LOG.log(System.Logger.Level.WARNING, "ws.frame playerId={0} type=unknown value={1}",
                        playerId, frame.type());
                sendError(ws, "unknown type: " + frame.type());
            }
        }
    }

    private static void sendError(ServerWebSocket ws, String message) {
        ws.writeTextMessage(
                Envelope.of("ERROR", new Payloads.Error(message)).toJson(Payloads.Error::toJson).encode());
    }

    /** Single typed view of the client frame; parsing JSON stays at this boundary. */
    record ClientFrame(String type, String matchId) {

        static ClientFrame parse(String text) {
            JsonObject json = new JsonObject(text);
            JsonObject payload = json.getJsonObject("payload");
            return new ClientFrame(json.getString("type"), payload != null ? payload.getString("matchId") : null);
        }
    }
}
