package com.victor.matchmaking.api.presentation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.victor.matchmaking.api.application.PendingConfirmationLookup;
import com.victor.matchmaking.api.application.PlayerPresence;
import com.victor.matchmaking.metrics.MetricNames;
import com.victor.matchmaking.redis.Envelope;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.RedisPublisher;
import com.victor.matchmaking.redis.RoutedEnvelope;
import com.victor.matchmaking.redis.message.Payloads;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;

/**
 * The connection registry: which players are connected **here**, and what to do when they are
 * not (W19, ADR 0026).
 *
 * <p>A socket lives on exactly one instance — nothing removes that, and a load balancer that opens
 * one WebSocket and keeps it open is not the problem. The problem is what the service believed
 * about it: that "connected" meant "connected to me", that a notification delivered by broadcast
 * reached its player, and that a client which reconnected somewhere else had somehow been told
 * what it missed. Every one of those beliefs was local to the instance the player happened to land
 * on, so the only thing that survived an instance was the socket that died with it.
 *
 * <p>Three things make the fact shared instead of local: the registry keeps its own list and
 * publishes it to {@link PlayerPresence} (so presence outlives the socket), counts a delivery as
 * missed when the player is not connected here (so "the broadcast reached everyone" is a number
 * and not an assumption), and answers a heartbeat with the pending confirmation the player still
 * has to answer (so a client that reconnects anywhere recovers what a lost notification owed it).
 */
public final class ConnectionRegistry {

    private static final System.Logger LOG = System.getLogger("matchmaking.api.ws");
    private static final String TYPE_MATCH_CONFIRM = "MATCH_CONFIRM";
    private static final String TYPE_MATCH_DECLINE = "MATCH_DECLINE";
    private static final String TYPE_MATCH_PENDING_CONFIRMATION = "MATCH_PENDING_CONFIRMATION";
    private static final String TYPE_PING = "PING";

    private final Map<String, ServerWebSocket> byPlayer = new ConcurrentHashMap<>();
    private final RedisPublisher publisher;
    private final String instanceId;
    private final PlayerPresence presence;
    private final PendingConfirmationLookup pendingConfirmations;
    private final AtomicInteger connections = new AtomicInteger();
    private final Counter replaced;
    private final Counter represented;
    private final Counter delivered;
    private final Counter missed;

    /** In-memory wiring (tests, dev): no shared state, nothing to publish, nothing to re-present. */
    public ConnectionRegistry(RedisPublisher publisher) {
        this(publisher, "local", PlayerPresence.none(), PendingConfirmationLookup.none(), new SimpleMeterRegistry());
    }

    public ConnectionRegistry(RedisPublisher publisher, String instanceId, PlayerPresence presence,
            PendingConfirmationLookup pendingConfirmations, MeterRegistry registry) {
        this.publisher = publisher;
        this.instanceId = instanceId;
        this.presence = presence;
        this.pendingConfirmations = pendingConfirmations;
        Tags instance = Tags.of(MetricNames.TAG_INSTANCE_ID, instanceId);
        Gauge.builder(MetricNames.WS_CONNECTIONS, connections, AtomicInteger::get).tags(instance).register(registry);
        this.replaced = registry.counter(MetricNames.WS_CONNECTIONS_REPLACED, instance);
        this.represented = registry.counter(MetricNames.WS_REPRESENTED, instance);
        this.delivered = registry.counter(MetricNames.WS_NOTIFICATIONS, instance.and(
                MetricNames.TAG_RESULT, MetricNames.RESULT_DELIVERED));
        this.missed = registry.counter(MetricNames.WS_NOTIFICATIONS, instance.and(
                MetricNames.TAG_RESULT, MetricNames.RESULT_NO_LOCAL_CONNECTION));
    }

    public String instanceId() {
        return instanceId;
    }

    /** The player is connected on this instance: open the socket, name the instance, remember it. */
    public void register(String playerId, ServerWebSocket ws) {
        ServerWebSocket previous = byPlayer.put(playerId, ws);
        if (previous != null && previous != ws) {
            LOG.log(System.Logger.Level.INFO, "ws.replaced playerId={0} instance={1}", playerId, instanceId);
            replaced.increment();
            previous.close();
        }
        connections.incrementAndGet();
        LOG.log(System.Logger.Level.INFO, "ws.connected playerId={0} instance={1}", playerId, instanceId);
        presence.markOnline(playerId, instanceId)
                .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                        "ws.presence.failed playerId=" + playerId + " stage=markOnline", err));
    }

    /** Only the connection that is still registered unregisters: a replaced socket must not clear. */
    public void unregister(String playerId, ServerWebSocket ws) {
        if (byPlayer.remove(playerId, ws)) {
            connections.decrementAndGet();
            LOG.log(System.Logger.Level.INFO, "ws.disconnected playerId={0} instance={1}", playerId, instanceId);
            presence.offline(playerId, instanceId)
                    .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                            "ws.presence.failed playerId=" + playerId + " stage=offline", err));
        }
    }

    /**
     * Forward a Pub/Sub notification to the local connection, and count whether there was one.
     * A miss is not an error: the player may be on another instance, or on none. It is the number
     * that says how much of the pub/sub fan-out did not land here.
     */
    public void deliver(RoutedEnvelope routed) {
        ServerWebSocket ws = byPlayer.get(routed.targetPlayerId());
        if (ws == null) {
            missed.increment();
            LOG.log(System.Logger.Level.INFO, "ws.notify.missed playerId={0} instance={1}",
                    routed.targetPlayerId(), instanceId);
            return;
        }
        String type = new JsonObject(routed.envelopeJson()).getString("type");
        LOG.log(System.Logger.Level.INFO, "ws.push playerId={0} type={1} instance={2}",
                routed.targetPlayerId(), type, instanceId);
        delivered.increment();
        ws.writeTextMessage(routed.envelopeJson());
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
            case TYPE_PING -> {
                // The heartbeat is three things at once: it keeps the socket alive, it refreshes
                // presence (which is what makes the instance's name survive in Redis), and it asks
                // the state what the player owes - a confirmation a lost notification never asked.
                ws.writeTextMessage(
                        Envelope.of("PONG", new Payloads.Empty()).toJson(Payloads.Empty::toJson).encode());
                presence.markOnline(playerId, instanceId)
                        .onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                                "ws.presence.failed playerId=" + playerId + " stage=heartbeat", err));
                rePresentPending(playerId, ws);
            }
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

    /**
     * Re-presents the pending confirmation, if the player still has one. This is the delivery half
     * of the notification contract: pub/sub is best effort, so the client's own heartbeat is the
     * one thing that runs again after a delivery is lost.
     */
    private void rePresentPending(String playerId, ServerWebSocket ws) {
        pendingConfirmations.findForPlayer(playerId).onSuccess(pending -> {
            if (pending == null) {
                return;
            }
            LOG.log(System.Logger.Level.INFO, "ws.represented playerId={0} matchId={1} instance={2}",
                    playerId, pending.matchId(), instanceId);
            represented.increment();
            ws.writeTextMessage(Envelope.of(TYPE_MATCH_PENDING_CONFIRMATION, pending)
                    .toJson(Payloads.MatchPending::toJson).encode());
        }).onFailure(err -> LOG.log(System.Logger.Level.ERROR,
                "ws.represent.failed playerId=" + playerId, err));
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
