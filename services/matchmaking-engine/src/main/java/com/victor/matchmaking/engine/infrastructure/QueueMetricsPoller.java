package com.victor.matchmaking.engine.infrastructure;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.redis.RedisKeys;

import io.vertx.core.Future;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.ResponseType;

/**
 * Reads the queues from Redis and publishes them as KPIs (ADR 0020).
 *
 * <p>Derived, not counted: the queue is the source of truth, so a number that disagrees with
 * Redis is impossible by construction. The cost is one pass over the queue keys every few
 * seconds; the queues are one key per mode and region, and each pass is a {@code ZCARD} plus
 * the head's score.
 *
 * <p>Every (mode, region) pair is published on every pass, zero included: a gauge that stops
 * being written keeps its last value forever, and a dashboard showing a queue that drained
 * hours ago as "3 waiting" is worse than no dashboard.
 */
public final class QueueMetricsPoller {

    private static final System.Logger LOG = System.getLogger("matchmaking.engine.kpis");

    private final io.vertx.redis.client.Redis redis;
    private final EngineKpis kpis;
    private final Clock clock;

    public QueueMetricsPoller(io.vertx.redis.client.Redis redis, EngineKpis kpis, Clock clock) {
        this.redis = redis;
        this.kpis = kpis;
        this.clock = clock;
    }

    /** One pass: read every configured queue and publish all of them. */
    public Future<Void> pollOnce() {
        Map<String, Long> depth = new HashMap<>();
        Map<String, Long> headQueuedAtMillis = new HashMap<>();
        List<Future<Void>> reads = new ArrayList<>();
        // The keys are built, not discovered: a SCAN page used to decide which queues existed,
        // and a queue that fell on a later page was published as an empty one (W16).
        for (String queueKey : RedisKeys.allQueueKeys()) {
            reads.add(readQueue(queueKey, depth, headQueuedAtMillis));
        }
        return Future.all(reads)
                .map(ignored -> {
                    publish(depth, headQueuedAtMillis);
                    return (Void) null;
                });
    }

    private Future<Void> readQueue(String queueKey, Map<String, Long> depth,
            Map<String, Long> headQueuedAtMillis) {
        return redis.send(Request.cmd(Command.ZCARD).arg(queueKey))
                .compose(card -> redis.send(Request.cmd(Command.ZRANGE)
                                .arg(queueKey).arg("0").arg("0").arg("WITHSCORES"))
                        .map(range -> {
                            long waiting = card.toLong() != null ? card.toLong() : 0L;
                            depth.put(queueKey, waiting);
                            // The score of the lowest entry is the enqueue time of the ticket
                            // that has been waiting longest (the queue is a ZSET by queuedAt).
                            Long headQueuedAt = waiting > 0 ? scoreMillis(range) : null;
                            if (headQueuedAt != null) {
                                headQueuedAtMillis.put(queueKey, headQueuedAt);
                            } else if (waiting > 0) {
                                // A non-empty queue whose head cannot be read would be a silent
                                // zero on the panel, so it is said out loud instead.
                                LOG.log(System.Logger.Level.WARNING,
                                        "kpis.queue.noScore key={0} waiting={1} reply={2}",
                                        queueKey, String.valueOf(waiting), String.valueOf(range));
                            }
                            return (Void) null;
                        }));
    }

    /**
     * The queuedAt of the head, in epoch millis, out of a {@code ZRANGE ... WITHSCORES} reply.
     * Two shapes are accepted: one nested {@code [member, score]} per member (what the client
     * hands back here) and a flat {@code [member, score]}.
     *
     * <p>The score is parsed as a double, not a long: Redis replies to a ZSET score with a
     * double, so an epoch in millis arrives as {@code 1.792232380054E12}. Millis fit in a
     * double without loss (2^53 is far above 1.8e12), so the rounding is exact.
     *
     * @return null when the reply carries no numeric score (logged, never silently zero)
     */
    private Long scoreMillis(Response range) {
        if (range == null || range.size() == 0) {
            return null;
        }
        Response first = range.get(0);
        Response score;
        if (first != null && first.type() == ResponseType.MULTI && first.size() >= 2) {
            score = first.get(1);
        } else if (range.size() >= 2) {
            score = range.get(1);
        } else {
            score = null;
        }
        if (score == null) {
            return null;
        }
        try {
            return Math.round(Double.parseDouble(score.toString()));
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }

    /**
     * Publishes every mode/region pair, whether or not Redis has a key for it: an empty queue
     * is depth 0 with no wait, not a series that silently disappeared.
     */
    private void publish(Map<String, Long> depth, Map<String, Long> headQueuedAtMillis) {
        Instant now = clock.instant();
        for (GameMode gameMode : GameMode.values()) {
            for (Region region : Region.values()) {
                String queueKey = RedisKeys.queueKey(gameMode.label(), region.name());
                long waiting = depth.getOrDefault(queueKey, 0L);
                Long head = headQueuedAtMillis.get(queueKey);
                Duration oldestWait = head == null ? null
                        : Duration.between(Instant.ofEpochMilli(head), now);
                kpis.queue(gameMode, region, waiting, oldestWait);
            }
        }
    }
}
