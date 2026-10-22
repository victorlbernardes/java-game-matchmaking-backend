package com.victor.matchmaking.redis;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * A correct {@code SCAN}: Redis answers one page per call and hands back a cursor, and the last
 * page is the one whose cursor is 0. Reading a single page and ignoring the cursor is a bug that
 * hides data instead of failing.
 *
 * <p>Found by the W16 load test, and it was the worst kind: with 404 keys in Redis,
 * {@code SCAN 0 MATCH queue:*} returned **no keys at all** (the queue key was on page 6 of 39),
 * so the engine paired one match and then stopped seeing queues entirely while Redis stayed full
 * of waiting players. It works with a fresh keyspace and goes silent under real use.
 *
 * <p>When the set of keys is a closed one (the queues are, see the engine), building the key names
 * directly beats scanning: it is exact and it costs one trivial read per key. This iterator is for
 * the keys that are genuinely unknown (the pending matches).
 */
public final class RedisScanner {

    /**
     * Pages are hints, not limits: Redis returns roughly COUNT keys per page and may return
     * more or fewer. A large COUNT keeps the number of round trips down for a big keyspace.
     */
    public static final int PAGE_HINT = 500;

    private RedisScanner() {
    }

    /** Every key matching the pattern, in whatever order SCAN returns them. */
    public static Future<List<String>> scanAll(Redis redis, String pattern) {
        return scanPage(redis, "0", pattern, new ArrayList<>());
    }

    private static Future<List<String>> scanPage(Redis redis, String cursor, String pattern,
            List<String> found) {
        return redis.send(Request.cmd(Command.SCAN).arg(cursor).arg("MATCH").arg(pattern)
                        .arg("COUNT").arg(String.valueOf(PAGE_HINT)))
                .compose(reply -> {
                    Response keys = reply.get(1);
                    if (keys != null) {
                        for (int i = 0; i < keys.size(); i++) {
                            found.add(keys.get(i).toString());
                        }
                    }
                    String next = reply.get(0).toString();
                    if ("0".equals(next)) {
                        return Future.succeededFuture(found);
                    }
                    return scanPage(redis, next, pattern, found);
                });
    }
}
