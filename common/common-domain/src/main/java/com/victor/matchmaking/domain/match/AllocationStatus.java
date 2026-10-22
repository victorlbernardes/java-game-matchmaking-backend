package com.victor.matchmaking.domain.match;

/**
 * Where a confirmed match sits in the allocator's workflow (W22, ADR 0031; W23, ADR 0032).
 *
 * <p>It is a durable vocabulary rather than the orchestrator's private enum: the column lives in the
 * shared schema ({@code match_allocations.status}) and two services read it — the orchestrator writes
 * it, and the engine reads {@link #REJECTED} rows to put their players back in the queue.
 *
 * <p>The arrows are the only transitions the allocator allows:
 *
 * <pre>
 *   PENDING_ALLOCATION --(a slot)--> ALLOCATED --(server says it began)--> STARTED --(release)--> RELEASED
 *          |                            |                                  |
 *          +---(no slot in time)--------+---(no start in time)-------------+---(server died)--> REJECTED
 * </pre>
 */
public enum AllocationStatus {

    /**
     * Confirmed, and no game server took it yet (the pool was full or absent). The watchdog retries
     * it every second and rejects it at the deadline (W23).
     */
    PENDING_ALLOCATION,

    /** A game server took the match; {@code match_allocations.server_id} says which. */
    ALLOCATED,

    /**
     * The game server reported that the match began. It is what makes the connection address and
     * the signed connection token available to the players (W23, ADR 0033), and the moment the
     * no-show window starts counting (ADR 0034).
     */
    STARTED,

    /**
     * The slot went back to the pool because the match ended. Keeping the row (instead of deleting
     * it) is what makes the release idempotent: the second release finds nothing in
     * {@code ALLOCATED} and does not return a second slot.
     */
    RELEASED,

    /**
     * The match could not run and its players go back to the queue: no slot before the deadline, no
     * start before its own deadline, the server died, or nobody connected. {@code rejection_reason}
     * says which, because "rejected" alone is a fact nobody can act on.
     */
    REJECTED
}
