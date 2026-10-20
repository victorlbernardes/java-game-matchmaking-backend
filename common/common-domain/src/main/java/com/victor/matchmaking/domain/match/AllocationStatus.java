package com.victor.matchmaking.domain.match;

/**
 * Where a confirmed match sits in the allocator's workflow (W22, ADR 0031).
 *
 * <p>It is a durable vocabulary rather than the orchestrator's private enum: the column lives in the
 * shared schema ({@code match_allocations.status}) and two services read it — the orchestrator writes
 * it, and the scenario (and any operator query) reads it to tell "waiting for a server" apart from
 * "running somewhere".
 */
public enum AllocationStatus {

    /** Confirmed, and no game server took it (the pool was full or absent). W23 closes the loop. */
    PENDING_ALLOCATION,

    /** A game server took the match; {@code match_allocations.server_id} says which. */
    ALLOCATED,

    /**
     * The slot went back to the pool because the match ended. Keeping the row (instead of deleting
     * it) is what makes the release idempotent: the second release finds nothing in
     * {@code ALLOCATED} and does not return a second slot.
     */
    RELEASED
}
