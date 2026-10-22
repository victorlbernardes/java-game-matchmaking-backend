package com.victor.matchmaking.redis;

/**
 * The queue is at its admission limit, so a new ticket was refused instead of accepted (W17, ADR
 * 0022).
 *
 * <p>This is an answer, not a failure: the request was well formed and the player did nothing
 * wrong — accepting him would only promise a wait the system cannot honour. It carries what the
 * boundary needs to say so (which queue, how deep, how deep it may get) so the presentation can
 * answer {@code 503} with a {@code Retry-After} without reading the state again.
 */
public final class QueueFullException extends RuntimeException {

    private final String queue;
    private final long depth;
    private final int capacity;

    public QueueFullException(String queue, long depth, int capacity) {
        super("queue " + queue + " is full (" + depth + "/" + capacity + ")");
        this.queue = queue;
        this.depth = depth;
        this.capacity = capacity;
    }

    public String queue() {
        return queue;
    }

    public long depth() {
        return depth;
    }

    public int capacity() {
        return capacity;
    }
}
