package com.victor.matchmaking.orchestrator.application;

/**
 * Destination of a record that cannot be handled.
 *
 * <p>The contract is durability, not fire-and-forget: when {@link #publish} returns, the
 * record is on the topic. That is what makes it safe to commit the offset afterwards —
 * and why a failure here must abort the commit instead of losing the record.
 */
public interface DeadLetterQueue {

    /**
     * @throws RuntimeException when the record could not be written; the caller then
     *         leaves the offset uncommitted, so the record is delivered again
     */
    void publish(DeadLetter letter);
}
