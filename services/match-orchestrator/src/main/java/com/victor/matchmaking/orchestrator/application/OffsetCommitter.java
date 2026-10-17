package com.victor.matchmaking.orchestrator.application;

/**
 * Marks the position a consumer group has fully processed, so a restart resumes after it.
 *
 * <p>Deliberately a separate port from {@link ConfirmedMatchInbox}: the two must be
 * callable in that order and never in the other one. Committing an offset whose outcome
 * is not durable - neither the effect nor a dead letter - is the one way to lose a record
 * silently: it is never retried, because from the broker's point of view it was handled.
 *
 * <p>It takes the raw record and not a decoded event because the dead letter path commits
 * offsets for payloads that never became an event.
 */
public interface OffsetCommitter {

    /** Commits every record up to and including {@code record} as processed. */
    void markProcessed(RawRecord record);
}
