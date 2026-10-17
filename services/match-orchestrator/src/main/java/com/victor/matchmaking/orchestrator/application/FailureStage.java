package com.victor.matchmaking.orchestrator.application;

/**
 * Which step of the pipeline gave up on a record.
 *
 * <p>Carried to the dead letter queue as a header because the two read very differently
 * for whoever drains it: {@link #DECODE} is usually a producer or a contract problem
 * (and a burst of it means the registry is unreachable, not that every event is bad),
 * while {@link #APPLY} points at the consumer's own data or database.
 */
public enum FailureStage {

    /** The payload never became an event: bad bytes, unknown schema, broken contract. */
    DECODE,

    /** The event was understood but its effect could not be applied. */
    APPLY
}
