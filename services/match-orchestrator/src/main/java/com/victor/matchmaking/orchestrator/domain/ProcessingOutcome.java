package com.victor.matchmaking.orchestrator.domain;

/** What the consumer did with a record, and the three things it can do. */
public enum ProcessingOutcome {

    /** First delivery: the effect was applied and the event recorded. */
    APPLIED,

    /**
     * The event was already applied by this consumer group. A duplicate is a
     * <b>success</b>, not an error: it is the delivery guarantee working, and the
     * offset can be committed like any other processed record.
     */
    DUPLICATE,

    /**
     * Every attempt failed, so the record was published to the dead letter queue and
     * the offset committed. The record is out of the way but not lost: it is the one
     * outcome an operator has to look at.
     */
    DEAD_LETTERED
}
