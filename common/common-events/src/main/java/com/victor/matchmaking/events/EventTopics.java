package com.victor.matchmaking.events;

/**
 * Topic names of the durable event log.
 *
 * <p>One topic per event type (ADR 0016): each topic has a single, self-contained
 * schema, so a consumer performs one schema resolution instead of decoding a
 * generic envelope. The matching Schema Registry subject is
 * {@code <topic>-value} (TopicNameStrategy) - the registry is the source of truth
 * for that name, so it is not duplicated here as a constant.
 */
public final class EventTopics {

    /** Published when a match reached its confirmations and was persisted. */
    public static final String MATCH_CONFIRMED = "matchmaking.events.match-confirmed";

    private EventTopics() {
    }
}
