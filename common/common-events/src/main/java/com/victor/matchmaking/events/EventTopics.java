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

    /**
     * Published when a match that started ended, with its result (W24, ADR 0035). The
     * game server reports the result; the orchestrator makes it durable and announces it.
     */
    public static final String MATCH_FINISHED = "matchmaking.events.match-finished";

    /**
     * Published by the skill service after it recalculated a match's players (W24). Telemetry
     * for whoever wants the history of ratings; the authoritative state is the table.
     */
    public static final String SKILL_UPDATED = "matchmaking.events.skill-updated";

    /** Suffix of the per-topic dead letter queue. */
    public static final String DEAD_LETTER_SUFFIX = "-dlq";

    /**
     * The dead letter topic of {@code topic}.
     *
     * <p>Per source topic, not one global queue: a record that lands there is still an
     * event of that contract, and whoever replays it needs to know which contract it
     * broke.
     *
     * <p>The dead letter topic is deliberately <b>not</b> schema-managed. A payload that
     * does not decode cannot be re-serialized against a registered schema, so its bytes
     * are forwarded exactly as they arrived and no subject is created for the topic.
     */
    public static String deadLetterOf(String topic) {
        return topic + DEAD_LETTER_SUFFIX;
    }

    private EventTopics() {
    }
}
