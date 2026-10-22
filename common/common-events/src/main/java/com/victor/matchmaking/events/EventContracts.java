package com.victor.matchmaking.events;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * The topic → contract mapping used to rebuild an event from stored bytes.
 *
 * <p>The outbox (ADR 0018) stores the bare Protobuf message of an event, without the
 * Confluent wire framing: the schema id is resolved at publish time, so the Schema
 * Registry is not part of the transaction that persists the fact. That means something
 * has to know which message a payload is, and the answer is the topic: one topic per
 * event type (ADR 0016) is exactly the routing table this needs.
 *
 * <p>A topic with no contract is refused loudly instead of being published as bytes: a
 * row whose topic the running code does not know is a bug or a hand-written insert, and
 * silently publishing it would move the failure into every consumer.
 */
public final class EventContracts {

    private EventContracts() {
    }

    /** Rebuilds the event a stored payload carries. */
    public static Message parse(String topic, byte[] payload) {
        if (EventTopics.MATCH_CONFIRMED.equals(topic)) {
            return parseMatchConfirmed(payload);
        }
        if (EventTopics.MATCH_FINISHED.equals(topic)) {
            return parseMatchFinished(payload);
        }
        if (EventTopics.SKILL_UPDATED.equals(topic)) {
            return parseSkillUpdated(payload);
        }
        throw new IllegalArgumentException("no contract is registered for topic " + topic);
    }

    /** Whether this build knows the contract of a topic (used to diagnose a parked row). */
    public static boolean isKnown(String topic) {
        return EventTopics.MATCH_CONFIRMED.equals(topic)
                || EventTopics.MATCH_FINISHED.equals(topic)
                || EventTopics.SKILL_UPDATED.equals(topic);
    }

    private static Message parseMatchConfirmed(byte[] payload) {
        try {
            return MatchConfirmed.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("payload does not decode as " + EventTopics.MATCH_CONFIRMED, e);
        }
    }

    private static Message parseMatchFinished(byte[] payload) {
        try {
            return MatchFinished.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("payload does not decode as " + EventTopics.MATCH_FINISHED, e);
        }
    }

    private static Message parseSkillUpdated(byte[] payload) {
        try {
            return SkillUpdated.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("payload does not decode as " + EventTopics.SKILL_UPDATED, e);
        }
    }
}
