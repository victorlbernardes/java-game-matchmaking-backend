package com.victor.matchmaking.engine.application;

import com.google.protobuf.Message;

import io.vertx.core.Future;

/**
 * Publishes one already-built event to the durable log.
 *
 * <p>Deliberately not a match-specific port any more (W14): the outbox decides *what*
 * to publish and *when*, and this is only the transport. The engine hands it bytes the
 * outbox stored, so the publisher cannot change the event on the way out.
 */
public interface EventPublisher {

    /**
     * Publishes a record.
     *
     * @param topic        the topic to publish to
     * @param partitionKey the key that fixes the partition
     * @param event        the message, built from the stored payload
     * @return completes once the broker acknowledged the record
     */
    Future<Void> publish(String topic, String partitionKey, Message event);
}
