package com.victor.matchmaking.orchestrator.application;

import com.victor.matchmaking.orchestrator.domain.AllocationRequest;
import com.victor.matchmaking.orchestrator.domain.ProcessedEvent;
import com.victor.matchmaking.orchestrator.domain.ProcessingOutcome;

import io.vertx.core.Future;

/**
 * The atomic unit of work of the idempotent consumer: record the event as processed
 * and apply its effect, or neither.
 *
 * <p>Both writes share one transaction on purpose. If the effect were committed and
 * the event not recorded, a redelivery would apply it twice; if the event were
 * recorded and the effect not committed, a redelivery would apply it never. Keeping
 * them in one transaction is what lets the consumer treat "already recorded" as
 * "already applied".
 */
public interface ConfirmedMatchInbox {

    /**
     * Applies the effect of {@code event} exactly once for its consumer group.
     *
     * @return {@link ProcessingOutcome#APPLIED} on the first delivery of the event,
     *         {@link ProcessingOutcome#DUPLICATE} when this group already applied it
     */
    Future<ProcessingOutcome> applyOnce(AllocationRequest allocation, ProcessedEvent event);
}
