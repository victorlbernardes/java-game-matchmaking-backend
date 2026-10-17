package com.victor.matchmaking.orchestrator.application;

import java.util.UUID;

import com.victor.matchmaking.orchestrator.domain.AllocationRequest;

/**
 * What one MATCH_CONFIRMED payload means to the orchestrator: the effect to apply and
 * the key that says whether it was already applied.
 */
public record DecodedEvent(AllocationRequest allocation, UUID eventId) {
}
