package com.victor.matchmaking.domain.ticket;

import java.util.Map;
import java.util.Set;

/**
 * Encodes the valid ticket state transitions in one place.
 * Terminal states (CONFIRMED, CANCELLED, EXPIRED) accept no further transitions.
 */
public final class TicketStateMachine {

    private static final Map<TicketState, Set<TicketState>> TRANSITIONS = Map.of(
            TicketState.SEARCHING, Set.of(TicketState.CONFIRMING, TicketState.CANCELLED, TicketState.EXPIRED),
            TicketState.CONFIRMING, Set.of(TicketState.CONFIRMED, TicketState.SEARCHING, TicketState.CANCELLED),
            TicketState.CONFIRMED, Set.of(),
            TicketState.CANCELLED, Set.of(),
            TicketState.EXPIRED, Set.of());

    private TicketStateMachine() {
    }

    public static boolean canTransition(TicketState from, TicketState to) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("from and to are required");
        }
        return TRANSITIONS.get(from).contains(to);
    }

    public static TicketState transition(TicketState from, TicketState to) {
        if (!canTransition(from, to)) {
            throw new InvalidStateTransitionException(from, to);
        }
        return to;
    }
}
