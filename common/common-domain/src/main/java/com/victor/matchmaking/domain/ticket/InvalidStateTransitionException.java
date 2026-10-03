package com.victor.matchmaking.domain.ticket;

/** Error thrown when a ticket state transition is not allowed. */
public class InvalidStateTransitionException extends RuntimeException {
    public InvalidStateTransitionException(TicketState from, TicketState to) {
        super("Invalid ticket state transition: " + from + " -> " + to);
    }
}
