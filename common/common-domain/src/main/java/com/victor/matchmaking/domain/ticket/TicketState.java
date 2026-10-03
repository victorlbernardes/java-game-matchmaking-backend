package com.victor.matchmaking.domain.ticket;

/** Ticket lifecycle states. No boolean flags: state is always explicit. */
public enum TicketState {
    SEARCHING,
    CONFIRMING,
    CONFIRMED,
    CANCELLED,
    EXPIRED
}
