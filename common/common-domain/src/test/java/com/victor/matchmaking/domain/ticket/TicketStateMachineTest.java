package com.victor.matchmaking.domain.ticket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TicketStateMachineTest {

    @Test
    void allowsSearchingToConfirming() {
        assertEquals(TicketState.CONFIRMING, TicketStateMachine.transition(TicketState.SEARCHING, TicketState.CONFIRMING));
    }

    @Test
    void allowsConfirmingBackToSearching() {
        assertEquals(TicketState.SEARCHING, TicketStateMachine.transition(TicketState.CONFIRMING, TicketState.SEARCHING));
    }

    @Test
    void confirmedIsTerminal() {
        assertThrows(InvalidStateTransitionException.class,
                () -> TicketStateMachine.transition(TicketState.CONFIRMED, TicketState.SEARCHING));
    }

    @Test
    void searchingCannotGoDirectlyToConfirmed() {
        assertThrows(InvalidStateTransitionException.class,
                () -> TicketStateMachine.transition(TicketState.SEARCHING, TicketState.CONFIRMED));
    }
}
