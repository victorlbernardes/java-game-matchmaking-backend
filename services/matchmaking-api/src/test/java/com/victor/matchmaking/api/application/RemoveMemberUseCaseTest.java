package com.victor.matchmaking.api.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.api.infrastructure.InMemoryPartyStore;
import com.victor.matchmaking.api.infrastructure.InMemoryTicketRepository;
import com.victor.matchmaking.redis.PartyStore;
import com.victor.matchmaking.redis.TicketRepository;

import io.vertx.core.Future;

/** W11 test: removing a member cancels the whole party unit atomically. */
class RemoveMemberUseCaseTest {

    private final TicketRepository ticketRepository = new InMemoryTicketRepository();
    private final PartyStore partyStore = new InMemoryPartyStore();
    private final RemoveMemberUseCase useCase = new RemoveMemberUseCase(partyStore, ticketRepository);

    @Test
    void removingAMemberCancelsEveryMemberTicketOfTheParty() {
        Party party = new Party("party-1", "leader-1", List.of("leader-1", "member-1", "member-2"),
                Instant.now(), true, List.of("ticket-1", "ticket-2", "ticket-3"));
        await(partyStore.save(party));
        await(ticketRepository.save(null, searchingTicket("ticket-1", "leader-1", "party-1")));
        await(ticketRepository.save(null, searchingTicket("ticket-2", "member-1", "party-1")));
        await(ticketRepository.save(null, searchingTicket("ticket-3", "member-2", "party-1")));

        Party updated = await(useCase.execute("party-1", "leader-1", "member-1"));

        // The leaving member's ticket and every other member ticket leave the queue.
        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-1")).status());
        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-2")).status());
        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-3")).status());
        // The party keeps the remaining members and stays active.
        assertTrue(updated.active());
        assertEquals(List.of("leader-1", "member-2"), updated.members());
    }

    @Test
    void removingTheLeaderDissolvesThePartyAndCancelsTheMemberTickets() {
        Party party = new Party("party-2", "leader-2", List.of("leader-2", "member-9"),
                Instant.now(), true, List.of("ticket-5", "ticket-6"));
        await(partyStore.save(party));
        await(ticketRepository.save(null, searchingTicket("ticket-5", "leader-2", "party-2")));
        await(ticketRepository.save(null, searchingTicket("ticket-6", "member-9", "party-2")));

        Party dissolved = await(useCase.execute("party-2", "leader-2", "leader-2"));

        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-5")).status());
        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-6")).status());
        assertFalse(dissolved.active());
    }

    private static MatchmakingTicket searchingTicket(String ticketId, String playerId, String partyId) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                Instant.now(), TicketState.SEARCHING, 0, partyId);
    }

    private static <T> T await(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }
}
