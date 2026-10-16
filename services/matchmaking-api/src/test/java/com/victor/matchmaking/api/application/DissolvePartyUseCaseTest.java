package com.victor.matchmaking.api.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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

/** W11 test: dissolving a party cancels the whole party unit. */
class DissolvePartyUseCaseTest {

    private final TicketRepository ticketRepository = new InMemoryTicketRepository();
    private final PartyStore partyStore = new InMemoryPartyStore();
    private final DissolvePartyUseCase useCase = new DissolvePartyUseCase(partyStore, ticketRepository);

    @Test
    void dissolvingCancelsEveryMemberTicketOfTheParty() {
        Party party = new Party("party-1", "leader-1", List.of("leader-1", "member-1", "member-2"),
                Instant.now(), true, List.of("ticket-1", "ticket-2", "ticket-3"));
        await(partyStore.save(party));
        await(ticketRepository.save(null, searchingTicket("ticket-1", "leader-1", "party-1")));
        await(ticketRepository.save(null, searchingTicket("ticket-2", "member-1", "party-1")));
        await(ticketRepository.save(null, searchingTicket("ticket-3", "member-2", "party-1")));

        Party dissolved = await(useCase.execute("party-1", "leader-1"));

        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-1")).status());
        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-2")).status());
        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-3")).status());
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
