package com.victor.matchmaking.api.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.api.infrastructure.DeterministicSkillProvider;
import com.victor.matchmaking.api.infrastructure.InMemoryPartyStore;
import com.victor.matchmaking.api.infrastructure.InMemoryTicketRepository;
import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.redis.PartyStore;

import io.vertx.core.Future;

/** W11 test: a party enqueue creates one party-tagged ticket per member. */
class CreateTicketUseCaseTest {

    private final InMemoryTicketRepository repository = new InMemoryTicketRepository();
    private final PartyStore partyStore = new InMemoryPartyStore();
    private final CreateTicketUseCase useCase = new CreateTicketUseCase(
            repository, new DeterministicSkillProvider(), partyStore);

    @Test
    void partyEnqueueTagsEveryTicketWithThePartyId() {
        await(partyStore.save(new Party("party-1", "leader-1", List.of("leader-1", "member-1"),
                Instant.now(), true, List.of())));
        CreateTicketCommand command = new CreateTicketCommand(GameMode.FIVE_V_FIVE, Region.NA, 80,
                "party-1", List.of("leader-1", "member-1"));

        CreateTicketUseCase.Result result = await(useCase.execute("leader-1", command, "idem-1"));

        assertEquals("party-1", result.ticket().partyId());
        List<String> ticketIds = await(partyStore.findById("party-1")).memberTicketIds();
        assertEquals(2, ticketIds.size());
        for (String ticketId : ticketIds) {
            MatchmakingTicket ticket = await(repository.findById(ticketId));
            assertEquals("party-1", ticket.partyId(),
                    "every party ticket must carry the partyId so the engine keeps the party together");
            assertEquals(TicketState.SEARCHING, ticket.status());
        }
    }

    @Test
    void partyEnqueueFailsWhenThePartyDoesNotExist() {
        CreateTicketCommand command = new CreateTicketCommand(GameMode.FIVE_V_FIVE, Region.NA, 80,
                "party-unknown", List.of("leader-1"));

        Future<CreateTicketUseCase.Result> future = useCase.execute("leader-1", command, "idem-2");

        assertTrue(future.failed());
        assertEquals(CreateTicketUseCase.PartyNotFoundException.class, future.cause().getClass());
    }

    private static <T> T await(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }
}
