package com.victor.matchmaking.api.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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
import io.vertx.core.Promise;

/** W11 test: cancelling a member ticket cancels the whole party unit. */
class CancelTicketUseCaseTest {

    private final TicketRepository ticketRepository = new InMemoryTicketRepository();
    private final PartyStore partyStore = new InMemoryPartyStore();
    private final CancelTicketUseCase useCase = new CancelTicketUseCase(ticketRepository, partyStore);

    @Test
    void cancellingAMemberCancelsEveryMemberTicketOfTheParty() {
        Party party = new Party("party-1", "leader-1", List.of("leader-1", "member-1", "member-2"),
                Instant.now(), true, List.of("ticket-1", "ticket-2", "ticket-3"));
        await(partyStore.save(party));
        await(ticketRepository.save(null, searchingTicket("ticket-1", "leader-1", "party-1")));
        await(ticketRepository.save(null, searchingTicket("ticket-2", "member-1", "party-1")));
        await(ticketRepository.save(null, searchingTicket("ticket-3", "member-2", "party-1")));

        await(useCase.execute("ticket-2", "member-1"));

        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-1")).status());
        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-2")).status());
        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-3")).status());
    }

    /**
     * The reply is the promise: when {@code execute}'s future completes, every member ticket of the
     * party is CANCELLED already.
     *
     * <p>This is the test the W23 end-to-end run asked for. The scenario cancelled one member and
     * read the other two immediately — the harness had become fast enough (one JVM, one socket) to
     * arrive inside a window that Python's {@code docker exec} per read had always hidden. The
     * product replied 200 while the party's other writes were still in flight, so "cancelling one
     * member cancels the whole party" was true a moment later, not when the client was told so.
     *
     * <p>The repository here completes each write only when the test drains it, which is what makes
     * the difference visible: with fire-and-forget the future completes after one drain and two
     * tickets are still SEARCHING.
     */
    @Test
    void theReplyWaitsForTheWholePartyToBeCancelled() {
        DeferredTicketRepository deferred = new DeferredTicketRepository();
        CancelTicketUseCase awaitingUseCase = new CancelTicketUseCase(deferred, partyStore);
        Party party = new Party("party-3", "leader-3", List.of("leader-3", "member-3", "member-4"),
                Instant.now(), true, List.of("ticket-5", "ticket-6", "ticket-7"));
        await(partyStore.save(party));
        deferred.seed(searchingTicket("ticket-5", "leader-3", "party-3"));
        deferred.seed(searchingTicket("ticket-6", "member-3", "party-3"));
        deferred.seed(searchingTicket("ticket-7", "member-4", "party-3"));

        Future<MatchmakingTicket> cancelled = awaitingUseCase.execute("ticket-6", "member-3");
        // Pump the writes the use case is waiting on: the future can only complete after all of
        // them, which is exactly the property under test.
        int guard = 0;
        while (!cancelled.isComplete() && guard++ < 20) {
            assertTrue(deferred.drainOne(), "the use case is waiting on a write that never came");
        }
        assertTrue(cancelled.isComplete(), "the use case never completed");

        assertEquals(TicketState.CANCELLED, await(deferred.findById("ticket-5")).status());
        assertEquals(TicketState.CANCELLED, await(deferred.findById("ticket-6")).status());
        assertEquals(TicketState.CANCELLED, await(deferred.findById("ticket-7")).status());
    }

    @Test
    void cancellingASoloTicketKeepsThePartyIntact() {
        Party party = new Party("party-2", "leader-2", List.of("leader-2"), Instant.now(), true, List.of("ticket-4"));
        await(partyStore.save(party));
        await(ticketRepository.save(null, searchingTicket("ticket-4", "leader-2", "party-2")));

        await(useCase.execute("ticket-4", "leader-2"));

        assertEquals(TicketState.CANCELLED, await(ticketRepository.findById("ticket-4")).status());
        assertTrue(await(partyStore.findById("party-2")).active());
    }

    private static MatchmakingTicket searchingTicket(String ticketId, String playerId, String partyId) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                Instant.now(), TicketState.SEARCHING, 0, partyId);
    }

    private static <T> T await(Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }

    /**
     * A repository whose {@code save} completes only when the test drains it: the window between
     * "the client was answered" and "the party was cancelled" becomes a thing the test can stand in.
     */
    private static final class DeferredTicketRepository implements TicketRepository {

        private final InMemoryTicketRepository delegate = new InMemoryTicketRepository();
        private final Deque<Runnable> pending = new ArrayDeque<>();

        @Override
        public Future<Void> save(String idempotencyKey, MatchmakingTicket ticket) {
            Promise<Void> completion = Promise.promise();
            pending.add(() -> {
                delegate.save(idempotencyKey, ticket);
                completion.complete();
            });
            return completion.future();
        }

        /** Writes a ticket straight through: the setup is not what is being deferred. */
        void seed(MatchmakingTicket ticket) {
            delegate.save(null, ticket);
        }

        /** Runs the oldest queued write; {@code false} when there was none. */
        boolean drainOne() {
            Runnable next = pending.poll();
            if (next == null) {
                return false;
            }
            next.run();
            return true;
        }

        @Override
        public Future<MatchmakingTicket> findById(String ticketId) {
            return delegate.findById(ticketId);
        }

        @Override
        public Future<MatchmakingTicket> findByIdempotencyKey(String key) {
            return delegate.findByIdempotencyKey(key);
        }

        @Override
        public Future<String> createIdempotent(String idempotencyKey, MatchmakingTicket ticket) {
            return delegate.createIdempotent(idempotencyKey, ticket);
        }

        @Override
        public Future<List<String>> createPartyTickets(List<MatchmakingTicket> tickets) {
            return delegate.createPartyTickets(tickets);
        }

        @Override
        public Future<Long> queueDepth(String gameModeLabel, String regionName) {
            return delegate.queueDepth(gameModeLabel, regionName);
        }

        @Override
        public Future<RequeueOutcome> requeue(MatchmakingTicket ticket) {
            return delegate.requeue(ticket);
        }

        @Override
        public String newTicketId() {
            return delegate.newTicketId();
        }

        @Override
        public Instant now() {
            return delegate.now();
        }
    }
}
