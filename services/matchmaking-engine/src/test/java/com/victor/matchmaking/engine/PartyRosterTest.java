package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;

/**
 * W11 tests: the pairing roster resolver must complete partial parties with
 * solo fillers and must never pair an incomplete party.
 */
class PartyRosterTest {

    private static MatchmakingTicket ticket(String id, String playerId, int skill, String partyId) {
        return new MatchmakingTicket(id, playerId, GameMode.ONE_V_ONE, Region.NA, 80, skill,
                Instant.now(), TicketState.SEARCHING, 0, partyId);
    }

    @Test
    void pairsSolosWhenNoPartyIsQueued() {
        List<MatchmakingTicket> roster = MatchmakingEngineApplication.buildRoster(
                List.of(ticket("t1", "p1", 50, null), ticket("t2", "p2", 50, null)),
                GameMode.ONE_V_ONE, Instant.now());
        assertEquals(2, roster.size());
        assertEquals(List.of("t1", "t2"), roster.stream().map(MatchmakingTicket::id).toList());
    }

    @Test
    void completesAPartialPartyWithTheClosestSolo() {
        List<MatchmakingTicket> candidates = List.of(
                ticket("t1", "p1", 40, "party-1"),
                ticket("t2", "p2", 40, null));
        List<MatchmakingTicket> roster = MatchmakingEngineApplication.buildRoster(
                candidates, GameMode.ONE_V_ONE, Instant.now());
        assertEquals(2, roster.size());
        assertEquals("t2", roster.get(1).id());
    }

    @Test
    void fillsAFiveVsFivePartyWithTheClosestSolos() {
        // A 5v5 party of three players plus seven solo candidates fills the
        // missing slots and the whole group becomes the roster.
        List<MatchmakingTicket> candidates = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            candidates.add(new MatchmakingTicket(
                    "t" + i, "p" + i, GameMode.FIVE_V_FIVE, Region.NA, 80, 50,
                    Instant.now(), TicketState.SEARCHING, 0, "party-1"));
        }
        for (int i = 3; i < 10; i++) {
            candidates.add(new MatchmakingTicket(
                    "t" + i, "p" + i, GameMode.FIVE_V_FIVE, Region.NA, 80, 50,
                    Instant.now(), TicketState.SEARCHING, 0, null));
        }
        List<MatchmakingTicket> roster = MatchmakingEngineApplication.buildRoster(
                candidates, GameMode.FIVE_V_FIVE, Instant.now());
        assertEquals(10, roster.size());
        // The three party members plus seven solo fillers form the roster.
        assertEquals(3, roster.stream().filter(t -> t.partyId() != null).count());
        assertEquals(7, roster.stream().filter(t -> t.partyId() == null).count());
    }

    @Test
    void skipsAPPartyThatCannotBeFilled() {
        // A 5v5 party of three cannot be filled with only four solo candidates.
        List<MatchmakingTicket> candidates = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            candidates.add(new MatchmakingTicket(
                    "t" + i, "p" + i, GameMode.FIVE_V_FIVE, Region.NA, 80, 50,
                    Instant.now(), TicketState.SEARCHING, 0, "party-1"));
        }
        for (int i = 3; i < 7; i++) {
            candidates.add(new MatchmakingTicket(
                    "t" + i, "p" + i, GameMode.FIVE_V_FIVE, Region.NA, 80, 50,
                    Instant.now(), TicketState.SEARCHING, 0, null));
        }
        List<MatchmakingTicket> roster = MatchmakingEngineApplication.buildRoster(
                candidates, GameMode.FIVE_V_FIVE, Instant.now());
        assertNull(roster, "the party must stay queued when it cannot be filled");
    }

    @Test
    void widensTheRosterWhenTheQueueHeadWaitedLongEnough() {
        // The engine passes the head ticket's queuedAt as the widening reference:
        // a skill gap of 5 is rejected for a fresh queue head but accepted after
        // the head has been waiting for a minute (ADR-0011).
        MatchmakingTicket head = new MatchmakingTicket("t1", "p1", GameMode.ONE_V_ONE, Region.NA, 80, 50,
                Instant.now().minusSeconds(60), TicketState.SEARCHING, 0, null);
        MatchmakingTicket peer = new MatchmakingTicket("t2", "p2", GameMode.ONE_V_ONE, Region.NA, 80, 55,
                Instant.now(), TicketState.SEARCHING, 0, null);
        List<MatchmakingTicket> roster = MatchmakingEngineApplication.buildRoster(
                List.of(head, peer), GameMode.ONE_V_ONE, head.queuedAt());
        assertEquals(2, roster.size());
    }
}
