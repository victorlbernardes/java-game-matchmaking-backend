package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.FillPolicy;

class FillPolicyTest {

    private static MatchmakingTicket ticket(String id, String playerId, int skill) {
        return new MatchmakingTicket(id, playerId, GameMode.ONE_V_ONE, Region.NA, 80, skill,
                Instant.now(), TicketState.SEARCHING, 0, null);
    }

    private static List<MatchmakingTicket> of(MatchmakingTicket... tickets) {
        return List.of(tickets);
    }

    @Test
    void returnsThePartyWhenItAlreadyHasAllPlayers() {
        List<MatchmakingTicket> party = of(ticket("t1", "p1", 50), ticket("t2", "p2", 50));
        List<MatchmakingTicket> solos = List.of(ticket("t3", "p3", 50));
        assertEquals(party, FillPolicy.fill(party, solos));
    }

    @Test
    void fillsMissingSlotsWithTheClosestSkills() {
        // The party has a single member so one more player is missing.
        List<MatchmakingTicket> party = List.of(ticket("t1", "p1", 40));
        List<MatchmakingTicket> solos = List.of(
                ticket("t3", "p3", 10),
                ticket("t4", "p4", 40),
                ticket("t5", "p5", 45));
        List<MatchmakingTicket> filled = FillPolicy.fill(party, solos);
        assertEquals(2, filled.size());
        assertTrue(filled.stream().anyMatch(t -> t.id().equals("t4")));
        assertTrue(filled.stream().noneMatch(t -> t.id().equals("t3")));
    }

    @Test
    void returnsNullWhenNotEnoughSoloCandidatesExist() {
        List<MatchmakingTicket> party = of(ticket("t1", "p1", 50));
        List<MatchmakingTicket> solos = List.of();
        assertNull(FillPolicy.fill(party, solos));
    }

    @Test
    void skipsCandidatesThatBreakTheAntiBoostRule() {
        // The closest candidate (skill 60) would push the dispersion beyond the
        // tolerated maximum, so it is skipped and the party cannot be filled.
        List<MatchmakingTicket> party = of(ticket("t1", "p1", 10));
        List<MatchmakingTicket> solos = List.of(ticket("t3", "p3", 60));
        assertNull(FillPolicy.fill(party, solos));
    }

    @Test
    void neverUsesCandidatesThatAlreadyBelongToAParty() {
        List<MatchmakingTicket> party = of(ticket("t1", "p1", 50));
        MatchmakingTicket memberOfParty = new MatchmakingTicket("t2", "p2", GameMode.ONE_V_ONE,
                Region.NA, 80, 50, Instant.now(), TicketState.SEARCHING, 0, "other-party");
        List<MatchmakingTicket> solos = List.of(memberOfParty, ticket("t3", "p3", 50));
        List<MatchmakingTicket> filled = FillPolicy.fill(party, solos);
        assertEquals(2, filled.size());
        assertTrue(filled.stream().noneMatch(t -> t.id().equals("t2")));
        assertTrue(filled.stream().anyMatch(t -> t.id().equals("t3")));
    }
}
