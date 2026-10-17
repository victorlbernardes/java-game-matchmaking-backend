package com.victor.matchmaking.engine.application;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;

/**
 * Pairing rules (W9 + W10): the candidate list must contain exactly the number
 * of players required by the game mode, all SEARCHING, with distinct players, in
 * the same game mode and region, and every skill inside the widening half-range
 * of the queued player's skill. Party and fill rules are out of scope here — they
 * belong to W11.
 */
public final class PairingPolicy {

    private PairingPolicy() {
    }

    /**
     * N-player pairing with progressive skill widening (ADR-0011).
     *
     * @param candidates the candidate tickets
     * @param queuedAt   the time the queue head has been waiting, used to choose the
     *                    widening half-range
     * @return true if the candidates form a valid match
     */
    public static boolean canPair(List<MatchmakingTicket> candidates, Instant queuedAt) {
        if (candidates == null) {
            return false;
        }
        MatchmakingTicket queued = candidates.get(0);
        int required = queued.gameMode().playersPerMatch();
        if (candidates.size() != required) {
            return false;
        }
        int halfRange = WideningPolicy.halfRange(java.time.Duration.between(queuedAt, Instant.now()).getSeconds());
        Set<String> players = new HashSet<>();
        for (MatchmakingTicket candidate : candidates) {
            if (candidate == null
                    || candidate.status() != TicketState.SEARCHING
                    || candidate.gameMode() != queued.gameMode()
                    || candidate.region() != queued.region()
                    || Math.abs(candidate.skill() - queued.skill()) > halfRange
                    || !players.add(candidate.playerId())) {
                return false;
            }
        }
        return true;
    }
}
