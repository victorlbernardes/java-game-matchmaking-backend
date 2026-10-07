package com.victor.matchmaking.engine.application;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;

/** Pairing rule for 1v1: same queue, both SEARCHING, distinct players, returned tickets are queued in score order. */
public final class PairingPolicy {

    private PairingPolicy() {
    }

    public static boolean canPair(MatchmakingTicket first, MatchmakingTicket second) {
        if (first == null || second == null) {
            return false;
        }
        return !first.playerId().equals(second.playerId())
                && first.gameMode() == second.gameMode()
                && first.region() == second.region();
    }
}
