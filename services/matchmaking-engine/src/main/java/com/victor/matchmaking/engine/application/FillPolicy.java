package com.victor.matchmaking.engine.application;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;

/**
 * Fill policy (W11): completes a party that is still missing players.
 *
 * <p>When the candidates of a queue contain a partial party (all members of a
 * party, but not all of them have queued), the remaining slots of that party
 * are filled with the closest solo candidates. Parties are always prioritized
 * over solo pairing, so a party is never split across two matches.</p>
 */
public final class FillPolicy {

    private FillPolicy() {
    }

    /**
     * Chooses the fillers and returns the combined list ready to be paired.
     *
     * @param party the candidates of the party (may be smaller than required)
     * @param solos candidates that are not part of any party
     * @return the combined list to pair, or null when the party cannot be filled
     */
    public static List<MatchmakingTicket> fill(List<MatchmakingTicket> party,
            List<MatchmakingTicket> solos) {
        int required = party.get(0).gameMode().playersPerMatch();
        if (party.size() >= required) {
            return party;
        }
        int missing = required - party.size();
        if (solos.size() < missing) {
            return null;
        }
        List<MatchmakingTicket> fillers = chooseFillers(party, missing, solos);
        if (fillers.size() < missing) {
            return null;
        }
        List<MatchmakingTicket> combined = new ArrayList<>(party);
        combined.addAll(fillers);
        return combined;
    }

    /**
     * Picks the {@code missing} solo candidates whose skill deviates least from
     * the party's aggregated skill. Candidates that would break the anti-boost
     * rule are skipped.
     */
    private static List<MatchmakingTicket> chooseFillers(List<MatchmakingTicket> party, int missing,
            List<MatchmakingTicket> solos) {
        List<MatchmakingTicket> ordered = solos.stream()
                .filter(candidate -> candidate.partyId() == null)
                .sorted(Comparator
                        .comparingInt((MatchmakingTicket candidate) -> deviation(candidate, party))
                        .thenComparing(candidate -> candidate.queuedAt()))
                .toList();
        List<MatchmakingTicket> chosen = new ArrayList<>();
        for (int i = 0; i < missing && i < ordered.size(); i++) {
            MatchmakingTicket candidate = ordered.get(i);
            if (dispersionOf(party, chosen, candidate) <= Party.MAX_PARTY_DISPERSION) {
                chosen.add(candidate);
            }
        }
        return chosen;
    }

    /** Absolute skill distance between a candidate and the party's aggregated skill. */
    private static int deviation(MatchmakingTicket candidate, List<MatchmakingTicket> party) {
        return Math.abs(candidate.skill() - aggregatedSkill(party));
    }

    /** Average skill of the party members, used as the reference of every deviation. */
    private static int aggregatedSkill(List<MatchmakingTicket> party) {
        int sum = 0;
        for (MatchmakingTicket t : party) {
            sum += t.skill();
        }
        return sum / party.size();
    }

    /** Skill dispersion of the combined party after the given fillers joined. */
    private static int dispersionOf(List<MatchmakingTicket> party, List<MatchmakingTicket> chosen,
            MatchmakingTicket last) {
        List<MatchmakingTicket> combined = new ArrayList<>(party);
        combined.addAll(chosen);
        combined.add(last);
        int smallest = Integer.MAX_VALUE;
        int largest = Integer.MIN_VALUE;
        for (MatchmakingTicket t : combined) {
            smallest = Math.min(smallest, t.skill());
            largest = Math.max(largest, t.skill());
        }
        return largest - smallest;
    }
}
