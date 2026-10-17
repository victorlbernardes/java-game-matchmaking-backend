package com.victor.matchmaking.api.application;

import java.util.List;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.domain.player.SkillProvider;
import com.victor.matchmaking.redis.PartyStore;

import io.vertx.core.Future;

/** Fetches a party state. Only confirmed members may consult it. */
public final class GetPartyUseCase {

    private final PartyStore partyStore;
    private final SkillProvider skillProvider;

    public GetPartyUseCase(PartyStore partyStore, SkillProvider skillProvider) {
        this.partyStore = partyStore;
        this.skillProvider = skillProvider;
    }

    /** Party state enriched with the aggregated skill and the internal dispersion. */
    public record PartyView(Party party, int aggregatedSkill, int dispersion) {
    }

    public Future<PartyView> execute(String partyId, String playerId) {
        return partyStore.findById(partyId).flatMap(party -> {
            if (party == null) {
                return Future.failedFuture(new PartyNotFoundException("party not found"));
            }
            if (!party.members().contains(playerId)) {
                return Future.failedFuture(new ForbiddenException("only party members may consult the party"));
            }
            List<Integer> memberSkills = party.members().stream()
                    .map(skillProvider::skillForPlayer)
                    .toList();
            int sum = memberSkills.stream().mapToInt(Integer::intValue).sum();
            int aggregatedSkill = sum / memberSkills.size();
            int minSkill = memberSkills.stream().mapToInt(Integer::intValue).min().orElse(0);
            int maxSkill = memberSkills.stream().mapToInt(Integer::intValue).max().orElse(0);
            int dispersion = maxSkill - minSkill;
            return Future.succeededFuture(new PartyView(party, aggregatedSkill, dispersion));
        });
    }

    public static final class PartyNotFoundException extends RuntimeException {
        PartyNotFoundException(String message) {
            super(message);
        }
    }

    public static final class ForbiddenException extends RuntimeException {
        ForbiddenException(String message) {
            super(message);
        }
    }
}
