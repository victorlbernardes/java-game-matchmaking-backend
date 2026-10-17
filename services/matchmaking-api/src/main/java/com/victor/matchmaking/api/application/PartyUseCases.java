package com.victor.matchmaking.api.application;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.domain.player.SkillProvider;
import com.victor.matchmaking.redis.PartyStore;
import com.victor.matchmaking.redis.TicketRepository;

import io.vertx.core.Future;

/** Use cases that manage parties, wired once per router. */
public final class PartyUseCases {

    private final CreatePartyUseCase createParty;
    private final AddMemberUseCase addMember;
    private final GetPartyUseCase getParty;
    private final RemoveMemberUseCase removeMember;
    private final DissolvePartyUseCase dissolveParty;
    private final PartyStore partyStore;

    public PartyUseCases(PartyStore partyStore, TicketRepository ticketRepository, SkillProvider skillProvider) {
        this.partyStore = partyStore;
        createParty = new CreatePartyUseCase(partyStore);
        addMember = new AddMemberUseCase(partyStore);
        getParty = new GetPartyUseCase(partyStore, skillProvider);
        removeMember = new RemoveMemberUseCase(partyStore, ticketRepository);
        dissolveParty = new DissolvePartyUseCase(partyStore, ticketRepository);
    }

    public CreatePartyUseCase createPartyUseCase() {
        return createParty;
    }

    public AddMemberUseCase addMemberUseCase() {
        return addMember;
    }

    public GetPartyUseCase getPartyUseCase() {
        return getParty;
    }

    public RemoveMemberUseCase removeMemberUseCase() {
        return removeMember;
    }

    public DissolvePartyUseCase dissolveUseCase() {
        return dissolveParty;
    }
}
