package com.victor.matchmaking.api.application;

import java.util.ArrayList;
import java.util.List;

import com.victor.matchmaking.domain.player.Party;
import com.victor.matchmaking.domain.player.SkillProvider;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.redis.PartyStore;
import com.victor.matchmaking.redis.TicketRepository;

import io.vertx.core.Future;

/** Creates matchmaking tickets. Idempotent on Idempotency-Key. */
public final class CreateTicketUseCase {

    private static final int INITIAL_VERSION = 0;

    private final TicketRepository repository;
    private final SkillProvider skillProvider;
    private final PartyStore partyStore;

    public CreateTicketUseCase(TicketRepository repository, SkillProvider skillProvider,
            PartyStore partyStore) {
        this.repository = repository;
        this.skillProvider = skillProvider;
        this.partyStore = partyStore;
    }

    public record Result(MatchmakingTicket ticket, boolean created) {
    }

    public Future<Result> execute(String playerId, CreateTicketCommand command, String idempotencyKey) {
        int requesterSkill = skillProvider.skillForPlayer(playerId);
        if (command.partyId() == null) {
            return enqueueSolo(playerId, command, requesterSkill, idempotencyKey);
        }
        return enqueueParty(command, requesterSkill, idempotencyKey);
    }

    /**
     * Solo enqueue: a single player queues a search on its own.
     */
    private Future<Result> enqueueSolo(String playerId, CreateTicketCommand command, int requesterSkill,
            String idempotencyKey) {
        MatchmakingTicket ticket = buildTicket(playerId, command.gameMode(), command.region(),
                command.maxLatencyMs(), requesterSkill, null);
        return repository.createIdempotent(idempotencyKey, ticket)
                .flatMap(ticketId -> {
                    boolean created = ticketId.equals(ticket.id());
                    return repository.findById(ticketId)
                            .map(found -> new Result(found, created));
                });
    }

    /**
     * Party enqueue: every member of the party queues a ticket and the new ticket
     * ids are linked to the party, so the party unit never splits in the queue.
     */
    private Future<Result> enqueueParty(CreateTicketCommand command, int requesterSkill,
            String idempotencyKey) {
        return partyStore.findById(command.partyId())
                .flatMap(party -> {
                    if (party == null) {
                        return Future.failedFuture(new PartyNotFoundException("party not found"));
                    }
                    List<MatchmakingTicket> tickets = command.members().stream()
                            .map(memberId -> buildTicket(memberId, command.gameMode(), command.region(),
                                    command.maxLatencyMs(), skillProvider.skillForPlayer(memberId),
                                    command.partyId()))
                            .toList();
                    MatchmakingTicket firstCreated = tickets.get(0);
                    return repository.createPartyTickets(tickets)
                            .flatMap(ticketIds -> linkToParty(party, firstCreated, ticketIds)
                                    .map(created -> new Result(firstCreated, true)));
                });
    }

    private Future<MatchmakingTicket> linkToParty(Party party, MatchmakingTicket firstCreated,
            List<String> ticketIds) {
        List<String> memberTicketIds = new ArrayList<>(party.memberTicketIds());
        for (String ticketId : ticketIds) {
            if (!memberTicketIds.contains(ticketId)) {
                memberTicketIds.add(ticketId);
            }
        }
        Party updated = new Party(
                party.id(), party.leaderId(), party.members(), party.createdAt(), true,
                List.copyOf(memberTicketIds));
        return partyStore.save(updated)
                .map(v -> firstCreated);
    }

    private MatchmakingTicket buildTicket(String playerId, GameMode mode, Region region, int maxLatencyMs,
            int skill, String partyId) {
        return new MatchmakingTicket(
                repository.newTicketId(),
                playerId,
                mode,
                region,
                maxLatencyMs,
                skill,
                repository.now(),
                TicketState.SEARCHING,
                INITIAL_VERSION,
                partyId);
    }

    /** The party referenced by a party enqueue does not exist. */
    public static final class PartyNotFoundException extends RuntimeException {
        PartyNotFoundException(String message) {
            super(message);
        }
    }
}
