package com.victor.matchmaking.api.application;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;

/** Creates matchmaking tickets. Idempotent on Idempotency-Key. */
public final class CreateTicketUseCase {

    private static final int DEV_SKILL = 50;
    private static final int INITIAL_VERSION = 0;

    private final TicketRepository repository;

    public CreateTicketUseCase(TicketRepository repository) {
        this.repository = repository;
    }

    public record Result(MatchmakingTicket ticket, boolean created) {
    }

    public Result execute(String playerId, CreateTicketCommand command, String idempotencyKey) {
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            MatchmakingTicket existing = repository.findByIdempotencyKey(idempotencyKey);
            if (existing != null) {
                return new Result(existing, false);
            }
        }
        MatchmakingTicket ticket = new MatchmakingTicket(
                repository.newTicketId(),
                playerId,
                command.gameMode(),
                command.region(),
                command.maxLatencyMs(),
                DEV_SKILL,
                repository.now(),
                TicketState.SEARCHING,
                INITIAL_VERSION);
        repository.save(idempotencyKey, ticket);
        return new Result(ticket, true);
    }
}
