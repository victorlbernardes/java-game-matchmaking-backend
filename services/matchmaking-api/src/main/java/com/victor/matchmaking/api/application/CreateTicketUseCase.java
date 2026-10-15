package com.victor.matchmaking.api.application;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.redis.TicketRepository;
import com.victor.matchmaking.domain.ticket.TicketState;

import io.vertx.core.Future;

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

    public Future<Result> execute(String playerId, CreateTicketCommand command, String idempotencyKey) {
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
        return repository.createIdempotent(idempotencyKey, ticket)
                .flatMap(ticketId -> {
                    boolean created = ticketId.equals(ticket.id());
                    if (created) {
                        return Future.succeededFuture(new Result(ticket, true));
                    }
                    return repository.findById(ticketId)
                            .map(found -> new Result(found, false));
                });
    }
}
