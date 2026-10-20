package com.victor.matchmaking.engine.application;

import java.util.OptionalInt;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;

/**
 * How many matches the pool can still host, as the pairing asks it (W22, ADR 0031).
 *
 * <p>The answer has three shapes, and the engine treats each differently on purpose:
 *
 * <ul>
 *   <li><b>a number</b> — the region has a registered pool and it is authoritative: 0 means stop
 *       forming matches, and any positive number is also the ceiling for this tick;</li>
 *   <li><b>empty</b> — the region has no pool at all, or the last poll failed. Unknown is not zero:
 *       a deployment with no game servers behaves as it did before W22 instead of refusing to match
 *       anybody, and a database that blinks must not empty the queues by pretending the answer is
 *       "no capacity".</li>
 * </ul>
 *
 * <p>The port exists so the pairing can be tested without Postgres, and so the decision is stated
 * in one place instead of in the loop.
 */
@FunctionalInterface
public interface Capacity {

    /** Slots available in this (mode, region); empty means unknown, which constrains nothing. */
    OptionalInt free(GameMode gameMode, Region region);

    /** The pre-W22 behaviour: no pool, no constraint. */
    Capacity UNLIMITED = (gameMode, region) -> OptionalInt.empty();
}
