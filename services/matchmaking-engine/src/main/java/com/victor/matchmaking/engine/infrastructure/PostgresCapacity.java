package com.victor.matchmaking.engine.infrastructure;

import java.util.Map;
import java.util.OptionalInt;

import com.victor.matchmaking.db.GameServerRepository;
import com.victor.matchmaking.db.GameServerRepository.RegionCapacity;
import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.ServedRegions;
import com.victor.matchmaking.engine.application.Capacity;

import io.vertx.core.Future;

/**
 * The pool's capacity as the engine sees it: one read a second, held in memory, read by the
 * pairing loop on every tick (W22, ADR 0031).
 *
 * <p><b>Why a poll and not a call per tick:</b> the pairing asks once per (mode, region) every
 * {@code PAIRING_INTERVAL_MS}, which with three regions and two modes is the wrong shape for a
 * query each. One snapshot answers all of them, and a second of staleness has a bounded
 * consequence — the engine may form one match into a slot the allocator just took, and the match
 * then sits in {@code PENDING_ALLOCATION} until W23's timeout. The alternative (reserve capacity
 * when forming) would leak a slot every time a match dissolves before confirmation, which is a
 * worse failure by far.
 *
 * <p><b>Why a failed poll keeps the last value:</b> "the database blinked" is not "there are no
 * servers". Emptying the answer would stop all matchmaking for a second, so the last known
 * snapshot stands and the failure is logged; the allocator remains the guard that cannot be
 * wrong, because it is the only writer of {@code allocated}.
 */
public final class PostgresCapacity implements Capacity {

    private final GameServerRepository servers;
    private final EngineKpis kpis;
    private final ServedRegions served;

    /** Replaced wholesale on every poll: a reader never sees half of two snapshots. */
    private volatile Map<Region, RegionCapacity> regions = Map.of();

    public PostgresCapacity(GameServerRepository servers, EngineKpis kpis, ServedRegions served) {
        this.servers = servers;
        this.kpis = kpis;
        this.served = served;
    }

    @Override
    public OptionalInt free(GameMode gameMode, Region region) {
        RegionCapacity capacity = regions.get(region);
        if (capacity == null || !capacity.authoritative()) {
            // No pool in this region: unknown, not zero. A deployment that never registered a
            // game server keeps the behaviour it had before W22 (see Capacity).
            return OptionalInt.empty();
        }
        // A pool exists, so the number is the truth - including zero, which is exactly the
        // situation the sprint is about: a region whose matches would have nowhere to run.
        return OptionalInt.of(capacity.free(gameMode));
    }

    /** Refreshes the snapshot and publishes what it says about the regions this engine serves. */
    public Future<Void> pollOnce() {
        return servers.snapshot().map(snapshot -> {
            regions = Map.copyOf(snapshot);
            for (GameMode gameMode : GameMode.values()) {
                for (Region region : served.list()) {
                    RegionCapacity capacity = snapshot.get(region);
                    if (capacity != null && capacity.authoritative()) {
                        kpis.capacityFree(gameMode, region, capacity.free(gameMode));
                    }
                }
            }
            return (Void) null;
        });
    }
}
