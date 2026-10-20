package com.victor.matchmaking.domain.ticket;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The regions one deployment serves (W21, ADR 0039).
 *
 * <p>Before this, "region" was a label: one Redis held the queues of every region and one engine
 * paired all of them, so a single instance was the failure domain of SA, NA and EU at once. A
 * region becomes a deployment decision only when a deployment can say which regions are its own -
 * and when the answer for a region it does not serve is a refusal, not a ticket nobody will pair.
 *
 * <p>The default is <em>all</em> regions: a deployment that does not say anything keeps the single
 * stack of W1-W20 byte for byte, and turning a region into a boundary is a deployment decision
 * ({@value #ENV}), not an accidental one.
 */
public final class ServedRegions {

    /** The deployment's regions, comma separated ({@code NA,EU}). Unset means every region. */
    public static final String ENV = "SERVED_REGIONS";

    private static final ServedRegions ALL = new ServedRegions(EnumSet.allOf(Region.class));

    private final Set<Region> regions;

    private ServedRegions(Set<Region> regions) {
        this.regions = Set.copyOf(regions);
    }

    public static ServedRegions all() {
        return ALL;
    }

    /**
     * Parses the deployment's list. {@code null} and blank mean every region; a typo fails the
     * boot instead of silently serving nothing, because a deployment that serves no region answers
     * every request with a refusal whose cause is a missing environment variable.
     */
    public static ServedRegions parse(String csv) {
        if (csv == null || csv.isBlank()) {
            return ALL;
        }
        EnumSet<Region> parsed = EnumSet.noneOf(Region.class);
        for (String raw : csv.split(",")) {
            String name = raw.trim().toUpperCase(Locale.ENGLISH);
            if (name.isEmpty()) {
                continue;
            }
            try {
                parsed.add(Region.valueOf(name));
            } catch (IllegalArgumentException unknown) {
                throw new IllegalArgumentException(
                        "unknown region '" + raw.trim() + "' (known: " + List.of(Region.values()) + ")");
            }
        }
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException(ENV + " is set but names no region");
        }
        return new ServedRegions(parsed);
    }

    public static ServedRegions of(Region... regions) {
        if (regions == null || regions.length == 0) {
            throw new IllegalArgumentException("at least one region is required");
        }
        EnumSet<Region> set = EnumSet.noneOf(Region.class);
        for (Region region : regions) {
            if (region == null) {
                throw new IllegalArgumentException("region must not be null");
            }
            set.add(region);
        }
        return new ServedRegions(set);
    }

    public boolean contains(Region region) {
        return regions.contains(region);
    }

    /** Enum order, so a log line, a response body and a test assertion agree. */
    public List<Region> list() {
        List<Region> ordered = new ArrayList<>(regions.size());
        for (Region region : Region.values()) {
            if (regions.contains(region)) {
                ordered.add(region);
            }
        }
        return List.copyOf(ordered);
    }

    public boolean servesEveryRegion() {
        return regions.size() == Region.values().length;
    }

    @Override
    public String toString() {
        return list().toString();
    }
}
