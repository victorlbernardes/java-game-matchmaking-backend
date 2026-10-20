package com.victor.matchmaking.api.application;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.ServedRegions;

/**
 * Where a player may be placed, and what that placement costs (W21, ADR 0039).
 *
 * <p>Two server-side numbers decide it, and the placement uses the larger:
 *
 * <ul>
 *   <li><b>declared</b> - the operator's distance matrix ({@code REGION_RTT_MS}), the best case
 *       those two regions can be apart. It is what stops a client from removing the constraint by
 *       not probing: the floor does not need the client's cooperation.</li>
 *   <li><b>measured</b> - the player's own probe RTT to that region, timed by the server. It is
 *       what stops a region "on paper" from being offered to a player who cannot reach it.</li>
 * </ul>
 *
 * <p>Neither can be lowered by the caller, which is the whole point: {@code maxLatencyMs} is a
 * ceiling the player declares, and the numbers it is compared against are the server's. When
 * neither is known - no matrix configured, no probe made - there is no constraint, and the
 * deployment behaves exactly as it did before this sprint.
 */
public final class RegionPlacement {

    /** The declared matrix: {@code EU-EU=20,EU-NA=120,NA-SA=140} (symmetric, one entry per pair). */
    public static final String ENV_REGION_RTT_MS = "REGION_RTT_MS";

    /** Why a ticket was refused. One enum rather than a string, so a typo cannot invent a reason. */
    public enum Reason {
        REGION_UNSUPPORTED,
        LATENCY_BUDGET
    }

    /**
     * A refusal, with the numbers that produced it. The route turns this into the response body;
     * keeping them typed means the body cannot disagree with the decision.
     */
    public record Denial(Reason reason, Region target, Integer rttMs, Integer declaredRttMs,
            Integer measuredRttMs, int maxLatencyMs) {
    }

    private final ServedRegions served;
    private final Map<String, Integer> declaredRttMs;

    private RegionPlacement(ServedRegions served, Map<String, Integer> declaredRttMs) {
        this.served = served;
        this.declaredRttMs = Map.copyOf(declaredRttMs);
    }

    /** A deployment with no distance model: every region it serves, no declared floor. */
    public static RegionPlacement all() {
        return new RegionPlacement(ServedRegions.all(), Map.of());
    }

    /**
     * Parses the deployment's world. {@code matrix} blank means "no declared distances"; a
     * malformed entry fails the boot, because a distance that silently disappears is a placement
     * rule that silently disappears with it.
     */
    public static RegionPlacement parse(ServedRegions served, String matrix) {
        Map<String, Integer> declared = new HashMap<>();
        if (matrix != null && !matrix.isBlank()) {
            for (String entry : matrix.split(",")) {
                String trimmed = entry.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                int equals = trimmed.indexOf('=');
                if (equals <= 0 || equals == trimmed.length() - 1) {
                    throw new IllegalArgumentException(
                            "REGION_RTT_MS entry must be REGION-REGION=ms, got '" + trimmed + "'");
                }
                String pair = trimmed.substring(0, equals).trim();
                int rttMs;
                try {
                    rttMs = Integer.parseInt(trimmed.substring(equals + 1).trim());
                } catch (NumberFormatException notANumber) {
                    throw new IllegalArgumentException(
                            "REGION_RTT_MS entry has a non-numeric rtt: '" + trimmed + "'");
                }
                if (rttMs <= 0) {
                    throw new IllegalArgumentException("REGION_RTT_MS entry must be positive: '" + trimmed + "'");
                }
                String key = pairKey(parsePair(pair));
                Integer previous = declared.put(key, rttMs);
                if (previous != null && previous != rttMs) {
                    throw new IllegalArgumentException("REGION_RTT_MS declares " + pair + " twice with "
                            + previous + " and " + rttMs);
                }
            }
        }
        return new RegionPlacement(served, declared);
    }

    public ServedRegions served() {
        return served;
    }

    public boolean hasDeclaredRtts() {
        return !declaredRttMs.isEmpty();
    }

    /** The declared distance between two regions, or null when the operator did not declare it. */
    public Integer declaredRttMs(Region from, Region to) {
        if (from == null || to == null) {
            return null;
        }
        return declaredRttMs.get(pairKey(new Region[] { from, to }));
    }

    /**
     * The number the placement is decided on: the larger of the declared floor and the player's own
     * measurement. Null when neither exists.
     */
    public Integer effectiveRttMs(Region target, Region homeRegion, Integer measuredRttMs) {
        Integer declared = declaredRttMs(homeRegion, target);
        if (declared == null) {
            return measuredRttMs;
        }
        if (measuredRttMs == null) {
            return declared;
        }
        return Math.max(declared, measuredRttMs);
    }

    /**
     * The admission rule. Empty means "admitted"; a denial names the region that cannot take the
     * player and the numbers behind it, so the client can pick another region instead of guessing.
     */
    public Optional<Denial> check(Region target, int maxLatencyMs, Region homeRegion, Integer measuredRttMs) {
        if (target == null || !served.contains(target)) {
            return Optional.of(new Denial(Reason.REGION_UNSUPPORTED, target, null, null, null, maxLatencyMs));
        }
        Integer declared = declaredRttMs(homeRegion, target);
        Integer rttMs = effectiveRttMs(target, homeRegion, measuredRttMs);
        if (rttMs != null && rttMs > maxLatencyMs) {
            return Optional.of(new Denial(Reason.LATENCY_BUDGET, target, rttMs, declared, measuredRttMs,
                    maxLatencyMs));
        }
        return Optional.empty();
    }

    private static Region[] parsePair(String pair) {
        int dash = pair.indexOf('-');
        if (dash <= 0 || dash == pair.length() - 1) {
            throw new IllegalArgumentException("REGION_RTT_MS pair must be REGION-REGION, got '" + pair + "'");
        }
        Region left = parseRegion(pair.substring(0, dash).trim());
        Region right = parseRegion(pair.substring(dash + 1).trim());
        return new Region[] { left, right };
    }

    private static Region parseRegion(String raw) {
        try {
            return Region.valueOf(raw.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException unknown) {
            throw new IllegalArgumentException("REGION_RTT_MS names an unknown region: '" + raw + "' (known: "
                    + List.of(Region.values()) + ")");
        }
    }

    /** A pair is one distance: {@code EU-NA} and {@code NA-EU} are the same declaration. */
    private static String pairKey(Region[] pair) {
        String a = pair[0].name();
        String b = pair[1].name();
        return a.compareTo(b) <= 0 ? a + "-" + b : b + "-" + a;
    }
}
