package com.victor.matchmaking.api.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.ServedRegions;

/**
 * Property: a player is never placed in a region the server has measured (or declared) to be
 * further away than the player accepts - and never in a region this deployment does not serve
 * (W21, ADR 0039).
 *
 * <p>The load-bearing rule is {@code max(declared, measured)}. Each of the two alone has a hole:
 * the declared matrix is a paper promise that a player at 400 ms would still pass, and the measured
 * probe is skipped by any client that simply does not probe. Together, a client can neither lower
 * the number (the measurement is the server's) nor remove it (the declaration is the operator's).
 * The last test is the default: nothing known means nothing refused, which is what keeps every
 * deployment without a matrix behaving exactly as it did before this sprint.
 */
class RegionPlacementTest {

    private static final int BUDGET = 60;

    @Test
    void theDeclaredFloorRefusesABudgetBelowIt() {
        RegionPlacement placement = placement(Region.NA, "EU-EU=20,EU-NA=120");

        Optional<RegionPlacement.Denial> denial = placement.check(Region.NA, BUDGET, Region.EU, null);

        assertTrue(denial.isPresent(), "120 ms declared is above a 60 ms budget");
        assertEquals(RegionPlacement.Reason.LATENCY_BUDGET, denial.get().reason());
        assertEquals(120, denial.get().rttMs());
        assertEquals(120, denial.get().declaredRttMs());
        assertNull(denial.get().measuredRttMs());
    }

    @Test
    void aMeasurementWorseThanTheDeclarationWins() {
        RegionPlacement placement = placement(Region.NA, "EU-EU=20,EU-NA=120");

        // The player measured 180 to NA: the declared 120 was optimistic for this player.
        Optional<RegionPlacement.Denial> denial = placement.check(Region.NA, 150, Region.EU, 180);

        assertTrue(denial.isPresent());
        assertEquals(180, denial.get().rttMs(), "the placement uses the larger of the two numbers");
        assertEquals(120, denial.get().declaredRttMs());
        assertEquals(180, denial.get().measuredRttMs());
    }

    @Test
    void anOptimisticMeasurementNeverBeatsTheDeclaredFloor() {
        RegionPlacement placement = placement(Region.NA, "EU-EU=20,EU-NA=120");

        // A probe answered instantly on a path that is really 120 ms away must not buy admission.
        Optional<RegionPlacement.Denial> denial = placement.check(Region.NA, BUDGET, Region.EU, 5);

        assertTrue(denial.isPresent(), "the floor is not the client's to lower");
        assertEquals(120, denial.get().rttMs());
    }

    @Test
    void aBudgetThatFitsIsAdmitted() {
        RegionPlacement placement = RegionPlacement.parse(ServedRegions.of(Region.NA, Region.EU),
                "EU-EU=20,EU-NA=120");

        assertTrue(placement.check(Region.NA, 150, Region.EU, 130).isEmpty());
        assertTrue(placement.check(Region.EU, 25, Region.EU, null).isEmpty(),
                "a player is placed inside its own region at 20 ms");
    }

    @Test
    void aMeasuredDistanceAloneRefusesAnUnreachableRegion() {
        // No matrix at all: only the probe knows, and it is enough.
        RegionPlacement placement = placement(Region.NA, "");

        Optional<RegionPlacement.Denial> denial = placement.check(Region.NA, BUDGET, Region.EU, 240);

        assertTrue(denial.isPresent());
        assertEquals(240, denial.get().rttMs());
        assertNull(denial.get().declaredRttMs());
    }

    @Test
    void nothingKnownAdmits() {
        RegionPlacement placement = placement(Region.NA, "");

        assertTrue(placement.check(Region.NA, 1, Region.EU, null).isEmpty(),
                "no matrix and no probe is the deployment of W1-W20: no constraint");
        assertTrue(placement.check(Region.NA, 1, null, null).isEmpty(),
                "a token without a home region cannot produce a declared distance");
    }

    @Test
    void aRegionThisDeploymentDoesNotServeIsRefused() {
        RegionPlacement placement = placement(Region.NA, "EU-EU=20,EU-NA=120");

        Optional<RegionPlacement.Denial> denial = placement.check(Region.EU, 1000, Region.EU, null);

        assertTrue(denial.isPresent());
        assertEquals(RegionPlacement.Reason.REGION_UNSUPPORTED, denial.get().reason(),
                "a huge budget does not make a region someone else's queue");
        assertEquals(Region.EU, denial.get().target());
    }

    @Test
    void theMatrixIsSymmetricAndRejectsContradictions() {
        RegionPlacement placement = placement(Region.NA, "EU-NA=120");

        assertEquals(120, placement.declaredRttMs(Region.EU, Region.NA));
        assertEquals(120, placement.declaredRttMs(Region.NA, Region.EU), "one distance, two readers");
        assertNull(placement.declaredRttMs(Region.NA, Region.SA), "an undeclared pair is unknown");

        assertThrows(IllegalArgumentException.class,
                () -> RegionPlacement.parse(ServedRegions.all(), "EU-NA=120,NA-EU=80"),
                "two values for one distance must fail the boot, not pick one");
    }

    @Test
    void aMalformedMatrixFailsAtStartup() {
        for (String matrix : new String[] { "EU-NA", "EU-NA=", "=120", "EU-NA=abc", "EU-NA=0", "XX-NA=20",
                "EU=20" }) {
            assertThrows(IllegalArgumentException.class,
                    () -> RegionPlacement.parse(ServedRegions.all(), matrix), "must reject '" + matrix + "'");
        }
    }

    private static RegionPlacement placement(Region served, String matrix) {
        return RegionPlacement.parse(ServedRegions.of(served), matrix);
    }
}
