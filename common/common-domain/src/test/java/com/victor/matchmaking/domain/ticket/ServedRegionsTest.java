package com.victor.matchmaking.domain.ticket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Property: a deployment's region list is exactly what it says, and an unset list is every region
 * (W21, ADR 0039).
 *
 * <p>The second half is the one that protects every earlier sprint: a deployment that does not
 * configure regions must keep serving all of them, or the single stack of W1-W20 starts refusing
 * tickets nobody asked it to refuse.
 */
class ServedRegionsTest {

    @Test
    void anUnsetListMeansEveryRegion() {
        assertEquals(List.of(Region.values()), ServedRegions.parse(null).list());
        assertEquals(List.of(Region.values()), ServedRegions.parse("  ").list());
        assertTrue(ServedRegions.parse(null).servesEveryRegion());
    }

    @Test
    void parsesACaseInsensitiveCommaSeparatedList() {
        ServedRegions served = ServedRegions.parse(" na , Eu ");
        assertEquals(List.of(Region.NA, Region.EU), served.list());
        assertTrue(served.contains(Region.NA));
        assertTrue(served.contains(Region.EU));
        assertFalse(served.contains(Region.SA));
        assertFalse(served.servesEveryRegion());
    }

    @Test
    void aTypoFailsInsteadOfServingNothing() {
        // Silently ignoring "N A" would leave a deployment that answers every arrival with
        // region_unsupported, and the log would say the region list was fine.
        assertThrows(IllegalArgumentException.class, () -> ServedRegions.parse("NA,XX"));
        assertThrows(IllegalArgumentException.class, () -> ServedRegions.parse(","));
    }

    @Test
    void theListIsInEnumOrderWhateverTheInputOrder() {
        assertEquals(List.of(Region.SA, Region.NA, Region.EU),
                ServedRegions.parse("EU,NA,SA").list(),
                "a body, a log line and a test assertion must read the same order");
    }

    @Test
    void duplicatesAreOneRegion() {
        ServedRegions served = ServedRegions.parse("NA,NA,eu,EU");
        assertEquals(List.of(Region.NA, Region.EU), served.list());
    }
}
