package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.engine.application.WideningPolicy;

class WideningPolicyTest {

    @Test
    void returnsInitialHalfRangeForShortWait() {
        assertEquals(2, WideningPolicy.halfRange(0));
    }

    @Test
    void growsHalfRangeAfterFirstThreshold() {
        assertEquals(5, WideningPolicy.halfRange(31));
    }

    @Test
    void growsHalfRangeAfterSecondThreshold() {
        assertEquals(10, WideningPolicy.halfRange(61));
    }

    @Test
    void returnsMaxHalfRangeAfterThirdThreshold() {
        assertEquals(20, WideningPolicy.halfRange(121));
    }

    @Test
    void returnsMaxHalfRangeForLongWaits() {
        assertEquals(20, WideningPolicy.halfRange(3600));
    }
}
