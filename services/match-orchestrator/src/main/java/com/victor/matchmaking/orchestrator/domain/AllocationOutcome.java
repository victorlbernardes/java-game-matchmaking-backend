package com.victor.matchmaking.orchestrator.domain;

/**
 * What happened when a confirmed match looked for a game server (W22, ADR 0031).
 *
 * <p>Two states that read alike and must not: {@link #ALLOCATED} is a match a server will run, and
 * {@link #NO_CAPACITY} is a match that is confirmed and has nowhere to go. The second is not an
 * error — the pool was full, draining or absent, which is a legitimate answer — and W23's timeout
 * is what works through the rows it leaves behind.
 */
public enum AllocationOutcome {
    ALLOCATED,
    NO_CAPACITY
}
