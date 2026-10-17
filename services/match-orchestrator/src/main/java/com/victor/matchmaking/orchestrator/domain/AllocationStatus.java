package com.victor.matchmaking.orchestrator.domain;

/** Where a confirmed match sits in the orchestrator's own workflow. */
public enum AllocationStatus {

    /**
     * The match is known and has no game server yet. W13 writes the row in this
     * state; W22 assigns a server and moves it on.
     */
    PENDING_ALLOCATION
}
