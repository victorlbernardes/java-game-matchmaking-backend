package com.victor.matchmaking.domain.match;

/** Match lifecycle states. */
public enum MatchStatus {
    PENDING,
    STARTING,
    STARTED,
    FINISHED,
    CANCELLED
}
