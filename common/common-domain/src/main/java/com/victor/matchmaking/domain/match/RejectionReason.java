package com.victor.matchmaking.domain.match;

/**
 * Why a confirmed match never ran (W23, ADR 0032).
 *
 * <p>It exists because "rejected" is a fact nobody can act on: {@link #NO_CAPACITY} is an operator
 * who needs more servers, {@link #START_TIMEOUT} is a game server that took a match and never
 * started it, {@link #SERVER_FAILED} is a machine that died mid-flight, and {@link #ALL_NO_SHOW}
 * is players who never connected. The database's {@code CHECK} constraint holds the same
 * vocabulary, so a wrong reason cannot be stored by a caller that skipped this enum.
 */
public enum RejectionReason {

    /** No game server took the match before {@code ALLOCATION_TIMEOUT_SECONDS}. */
    NO_CAPACITY("no_capacity"),

    /** A server held the slot, but the match was never reported started before its deadline. */
    START_TIMEOUT("start_timeout"),

    /** The server that held the match stopped heartbeating: the match cannot run there. */
    SERVER_FAILED("server_failed"),

    /** The match started and not one player connected inside the connect window. */
    ALL_NO_SHOW("all_no_show");

    private final String wireName;

    RejectionReason(String wireName) {
        this.wireName = wireName;
    }

    /** The value stored in {@code match_allocations.rejection_reason} and sent to clients. */
    public String wireName() {
        return wireName;
    }

    /** Reads a value the database accepted (its {@code CHECK} holds the same vocabulary). */
    public static RejectionReason fromWireName(String raw) {
        for (RejectionReason reason : values()) {
            if (reason.wireName.equals(raw)) {
                return reason;
            }
        }
        throw new IllegalArgumentException("unknown rejection reason: " + raw);
    }
}
