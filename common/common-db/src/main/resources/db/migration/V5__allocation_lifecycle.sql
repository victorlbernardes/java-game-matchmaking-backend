-- The allocation lifecycle (W23, ADR 0032): a confirmed match is no longer a row that either
-- gets a server or stays pending forever. The columns below are the timestamps of the states
-- the watchdog moves through, and the one that says whether the players already went back to
-- the queue.
--
-- `requeued_at` is written by the engine, not by the orchestrator, and that split is the point:
-- the orchestrator decides that a match cannot run (a durable fact, in a row), and the engine -
-- the only process that owns the queue - acts on it. A crash between the two is a repeat of an
-- idempotent requeue, never a lost player, which is why the marker is the *last* write.
ALTER TABLE match_allocations
    ADD COLUMN started_at       TIMESTAMPTZ,
    ADD COLUMN rejected_at      TIMESTAMPTZ,
    ADD COLUMN rejection_reason VARCHAR(32),
    ADD COLUMN requeued_at      TIMESTAMPTZ,
    -- The vocabulary is the domain's (`RejectionReason`), and the database refuses anything else:
    -- a wrong reason is worse than no reason, because it is the field an operator acts on.
    ADD CONSTRAINT match_allocations_rejection_reason CHECK (rejection_reason IS NULL
        OR rejection_reason IN ('no_capacity', 'start_timeout', 'server_failed', 'all_no_show'));

-- The roster of an allocation, with the ticket each player was paired with. It is not a copy
-- of `match_players` for convenience: this one is the *allocator's* working set (who is in the
-- match, which queue entry must come back, who connected), and `match_players` is the finished
-- match's record. They are written by different consumers at different moments (W13's projection
-- and W23's allocation), and a match that is rejected never becomes a `matches` row at all.
--
-- `ticket_id` is what makes the requeue possible: the queue's member is the ticket, and a
-- player id cannot find a queue entry.
CREATE TABLE match_roster (
    match_id       VARCHAR(64) NOT NULL REFERENCES match_allocations (match_id) ON DELETE CASCADE,
    player_id      VARCHAR(64) NOT NULL,
    -- Nullable on purpose: an event produced before W23 does not carry a ticket id, and a replay
    -- of the topic's history must still project. Such a match can be allocated but cannot be put
    -- back in the queue (the queue entry is unknown), which the requeue reports instead of guessing.
    ticket_id      VARCHAR(64),
    team           INT         NOT NULL CHECK (team IN (1, 2)),
    skill_at_match INT         NOT NULL CHECK (skill_at_match BETWEEN 0 AND 100),
    -- When the game server reported the player in (W23, ADR 0034). Null means "not yet", and
    -- `no_show_at` is the watchdog's conclusion once the connect window is over.
    connected_at   TIMESTAMPTZ,
    no_show_at     TIMESTAMPTZ,
    PRIMARY KEY (match_id, player_id)
);

CREATE INDEX idx_match_roster_ticket ON match_roster (ticket_id);

-- The two scans the watchdog and the engine make every second: pending matches to retry, and
-- rejected matches whose players have not gone back to the queue yet.
CREATE INDEX idx_match_allocations_pending ON match_allocations (confirmed_at)
    WHERE status = 'PENDING_ALLOCATION';
CREATE INDEX idx_match_allocations_requeue ON match_allocations (rejected_at)
    WHERE requeued_at IS NULL;
