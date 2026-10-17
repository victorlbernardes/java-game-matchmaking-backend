-- Orchestrator projection (W13): one row per confirmed match waiting for a game
-- server. The allocator fills it in W22; what the row must survive until then is
-- a consumer replay, which is why it is written by the idempotent consumer below.
CREATE TABLE match_allocations (
    match_id     VARCHAR(64) PRIMARY KEY,
    game_mode    VARCHAR(32) NOT NULL,
    region       VARCHAR(32) NOT NULL,
    player_count INT         NOT NULL,
    status       VARCHAR(32) NOT NULL,
    confirmed_at TIMESTAMPTZ NOT NULL,
    -- When the consumer projected the row. `confirmed_at` is the event's clock
    -- (when the match was confirmed), this one is ours: the difference is the lag
    -- a replay costs, and it cannot be reconstructed after the fact.
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Inbox of the idempotent consumer (W13): the events a consumer group has already
-- applied. The event and its effect are written in one transaction, so this primary
-- key - not the offset commit - is what makes "at most one effect per event" true
-- across rebalances, retries and manual offset resets.
CREATE TABLE processed_events (
    event_id       UUID PRIMARY KEY,
    consumer_group VARCHAR(128) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    partition_id   INT          NOT NULL,
    record_offset  BIGINT       NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL
);

-- Replay forensics: "which consumer read this record, when, and where was it".
CREATE INDEX idx_processed_events_group ON processed_events (consumer_group, processed_at DESC);
CREATE INDEX idx_processed_events_source ON processed_events (topic, partition_id, record_offset);
