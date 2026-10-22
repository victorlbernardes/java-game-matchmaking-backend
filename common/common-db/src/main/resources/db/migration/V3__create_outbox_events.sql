-- Transactional outbox (W14): the event a service means to publish is written in the
-- same transaction as the state change it announces, so "the row exists" and "the
-- event exists" stop being two facts that can disagree.
--
-- The payload is the bare Protobuf message, WITHOUT the Confluent wire framing: the
-- schema id is resolved by the producer when the record is published. Framing here
-- would make the Schema Registry a dependency of the confirmation transaction - a
-- registry that is down would stop matches from being persisted. It delays the event
-- instead, which is what the outbox is for.
CREATE TABLE outbox_events (
    event_id        UUID         PRIMARY KEY,
    topic           VARCHAR(255) NOT NULL,
    partition_key   VARCHAR(128) NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    payload         BYTEA        NOT NULL,
    -- The event's clock (when the fact happened), distinct from created_at below: the
    -- difference is the lag the relay adds, and it cannot be reconstructed later.
    occurred_at     TIMESTAMPTZ  NOT NULL,
    status          VARCHAR(16)  NOT NULL,
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      TEXT,
    -- When this row may be published again: the retry backoff lives in the row, not in
    -- the relay's memory, so a restart does not forget that a broker was flapping.
    next_attempt_at TIMESTAMPTZ,
    -- A claim is a lease: a row is handed to one relay at a time, and a relay that dies
    -- holding it does not take the event with it - the lease expires and the row comes
    -- back to the queue.
    claimed_at      TIMESTAMPTZ,
    claimed_by      VARCHAR(128),
    created_at      TIMESTAMPTZ  NOT NULL,
    published_at    TIMESTAMPTZ
);

-- The claim reads pending rows in insertion order and nothing else.
CREATE INDEX idx_outbox_events_pending ON outbox_events (created_at, event_id) WHERE status = 'PENDING';

-- Ops: "what is waiting", "what is parked and why" (the relay's dashboard, W15).
CREATE INDEX idx_outbox_events_status ON outbox_events (status, created_at);
