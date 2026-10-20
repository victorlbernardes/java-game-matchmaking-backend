-- The game server pool (W22): the servers a confirmed match can be allocated to.
--
-- One row per server, not per (server, mode): `capacity` is how many matches the
-- server runs *at the same time*, and that number is shared by every mode it accepts,
-- so a per-mode row would double-count the same slot. `allocated` is our bookkeeping
-- (the orchestrator increments it in the allocation transaction and decrements it on
-- release); the server itself only reports what it can host.
--
-- `last_seen` is the heartbeat: a server that stops renewing it is not eligible, and
-- the eligibility predicate lives in the allocation query - there is no sweep that has
-- to run for correctness, only a derived state for the listing.
CREATE TABLE game_servers (
    server_id  VARCHAR(64) PRIMARY KEY,
    region     VARCHAR(32) NOT NULL,
    endpoint   VARCHAR(255) NOT NULL,
    -- The modes this server accepts ("1v1", "5v5"). TEXT[] so the pool can be read
    -- with `mode = ANY(game_modes)` instead of a join table for two values.
    game_modes TEXT[]      NOT NULL,
    capacity   INT         NOT NULL CHECK (capacity >= 0),
    allocated  INT         NOT NULL DEFAULT 0 CHECK (allocated >= 0),
    draining   BOOLEAN     NOT NULL DEFAULT FALSE,
    last_seen  TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The invariant the allocator maintains per row, and the reason a capacity decrease
    -- below what is already running is refused instead of silently over-committing.
    CONSTRAINT game_servers_allocated_within_capacity CHECK (allocated <= capacity)
);

CREATE INDEX idx_game_servers_region ON game_servers (region);

-- Where the allocation lands: the projection now says which server took the match and
-- when. Both nullable, because a confirmed match with no capacity stays unallocated
-- until W23 closes the loop (timeout + requeue).
ALTER TABLE match_allocations ADD COLUMN server_id    VARCHAR(64);
ALTER TABLE match_allocations ADD COLUMN allocated_at TIMESTAMPTZ;

CREATE INDEX idx_match_allocations_status ON match_allocations (status);
CREATE INDEX idx_match_allocations_server ON match_allocations (server_id) WHERE server_id IS NOT NULL;
