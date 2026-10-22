-- Skill service state (W24): the current rating of every player the skill service knows.
-- The table is the authoritative state; the Redis cache is an acceleration.
CREATE TABLE player_skills (
    player_id       VARCHAR(64) PRIMARY KEY,
    rating          NUMERIC(5,2) NOT NULL CHECK (rating BETWEEN 0 AND 100),
    matches_played  INT          NOT NULL DEFAULT 0,
    provisional     BOOL         NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- The allocation lifecycle (W24): a finished match reports its result, and the
-- orchestrator projects it. `winner_team` is NULL for a draw (the wire value is
-- DRAW, the database keeps it as 0 for the CHECK), and `finished_at` is when the
-- game server reported it (not when the match started).
ALTER TABLE match_allocations
    ADD COLUMN winner_team SMALLINT CHECK (winner_team IS NULL OR winner_team IN (1, 2)),
    ADD COLUMN finished_at TIMESTAMPTZ;

-- The match itself is FINISHED when the game server reports the result. The
-- `finished_at` here is the same clock as match_allocations.finished_at (the
-- game server's report), not the orchestrator's.
ALTER TABLE matches
    ADD COLUMN finished_at TIMESTAMPTZ;

-- The `finished_at` column already existed in V1; V5 added the other allocation
-- columns. The CHECK on status already permits 'FINISHED' (it was the CHECK
-- vocabulary in V5). Here we add the result column and its timestamp to the
-- match projection, so the history endpoint can show when a match ended and who
-- won.
ALTER TABLE matches
    ADD COLUMN winner_team SMALLINT CHECK (winner_team IS NULL OR winner_team IN (1, 2));

-- Index for the orchestrator's FINISHED projection: find the allocation by
-- match id (the game server reports by match id, and the orchestrator must
-- verify the server owns it).
CREATE INDEX idx_match_allocations_match ON match_allocations (match_id);

-- Index for the skill service's consumer: the processed_events table (V2) is
-- shared, but the skill service uses its own consumer group. The unique index
-- on event_id is already the inbox's guarantee.