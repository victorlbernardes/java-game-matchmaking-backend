CREATE TABLE matches (
    id          VARCHAR(64) PRIMARY KEY,
    game_mode   VARCHAR(32)  NOT NULL,
    region      VARCHAR(32)  NOT NULL,
    status      VARCHAR(32)  NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE TABLE match_players (
    match_id    VARCHAR(64) NOT NULL REFERENCES matches(id),
    player_id   VARCHAR(64) NOT NULL,
    team        VARCHAR(8)  NOT NULL,
    skill_at_match INT      NOT NULL,
    PRIMARY KEY (match_id, player_id)
);

CREATE INDEX idx_match_players_player ON match_players(player_id);
CREATE INDEX idx_matches_created_at ON matches(created_at DESC, id DESC);
