package com.victor.matchmaking.domain.skill;

import com.victor.matchmaking.domain.player.Player;

/**
 * One player of a finished match, as the skill service needs it (W24).
 *
 * <p>This is the domain's view of the {@code MatchPlayerResult} from the
 * {@code MATCH_FINISHED} event, without depending on the protobuf types.
 */
public record MatchResultPlayer(String playerId, int team, int skillAtMatch) {

    public MatchResultPlayer {
        if (playerId == null || playerId.isBlank()) {
            throw new IllegalArgumentException("playerId is required");
        }
        if (team != 1 && team != 2) {
            throw new IllegalArgumentException("team must be 1 or 2");
        }
        if (skillAtMatch < Player.MIN_SKILL || skillAtMatch > Player.MAX_SKILL) {
            throw new IllegalArgumentException("skillAtMatch must be between 0 and 100");
        }
    }
}