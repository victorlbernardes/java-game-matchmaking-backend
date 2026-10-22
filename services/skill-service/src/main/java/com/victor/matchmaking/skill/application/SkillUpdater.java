package com.victor.matchmaking.skill.application;

import java.util.List;
import java.util.UUID;

import com.victor.matchmaking.domain.skill.PlayerSkill;
import com.victor.matchmaking.events.MatchFinished;
import com.victor.matchmaking.events.RawRecord;
import com.victor.matchmaking.skill.domain.SkillApplication;

import io.vertx.core.Future;

/**
 * Applies a {@code MATCH_FINISHED} event: recalculates the involved players,
 * writes the new skills and publishes {@code SKILL_UPDATED}.
 *
 * <p>The effect is idempotent: the inbox (event id + consumer group) guarantees the
 * upsert happens at most once, so a replay does not move the rating twice.
 */
public interface SkillUpdater {

    /**
     * Applies the match result to the players' skills.
     *
     * @return the list of skill applications (one per player in the match)
     */
    Future<List<SkillApplication>> apply(MatchFinished event, UUID eventId, RawRecord record);

    /**
     * Returns the current skill of a player, creating the calibration row if absent.
     * The calibration default is 50, provisional=true, matchesPlayed=0.
     */
    Future<PlayerSkill> getOrCreate(String playerId);
}