-- pair.lua
-- Atomically: take N from the queue, remove them, create the pending match.
-- KEYS[1] = queue key (e.g. queue:1v1:NA)
-- ARGV[1] = matchId (e.g. match-abc123)
-- ARGV[2] = pending match JSON (e.g. {"players":[...],"expiresAt":...})
-- ARGV[3] = pending-match key TTL in seconds. It must exceed the confirmation
--           deadline so the sweeper can still read the key, requeue the tickets
--           and notify the players when the deadline passes.
-- ARGV[4] = number of players to take (e.g. 10 for 5v5, 2 for 1v1)
-- Returns: matchId if paired, nil if another worker won the race

local queueKey = KEYS[1]
local count = tonumber(ARGV[4])
if count < 2 then
    return nil
end

local tickets = redis.call('ZRANGE', queueKey, 0, count - 1)
if #tickets < count then
    return nil
end

local removed = 0
for i = 1, #tickets do
    if redis.call('ZREM', queueKey, tickets[i]) == 1 then
        removed = removed + 1
    end
end
if removed < count then
    return nil
end

redis.call('SET', 'match-pending:' .. ARGV[1], ARGV[2], 'EX', tonumber(ARGV[3]))
return ARGV[1]
