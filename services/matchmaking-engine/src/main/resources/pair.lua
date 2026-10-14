-- pair.lua
-- Atomically: take 2 from queue, remove them, create pending match.
-- KEYS[1] = queue key (e.g. queue:1v1:NA)
-- ARGV[1] = matchId (e.g. match-abc123)
-- ARGV[2] = pending match JSON (e.g. {"players":[...],"expiresAt":...})
-- ARGV[3] = TTL seconds (e.g. 30)
-- Returns: matchId if paired, nil if another worker won the race

local queueKey = KEYS[1]
local tickets = redis.call('ZRANGE', queueKey, 0, 1)
if #tickets < 2 then
    return nil
end

local removed = redis.call('ZREM', queueKey, tickets[1], tickets[2])
if removed < 2 then
    return nil
end

redis.call('SET', 'match-pending:' .. ARGV[1], ARGV[2], 'EX', tonumber(ARGV[3]))
return ARGV[1]
