-- pair.lua
-- Atomically: take the given tickets off the queue and create the pending match.
-- KEYS[1] = queue key (e.g. queue:1v1:NA)
-- ARGV[1] = matchId (e.g. match-abc123)
-- ARGV[2] = pending match JSON (e.g. {"players":[...],"expiresAt":...})
-- ARGV[3] = pending-match key TTL in seconds. It must exceed the confirmation
--           deadline so the sweeper can still read the key, requeue the tickets
--           and notify the players when the deadline passes.
-- ARGV[4] = number of players to take (e.g. 10 for 5v5, 2 for 1v1)
-- ARGV[5..] = the ticket ids the caller paired, in roster order
-- Returns: matchId if paired, nil if another worker won the race
--
-- The members are named by the caller, not read from the queue here (W16). Taking "the head of
-- the queue" while the caller stores the roster it read a moment ago looks equivalent and is
-- not: two pairing ticks whose reads overlapped removed the head twice and the second one
-- stored the first one's roster, so two members nobody had paired were taken off the queue and
-- left out of every match, while the first roster ended up in two matches at once. The drain
-- loop (many pairings per tick) is what made overlapping reads normal, and the load test then
-- showed it: 40 matches refused as "tickets not in CONFIRMING", 40 dissolved, and 186 tickets
-- off the queue with nothing to pair them.
--
-- All-or-nothing: every member must still be on the queue, or nothing is removed. Redis runs
-- the script as a single command, so exactly one caller finds all of its members there.

local queueKey = KEYS[1]
local count = tonumber(ARGV[4])
if count < 2 then
    return nil
end

local members = {}
for i = 5, #ARGV do
    members[#members + 1] = ARGV[i]
end
if #members ~= count then
    return nil
end

for i = 1, #members do
    if redis.call('ZSCORE', queueKey, members[i]) == false then
        return nil
    end
end

for i = 1, #members do
    redis.call('ZREM', queueKey, members[i])
end

redis.call('SET', 'match-pending:' .. ARGV[1], ARGV[2], 'EX', tonumber(ARGV[3]))
return ARGV[1]
