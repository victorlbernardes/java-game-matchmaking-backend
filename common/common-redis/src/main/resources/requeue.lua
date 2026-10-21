-- requeue.lua
-- Puts a ticket back in its queue after the match it was part of could not run (W23, ADR 0032).
-- KEYS[1] = ticket key (ticket:{ticketId})
-- KEYS[2] = queue key (the ticket's own partition, computed from the same enqueue key as the create)
-- ARGV[1] = ticketId (the ZSET member)
-- ARGV[2] = score (the ticket's ORIGINAL queuedAt, epoch millis)
-- ARGV[3] = ticket TTL seconds
-- Returns: {status}
--   REQUEUED  -> the ticket was CONFIRMING/CONFIRMED and is SEARCHING again, at its old position
--   UNCHANGED -> the ticket is in any other state (already searching, cancelled, expired): nothing to do
--   MISSING   -> the ticket key is gone (its TTL ran out while the match waited for a server)
--
-- Why the score is passed in and not recomputed here: the position is the one fact a requeue must
-- not change. A player who waited 40 s before the match was confirmed goes back with the 40 s he
-- already served, not to the end of the queue. The caller reads queuedAt from the ticket and passes
-- it; the script's job is the atomic check-and-write (a ticket cancelled between the read and the
-- write must not come back to life).
--
-- Why the status check is the gate: the caller may retry after a crash (the marker it writes is the
-- last write, on purpose). A second call finds SEARCHING and answers UNCHANGED, which is what makes
-- "requeue then mark" safe to repeat instead of a duplicate queue member.
local json = redis.call('GET', KEYS[1])
if not json then
    return { 'MISSING' }
end

local ticket = cjson.decode(json)
local status = ticket['status']
if status ~= 'CONFIRMING' and status ~= 'CONFIRMED' then
    return { 'UNCHANGED' }
end

ticket['status'] = 'SEARCHING'
redis.call('SET', KEYS[1], cjson.encode(ticket), 'EX', tonumber(ARGV[3]))
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return { 'REQUEUED' }
