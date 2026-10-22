-- latency.lua
-- Record a measured rtt for one player and one region, keeping the best recent value (W21, ADR 0039).
--
-- Why a script and not HSET: the profile is read right before every ticket creation, and a single
-- bad sample - one probe answered while the machine was busy - would raise the number and refuse
-- a player who can reach the region perfectly well. The minimum can only improve, so a hiccup
-- costs one probe, not a session. Read, compare and write have to be one step, or two instances
-- can interleave and keep the worse value.
--
-- KEYS[1] = latency:{playerId}
-- ARGV[1] = region
-- ARGV[2] = measured rtt in millis
-- ARGV[3] = profile TTL in seconds
-- Returns: the rtt the profile now holds for that region

local measured = tonumber(ARGV[2])
local stored = tonumber(redis.call('HGET', KEYS[1], ARGV[1]))
local kept
if stored == nil or measured < stored then
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    kept = measured
else
    kept = stored
end
-- The TTL belongs to the profile, not to the sample: every probe renews it, which is what makes
-- "measured 5 minutes ago" the useful horizon and an old measurement disappear on its own.
redis.call('EXPIRE', KEYS[1], ARGV[3])
return kept
