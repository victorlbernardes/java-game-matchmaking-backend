-- latency-probe.lua
-- Consume a latency probe, but only for the player and the region that asked for it (W21, ADR 0039).
--
-- Why a script and not GET then DEL: the nonce is single use, and the check and the delete have to
-- be one step or two pongs can both find it. Why consume only on a match: an honest client that
-- answers at the wrong region's endpoint (the confusion this feature makes possible) would
-- otherwise lose the probe it legitimately holds and have to start over.
--
-- KEYS[1] = latency-probe:{nonce}
-- ARGV[1] = playerId, ARGV[2] = region
-- Returns: the issue instant (epoch millis, as written) when the probe is consumed, nil otherwise

if redis.call('HGET', KEYS[1], 'player') ~= ARGV[1] then
    return nil
end
if redis.call('HGET', KEYS[1], 'region') ~= ARGV[2] then
    return nil
end
local issuedAt = redis.call('HGET', KEYS[1], 'at')
redis.call('DEL', KEYS[1])
return issuedAt
