-- ratelimit.lua
-- One distributed token bucket (W17, ADR 0023).
--
-- Why a script and not INCR + EXPIRE: INCR counts, it does not refill, and it cannot answer
-- "when may I try again" - the Retry-After the caller owes its client. A bucket is a
-- read-modify-write of two numbers (tokens and the instant they were last refilled), and doing
-- that from the caller is a race with every other instance of the service.
--
-- Why TIME instead of a timestamp passed in: the bucket is shared by every instance, so the
-- clock must be the one they share. A caller's clock would let a skewed instance refill faster
-- than the others.
--
-- KEYS[1] = bucket key (ratelimit:{scope}:{id})
-- ARGV[1] = capacity (burst, in tokens)
-- ARGV[2] = refill rate (tokens per second)
-- ARGV[3] = cost of this request (tokens)
-- ARGV[4] = key TTL in seconds (the bucket must outlive at least one full refill, or a
--           throttled caller gets a fresh bucket the moment the key expires)
-- Returns: {allowed (1/0), remaining tokens (floor), retry-after seconds (ceil, 0 when allowed)}

local capacity = tonumber(ARGV[1])
local refillPerSecond = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttlSeconds = tonumber(ARGV[4])

local now = redis.call('TIME')
local nowMillis = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local stored = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(stored[1])
local lastMillis = tonumber(stored[2])
if tokens == nil then
    tokens = capacity
    lastMillis = nowMillis
end

local elapsedMillis = nowMillis - lastMillis
if elapsedMillis < 0 then
    elapsedMillis = 0
end
tokens = math.min(capacity, tokens + (elapsedMillis / 1000) * refillPerSecond)

local allowed = 0
local retryAfterSeconds = 0
if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
else
    retryAfterSeconds = math.ceil((cost - tokens) / refillPerSecond)
end

-- tostring, not the raw number: Redis converts a Lua number used as a command argument with a
-- precision that loses the fraction, and a bucket that never remembers its fraction refills too
-- generously.
redis.call('HSET', KEYS[1], 'tokens', string.format('%.4f', tokens),
        'ts', string.format('%.0f', nowMillis))
redis.call('EXPIRE', KEYS[1], ttlSeconds)
return { allowed, math.floor(tokens), retryAfterSeconds }
