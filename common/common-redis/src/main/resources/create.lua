-- create.lua
-- Atomic idempotent ticket creation.
-- KEYS[1] = idempotency key (idem:{key})
-- ARGV[1] = ticket JSON
-- ARGV[2] = ticketId
-- ARGV[3] = queue key (queue:{gameMode}:{region})
-- ARGV[4] = queuedAt (ZSET score)
-- ARGV[5] = ticket TTL seconds
-- ARGV[6] = idempotency TTL seconds
-- Returns: ticketId (existing or newly created)

local idemKey = KEYS[1]
local existing = redis.call('GET', idemKey)
if existing then
    return existing
end

local ticketJson = ARGV[1]
local ticketId = ARGV[2]
local queueKey = ARGV[3]
local queuedAt = ARGV[4]
local ticketTtl = ARGV[5]
local idemTtl = ARGV[6]

redis.call('SET', 'ticket:' .. ticketId, ticketJson, 'EX', tonumber(ticketTtl))
redis.call('ZADD', queueKey, queuedAt, ticketId)
redis.call('SET', idemKey, ticketId, 'EX', tonumber(idemTtl))
return ticketId
