-- create.lua
-- Atomic ticket creation: idempotency when a key is given, admission control always (W17, ADR
-- 0022).
-- KEYS[1] = idempotency key (idem:{key}), or the empty string when the caller sent none
-- ARGV[1] = ticket JSON
-- ARGV[2] = ticketId
-- ARGV[3] = queue key (queue:{gameMode}:{region})
-- ARGV[4] = queuedAt (ZSET score)
-- ARGV[5] = ticket TTL seconds
-- ARGV[6] = idempotency TTL seconds
-- ARGV[7] = queue capacity in tickets (0 or absent = unbounded)
-- Returns: {status, payload}
--   OK     -> { 'OK', ticketId }
--   EXISTS -> { 'EXISTS', existingTicketId }   (an idempotent replay; not a new admission)
--   FULL   -> { 'FULL', currentDepth }
--
-- Why the capacity check is here and not in Java: a ZCARD read followed by a ZADD is a check that
-- N concurrent requests all pass at the same time, so a queue "capped" at M grows without bound
-- under the exact load the cap exists for. The read and the write have to be one atomic step, the
-- same reason idempotency lives here (W6) and the roster in pair.lua (W16).
--
-- Why the idempotency key is optional and not the price of admission: the two are independent
-- questions. A caller who sends no key has no replay to be protected against, but the queue is
-- just as full for him - and a cap that only applies to callers who sent a header is a cap the
-- load generator walks through (measured in W17: 4000 arrivals, zero refusals).

local idemKey = KEYS[1]
local hasIdempotencyKey = idemKey ~= ''
if hasIdempotencyKey then
    local existing = redis.call('GET', idemKey)
    if existing then
        return { 'EXISTS', existing }
    end
end

local ticketJson = ARGV[1]
local ticketId = ARGV[2]
local queueKey = ARGV[3]
local queuedAt = ARGV[4]
local ticketTtl = ARGV[5]
local idemTtl = ARGV[6]
local capacity = tonumber(ARGV[7])

if capacity and capacity > 0 then
    local depth = redis.call('ZCARD', queueKey)
    if depth >= capacity then
        return { 'FULL', tostring(depth) }
    end
end

redis.call('SET', 'ticket:' .. ticketId, ticketJson, 'EX', tonumber(ticketTtl))
redis.call('ZADD', queueKey, queuedAt, ticketId)
if hasIdempotencyKey then
    redis.call('SET', idemKey, ticketId, 'EX', tonumber(idemTtl))
end
return { 'OK', ticketId }
