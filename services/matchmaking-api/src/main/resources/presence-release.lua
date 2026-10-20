-- presence-release.lua
-- Clear a player's presence entry, but only while it still names the caller.
--
-- KEYS[1] = presence key (e.g. presence:player-1)
-- ARGV[1] = this instance's id (e.g. api-8080)
-- Returns: 1 if the entry was removed, 0 if it belonged to another instance
--
-- Compare-and-clear, not a plain DEL (W19, ADR 0026). A reconnect opens the new socket on another
-- instance before the old instance's close handler runs, so a blind DEL would erase the entry the
-- new instance just wrote - presence would point at the instance the player already left. Redis
-- runs the script as one command, so the read and the delete cannot be split by a concurrent write.

local owner = redis.call('GET', KEYS[1])
if owner == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
