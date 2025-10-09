local key = KEYS[1]
local configExpiry = tonumber(ARGV[1])
local resetAt = tonumber(ARGV[2])
local delta = tonumber(ARGV[3])
local lastSynced = tonumber(ARGV[4])

-- Default expiry = -1 (no expiry)
local expiry = -1

-- If configExpiry > 0, set expiry to (current time - keyExpiry)
if configExpiry > 0 then
    -- Redis TIME returns two values: seconds and microseconds
    local t = redis.call('TIME')
    local now_ns = t[1] * 1e9 + t[2] * 1000
    expiry = now_ns - configExpiry
end

local hasSyncedBefore = lastSynced ~= nil and lastSynced > 0
local currentValue = nil

-- Case 1: first sync, key not yet synced anywhere
if not hasSyncedBefore and resetAt > 0 then
    local wasSet = redis.call('SET', key, resetAt, 'NX')
    if wasSet then
        -- Successfully created the key
        return { 'ok', resetAt }
    end
    -- Key already exists, fall through
end

-- Case 2: push delta or get value
if delta > 0 then
    currentValue = redis.call('INCRBY', key, delta)
else
    currentValue = redis.call('GET', key)
    if not currentValue then
        -- key missing
        if hasSyncedBefore then
            return { 'corrupted_remote', lastSynced }
        else
            return { 'ok' } -- nothing to sync yet
        end
    end
    currentValue = tonumber(currentValue)
end

if not hasSyncedBefore then
    -- Case: The key is set by another server and the current server joins the cluster later
    -- If the remote value is greater than the local resetAt due to clock drifts between servers,
    -- this newly joined server will be penalized, we assume that the clock drift is not significant enough
    -- Besides, the clock drift disadvantage is not permanent
    -- After the first sync, the key will only sync the delta of the previously synced value and the next remote value
    if currentValue > resetAt then
        -- Penalized by drift but okay, adjust local
        local drift = currentValue - resetAt
        return { 'adjust_local', drift, currentValue }
    end
    return { 'ok', currentValue }
end

-- Case 3: remote corrupted (value decreased)
if currentValue < lastSynced then
    return { 'corrupted_remote', lastSynced }
end

-- Case 4: compute diff to see if others incremented
local diff = currentValue - lastSynced - delta

-- Case 5: expired key cleanup
if resetAt < expiry and delta == 0 then
    if diff == 0 then
        -- sync state clean, expire key if not yet expiring
        local t = redis.call('TIME')
        local now = tonumber(t[1]) * 1e9 + tonumber(t[2]) * 1000  -- redis TIME -> nanoseconds
        local ttl_ns = math.max(configExpiry, currentValue - now)

        local ttl_sec = math.floor(ttl_ns / 1e9)
        if ttl_sec > 0 then
            redis.call('EXPIRE', key, ttl_sec, 'NX')
        end
    end
    return { 'expired', currentValue }
end

-- Case 6: adjust local if other servers incremented
if diff > 0 then
    return { 'adjust_local', diff, currentValue }
end

-- Default: just synced
return { 'ok', currentValue }
