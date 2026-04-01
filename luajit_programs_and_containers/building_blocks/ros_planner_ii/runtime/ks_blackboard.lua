--[[
    ks_blackboard.lua -- NATS KeyStore-backed blackboard for virtual hub.

    Drop-in replacement for the in-memory blackboard table.
    Reads/writes go through NATS KeyStore (JetStream RAM-only).

    Uses Lua metatables to intercept __index and __newindex.
    Caches locally per-tick to avoid redundant round-trips,
    then flushes dirty fields at end of tick.

    Usage:
        local ks_bb = require("ks_blackboard")
        local bb = ks_bb.new(robot_id, server)
        handle.blackboard = bb

        -- Normal blackboard usage — transparently backed by KeyStore
        bb.active_kb = "init_check"
        print(bb.active_kb)

        -- End of tick: flush dirty fields to KeyStore
        bb:flush()
]]

local json_util = require("json_util")

local M = {}

function M.new(robot_id, server)
    server = server or "nats://127.0.0.1:4222"

    local ks_lib = require("lib.nats_key_store")
    local ks = ks_lib.KeyStore.new({
        server = server,
        bucket = "bb_" .. robot_id,
        create_bucket = true,
        history = 1,
    })
    ks:connect()

    local prefix = "hub." .. robot_id .. "."

    -- Local cache: field_name → value
    local cache = {}
    -- Dirty set: field_name → true (needs flush to KeyStore)
    local dirty = {}
    -- Known fields (avoid putting internal methods)
    local internal = { flush = true, close = true, sync = true,
                       _ks = true, _prefix = true, _cache = true }

    local bb = {}

    -- Flush dirty fields to KeyStore
    function bb.flush()
        for field, _ in pairs(dirty) do
            local val = cache[field]
            local json_val
            if type(val) == "table" then
                json_val = json_util.encode(val)
            elseif type(val) == "boolean" then
                json_val = val and "true" or "false"
            elseif val == nil then
                json_val = "null"
            else
                json_val = tostring(val)
            end
            pcall(ks.put, ks, prefix .. field, json_val)
        end
        dirty = {}
    end

    -- Sync a field from KeyStore to cache
    function bb.sync(field)
        local ok, val = pcall(ks.get, ks, prefix .. field)
        if ok and val then
            -- Try to decode as JSON, fall back to raw string
            local dok, decoded = pcall(json_util.decode, val)
            if dok and decoded ~= nil then
                cache[field] = decoded
            else
                -- Try number
                local num = tonumber(val)
                if num then
                    cache[field] = num
                elseif val == "true" then
                    cache[field] = true
                elseif val == "false" then
                    cache[field] = false
                elseif val == "null" then
                    cache[field] = nil
                else
                    cache[field] = val
                end
            end
        end
        return cache[field]
    end

    function bb.close()
        pcall(function() ks:disconnect() end)
        pcall(function() ks:destroy() end)
    end

    -- Expose internals for debug
    bb._ks = ks
    bb._prefix = prefix
    bb._cache = cache

    -- Metatable: intercept field access
    local mt = {}

    mt.__index = function(t, key)
        -- Internal methods
        if internal[key] then return rawget(t, key) end
        -- Return from cache (fast path)
        return cache[key]
    end

    mt.__newindex = function(t, key, value)
        if internal[key] then rawset(t, key, value); return end
        cache[key] = value
        dirty[key] = true
    end

    return setmetatable(bb, mt)
end

return M
