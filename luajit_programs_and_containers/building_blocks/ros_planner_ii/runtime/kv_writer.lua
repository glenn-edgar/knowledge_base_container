--[[
    kv_writer.lua -- Coalescing KV write queue.

    Non-blocking enqueue from robot message handlers, background
    drain by coroutine (1-2 writes per tick). Same key queued
    twice → only the latest value is written.

    Usage:
        local kv_writer = require("kv_writer")
        local writer = kv_writer.new(status_ks)

        -- Fast: queue a write (non-blocking)
        writer:push("site.robots.rover_1.status.link", json_str)

        -- In tick loop: drain one entry
        writer:tick()

        -- Flush all remaining on shutdown
        writer:flush()
]]

local M = {}
M.__index = M

function M.new(status_ks)
    return setmetatable({
        ks     = status_ks,
        -- Coalescing map: key → latest value
        pending = {},
        -- Ordered key list for FIFO drain (keys in insertion order)
        order   = {},
        -- Set of keys in order list (for dedup)
        in_order = {},
        -- Stats
        queued  = 0,
        written = 0,
        coalesced = 0,
    }, M)
end

---------------------------------------------------------------------------
-- Push: enqueue a KV write (non-blocking, coalesces)
---------------------------------------------------------------------------

function M:push(key, value)
    if self.pending[key] then
        -- Key already queued — coalesce (overwrite with latest value)
        self.pending[key] = value
        self.coalesced = self.coalesced + 1
    else
        -- New key — add to pending and order list
        self.pending[key] = value
        self.order[#self.order + 1] = key
        self.in_order[key] = true
    end
    self.queued = self.queued + 1
end

---------------------------------------------------------------------------
-- Tick: write one entry (call once per main loop tick)
---------------------------------------------------------------------------

function M:tick()
    -- Find next key to write
    while #self.order > 0 do
        local key = table.remove(self.order, 1)
        self.in_order[key] = nil
        local value = self.pending[key]
        if value then
            self.pending[key] = nil
            pcall(self.ks.put, self.ks, key, value)
            self.written = self.written + 1
            return true
        end
        -- Value was nil (deleted between push and drain), skip
    end
    return false
end

---------------------------------------------------------------------------
-- Delete: queue a KV delete
---------------------------------------------------------------------------

function M:delete(key)
    -- Remove from pending if queued
    self.pending[key] = nil
    -- Queue actual delete
    pcall(self.ks.delete, self.ks, key)
end

---------------------------------------------------------------------------
-- Flush: drain all remaining writes (call on shutdown)
---------------------------------------------------------------------------

function M:flush()
    while #self.order > 0 do
        self:tick()
    end
end

---------------------------------------------------------------------------
-- Stats
---------------------------------------------------------------------------

function M:stats()
    return {
        queued    = self.queued,
        written   = self.written,
        coalesced = self.coalesced,
        pending   = #self.order,
    }
end

---------------------------------------------------------------------------
-- Depth: number of pending writes
---------------------------------------------------------------------------

function M:depth()
    return #self.order
end

return M
