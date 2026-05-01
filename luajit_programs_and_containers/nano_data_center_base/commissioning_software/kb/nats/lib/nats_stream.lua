--[[
  nats_stream.lua — Persistent circular-buffer stream subscription

  Backed by PubSub (subscription) + KeyStore (JetStream KV persistence).
  Messages are written through to KV on arrival and reloaded on restart.

  Usage:
    local ks = require("lib.nats_key_store").KeyStore.new({
        server = "nats://127.0.0.1:4222", bucket = "streams"
    })
    ks:connect()

    local ps = require("lib.nats_pubsub").PubSub.new({
        server = "nats://127.0.0.1:4222"
    })
    ps:connect()

    local stream = require("lib.nats_stream")
    local buf = stream.StreamBuffer.new(ps, ks, "sensor.temp", 10)

    -- Messages arrive asynchronously and persist to KV...
    local latest = buf:latest()        -- most recent entry or nil
    local all    = buf:entries()       -- ordered oldest→newest
    local n      = buf:count()         -- entries currently stored
    buf:clear()                        -- reset buffer + KV
    buf:stop()                         -- unsubscribe (KV data preserved)
    buf:purge()                        -- delete all KV data
]]

-- ----------------------------------------------------------------
--  StreamBuffer class
-- ----------------------------------------------------------------

local StreamBuffer = {}
StreamBuffer.__index = StreamBuffer

--- Create a new persistent StreamBuffer.
-- Loads existing state from KeyStore, then subscribes to the subject.
-- @param pubsub      connected PubSub instance
-- @param keystore    connected KeyStore instance (JetStream KV)
-- @param subject     string  subject to subscribe to
-- @param size        number  max entries in the circular buffer
-- @param queue_group string or nil  optional queue group
-- @return StreamBuffer
function StreamBuffer.new(pubsub, keystore, subject, size, queue_group)
    assert(pubsub, "pubsub instance required")
    assert(keystore, "keystore instance required")
    assert(subject, "subject required")
    assert(type(size) == "number" and size >= 1, "size must be >= 1")
    size = math.floor(size)

    local self = setmetatable({}, StreamBuffer)
    self._pubsub  = pubsub
    self._ks      = keystore
    self._subject = subject
    self._size    = size
    self._buf     = {}      -- array [1..size], slots reused
    self._head    = 0       -- next write position (0-based, wraps)
    self._count   = 0       -- entries stored (capped at size)
    self._total   = 0       -- total messages received (unbounded)
    self._sub     = nil

    -- Build KV key prefix from subject (dots → underscores)
    self._prefix = subject:gsub("%.", "_")

    -- Load persisted state before subscribing
    self:_load()

    -- Subscribe and wire callback
    self._sub = pubsub:subscribe(subject, function(msg)
        self:_on_message(msg)
    end, queue_group)

    return self
end

-- ----------------------------------------------------------------
--  KV key helpers
-- ----------------------------------------------------------------

function StreamBuffer:_meta_key()
    return self._prefix .. ".__meta"
end

function StreamBuffer:_slot_key(slot_index)
    return self._prefix .. ".__" .. tostring(slot_index)
end

-- ----------------------------------------------------------------
--  Persistence: save / load
-- ----------------------------------------------------------------

function StreamBuffer:_save_meta()
    local json = string.format(
        '{"head":%d,"count":%d,"total":%d,"size":%d}',
        self._head, self._count, self._total, self._size)
    local key = self:_meta_key()
    self._ks:put(key, json)
end

function StreamBuffer:_save_slot(slot_0based, msg)
    local key = self:_slot_key(slot_0based)
    self._ks:put(key, msg.data)
end

function StreamBuffer:_load()
    local key = self:_meta_key()
    local meta_json = self._ks:get(key)
    if not meta_json then return end

    local head  = tonumber(meta_json:match('"head":(-?%d+)'))
    local count = tonumber(meta_json:match('"count":(%d+)'))
    local total = tonumber(meta_json:match('"total":(%d+)'))
    local saved_size = tonumber(meta_json:match('"size":(%d+)'))

    if not head or not count then return end

    -- If buffer size changed since last run, start fresh
    if saved_size ~= self._size then
        self:_delete_kv()
        return
    end

    self._head  = head
    self._count = count
    self._total = total

    -- Reload slot data into in-memory buffer
    local start = head - count
    for i = 0, count - 1 do
        local slot_0 = (start + i) % self._size
        local slot_key = self:_slot_key(slot_0)
        local data = self._ks:get(slot_key)
        if data then
            self._buf[slot_0 + 1] = {
                subject          = self._subject,
                original_subject = self._subject,
                data             = data,
                data_len         = #data,
                reply_to         = nil,
            }
        end
    end
end

-- ----------------------------------------------------------------
--  Message handling
-- ----------------------------------------------------------------

function StreamBuffer:_on_message(msg)
    local slot_0 = self._head % self._size
    self._buf[slot_0 + 1] = msg
    self._head  = self._head + 1
    self._total = self._total + 1
    if self._count < self._size then
        self._count = self._count + 1
    end

    -- Write-through to KV
    self:_save_slot(slot_0, msg)
    self:_save_meta()
end

-- ----------------------------------------------------------------
--  Read API
-- ----------------------------------------------------------------

--- Return the most recent entry, or nil if empty.
function StreamBuffer:latest()
    if self._count == 0 then return nil end
    local slot = ((self._head - 1) % self._size) + 1
    return self._buf[slot]
end

--- Return entries ordered oldest → newest.
-- @return table  array of message tables
function StreamBuffer:entries()
    if self._count == 0 then return {} end
    local out = {}
    local start = self._head - self._count
    for i = 0, self._count - 1 do
        local slot = ((start + i) % self._size) + 1
        out[#out + 1] = self._buf[slot]
    end
    return out
end

--- Return number of entries currently in the buffer.
function StreamBuffer:count()
    return self._count
end

--- Return total messages received since creation (including overwritten).
function StreamBuffer:total_received()
    return self._total
end

--- Return the configured buffer size.
function StreamBuffer:capacity()
    return self._size
end

--- Return true if the buffer is full.
function StreamBuffer:is_full()
    return self._count >= self._size
end

-- ----------------------------------------------------------------
--  Mutation API
-- ----------------------------------------------------------------

--- Delete all KV keys for this stream.
function StreamBuffer:_delete_kv()
    local meta = self:_meta_key()
    pcall(function() self._ks:delete(meta) end)
    for i = 0, self._size - 1 do
        local sk = self:_slot_key(i)
        pcall(function() self._ks:delete(sk) end)
    end
end

--- Clear all entries (in-memory and KV). Subscription stays active.
function StreamBuffer:clear()
    self._buf   = {}
    self._head  = 0
    self._count = 0
    -- total is intentionally NOT reset — it's a lifetime counter
    self:_delete_kv()
    self:_save_meta()
end

--- Unsubscribe from PubSub. KV data is preserved for reload.
function StreamBuffer:stop()
    if self._sub then
        self._pubsub:unsubscribe(self._sub)
        self._sub = nil
    end
end

--- Delete all KV data for this stream. Call after stop() when
-- the stream is no longer needed.
function StreamBuffer:purge()
    self._buf   = {}
    self._head  = 0
    self._count = 0
    self._total = 0
    self:_delete_kv()
end

-- ----------------------------------------------------------------
--  Module
-- ----------------------------------------------------------------

return {
    StreamBuffer = StreamBuffer,
}
