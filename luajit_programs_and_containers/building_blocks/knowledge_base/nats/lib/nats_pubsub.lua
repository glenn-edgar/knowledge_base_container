--[[
  nats_pubsub.lua — LuaJIT FFI binding for libnats_pubsub

  Usage:
    local ps = require("lib.nats_pubsub")
    local pub = ps.PubSub.new({ server = "nats://127.0.0.1:4222" })
    pub:connect()

    -- Subscribe
    local sub = pub:subscribe("sensor.temp", function(msg)
        print("Got:", msg.subject, msg.data)
    end)

    -- Publish
    pub:publish("sensor.temp", '{"value":23.5}')

    -- Request/Reply
    local reply = pub:request("service.echo", "hello", 5.0)

    pub:unsubscribe(sub)
    pub:disconnect()
    pub:destroy()
]]

local ffi = require("ffi")

-- ----------------------------------------------------------------
--  C declarations
-- ----------------------------------------------------------------

ffi.cdef[[
/* ---- PubSub Status codes ---- */
typedef enum {
    PS_OK = 0,
    PS_ERR_INVALID_ARG,
    PS_ERR_CONNECTION,
    PS_ERR_TIMEOUT,
    PS_ERR_MEMORY,
    PS_ERR_NOT_CONNECTED,
    PS_ERR_NATS,
    PS_EMPTY,
} ps_status_t;

const char *ps_status_str(ps_status_t st);

/* ---- Configuration ---- */
typedef struct {
    const char *server;
    const char *namespace_;
    const char *client_name;
} PubSubConfig;

void pubsub_config_defaults(PubSubConfig *cfg);

/* ---- Message ---- */
typedef struct {
    const char *subject;
    const char *original_subject;
    const char *data;
    int         data_len;
    const char *reply_to;
} PubSubMsg;

/* ---- Callback ---- */
typedef void (*pubsub_msg_cb)(const PubSubMsg *msg, void *user_data);

/* ---- Opaque handles ---- */
typedef struct PubSub    PubSub;
typedef struct PubSubSub PubSubSub;

/* ---- Lifecycle ---- */
ps_status_t pubsub_create(PubSub **out, const PubSubConfig *cfg);
void        pubsub_destroy(PubSub *ps);
ps_status_t pubsub_connect(PubSub *ps);
ps_status_t pubsub_disconnect(PubSub *ps);
bool        pubsub_is_connected(const PubSub *ps);
const char *pubsub_namespace(const PubSub *ps);
const char *pubsub_client_name(const PubSub *ps);

/* ---- Publish ---- */
ps_status_t pubsub_publish(PubSub *ps, const char *subject,
                           const void *data, int data_len);
ps_status_t pubsub_publish_str(PubSub *ps, const char *subject,
                               const char *data);

/* ---- Subscribe ---- */
ps_status_t pubsub_subscribe(PubSub *ps, const char *subject,
                             pubsub_msg_cb cb, void *user_data,
                             const char *queue_group,
                             PubSubSub **out);

ps_status_t pubsub_subscribe_raw(PubSub *ps, const char *full_subject,
                                 pubsub_msg_cb cb, void *user_data,
                                 const char *queue_group,
                                 PubSubSub **out);

ps_status_t pubsub_unsubscribe(PubSub *ps, PubSubSub *sub);
ps_status_t pubsub_unsubscribe_all(PubSub *ps);

/* ---- Request/Reply ---- */
ps_status_t pubsub_request(PubSub *ps, const char *subject,
                           const void *data, int data_len,
                           double timeout_s,
                           char **reply_data, int *reply_len);

ps_status_t pubsub_request_str(PubSub *ps, const char *subject,
                               const char *data, double timeout_s,
                               char **reply);

/* ---- Reply to a message ---- */
ps_status_t pubsub_reply(PubSub *ps, const PubSubMsg *msg,
                         const void *data, int data_len);

/* ---- Statistics ---- */
typedef struct {
    int64_t messages_published;
    int64_t messages_received;
    int     active_subscriptions;
} PubSubStats;

ps_status_t pubsub_get_stats(const PubSub *ps, PubSubStats *stats);

/* ---- Queue+poll subscribe (LuaJIT-safe — no ffi.cast on nats.c thread) ---- */
typedef struct {
    char    *subject;
    char    *original_subject;
    uint8_t *data;
    int      data_len;
    char    *reply_to;
} PubSubOwnedMsg;

void pubsub_msg_free(PubSubOwnedMsg *msg);

ps_status_t pubsub_subscribe_queue(PubSub *ps, const char *subject,
                                   size_t queue_depth,
                                   const char *queue_group,
                                   bool raw,
                                   PubSubSub **out_sub);
ps_status_t pubsub_poll(PubSubSub *sub, PubSubOwnedMsg *out_msg);
size_t      pubsub_pending(PubSubSub *sub);
size_t      pubsub_dropped(PubSubSub *sub);
void        pubsub_reset_dropped(PubSubSub *sub);
]]

-- ----------------------------------------------------------------
--  Load shared library
-- ----------------------------------------------------------------

local C = ffi.load("nats_pubsub")

-- ----------------------------------------------------------------
--  Helpers
-- ----------------------------------------------------------------

local PS_OK = 0

local function check(st)
    if st == PS_OK then return true end
    error("PubSub error: " .. ffi.string(C.ps_status_str(st)), 2)
end

-- ----------------------------------------------------------------
--  Keep Lua callbacks alive (prevent GC)
-- ----------------------------------------------------------------

-- (Previously kept ffi.cast callbacks alive; no longer needed — queue+poll
--  replaces the cross-thread callback pattern entirely.)
local _sub_callbacks = {}  -- keyed by PubSubSub* pointer

-- ----------------------------------------------------------------
--  PubSub class
-- ----------------------------------------------------------------

local PubSub = {}
PubSub.__index = PubSub

--- Create a new PubSub client.
-- @param opts table  { server, namespace_, client_name }
function PubSub.new(opts)
    opts = opts or {}
    local cfg = ffi.new("PubSubConfig")
    C.pubsub_config_defaults(cfg)
    if opts.server      then cfg.server      = opts.server end
    if opts.namespace_  then cfg.namespace_   = opts.namespace_ end
    if opts.client_name then cfg.client_name  = opts.client_name end

    local handle = ffi.new("PubSub*[1]")
    check(C.pubsub_create(handle, cfg))
    local self = setmetatable({}, PubSub)
    self._handle = handle[0]
    self._subs = {}  -- track subscription handles for cleanup
    return self
end

--- Destroy the PubSub client.
function PubSub:destroy()
    if self._handle ~= nil then
        C.pubsub_destroy(self._handle)
        self._handle = nil
    end
end

--- Connect to NATS.
function PubSub:connect()
    check(C.pubsub_connect(self._handle))
end

--- Disconnect.
function PubSub:disconnect()
    check(C.pubsub_disconnect(self._handle))
end

--- Check if connected.
function PubSub:is_connected()
    return C.pubsub_is_connected(self._handle)
end

--- Get namespace.
function PubSub:namespace()
    return ffi.string(C.pubsub_namespace(self._handle))
end

--- Get client name.
function PubSub:client_name()
    return ffi.string(C.pubsub_client_name(self._handle))
end

-- ----------------------------------------------------------------
--  Publish
-- ----------------------------------------------------------------

--- Publish raw data to a subject.
-- @param subject  string
-- @param data     string (raw bytes)
function PubSub:publish(subject, data)
    if type(data) == "string" then
        check(C.pubsub_publish(self._handle, subject, data, #data))
    else
        check(C.pubsub_publish(self._handle, subject, data, ffi.sizeof(data)))
    end
end

--- Publish a string to a subject.
-- @param subject string
-- @param str     string
function PubSub:publish_str(subject, str)
    check(C.pubsub_publish_str(self._handle, subject, str))
end

-- ----------------------------------------------------------------
--  Subscribe
-- ----------------------------------------------------------------

--- Subscribe to a subject (namespace auto-prepended).
-- @param subject     string
-- @param callback    function(msg_table) — invoked when PubSub:poll() drains
-- @param queue_group string or nil  (NATS queue group for load balancing)
-- @return subscription handle (opaque, pass to unsubscribe)
--
-- IMPORTANT: callbacks are NO LONGER called from nats.c's dispatch thread.
-- They are invoked synchronously from PubSub:poll(), which the caller must
-- drive from their main loop. This is the LuaJIT-safe pattern that
-- replaces the previous ffi.cast async-callback model.
function PubSub:subscribe(subject, callback, queue_group)
    return self:_do_subscribe(subject, callback, queue_group, false)
end

--- Subscribe to a raw (non-namespaced) subject.
function PubSub:subscribe_raw(full_subject, callback, queue_group)
    return self:_do_subscribe(full_subject, callback, queue_group, true)
end

--- Internal: subscribe via the C queue+poll API.
function PubSub:_do_subscribe(subject, callback, queue_group, raw)
    if type(callback) ~= "function" then
        error("PubSub:subscribe: callback must be a function", 3)
    end
    local sub_handle = ffi.new("PubSubSub*[1]")
    check(C.pubsub_subscribe_queue(self._handle, subject, 64,
                                   queue_group, raw, sub_handle))
    local sub = sub_handle[0]
    self._subs[tostring(sub)] = { handle = sub, callback = callback }
    return sub
end

--- Drain pending messages from all subscriptions, invoking their
--- registered callbacks on the caller's thread.
--
-- @param max_per_sub  optional limit per subscription per call (default unlimited)
-- @return total number of messages dispatched
function PubSub:poll(max_per_sub)
    local total = 0
    local msg = ffi.new("PubSubOwnedMsg")
    for _, info in pairs(self._subs) do
        local dispatched = 0
        while max_per_sub == nil or dispatched < max_per_sub do
            local st = C.pubsub_poll(info.handle, msg)
            if st == C.PS_EMPTY then break end
            if st ~= C.PS_OK then
                error("PubSub:poll: " .. ffi.string(C.ps_status_str(st)), 2)
            end
            local t = {
                subject          = msg.subject ~= nil and ffi.string(msg.subject) or "",
                original_subject = msg.original_subject ~= nil and ffi.string(msg.original_subject) or "",
                data             = msg.data_len > 0 and ffi.string(msg.data, msg.data_len) or "",
                data_len         = msg.data_len,
                reply_to         = msg.reply_to ~= nil and ffi.string(msg.reply_to) or nil,
            }
            C.pubsub_msg_free(msg)
            local ok, err = pcall(info.callback, t)
            if not ok then
                io.stderr:write("PubSub callback error: " .. tostring(err) .. "\n")
            end
            dispatched = dispatched + 1
            total = total + 1
        end
    end
    return total
end

--- Return total pending (undrained) messages across all subscriptions.
function PubSub:pending()
    local total = 0
    for _, info in pairs(self._subs) do
        total = total + tonumber(C.pubsub_pending(info.handle))
    end
    return total
end

--- Unsubscribe from a subscription.
-- @param sub  subscription handle returned by subscribe()
function PubSub:unsubscribe(sub)
    check(C.pubsub_unsubscribe(self._handle, sub))
    self._subs[tostring(sub)] = nil
end

--- Unsubscribe from all subscriptions.
function PubSub:unsubscribe_all()
    check(C.pubsub_unsubscribe_all(self._handle))
    self._subs = {}
end

-- ----------------------------------------------------------------
--  Request / Reply
-- ----------------------------------------------------------------

--- Send a request and wait for a reply (string).
-- @param subject  string
-- @param data     string
-- @param timeout_s number (default 5.0)
-- @return reply string
function PubSub:request(subject, data, timeout_s)
    timeout_s = timeout_s or 5.0
    local reply = ffi.new("char*[1]")
    check(C.pubsub_request_str(self._handle, subject, data, timeout_s, reply))
    local result = ffi.string(reply[0])
    ffi.C.free(reply[0])
    return result
end

--- Send a request with raw bytes and get raw reply.
-- @param subject    string
-- @param data       string (raw bytes)
-- @param timeout_s  number (default 5.0)
-- @return reply string (raw bytes)
function PubSub:request_raw(subject, data, timeout_s)
    timeout_s = timeout_s or 5.0
    local reply_data = ffi.new("char*[1]")
    local reply_len  = ffi.new("int[1]")
    check(C.pubsub_request(self._handle, subject, data, #data,
                           timeout_s, reply_data, reply_len))
    local result = ffi.string(reply_data[0], reply_len[0])
    ffi.C.free(reply_data[0])
    return result
end

--- Reply to a received message (inside a callback).
-- @param raw_msg  the _raw_msg field from the callback msg table
-- @param data     string (reply payload)
function PubSub:reply(raw_msg, data)
    check(C.pubsub_reply(self._handle, raw_msg, data, #data))
end

-- ----------------------------------------------------------------
--  Statistics
-- ----------------------------------------------------------------

--- Get pubsub stats.
-- @return table { messages_published, messages_received, active_subscriptions }
function PubSub:get_stats()
    local stats = ffi.new("PubSubStats")
    check(C.pubsub_get_stats(self._handle, stats))
    return {
        messages_published    = tonumber(stats.messages_published),
        messages_received     = tonumber(stats.messages_received),
        active_subscriptions  = stats.active_subscriptions,
    }
end

-- ----------------------------------------------------------------
--  Module
-- ----------------------------------------------------------------

return {
    PubSub = PubSub,
    status_str = function(st)
        return ffi.string(C.ps_status_str(st))
    end,
    PS_OK              = 0,
    PS_ERR_INVALID_ARG = 1,
    PS_ERR_CONNECTION  = 2,
    PS_ERR_TIMEOUT     = 3,
    PS_ERR_MEMORY      = 4,
    PS_ERR_NOT_CONNECTED = 5,
    PS_ERR_NATS        = 6,
    _C = C,
}

