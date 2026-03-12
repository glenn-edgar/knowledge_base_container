--[[
  nats_kb_store.lua — LuaJIT FFI binding for libnats_kb_store

  Usage:
    local kb_lib = require("lib.nats_kb_store")
    local kb = kb_lib.KbStore.new("nats://127.0.0.1:4222", "my_kb", "Description")
    local ks = kb:get_keystore()   -- returns raw KeyStore* for connect/disconnect
    kb:connect()
    local key = kb:store("topic", "label", "node", label_json, node_json, true)
    local entry = kb:get_entry(key)
    kb:disconnect()
    kb:destroy()
]]

local ffi = require("ffi")

-- ----------------------------------------------------------------
--  C declarations
-- ----------------------------------------------------------------

ffi.cdef[[
/* ---- KbStore opaque handle ---- */
typedef struct KbStore KbStore;

/* ---- KbEntry ---- */
typedef struct {
    char *label_json;
    char *node_json;
} KbEntry;

void kb_entry_free(KbEntry *e);

/* ---- Lifecycle ---- */
ks_status_t kb_create(KbStore **out,
                      const char *server,
                      const char *bucket,
                      const char *description);
void        kb_destroy(KbStore *kb);
KeyStore   *kb_get_keystore(KbStore *kb);

/* ---- Validation ---- */
ks_status_t kb_validate_topic(const char *base_topic);
ks_status_t kb_validate_label_name(const char *label_name);
ks_status_t kb_validate_node_name(const char *node_name);
bool        kb_validate_key_format(const char *kb_key);

/* ---- Core operations ---- */
ks_status_t kb_store(KbStore *kb,
                     const char *base_topic,
                     const char *label_name,
                     const char *node_name,
                     const char *label_json,
                     const char *node_json,
                     bool composite,
                     char **out_key);

ks_status_t kb_get(KbStore *kb, const char *kb_key, KbEntry *entry);
ks_status_t kb_delete(KbStore *kb, const char *kb_key);
ks_status_t kb_pop_key(const char *kb_key, char **out);

ks_status_t kb_list_keys(KbStore *kb, const char *base_topic,
                         char ***keys, size_t *count);

/* ---- Statistics ---- */
typedef struct {
    size_t   total_kb_keys;
    size_t   total_topics;
    size_t   all_keys_count;
    char   **topic_names;
    size_t  *topic_counts;
    size_t   topic_array_len;
} KbStats;

ks_status_t kb_get_stats(KbStore *kb, KbStats *stats);
void        kb_stats_free(KbStats *stats);

/* ---- Sync-with-lifecycle ---- */
ks_status_t kb_store_sync(KbStore *kb,
                          const char *base_topic,
                          const char *label_name,
                          const char *node_name,
                          const char *label_json,
                          const char *node_json,
                          bool composite,
                          char **out_key);
ks_status_t kb_get_sync(KbStore *kb, const char *kb_key, KbEntry *entry);
ks_status_t kb_delete_sync(KbStore *kb, const char *kb_key);
ks_status_t kb_list_keys_sync(KbStore *kb, const char *base_topic,
                              char ***keys, size_t *count);
ks_status_t kb_get_stats_sync(KbStore *kb, KbStats *stats);
]]

-- ----------------------------------------------------------------
--  Load shared library
-- ----------------------------------------------------------------

local ks_lib = require("lib.nats_key_store")  -- ensure key_store cdef loaded first
local C = ffi.load("nats_kb_store")

-- ----------------------------------------------------------------
--  Helpers
-- ----------------------------------------------------------------

local KS_OK = 0
local KS_ERR_NOT_FOUND = 3

local function check(st, allow_not_found)
    if st == KS_OK then return true end
    if allow_not_found and st == KS_ERR_NOT_FOUND then return false end
    error("KbStore error: " .. ffi.string(ks_lib._C.ks_status_str(st)), 2)
end

-- ----------------------------------------------------------------
--  KbStore class
-- ----------------------------------------------------------------

local KbStore = {}
KbStore.__index = KbStore

--- Create a new KbStore.
-- @param server string  NATS server URL
-- @param bucket string  KV bucket name
-- @param description string (optional)
-- @return KbStore object
function KbStore.new(server, bucket, description)
    local handle = ffi.new("KbStore*[1]")
    check(C.kb_create(handle, server, bucket, description))
    local self = setmetatable({}, KbStore)
    self._handle = handle[0]
    self._destroyed = false
    return self
end

--- Destroy the KbStore.
function KbStore:destroy()
    if not self._destroyed and self._handle ~= nil then
        C.kb_destroy(self._handle)
        self._destroyed = true
    end
end

--- Get the underlying KeyStore handle (for connect/disconnect).
-- Returns a wrapped KeyStore object from nats_key_store module.
function KbStore:get_keystore_handle()
    return C.kb_get_keystore(self._handle)
end

--- Connect the underlying KeyStore.
function KbStore:connect()
    local ks_handle = C.kb_get_keystore(self._handle)
    check(ks_lib._C.ks_connect(ks_handle))
end

--- Disconnect the underlying KeyStore.
function KbStore:disconnect()
    local ks_handle = C.kb_get_keystore(self._handle)
    check(ks_lib._C.ks_disconnect(ks_handle))
end

-- ----------------------------------------------------------------
--  Validation
-- ----------------------------------------------------------------

--- Validate a base topic string.
function KbStore.validate_topic(topic)
    return C.kb_validate_topic(topic) == KS_OK
end

--- Validate a label name.
function KbStore.validate_label_name(name)
    return C.kb_validate_label_name(name) == KS_OK
end

--- Validate a node name.
function KbStore.validate_node_name(name)
    return C.kb_validate_node_name(name) == KS_OK
end

--- Validate that a key has the correct format (>= 3 segments).
function KbStore.validate_key_format(kb_key)
    return C.kb_validate_key_format(kb_key)
end

-- ----------------------------------------------------------------
--  Core operations
-- ----------------------------------------------------------------

--- Store a KB entry.
-- @param base_topic string
-- @param label_name string
-- @param node_name  string
-- @param label_json string (JSON)
-- @param node_json  string (JSON)
-- @param composite  boolean  If true, returned key is full composite key
-- @return key string
function KbStore:store(base_topic, label_name, node_name,
                       label_json, node_json, composite)
    local out = ffi.new("char*[1]")
    check(C.kb_store(self._handle, base_topic, label_name, node_name,
                     label_json, node_json, composite or false, out))
    local key = ffi.string(out[0])
    ffi.C.free(out[0])
    return key
end

--- Get a KB entry.
-- @param kb_key string
-- @return table { label_json = string, node_json = string } or nil
function KbStore:get_entry(kb_key)
    local entry = ffi.new("KbEntry")
    local st = C.kb_get(self._handle, kb_key, entry)
    if st == KS_ERR_NOT_FOUND then return nil end
    check(st)
    local result = {
        label_json = ffi.string(entry.label_json),
        node_json  = ffi.string(entry.node_json),
    }
    C.kb_entry_free(entry)
    return result
end

--- Delete a KB key.
function KbStore:delete(kb_key)
    check(C.kb_delete(self._handle, kb_key))
end

--- Pop the last two segments from a key (label + node).
-- @param kb_key string
-- @return base_topic string
function KbStore.pop_key(kb_key)
    local out = ffi.new("char*[1]")
    check(C.kb_pop_key(kb_key, out))
    local result = ffi.string(out[0])
    ffi.C.free(out[0])
    return result
end

--- List KB keys, optionally filtered by base topic.
-- @param base_topic string or nil
-- @return table of strings
function KbStore:list_keys(base_topic)
    local kk  = ffi.new("char**[1]")
    local cnt = ffi.new("size_t[1]")
    check(C.kb_list_keys(self._handle, base_topic, kk, cnt))
    local result = {}
    local n = tonumber(cnt[0])
    for i = 0, n - 1 do
        result[#result + 1] = ffi.string(kk[0][i])
    end
    -- free using ks_free_keys from key_store lib
    ks_lib._C.ks_free_keys(kk[0], cnt[0])
    return result
end

--- Get statistics.
-- @return table with total_kb_keys, total_topics, topics (table of {name, count})
function KbStore:get_stats()
    local stats = ffi.new("KbStats")
    check(C.kb_get_stats(self._handle, stats))
    local result = {
        total_kb_keys  = tonumber(stats.total_kb_keys),
        total_topics   = tonumber(stats.total_topics),
        all_keys_count = tonumber(stats.all_keys_count),
        topics = {},
    }
    for i = 0, tonumber(stats.topic_array_len) - 1 do
        result.topics[#result.topics + 1] = {
            name  = ffi.string(stats.topic_names[i]),
            count = tonumber(stats.topic_counts[i]),
        }
    end
    C.kb_stats_free(stats)
    return result
end

-- ----------------------------------------------------------------
--  Sync-with-lifecycle operations
-- ----------------------------------------------------------------

function KbStore:store_sync(base_topic, label_name, node_name,
                            label_json, node_json, composite)
    local out = ffi.new("char*[1]")
    check(C.kb_store_sync(self._handle, base_topic, label_name, node_name,
                          label_json, node_json, composite or false, out))
    local key = ffi.string(out[0])
    ffi.C.free(out[0])
    return key
end

function KbStore:get_entry_sync(kb_key)
    local entry = ffi.new("KbEntry")
    local st = C.kb_get_sync(self._handle, kb_key, entry)
    if st == KS_ERR_NOT_FOUND then return nil end
    check(st)
    local result = {
        label_json = ffi.string(entry.label_json),
        node_json  = ffi.string(entry.node_json),
    }
    C.kb_entry_free(entry)
    return result
end

function KbStore:delete_sync(kb_key)
    check(C.kb_delete_sync(self._handle, kb_key))
end

function KbStore:list_keys_sync(base_topic)
    local kk  = ffi.new("char**[1]")
    local cnt = ffi.new("size_t[1]")
    check(C.kb_list_keys_sync(self._handle, base_topic, kk, cnt))
    local result = {}
    for i = 0, tonumber(cnt[0]) - 1 do
        result[#result + 1] = ffi.string(kk[0][i])
    end
    ks_lib._C.ks_free_keys(kk[0], cnt[0])
    return result
end

function KbStore:get_stats_sync()
    local stats = ffi.new("KbStats")
    check(C.kb_get_stats_sync(self._handle, stats))
    local result = {
        total_kb_keys  = tonumber(stats.total_kb_keys),
        total_topics   = tonumber(stats.total_topics),
        all_keys_count = tonumber(stats.all_keys_count),
        topics = {},
    }
    for i = 0, tonumber(stats.topic_array_len) - 1 do
        result.topics[#result.topics + 1] = {
            name  = ffi.string(stats.topic_names[i]),
            count = tonumber(stats.topic_counts[i]),
        }
    end
    C.kb_stats_free(stats)
    return result
end

-- ----------------------------------------------------------------
--  Module
-- ----------------------------------------------------------------

return {
    KbStore = KbStore,
    _C = C,
}

