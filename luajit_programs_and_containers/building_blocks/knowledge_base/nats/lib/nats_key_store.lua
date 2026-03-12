--[[
  nats_key_store.lua — LuaJIT FFI binding for libnats_key_store

  Usage:
    local ks_lib = require("lib.nats_key_store")
    local ks = ks_lib.KeyStore.new({ server = "nats://127.0.0.1:4222", bucket = "my_bucket" })
    ks:connect()
    ks:put("key", '{"data":1}')
    local val = ks:get("key")
    ks:disconnect()
    ks:destroy()
]]

local ffi = require("ffi")

-- ----------------------------------------------------------------
--  C stdlib (needed for free)
-- ----------------------------------------------------------------

ffi.cdef[[
void free(void *ptr);
]]

-- ----------------------------------------------------------------
--  C declarations
-- ----------------------------------------------------------------

ffi.cdef[[
/* ---- Status codes ---- */
typedef enum {
    KS_OK = 0,
    KS_ERR_INVALID_ARG,
    KS_ERR_CONNECTION,
    KS_ERR_NOT_FOUND,
    KS_ERR_BUCKET,
    KS_ERR_ENCODE,
    KS_ERR_DECODE,
    KS_ERR_MEMORY,
    KS_ERR_RETRY_EXHAUSTED,
    KS_ERR_NOT_NUMERIC,
    KS_ERR_NATS,
} ks_status_t;

const char *ks_status_str(ks_status_t st);

/* ---- Configuration ---- */
typedef struct {
    const char *server;
    const char *bucket;
    const char *description;
    const char *client_name;
    bool        create_bucket;
    int         history;
    int64_t     ttl_seconds;
    int         max_reconnect;
    double      reconnect_delay_s;
} KeyStoreConfig;

void ks_config_defaults(KeyStoreConfig *cfg);

/* ---- Opaque handle ---- */
typedef struct KeyStore KeyStore;

/* ---- Lifecycle ---- */
ks_status_t ks_create(KeyStore **out, const KeyStoreConfig *cfg);
void        ks_destroy(KeyStore *ks);
ks_status_t ks_connect(KeyStore *ks);
ks_status_t ks_disconnect(KeyStore *ks);
bool        ks_is_connected(const KeyStore *ks);

/* ---- Core operations ---- */
ks_status_t ks_put(KeyStore *ks, const char *key,
                   const char *value, uint64_t *revision);
ks_status_t ks_get(KeyStore *ks, const char *key, char **value);
ks_status_t ks_get_bytes(KeyStore *ks, const char *key,
                         void **data, size_t *len);
ks_status_t ks_delete(KeyStore *ks, const char *key);
ks_status_t ks_exists(KeyStore *ks, const char *key, bool *exists);

/* ---- Key listing ---- */
ks_status_t ks_keys(KeyStore *ks, const char *pattern,
                    char ***keys, size_t *count);
ks_status_t ks_keys_prefix(KeyStore *ks, const char *prefix,
                           char ***keys, size_t *count);
void        ks_free_keys(char **keys, size_t count);

/* ---- Counters ---- */
ks_status_t ks_increment(KeyStore *ks, const char *key,
                         int64_t delta, int64_t *new_value);
ks_status_t ks_decrement(KeyStore *ks, const char *key,
                         int64_t delta, int64_t *new_value);

/* ---- Sync-with-lifecycle ---- */
ks_status_t ks_put_sync(KeyStore *ks, const char *key,
                        const char *value, uint64_t *revision);
ks_status_t ks_get_sync(KeyStore *ks, const char *key, char **value);
ks_status_t ks_delete_sync(KeyStore *ks, const char *key);
ks_status_t ks_exists_sync(KeyStore *ks, const char *key, bool *exists);
]]

-- ----------------------------------------------------------------
--  Load shared library
-- ----------------------------------------------------------------

local C = ffi.load("nats_key_store")

-- ----------------------------------------------------------------
--  Status check helper
-- ----------------------------------------------------------------

local KS_OK = 0
local KS_ERR_NOT_FOUND = 3

local function check(st, allow_not_found)
    if st == KS_OK then return true end
    if allow_not_found and st == KS_ERR_NOT_FOUND then return false end
    error("KeyStore error: " .. ffi.string(C.ks_status_str(st)), 2)
end

-- ----------------------------------------------------------------
--  KeyStore class
-- ----------------------------------------------------------------

local KeyStore = {}
KeyStore.__index = KeyStore

--- Create a new KeyStore.
-- @param opts table with optional fields:
--   server (string), bucket (string), description (string),
--   client_name (string), create_bucket (bool), history (int),
--   ttl_seconds (int), max_reconnect (int), reconnect_delay_s (number)
-- @return KeyStore object
function KeyStore.new(opts)
    opts = opts or {}
    local cfg = ffi.new("KeyStoreConfig")
    C.ks_config_defaults(cfg)

    if opts.server       then cfg.server            = opts.server end
    if opts.bucket       then cfg.bucket            = opts.bucket end
    if opts.description  then cfg.description       = opts.description end
    if opts.client_name  then cfg.client_name       = opts.client_name end
    if opts.create_bucket ~= nil then cfg.create_bucket = opts.create_bucket end
    if opts.history      then cfg.history           = opts.history end
    if opts.ttl_seconds  then cfg.ttl_seconds       = opts.ttl_seconds end
    if opts.max_reconnect then cfg.max_reconnect    = opts.max_reconnect end
    if opts.reconnect_delay_s then cfg.reconnect_delay_s = opts.reconnect_delay_s end

    local handle = ffi.new("KeyStore*[1]")
    check(C.ks_create(handle, cfg))

    local self = setmetatable({}, KeyStore)
    self._handle = handle[0]
    self._destroyed = false
    return self
end

--- Get the raw C handle (for passing to KbStore/JobQueue).
function KeyStore:handle()
    return self._handle
end

--- Destroy the KeyStore.
function KeyStore:destroy()
    if not self._destroyed and self._handle ~= nil then
        C.ks_destroy(self._handle)
        self._destroyed = true
    end
end

--- Connect to NATS.
function KeyStore:connect()
    check(C.ks_connect(self._handle))
end

--- Disconnect from NATS.
function KeyStore:disconnect()
    check(C.ks_disconnect(self._handle))
end

--- Check if connected.
function KeyStore:is_connected()
    return C.ks_is_connected(self._handle)
end

--- Put a key-value pair.
-- @param key string
-- @param value string (JSON)
-- @return revision (number) or nil
function KeyStore:put(key, value)
    local rev = ffi.new("uint64_t[1]")
    check(C.ks_put(self._handle, key, value, rev))
    return tonumber(rev[0])
end

--- Get a value by key.
-- @param key string
-- @return string or nil if not found
function KeyStore:get(key)
    local val = ffi.new("char*[1]")
    local st = C.ks_get(self._handle, key, val)
    if st == KS_ERR_NOT_FOUND then return nil end
    check(st)
    local result = ffi.string(val[0])
    ffi.C.free(val[0])
    return result
end

--- Get raw bytes by key.
-- @param key string
-- @return string (raw bytes) or nil if not found
function KeyStore:get_bytes(key)
    local data = ffi.new("void*[1]")
    local len  = ffi.new("size_t[1]")
    local st = C.ks_get_bytes(self._handle, key, data, len)
    if st == KS_ERR_NOT_FOUND then return nil end
    check(st)
    local result = ffi.string(data[0], len[0])
    ffi.C.free(data[0])
    return result
end

--- Delete a key.
-- @param key string
function KeyStore:delete(key)
    check(C.ks_delete(self._handle, key))
end

--- Check if a key exists.
-- @param key string
-- @return boolean
function KeyStore:exists(key)
    local found = ffi.new("bool[1]")
    check(C.ks_exists(self._handle, key, found))
    return found[0]
end

--- List keys matching a glob pattern.
-- @param pattern string (e.g. "prefix.*")
-- @return table of strings
function KeyStore:keys(pattern)
    local kk = ffi.new("char**[1]")
    local cnt = ffi.new("size_t[1]")
    check(C.ks_keys(self._handle, pattern, kk, cnt))
    local result = {}
    for i = 0, tonumber(cnt[0]) - 1 do
        result[#result + 1] = ffi.string(kk[0][i])
    end
    C.ks_free_keys(kk[0], cnt[0])
    return result
end

--- List keys with a given prefix.
-- @param prefix string
-- @return table of strings
function KeyStore:keys_prefix(prefix)
    local kk = ffi.new("char**[1]")
    local cnt = ffi.new("size_t[1]")
    check(C.ks_keys_prefix(self._handle, prefix, kk, cnt))
    local result = {}
    for i = 0, tonumber(cnt[0]) - 1 do
        result[#result + 1] = ffi.string(kk[0][i])
    end
    C.ks_free_keys(kk[0], cnt[0])
    return result
end

--- Increment a counter key.
-- @param key string
-- @param delta number (default 1)
-- @return new value (number)
function KeyStore:increment(key, delta)
    delta = delta or 1
    local val = ffi.new("int64_t[1]")
    check(C.ks_increment(self._handle, key, delta, val))
    return tonumber(val[0])
end

--- Decrement a counter key.
-- @param key string
-- @param delta number (default 1)
-- @return new value (number)
function KeyStore:decrement(key, delta)
    delta = delta or 1
    local val = ffi.new("int64_t[1]")
    check(C.ks_decrement(self._handle, key, delta, val))
    return tonumber(val[0])
end

--- Put with auto connect/disconnect.
function KeyStore:put_sync(key, value)
    local rev = ffi.new("uint64_t[1]")
    check(C.ks_put_sync(self._handle, key, value, rev))
    return tonumber(rev[0])
end

--- Get with auto connect/disconnect.
function KeyStore:get_sync(key)
    local val = ffi.new("char*[1]")
    local st = C.ks_get_sync(self._handle, key, val)
    if st == KS_ERR_NOT_FOUND then return nil end
    check(st)
    local result = ffi.string(val[0])
    ffi.C.free(val[0])
    return result
end

--- Delete with auto connect/disconnect.
function KeyStore:delete_sync(key)
    check(C.ks_delete_sync(self._handle, key))
end

--- Exists with auto connect/disconnect.
function KeyStore:exists_sync(key)
    local found = ffi.new("bool[1]")
    check(C.ks_exists_sync(self._handle, key, found))
    return found[0]
end

-- ----------------------------------------------------------------
--  Module
-- ----------------------------------------------------------------

return {
    KeyStore = KeyStore,
    status_str = function(st)
        return ffi.string(C.ks_status_str(st))
    end,
    KS_OK              = 0,
    KS_ERR_INVALID_ARG = 1,
    KS_ERR_CONNECTION  = 2,
    KS_ERR_NOT_FOUND   = 3,
    KS_ERR_BUCKET      = 4,
    KS_ERR_ENCODE      = 5,
    KS_ERR_DECODE      = 6,
    KS_ERR_MEMORY      = 7,
    KS_ERR_RETRY_EXHAUSTED = 8,
    KS_ERR_NOT_NUMERIC = 9,
    KS_ERR_NATS        = 10,
    _C = C,  -- expose raw FFI handle for other modules
}