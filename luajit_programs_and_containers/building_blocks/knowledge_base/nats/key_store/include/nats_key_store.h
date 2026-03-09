/**
 * @file nats_key_store.h
 * @brief NATS JetStream Key-Value Store C API
 *
 * Provides synchronous key-value operations backed by NATS JetStream KV.
 * Features:
 *   - JSON encoding/decoding of values via cJSON
 *   - Atomic increment/decrement of integer keys
 *   - Pattern-based key listing (glob via fnmatch)
 *   - Session-based connection management
 *   - Configurable TTL, history, and reconnect behaviour
 */

#ifndef NATS_KEY_STORE_H
#define NATS_KEY_STORE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <nats/nats.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  Error codes                                                        */
/* ------------------------------------------------------------------ */

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
    KS_ERR_NATS,           /* generic NATS error – check ks_last_nats_status() */
} ks_status_t;

const char *ks_status_str(ks_status_t st);

/* ------------------------------------------------------------------ */
/*  Configuration                                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *server;              /* e.g. "nats://127.0.0.1:4222"   */
    const char *bucket;              /* KV bucket name                  */
    const char *description;         /* bucket description              */
    const char *client_name;         /* NATS client name                */
    bool        create_bucket;       /* create bucket if missing        */
    int         history;             /* number of history entries        */
    int64_t     ttl_seconds;         /* 0 = no TTL                      */
    int         max_reconnect;       /* max reconnect attempts          */
    double      reconnect_delay_s;   /* seconds between attempts        */
} KeyStoreConfig;

/**
 * Fill cfg with sensible defaults.
 */
void ks_config_defaults(KeyStoreConfig *cfg);

/* ------------------------------------------------------------------ */
/*  KeyStore handle                                                    */
/* ------------------------------------------------------------------ */

typedef struct KeyStore KeyStore;

/**
 * Create a new KeyStore (does NOT connect yet).
 */
ks_status_t ks_create(KeyStore **out, const KeyStoreConfig *cfg);

/**
 * Destroy the KeyStore and free all resources.  Disconnects if needed.
 */
void ks_destroy(KeyStore *ks);

/* ------------------------------------------------------------------ */
/*  Connection                                                         */
/* ------------------------------------------------------------------ */

ks_status_t ks_connect(KeyStore *ks);
ks_status_t ks_disconnect(KeyStore *ks);
bool        ks_is_connected(const KeyStore *ks);

/* ------------------------------------------------------------------ */
/*  Core operations                                                    */
/* ------------------------------------------------------------------ */

/**
 * Store a value (JSON-encoded string).
 * @param value  A JSON string (the caller is responsible for encoding).
 * @param[out] revision  If non-NULL, receives the revision number.
 */
ks_status_t ks_put(KeyStore *ks, const char *key,
                   const char *value, uint64_t *revision);

/**
 * Retrieve a value.
 * @param[out] value  Caller must free() the returned string.
 * @return KS_ERR_NOT_FOUND if key does not exist.
 */
ks_status_t ks_get(KeyStore *ks, const char *key, char **value);

/**
 * Retrieve raw bytes.
 * @param[out] data  Caller must free().
 * @param[out] len   Length of returned data.
 */
ks_status_t ks_get_bytes(KeyStore *ks, const char *key,
                         void **data, size_t *len);

/**
 * Delete a key.
 */
ks_status_t ks_delete(KeyStore *ks, const char *key);

/**
 * Check whether a key exists.
 */
ks_status_t ks_exists(KeyStore *ks, const char *key, bool *exists);

/* ------------------------------------------------------------------ */
/*  Key listing                                                        */
/* ------------------------------------------------------------------ */

/**
 * List keys matching an optional glob pattern.
 * @param pattern  NULL or "" for all keys; otherwise fnmatch pattern.
 * @param[out] keys   Caller must free each string AND the array.
 * @param[out] count  Number of keys returned.
 */
ks_status_t ks_keys(KeyStore *ks, const char *pattern,
                    char ***keys, size_t *count);

/**
 * Free a key list returned by ks_keys().
 */
void ks_free_keys(char **keys, size_t count);

/* ------------------------------------------------------------------ */
/*  Atomic counters                                                    */
/* ------------------------------------------------------------------ */

/**
 * Atomically increment an integer key.
 * Creates the key with value delta if it does not exist.
 */
ks_status_t ks_increment(KeyStore *ks, const char *key,
                         int64_t delta, int64_t *new_value);

/**
 * Convenience: ks_increment(ks, key, -delta, new_value).
 */
ks_status_t ks_decrement(KeyStore *ks, const char *key,
                         int64_t delta, int64_t *new_value);

/* ------------------------------------------------------------------ */
/*  Sync-with-lifecycle helpers                                        */
/*  Each call connects, performs the op, and disconnects.               */
/* ------------------------------------------------------------------ */

ks_status_t ks_put_sync(KeyStore *ks, const char *key,
                        const char *value, uint64_t *revision);
ks_status_t ks_get_sync(KeyStore *ks, const char *key, char **value);
ks_status_t ks_delete_sync(KeyStore *ks, const char *key);
ks_status_t ks_exists_sync(KeyStore *ks, const char *key, bool *exists);
ks_status_t ks_keys_sync(KeyStore *ks, const char *pattern,
                         char ***keys, size_t *count);
ks_status_t ks_increment_sync(KeyStore *ks, const char *key,
                              int64_t delta, int64_t *new_value);
ks_status_t ks_decrement_sync(KeyStore *ks, const char *key,
                              int64_t delta, int64_t *new_value);

/* ------------------------------------------------------------------ */
/*  Diagnostics                                                        */
/* ------------------------------------------------------------------ */

natsStatus ks_last_nats_status(const KeyStore *ks);

#ifdef __cplusplus
}
#endif

#endif /* NATS_KEY_STORE_H */

