/**
 * @file nats_key_store.c
 * @brief NATS JetStream Key-Value Store implementation
 */

 #define _POSIX_C_SOURCE 200809L   /* strdup */

 #include "nats_key_store.h"
 
 #include <fnmatch.h>
 #include <inttypes.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 
 /* ------------------------------------------------------------------ */
 /*  Internal struct                                                    */
 /* ------------------------------------------------------------------ */
 
 struct KeyStore {
     KeyStoreConfig  cfg;        /* copies of all strings */
     natsConnection *nc;
     jsCtx          *js;
     kvStore        *kv;
     bool            connected;
     natsStatus      last_nats;  /* last NATS error code  */
 };
 
 /* ------------------------------------------------------------------ */
 /*  Helpers                                                            */
 /* ------------------------------------------------------------------ */
 
 static char *dup_str(const char *s)
 {
     return s ? strdup(s) : NULL;
 }
 
 static void copy_config(KeyStoreConfig *dst, const KeyStoreConfig *src)
 {
     dst->server            = dup_str(src->server);
     dst->bucket            = dup_str(src->bucket);
     dst->description       = dup_str(src->description);
     dst->client_name       = dup_str(src->client_name);
     dst->create_bucket     = src->create_bucket;
     dst->history           = src->history;
     dst->ttl_seconds       = src->ttl_seconds;
     dst->max_reconnect     = src->max_reconnect;
     dst->reconnect_delay_s = src->reconnect_delay_s;
 }
 
 static void free_config_strings(KeyStoreConfig *cfg)
 {
     free((char *)cfg->server);
     free((char *)cfg->bucket);
     free((char *)cfg->description);
     free((char *)cfg->client_name);
     cfg->server = cfg->bucket = cfg->description = cfg->client_name = NULL;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Error string table                                                 */
 /* ------------------------------------------------------------------ */
 
 const char *ks_status_str(ks_status_t st)
 {
     switch (st) {
     case KS_OK:                  return "OK";
     case KS_ERR_INVALID_ARG:     return "invalid argument";
     case KS_ERR_CONNECTION:      return "connection error";
     case KS_ERR_NOT_FOUND:       return "key not found";
     case KS_ERR_BUCKET:          return "bucket error";
     case KS_ERR_ENCODE:          return "encode error";
     case KS_ERR_DECODE:          return "decode error";
     case KS_ERR_MEMORY:          return "out of memory";
     case KS_ERR_RETRY_EXHAUSTED: return "retry exhausted";
     case KS_ERR_NOT_NUMERIC:     return "value is not numeric";
     case KS_ERR_NATS:            return "NATS error";
     default:                     return "unknown error";
     }
 }
 
 /* ------------------------------------------------------------------ */
 /*  Config defaults                                                    */
 /* ------------------------------------------------------------------ */
 
 void ks_config_defaults(KeyStoreConfig *cfg)
 {
     memset(cfg, 0, sizeof(*cfg));
     cfg->server            = "nats://127.0.0.1:4222";
     cfg->bucket            = "keystore";
     cfg->description       = "NATS JetStream KeyStore";
     cfg->client_name       = "keystore-client";
     cfg->create_bucket     = true;
     cfg->history           = 1;
     cfg->ttl_seconds       = 0;
     cfg->max_reconnect     = 3;
     cfg->reconnect_delay_s = 1.0;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Create / Destroy                                                   */
 /* ------------------------------------------------------------------ */
 
 ks_status_t ks_create(KeyStore **out, const KeyStoreConfig *cfg)
 {
     if (!out || !cfg)
         return KS_ERR_INVALID_ARG;
 
     KeyStore *ks = calloc(1, sizeof(*ks));
     if (!ks)
         return KS_ERR_MEMORY;
 
     copy_config(&ks->cfg, cfg);
     *out = ks;
     return KS_OK;
 }
 
 void ks_destroy(KeyStore *ks)
 {
     if (!ks)
         return;
     ks_disconnect(ks);
     free_config_strings(&ks->cfg);
     free(ks);
 }
 
 /* ------------------------------------------------------------------ */
 /*  Connection                                                         */
 /* ------------------------------------------------------------------ */
 
 ks_status_t ks_connect(KeyStore *ks)
 {
     if (!ks)
         return KS_ERR_INVALID_ARG;
     if (ks->connected)
         return KS_OK;
 
     natsOptions *opts = NULL;
     natsStatus   s;
 
     for (int attempt = 0; attempt < ks->cfg.max_reconnect; attempt++) {
 
         s = natsOptions_Create(&opts);
         if (s != NATS_OK) goto retry;
 
         s = natsOptions_SetURL(opts, ks->cfg.server);
         if (s != NATS_OK) goto retry;
 
         s = natsOptions_SetName(opts, ks->cfg.client_name);
         if (s != NATS_OK) goto retry;
 
         s = natsConnection_Connect(&ks->nc, opts);
         if (s != NATS_OK) goto retry;
 
         s = jsOptions_Init(&(jsOptions){0}) == NATS_OK
             ? natsConnection_JetStream(&ks->js, ks->nc, NULL)
             : NATS_ERR;
         if (s != NATS_OK) goto retry;
 
         /* Try to bind to existing bucket */
         s = js_KeyValue(&ks->kv, ks->js, ks->cfg.bucket);
         if (s != NATS_OK) {
             if (!ks->cfg.create_bucket) {
                 ks->last_nats = s;
                 natsOptions_Destroy(opts);
                 return KS_ERR_BUCKET;
             }
 
             /* Create the bucket */
             kvConfig kvc;
             memset(&kvc, 0, sizeof(kvc));
             kvc.Bucket      = ks->cfg.bucket;
             kvc.Description = ks->cfg.description;
             kvc.History     = (int64_t)ks->cfg.history;
             kvc.StorageType = js_MemoryStorage;
             if (ks->cfg.ttl_seconds > 0)
                 kvc.TTL = (int64_t)ks->cfg.ttl_seconds * 1000000000LL; /* ns */
 
             s = js_CreateKeyValue(&ks->kv, ks->js, &kvc);
             if (s != NATS_OK) goto retry;
         }
 
         natsOptions_Destroy(opts);
         ks->connected = true;
         return KS_OK;
 
     retry:
         ks->last_nats = s;
         if (ks->nc) { natsConnection_Destroy(ks->nc); ks->nc = NULL; }
         ks->js = NULL;
         ks->kv = NULL;
         natsOptions_Destroy(opts);
         opts = NULL;
 
         if (attempt < ks->cfg.max_reconnect - 1) {
             nats_Sleep((int64_t)(ks->cfg.reconnect_delay_s * 1000));
         }
     }
 
     return KS_ERR_CONNECTION;
 }
 
 ks_status_t ks_disconnect(KeyStore *ks)
 {
     if (!ks)
         return KS_ERR_INVALID_ARG;
 
     if (ks->kv) {
         kvStore_Destroy(ks->kv);
         ks->kv = NULL;
     }
     if (ks->js) {
         jsCtx_Destroy(ks->js);
         ks->js = NULL;
     }
     if (ks->nc) {
         natsConnection_Close(ks->nc);
         natsConnection_Destroy(ks->nc);
         ks->nc = NULL;
     }
     ks->connected = false;
     return KS_OK;
 }
 
 bool ks_is_connected(const KeyStore *ks)
 {
     return ks && ks->connected;
 }
 
 natsStatus ks_last_nats_status(const KeyStore *ks)
 {
     return ks ? ks->last_nats : NATS_ERR;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Ensure connected helper                                            */
 /* ------------------------------------------------------------------ */
 
 static ks_status_t ensure_connected(KeyStore *ks)
 {
     if (!ks)
         return KS_ERR_INVALID_ARG;
     if (!ks->connected) {
         ks_status_t st = ks_connect(ks);
         if (st != KS_OK)
             return st;
     }
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Put                                                                */
 /* ------------------------------------------------------------------ */
 
 ks_status_t ks_put(KeyStore *ks, const char *key,
                    const char *value, uint64_t *revision)
 {
     if (!key || !value)
         return KS_ERR_INVALID_ARG;
 
     ks_status_t st = ensure_connected(ks);
     if (st != KS_OK) return st;
 
     uint64_t rev = 0;
     natsStatus s = kvStore_PutString(&rev, ks->kv, key, value);
     if (s != NATS_OK) {
         ks->last_nats = s;
         return KS_ERR_NATS;
     }
     if (revision)
         *revision = rev;
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Get                                                                */
 /* ------------------------------------------------------------------ */
 
 ks_status_t ks_get(KeyStore *ks, const char *key, char **value)
 {
     if (!key || !value)
         return KS_ERR_INVALID_ARG;
 
     ks_status_t st = ensure_connected(ks);
     if (st != KS_OK) return st;
 
     kvEntry *entry = NULL;
     natsStatus s = kvStore_Get(&entry, ks->kv, key);
     if (s != NATS_OK) {
         ks->last_nats = s;
         if (s == NATS_NOT_FOUND)
             return KS_ERR_NOT_FOUND;
         return KS_ERR_NATS;
     }
 
     /* Check for deleted/purged entries */
     kvOperation op = kvEntry_Operation(entry);
     if (op == kvOp_Delete || op == kvOp_Purge) {
         kvEntry_Destroy(entry);
         return KS_ERR_NOT_FOUND;
     }
 
     const char *val = kvEntry_ValueString(entry);
     *value = val ? strdup(val) : NULL;
     kvEntry_Destroy(entry);
 
     if (!*value && val)
         return KS_ERR_MEMORY;
     if (!*value)
         return KS_ERR_NOT_FOUND;
 
     return KS_OK;
 }
 
 ks_status_t ks_get_bytes(KeyStore *ks, const char *key,
                          void **data, size_t *len)
 {
     if (!key || !data || !len)
         return KS_ERR_INVALID_ARG;
 
     ks_status_t st = ensure_connected(ks);
     if (st != KS_OK) return st;
 
     kvEntry *entry = NULL;
     natsStatus s = kvStore_Get(&entry, ks->kv, key);
     if (s != NATS_OK) {
         ks->last_nats = s;
         return (s == NATS_NOT_FOUND) ? KS_ERR_NOT_FOUND : KS_ERR_NATS;
     }
 
     kvOperation op = kvEntry_Operation(entry);
     if (op == kvOp_Delete || op == kvOp_Purge) {
         kvEntry_Destroy(entry);
         return KS_ERR_NOT_FOUND;
     }
 
     const void *raw = kvEntry_Value(entry);
     int raw_len = kvEntry_ValueLen(entry);
     if (!raw || raw_len <= 0) {
         kvEntry_Destroy(entry);
         return KS_ERR_NOT_FOUND;
     }
 
     *data = malloc((size_t)raw_len);
     if (!*data) {
         kvEntry_Destroy(entry);
         return KS_ERR_MEMORY;
     }
     memcpy(*data, raw, (size_t)raw_len);
     *len = (size_t)raw_len;
 
     kvEntry_Destroy(entry);
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Delete                                                             */
 /* ------------------------------------------------------------------ */
 
 ks_status_t ks_delete(KeyStore *ks, const char *key)
 {
     if (!key)
         return KS_ERR_INVALID_ARG;
 
     ks_status_t st = ensure_connected(ks);
     if (st != KS_OK) return st;
 
     natsStatus s = kvStore_Delete(ks->kv, key);
     if (s != NATS_OK) {
         ks->last_nats = s;
         return KS_ERR_NATS;
     }
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Exists                                                             */
 /* ------------------------------------------------------------------ */
 
 ks_status_t ks_exists(KeyStore *ks, const char *key, bool *exists)
 {
     if (!key || !exists)
         return KS_ERR_INVALID_ARG;
 
     ks_status_t st = ensure_connected(ks);
     if (st != KS_OK) return st;
 
     kvEntry *entry = NULL;
     natsStatus s = kvStore_Get(&entry, ks->kv, key);
     if (s == NATS_NOT_FOUND) {
         *exists = false;
         return KS_OK;
     }
     if (s != NATS_OK) {
         ks->last_nats = s;
         *exists = false;
         return KS_ERR_NATS;
     }
 
     kvOperation op = kvEntry_Operation(entry);
     *exists = (op != kvOp_Delete && op != kvOp_Purge);
     kvEntry_Destroy(entry);
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Keys                                                               */
 /* ------------------------------------------------------------------ */
 
 ks_status_t ks_keys(KeyStore *ks, const char *pattern,
                     char ***keys_out, size_t *count_out)
 {
     if (!keys_out || !count_out)
         return KS_ERR_INVALID_ARG;
 
     *keys_out = NULL;
     *count_out = 0;
 
     ks_status_t st = ensure_connected(ks);
     if (st != KS_OK) return st;
 
     kvKeysList kl;
     memset(&kl, 0, sizeof(kl));
 
     natsStatus s = kvStore_Keys(&kl, ks->kv, NULL);
     if (s != NATS_OK) {
         ks->last_nats = s;
         /* NATS_NOT_FOUND means no keys – return empty */
         if (s == NATS_NOT_FOUND)
             return KS_OK;
         return KS_ERR_NATS;
     }
 
     /* Allocate worst-case array */
     char **arr = calloc((size_t)kl.Count, sizeof(char *));
     if (!arr) {
         kvKeysList_Destroy(&kl);
         return KS_ERR_MEMORY;
     }
 
     size_t n = 0;
     for (int i = 0; i < kl.Count; i++) {
         const char *k = kl.Keys[i];
         if (!k) continue;
 
         if (pattern && pattern[0] != '\0') {
             if (fnmatch(pattern, k, 0) != 0)
                 continue;
         }
         arr[n] = strdup(k);
         if (!arr[n]) {
             ks_free_keys(arr, n);
             kvKeysList_Destroy(&kl);
             return KS_ERR_MEMORY;
         }
         n++;
     }
 
     kvKeysList_Destroy(&kl);
 
     /* Sort the keys */
     if (n > 1) {
         for (size_t i = 0; i < n - 1; i++) {
             for (size_t j = i + 1; j < n; j++) {
                 if (strcmp(arr[i], arr[j]) > 0) {
                     char *tmp = arr[i];
                     arr[i] = arr[j];
                     arr[j] = tmp;
                 }
             }
         }
     }
 
     *keys_out = arr;
     *count_out = n;
     return KS_OK;
 }
 
 void ks_free_keys(char **keys, size_t count)
 {
     if (!keys) return;
     for (size_t i = 0; i < count; i++)
         free(keys[i]);
     free(keys);
 }
 
 /* ------------------------------------------------------------------ */
 /*  Increment / Decrement                                              */
 /* ------------------------------------------------------------------ */
 
 ks_status_t ks_increment(KeyStore *ks, const char *key,
                          int64_t delta, int64_t *new_value)
 {
     if (!key)
         return KS_ERR_INVALID_ARG;
 
     ks_status_t st = ensure_connected(ks);
     if (st != KS_OK) return st;
 
     for (int attempt = 0; attempt < 20; attempt++) {
         kvEntry *entry = NULL;
         natsStatus s = kvStore_Get(&entry, ks->kv, key);
 
         int64_t current = 0;
         uint64_t revision = 0;
         bool have_entry = false;
 
         if (s == NATS_OK && entry) {
             kvOperation op = kvEntry_Operation(entry);
             if (op != kvOp_Delete && op != kvOp_Purge) {
                 const char *val = kvEntry_ValueString(entry);
                 if (val) {
                     char *endp = NULL;
                     current = strtoll(val, &endp, 10);
                     if (endp == val || *endp != '\0') {
                         kvEntry_Destroy(entry);
                         return KS_ERR_NOT_NUMERIC;
                     }
                 }
                 revision = kvEntry_Revision(entry);
                 have_entry = true;
             }
             kvEntry_Destroy(entry);
         }
 
         int64_t nv = current + delta;
         char buf[32];
         snprintf(buf, sizeof(buf), "%" PRId64, nv);
 
         if (have_entry && revision > 0) {
             uint64_t new_rev = 0;
             s = kvStore_Update(&new_rev, ks->kv, key, buf, (int)strlen(buf), revision);
             if (s == NATS_OK) {
                 if (new_value) *new_value = nv;
                 return KS_OK;
             }
             /* CAS failure – retry */
         } else {
             uint64_t rev = 0;
             s = kvStore_PutString(&rev, ks->kv, key, buf);
             if (s == NATS_OK) {
                 if (new_value) *new_value = nv;
                 return KS_OK;
             }
         }
 
         nats_Sleep((int64_t)(1 + attempt));
     }
 
     return KS_ERR_RETRY_EXHAUSTED;
 }
 
 ks_status_t ks_decrement(KeyStore *ks, const char *key,
                          int64_t delta, int64_t *new_value)
 {
     return ks_increment(ks, key, -delta, new_value);
 }
 
 /* ------------------------------------------------------------------ */
 /*  Sync-with-lifecycle wrappers                                       */
 /* ------------------------------------------------------------------ */
 
 #define SYNC_WRAP_BEGIN(ks)            \
     ks_status_t _st = ks_connect(ks);  \
     if (_st != KS_OK) return _st;
 
 #define SYNC_WRAP_END(ks, result) \
     ks_disconnect(ks);            \
     return (result);
 
 ks_status_t ks_put_sync(KeyStore *ks, const char *key,
                         const char *value, uint64_t *revision)
 {
     SYNC_WRAP_BEGIN(ks);
     ks_status_t r = ks_put(ks, key, value, revision);
     SYNC_WRAP_END(ks, r);
 }
 
 ks_status_t ks_get_sync(KeyStore *ks, const char *key, char **value)
 {
     SYNC_WRAP_BEGIN(ks);
     ks_status_t r = ks_get(ks, key, value);
     SYNC_WRAP_END(ks, r);
 }
 
 ks_status_t ks_delete_sync(KeyStore *ks, const char *key)
 {
     SYNC_WRAP_BEGIN(ks);
     ks_status_t r = ks_delete(ks, key);
     SYNC_WRAP_END(ks, r);
 }
 
 ks_status_t ks_exists_sync(KeyStore *ks, const char *key, bool *exists)
 {
     SYNC_WRAP_BEGIN(ks);
     ks_status_t r = ks_exists(ks, key, exists);
     SYNC_WRAP_END(ks, r);
 }
 
 ks_status_t ks_keys_sync(KeyStore *ks, const char *pattern,
                          char ***keys, size_t *count)
 {
     SYNC_WRAP_BEGIN(ks);
     ks_status_t r = ks_keys(ks, pattern, keys, count);
     SYNC_WRAP_END(ks, r);
 }
 
 ks_status_t ks_increment_sync(KeyStore *ks, const char *key,
                               int64_t delta, int64_t *new_value)
 {
     SYNC_WRAP_BEGIN(ks);
     ks_status_t r = ks_increment(ks, key, delta, new_value);
     SYNC_WRAP_END(ks, r);
 }
 
 ks_status_t ks_decrement_sync(KeyStore *ks, const char *key,
                               int64_t delta, int64_t *new_value)
 {
     SYNC_WRAP_BEGIN(ks);
     ks_status_t r = ks_decrement(ks, key, delta, new_value);
     SYNC_WRAP_END(ks, r);
 }