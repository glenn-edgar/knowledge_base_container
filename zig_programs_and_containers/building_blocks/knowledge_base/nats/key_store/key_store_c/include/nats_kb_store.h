/**
 * @file nats_kb_store.h
 * @brief Knowledge-Base extension for KeyStore
 *
 * Stores structured label + node data as JSON arrays under composite keys
 * of the form  base_topic.label_name.node_name.
 */

#ifndef NATS_KB_STORE_H
#define NATS_KB_STORE_H

#include "nats_key_store.h"
#include <cjson/cJSON.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  KbStore handle  (wraps a KeyStore)                                 */
/* ------------------------------------------------------------------ */

typedef struct KbStore KbStore;

/**
 * Create a KbStore.  Internally creates its own KeyStore.
 */
ks_status_t kb_create(KbStore **out,
                      const char *server,
                      const char *bucket,
                      const char *description);

void kb_destroy(KbStore *kb);

/** Access the underlying KeyStore (e.g. for connect/disconnect). */
KeyStore *kb_get_keystore(KbStore *kb);

/* ------------------------------------------------------------------ */
/*  Validation helpers (return KS_OK or KS_ERR_INVALID_ARG)           */
/* ------------------------------------------------------------------ */

ks_status_t kb_validate_topic(const char *base_topic);
ks_status_t kb_validate_label_name(const char *label_name);
ks_status_t kb_validate_node_name(const char *node_name);

/* ------------------------------------------------------------------ */
/*  KB entry: label_json + node_json                                   */
/* ------------------------------------------------------------------ */

typedef struct {
    char *label_json;   /* caller must free */
    char *node_json;    /* caller must free */
} KbEntry;

void kb_entry_free(KbEntry *e);

/* ------------------------------------------------------------------ */
/*  Core operations                                                    */
/* ------------------------------------------------------------------ */

/**
 * Store a KB key.
 *
 * @param base_topic    Dot-separated base topic.
 * @param label_name    Label identifier.
 * @param node_name     Node identifier.
 * @param label_json    JSON string for the label dict.
 * @param node_json     JSON string for the node dict.
 * @param composite     If true, *out_key receives the full key;
 *                      otherwise it receives a copy of base_topic.
 * @param[out] out_key  Caller must free().  May be NULL.
 */
ks_status_t kb_store(KbStore *kb,
                     const char *base_topic,
                     const char *label_name,
                     const char *node_name,
                     const char *label_json,
                     const char *node_json,
                     bool composite,
                     char **out_key);

/**
 * Retrieve a KB entry.
 */
ks_status_t kb_get(KbStore *kb, const char *kb_key, KbEntry *entry);

/**
 * Delete a KB key.
 */
ks_status_t kb_delete(KbStore *kb, const char *kb_key);

/**
 * Pop the last two segments from a KB key (label + node).
 * @param[out] out  Caller must free().
 */
ks_status_t kb_pop_key(const char *kb_key, char **out);

/**
 * List KB keys, optionally filtered by base topic.
 */
ks_status_t kb_list_keys(KbStore *kb, const char *base_topic,
                         char ***keys, size_t *count);

/**
 * Validate that a key has >= 3 dot-separated segments and each
 * component passes its respective validator.
 */
bool kb_validate_key_format(const char *kb_key);

/* ------------------------------------------------------------------ */
/*  Statistics                                                         */
/* ------------------------------------------------------------------ */

typedef struct {
    size_t total_kb_keys;
    size_t total_topics;
    size_t all_keys_count;
    /* topics and their counts – parallel arrays */
    char  **topic_names;   /* caller must free each + array */
    size_t *topic_counts;  /* caller must free              */
    size_t  topic_array_len;
} KbStats;

ks_status_t kb_get_stats(KbStore *kb, KbStats *stats);
void        kb_stats_free(KbStats *stats);

/* ------------------------------------------------------------------ */
/*  Sync-with-lifecycle helpers                                        */
/* ------------------------------------------------------------------ */

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

#ifdef __cplusplus
}
#endif

#endif /* NATS_KB_STORE_H */

