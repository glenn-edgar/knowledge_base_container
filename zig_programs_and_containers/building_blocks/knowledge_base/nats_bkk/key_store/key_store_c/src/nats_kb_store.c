/**
 * @file nats_kb_store.c
 * @brief Knowledge-Base extension for KeyStore – implementation
 */

 #define _POSIX_C_SOURCE 200809L   /* strdup */

 #include "nats_kb_store.h"
 
 #include <ctype.h>
 #include <fnmatch.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <cjson/cJSON.h>
 
 /* ------------------------------------------------------------------ */
 /*  Internal struct                                                    */
 /* ------------------------------------------------------------------ */
 
 struct KbStore {
     KeyStore *ks;
     bool      owns_ks;   /* true → we created it and must destroy it */
 };
 
 /* ------------------------------------------------------------------ */
 /*  Create / Destroy                                                   */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_create(KbStore **out,
                       const char *server,
                       const char *bucket,
                       const char *description)
 {
     if (!out || !server || !bucket)
         return KS_ERR_INVALID_ARG;
 
     KbStore *kb = calloc(1, sizeof(*kb));
     if (!kb)
         return KS_ERR_MEMORY;
 
     KeyStoreConfig cfg;
     ks_config_defaults(&cfg);
     cfg.server        = server;
     cfg.bucket        = bucket;
     cfg.description   = description ? description : "Knowledge Base";
     cfg.create_bucket = true;
 
     ks_status_t st = ks_create(&kb->ks, &cfg);
     if (st != KS_OK) {
         free(kb);
         return st;
     }
     kb->owns_ks = true;
     *out = kb;
     return KS_OK;
 }
 
 void kb_destroy(KbStore *kb)
 {
     if (!kb) return;
     if (kb->owns_ks)
         ks_destroy(kb->ks);
     free(kb);
 }
 
 KeyStore *kb_get_keystore(KbStore *kb)
 {
     return kb ? kb->ks : NULL;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Validation helpers                                                 */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_validate_topic(const char *t)
 {
     if (!t || !*t)
         return KS_ERR_INVALID_ARG;
     if (t[0] == '.' || t[strlen(t) - 1] == '.')
         return KS_ERR_INVALID_ARG;
     if (strstr(t, ".."))
         return KS_ERR_INVALID_ARG;
 
     for (const char *p = t; *p; p++) {
         char c = *p;
         if (isalnum((unsigned char)c) || c == '.' || c == '_' || c == '-')
             continue;
         return KS_ERR_INVALID_ARG;
     }
     return KS_OK;
 }
 
 ks_status_t kb_validate_label_name(const char *n)
 {
     if (!n || !*n)
         return KS_ERR_INVALID_ARG;
     if (strlen(n) > 100)
         return KS_ERR_INVALID_ARG;
 
     for (const char *p = n; *p; p++) {
         char c = *p;
         if (isalnum((unsigned char)c) || c == '_' || c == '-')
             continue;
         return KS_ERR_INVALID_ARG;
     }
     return KS_OK;
 }
 
 ks_status_t kb_validate_node_name(const char *n)
 {
     if (!n || !*n)
         return KS_ERR_INVALID_ARG;
     if (strlen(n) > 100)
         return KS_ERR_INVALID_ARG;
 
     for (const char *p = n; *p; p++) {
         char c = *p;
         if (isalnum((unsigned char)c) || c == '_' || c == '.' || c == '-')
             continue;
         return KS_ERR_INVALID_ARG;
     }
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Entry helpers                                                      */
 /* ------------------------------------------------------------------ */
 
 void kb_entry_free(KbEntry *e)
 {
     if (!e) return;
     free(e->label_json);
     free(e->node_json);
     e->label_json = NULL;
     e->node_json  = NULL;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Pop key (remove last 2 segments)                                   */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_pop_key(const char *kb_key, char **out)
 {
     if (!kb_key || !*kb_key || !out)
         return KS_ERR_INVALID_ARG;
 
     /* Count segments */
     int seg_count = 1;
     for (const char *p = kb_key; *p; p++)
         if (*p == '.') seg_count++;
 
     if (seg_count < 3)
         return KS_ERR_INVALID_ARG;
 
     /* Find the position of the (seg_count - 2)th dot */
     const char *end = kb_key + strlen(kb_key);
     int dots_from_end = 0;
     const char *cut = end;
     while (cut > kb_key) {
         cut--;
         if (*cut == '.') {
             dots_from_end++;
             if (dots_from_end == 2)
                 break;
         }
     }
 
     if (dots_from_end < 2 || cut <= kb_key)
         return KS_ERR_INVALID_ARG;
 
     size_t len = (size_t)(cut - kb_key);
     *out = malloc(len + 1);
     if (!*out) return KS_ERR_MEMORY;
     memcpy(*out, kb_key, len);
     (*out)[len] = '\0';
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Validate key format                                                */
 /* ------------------------------------------------------------------ */
 
 bool kb_validate_key_format(const char *kb_key)
 {
     if (!kb_key || !*kb_key)
         return false;
 
     /* Count segments */
     int seg_count = 1;
     for (const char *p = kb_key; *p; p++)
         if (*p == '.') seg_count++;
 
     if (seg_count < 3)
         return false;
 
     /* Find last two dots to split */
     const char *last_dot = strrchr(kb_key, '.');
     if (!last_dot || last_dot == kb_key) return false;
 
     const char *node_name = last_dot + 1;
 
     /* Find second-to-last dot */
     const char *prev = last_dot - 1;
     while (prev > kb_key && *prev != '.')
         prev--;
     if (*prev == '.')
         prev++;  /* skip the dot to get label start */
 
     /* Extract label_name (between prev and last_dot) */
     size_t label_len = (size_t)(last_dot - prev);
     char *label = malloc(label_len + 1);
     if (!label) return false;
     memcpy(label, prev, label_len);
     label[label_len] = '\0';
 
     /* Extract base_topic */
     size_t base_len = (size_t)(prev - kb_key);
     if (base_len > 0 && kb_key[base_len - 1] == '.')
         base_len--;
     char *base = malloc(base_len + 1);
     if (!base) { free(label); return false; }
     memcpy(base, kb_key, base_len);
     base[base_len] = '\0';
 
     bool valid = (kb_validate_topic(base) == KS_OK &&
                   kb_validate_label_name(label) == KS_OK &&
                   kb_validate_node_name(node_name) == KS_OK);
 
     free(label);
     free(base);
     return valid;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Store                                                              */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_store(KbStore *kb,
                      const char *base_topic,
                      const char *label_name,
                      const char *node_name,
                      const char *label_json,
                      const char *node_json,
                      bool composite,
                      char **out_key)
 {
     if (!kb)
         return KS_ERR_INVALID_ARG;
 
     ks_status_t st;
     if ((st = kb_validate_topic(base_topic)) != KS_OK) return st;
     if ((st = kb_validate_label_name(label_name)) != KS_OK) return st;
     if ((st = kb_validate_node_name(node_name)) != KS_OK) return st;
     if (!label_json || !node_json)
         return KS_ERR_INVALID_ARG;
 
     /* Verify both are valid JSON objects */
     cJSON *lj = cJSON_Parse(label_json);
     if (!lj || !cJSON_IsObject(lj)) {
         cJSON_Delete(lj);
         return KS_ERR_INVALID_ARG;
     }
     cJSON *nj = cJSON_Parse(node_json);
     if (!nj || !cJSON_IsObject(nj)) {
         cJSON_Delete(lj);
         cJSON_Delete(nj);
         return KS_ERR_INVALID_ARG;
     }
 
     /* Build the payload: JSON array [label_dict, node_dict] */
     cJSON *arr = cJSON_CreateArray();
     cJSON_AddItemToArray(arr, lj);
     cJSON_AddItemToArray(arr, nj);
 
     char *payload = cJSON_PrintUnformatted(arr);
     cJSON_Delete(arr);  /* this also frees lj and nj */
 
     if (!payload)
         return KS_ERR_ENCODE;
 
     /* Build the key */
     size_t key_len = strlen(base_topic) + 1 + strlen(label_name) + 1 + strlen(node_name) + 1;
     char *kb_key = malloc(key_len);
     if (!kb_key) {
         free(payload);
         return KS_ERR_MEMORY;
     }
     snprintf(kb_key, key_len, "%s.%s.%s", base_topic, label_name, node_name);
 
     st = ks_put(kb->ks, kb_key, payload, NULL);
     free(payload);
 
     if (st != KS_OK) {
         free(kb_key);
         return st;
     }
 
     if (out_key) {
         if (composite) {
             *out_key = kb_key;
         } else {
             *out_key = strdup(base_topic);
             free(kb_key);
             if (!*out_key)
                 return KS_ERR_MEMORY;
         }
     } else {
         free(kb_key);
     }
 
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Get                                                                */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_get(KbStore *kb, const char *kb_key, KbEntry *entry)
 {
     if (!kb || !kb_key || !*kb_key || !entry)
         return KS_ERR_INVALID_ARG;
 
     memset(entry, 0, sizeof(*entry));
 
     char *raw = NULL;
     ks_status_t st = ks_get(kb->ks, kb_key, &raw);
     if (st != KS_OK)
         return st;
 
     cJSON *arr = cJSON_Parse(raw);
     free(raw);
 
     if (!arr || !cJSON_IsArray(arr) || cJSON_GetArraySize(arr) != 2) {
         cJSON_Delete(arr);
         return KS_ERR_DECODE;
     }
 
     cJSON *label = cJSON_GetArrayItem(arr, 0);
     cJSON *node  = cJSON_GetArrayItem(arr, 1);
 
     if (!cJSON_IsObject(label) || !cJSON_IsObject(node)) {
         cJSON_Delete(arr);
         return KS_ERR_DECODE;
     }
 
     entry->label_json = cJSON_PrintUnformatted(label);
     entry->node_json  = cJSON_PrintUnformatted(node);
     cJSON_Delete(arr);
 
     if (!entry->label_json || !entry->node_json) {
         kb_entry_free(entry);
         return KS_ERR_MEMORY;
     }
 
     return KS_OK;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Delete                                                             */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_delete(KbStore *kb, const char *kb_key)
 {
     if (!kb || !kb_key || !*kb_key)
         return KS_ERR_INVALID_ARG;
     return ks_delete(kb->ks, kb_key);
 }
 
 /* ------------------------------------------------------------------ */
 /*  List keys                                                          */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_list_keys(KbStore *kb, const char *base_topic,
                          char ***keys, size_t *count)
 {
     if (!kb || !keys || !count)
         return KS_ERR_INVALID_ARG;
 
     char *pattern = NULL;
     if (base_topic && *base_topic) {
         ks_status_t st = kb_validate_topic(base_topic);
         if (st != KS_OK) return st;
 
         size_t plen = strlen(base_topic) + 3;
         pattern = malloc(plen);
         if (!pattern) return KS_ERR_MEMORY;
         snprintf(pattern, plen, "%s.*", base_topic);
     }
 
     ks_status_t st = ks_keys(kb->ks, pattern, keys, count);
     free(pattern);
     return st;
 }
 
 /* ------------------------------------------------------------------ */
 /*  Stats                                                              */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_get_stats(KbStore *kb, KbStats *stats)
 {
     if (!kb || !stats)
         return KS_ERR_INVALID_ARG;
 
     memset(stats, 0, sizeof(*stats));
 
     char **all_keys = NULL;
     size_t all_count = 0;
     ks_status_t st = ks_keys(kb->ks, NULL, &all_keys, &all_count);
     if (st != KS_OK)
         return st;
 
     stats->all_keys_count = all_count;
 
     /* Count KB keys (>= 3 segments) and group by topic */
     size_t cap = 16;
     stats->topic_names  = calloc(cap, sizeof(char *));
     stats->topic_counts = calloc(cap, sizeof(size_t));
     if (!stats->topic_names || !stats->topic_counts) {
         ks_free_keys(all_keys, all_count);
         return KS_ERR_MEMORY;
     }
 
     for (size_t i = 0; i < all_count; i++) {
         /* Count dots */
         int dots = 0;
         for (const char *p = all_keys[i]; *p; p++)
             if (*p == '.') dots++;
         if (dots < 2) continue;
 
         stats->total_kb_keys++;
 
         char *base = NULL;
         if (kb_pop_key(all_keys[i], &base) != KS_OK)
             continue;
 
         /* Find or add topic */
         bool found = false;
         for (size_t t = 0; t < stats->topic_array_len; t++) {
             if (strcmp(stats->topic_names[t], base) == 0) {
                 stats->topic_counts[t]++;
                 found = true;
                 break;
             }
         }
         if (!found) {
             if (stats->topic_array_len >= cap) {
                 cap *= 2;
                 stats->topic_names  = realloc(stats->topic_names,  cap * sizeof(char *));
                 stats->topic_counts = realloc(stats->topic_counts, cap * sizeof(size_t));
             }
             stats->topic_names[stats->topic_array_len]  = base;
             stats->topic_counts[stats->topic_array_len] = 1;
             stats->topic_array_len++;
             base = NULL; /* ownership transferred */
         }
         free(base);
     }
 
     stats->total_topics = stats->topic_array_len;
     ks_free_keys(all_keys, all_count);
     return KS_OK;
 }
 
 void kb_stats_free(KbStats *stats)
 {
     if (!stats) return;
     for (size_t i = 0; i < stats->topic_array_len; i++)
         free(stats->topic_names[i]);
     free(stats->topic_names);
     free(stats->topic_counts);
     memset(stats, 0, sizeof(*stats));
 }
 
 /* ------------------------------------------------------------------ */
 /*  Sync-with-lifecycle wrappers                                       */
 /* ------------------------------------------------------------------ */
 
 ks_status_t kb_store_sync(KbStore *kb,
                           const char *base_topic,
                           const char *label_name,
                           const char *node_name,
                           const char *label_json,
                           const char *node_json,
                           bool composite,
                           char **out_key)
 {
     if (!kb) return KS_ERR_INVALID_ARG;
     ks_status_t st = ks_connect(kb->ks);
     if (st != KS_OK) return st;
     st = kb_store(kb, base_topic, label_name, node_name,
                   label_json, node_json, composite, out_key);
     ks_disconnect(kb->ks);
     return st;
 }
 
 ks_status_t kb_get_sync(KbStore *kb, const char *kb_key, KbEntry *entry)
 {
     if (!kb) return KS_ERR_INVALID_ARG;
     ks_status_t st = ks_connect(kb->ks);
     if (st != KS_OK) return st;
     st = kb_get(kb, kb_key, entry);
     ks_disconnect(kb->ks);
     return st;
 }
 
 ks_status_t kb_delete_sync(KbStore *kb, const char *kb_key)
 {
     if (!kb) return KS_ERR_INVALID_ARG;
     ks_status_t st = ks_connect(kb->ks);
     if (st != KS_OK) return st;
     st = kb_delete(kb, kb_key);
     ks_disconnect(kb->ks);
     return st;
 }
 
 ks_status_t kb_list_keys_sync(KbStore *kb, const char *base_topic,
                               char ***keys, size_t *count)
 {
     if (!kb) return KS_ERR_INVALID_ARG;
     ks_status_t st = ks_connect(kb->ks);
     if (st != KS_OK) return st;
     st = kb_list_keys(kb, base_topic, keys, count);
     ks_disconnect(kb->ks);
     return st;
 }
 
 ks_status_t kb_get_stats_sync(KbStore *kb, KbStats *stats)
 {
     if (!kb) return KS_ERR_INVALID_ARG;
     ks_status_t st = ks_connect(kb->ks);
     if (st != KS_OK) return st;
     st = kb_get_stats(kb, stats);
     ks_disconnect(kb->ks);
     return st;
 }