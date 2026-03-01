/*
 * kb_document.h
 * Knowledge Base C Library (PostgreSQL) — JSONB document operations
 *
 * Mirrors LuaJIT kb_document_table.lua (545 lines).
 * Table: {database}_document
 * Columns: id, ltree (ltree), type (text), data (jsonb), updated_at
 *
 * Provides:
 *   - Core JSONB read:  get (whole doc, key, nested path, as text)
 *   - Core JSONB write: set (whole doc, key, nested path), delete key/path
 *   - Key existence:    has_key, has_any_keys, has_all_keys
 *   - Containment:      contains (@>), contained_by (<@)
 *   - JSONPath:         path_exists, path_query
 *   - JSONB filter:     query (data @> filter)
 *   - Array ops:        append, prepend, remove_index, contains, elements
 *   - Queue (FIFO):     enqueue, dequeue, peek, size, is_empty, clear, get_all
 *   - Stack (LIFO):     push, pop
 *   - Metadata:         get_metadata, set_metadata
 *
 * All returned JSON strings are heap-allocated; caller must free().
 * doc_type may be NULL to skip the type filter.
 * json_path uses dot notation: "address.city" → '{address,city}'.
 * Empty json_path ("" or NULL) means operate on the entire data field.
 */

#ifndef KB_DOCUMENT_H
#define KB_DOCUMENT_H

#include "kb_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ================================================================
 * Core JSONB Read
 * ================================================================ */

/*
 * Get a value from the document's JSONB data field.
 *
 * json_path: dot-separated path ("name", "address.city"), or NULL/"" for
 *            entire document.
 * as_text:   true → use ->> / #>> (returns text), false → -> / #> (JSON).
 * doc_type:  optional type filter, or NULL.
 *
 * Returns heap-allocated JSON/text string. Caller must free().
 */
kb_error_t kb_doc_get(kb_conn_t *c, const char *database,
                      const char *ltree_path, const char *json_path,
                      bool as_text, const char *doc_type,
                      char **value_out);

/* ================================================================
 * Core JSONB Write
 * ================================================================ */

/*
 * Set a value in the document.
 *
 * json_path: dot-separated path, or NULL/"" to replace entire data.
 * value_json: JSON-encoded value string.
 * create_missing: if true, create intermediate keys.
 */
kb_error_t kb_doc_set(kb_conn_t *c, const char *database,
                      const char *ltree_path, const char *json_path,
                      const char *value_json, bool create_missing,
                      const char *doc_type);

/* Delete a top-level key (data - 'key') */
kb_error_t kb_doc_delete_key(kb_conn_t *c, const char *database,
                             const char *ltree_path, const char *key,
                             const char *doc_type);

/* Delete a nested path (data #- '{a,b}') */
kb_error_t kb_doc_delete_path(kb_conn_t *c, const char *database,
                              const char *ltree_path, const char *json_path,
                              const char *doc_type);

/* ================================================================
 * Key Existence
 * ================================================================ */

/* jsonb_exists(data, key) */
kb_error_t kb_doc_has_key(kb_conn_t *c, const char *database,
                          const char *ltree_path, const char *key,
                          const char *doc_type, bool *result_out);

/* jsonb_exists_any(data, ARRAY[...]) */
kb_error_t kb_doc_has_any_keys(kb_conn_t *c, const char *database,
                               const char *ltree_path,
                               const char **keys, int nkeys,
                               const char *doc_type, bool *result_out);

/* jsonb_exists_all(data, ARRAY[...]) */
kb_error_t kb_doc_has_all_keys(kb_conn_t *c, const char *database,
                               const char *ltree_path,
                               const char **keys, int nkeys,
                               const char *doc_type, bool *result_out);

/* ================================================================
 * Containment
 * ================================================================ */

/* data @> contained_json */
kb_error_t kb_doc_contains(kb_conn_t *c, const char *database,
                           const char *ltree_path,
                           const char *contained_json,
                           const char *doc_type, bool *result_out);

/* data <@ container_json */
kb_error_t kb_doc_contained_by(kb_conn_t *c, const char *database,
                               const char *ltree_path,
                               const char *container_json,
                               const char *doc_type, bool *result_out);

/* ================================================================
 * JSONPath Operations
 * ================================================================ */

/* jsonb_path_exists(data, jsonpath) */
kb_error_t kb_doc_path_exists(kb_conn_t *c, const char *database,
                              const char *ltree_path,
                              const char *jsonpath_query,
                              const char *doc_type, bool *result_out);

/* jsonb_path_query_array(data, jsonpath) → JSON array string */
kb_error_t kb_doc_path_query(kb_conn_t *c, const char *database,
                             const char *ltree_path,
                             const char *jsonpath_query,
                             const char *doc_type, char **json_out);

/* data @> filter (returns full row if match) */
kb_error_t kb_doc_query(kb_conn_t *c, const char *database,
                        const char *ltree_path,
                        const char *jsonb_filter,
                        const char *doc_type,
                        kb_resultset_t **rs_out);

/* ================================================================
 * Array Operations
 * ================================================================ */

/* Append item to array at json_path */
kb_error_t kb_doc_array_append(kb_conn_t *c, const char *database,
                               const char *ltree_path,
                               const char *json_path,
                               const char *item_json,
                               const char *doc_type);

/* Prepend item to array at json_path */
kb_error_t kb_doc_array_prepend(kb_conn_t *c, const char *database,
                                const char *ltree_path,
                                const char *json_path,
                                const char *item_json,
                                const char *doc_type);

/* Remove item at index, returns removed item JSON. Caller must free. */
kb_error_t kb_doc_array_remove_index(kb_conn_t *c, const char *database,
                                     const char *ltree_path,
                                     const char *json_path, int index,
                                     const char *doc_type,
                                     char **removed_out);

/* Check if array at json_path contains item */
kb_error_t kb_doc_array_contains(kb_conn_t *c, const char *database,
                                 const char *ltree_path,
                                 const char *json_path,
                                 const char *item_json,
                                 const char *doc_type, bool *result_out);

/* Get array elements as result set (one row per element) */
kb_error_t kb_doc_array_elements(kb_conn_t *c, const char *database,
                                 const char *ltree_path,
                                 const char *json_path,
                                 const char *doc_type,
                                 kb_resultset_t **rs_out);

/* ================================================================
 * Queue (FIFO) / Stack (LIFO) Abstractions
 * ================================================================
 * queue_path defaults to "items" if NULL.
 */

kb_error_t kb_doc_enqueue(kb_conn_t *c, const char *database,
                          const char *ltree_path, const char *item_json,
                          const char *queue_path, const char *doc_type);

kb_error_t kb_doc_dequeue(kb_conn_t *c, const char *database,
                          const char *ltree_path,
                          const char *queue_path, const char *doc_type,
                          char **item_out);

kb_error_t kb_doc_peek(kb_conn_t *c, const char *database,
                       const char *ltree_path,
                       const char *queue_path, int index,
                       const char *doc_type, char **item_out);

kb_error_t kb_doc_queue_size(kb_conn_t *c, const char *database,
                             const char *ltree_path,
                             const char *queue_path, const char *doc_type,
                             int *size_out);

kb_error_t kb_doc_queue_is_empty(kb_conn_t *c, const char *database,
                                 const char *ltree_path,
                                 const char *queue_path, const char *doc_type,
                                 bool *empty_out);

kb_error_t kb_doc_queue_clear(kb_conn_t *c, const char *database,
                              const char *ltree_path,
                              const char *queue_path, const char *doc_type);

kb_error_t kb_doc_queue_get_all(kb_conn_t *c, const char *database,
                                const char *ltree_path,
                                const char *queue_path, const char *doc_type,
                                char **json_out);

/* Stack: push = prepend, pop = remove last */
kb_error_t kb_doc_push(kb_conn_t *c, const char *database,
                       const char *ltree_path, const char *item_json,
                       const char *queue_path, const char *doc_type);

kb_error_t kb_doc_pop(kb_conn_t *c, const char *database,
                      const char *ltree_path,
                      const char *queue_path, const char *doc_type,
                      char **item_out);

/* Metadata */
kb_error_t kb_doc_get_metadata(kb_conn_t *c, const char *database,
                               const char *ltree_path,
                               const char *metadata_path,
                               const char *doc_type, char **json_out);

kb_error_t kb_doc_set_metadata(kb_conn_t *c, const char *database,
                               const char *ltree_path,
                               const char *metadata_path,
                               const char *metadata_json,
                               const char *doc_type);

#ifdef __cplusplus
}
#endif

#endif /* KB_DOCUMENT_H */
