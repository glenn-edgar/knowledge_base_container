/*
 * kb_bit_structures.h
 * Knowledge Base C Library (PostgreSQL) — Bit mask operations + S-expression evaluator
 *
 * Table: bit_mask_table (global)
 * Columns: node_id (varchar PK), bit_mask (bigint)
 *
 * Concurrent write safety:
 *   - get_mask/get: read-only, no lock
 *   - set_mask: FOR UPDATE on row + retry
 *   - set (single bit): FOR UPDATE on row (atomic read-modify-write) + retry
 */

 #ifndef KB_BIT_STRUCTURES_H
 #define KB_BIT_STRUCTURES_H
 
 #include "kb_common.h"
 #include "kb_search.h"
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ================================================================
  * Low-level bit mask operations
  * ================================================================ */
 
 /* Get the current mask value (read-only, no lock) */
 kb_error_t kb_bit_get_mask(kb_conn_t *c, const char *database,
                            const char *path, int64_t *mask_out);
 
 /* Set the entire mask (FOR UPDATE + retry) */
 kb_error_t kb_bit_set_mask(kb_conn_t *c, const char *database,
                            const char *path, int64_t mask,
                            int max_retries, int base_delay_ms);
 
 /* Set a single bit (0-63) — atomic read-modify-write with FOR UPDATE + retry */
 kb_error_t kb_bit_set(kb_conn_t *c, const char *database,
                       const char *path, int bit_pos, bool value,
                       int max_retries, int base_delay_ms);
 
 /* Get a single bit (0-63) (read-only, no lock) */
 kb_error_t kb_bit_get(kb_conn_t *c, const char *database,
                       const char *path, int bit_pos, bool *value_out);
 
 /* ================================================================
  * Assembled bit data (from knowledge base properties)
  * ================================================================ */
 
 typedef struct {
     char *name;
     int   bit_position;
 } kb_bit_def_t;
 
 typedef struct {
     kb_bit_def_t *defs;
     int           count;
 } kb_bit_defs_t;
 
 kb_error_t kb_bit_assemble_data(kb_search_t *ks, const char *path,
                                 kb_bit_defs_t **defs_out);
 void       kb_bit_defs_free(kb_bit_defs_t *defs);
 
 /* ================================================================
  * S-expression evaluator
  * ================================================================
  *
  * Evaluates boolean expressions over bit masks.
  * Supported forms:
  *   ("and" expr1 expr2 ...)
  *   ("or"  expr1 expr2 ...)
  *   ("not" expr)
  *   ("bit" <n>)              — true if bit is set
  *   ("bit_changed" <n>)      — true if bit changed since last eval
  *
  * S-expressions are passed as JSON arrays:
  *   ["and", ["bit", "sensor_a"], ["not", ["bit", "sensor_b"]]]
  */
 
 kb_error_t kb_bit_eval_sexpr(kb_conn_t *c, const char *database,
                              const char *path,
                              const char *sexpr_json,
                              const kb_bit_defs_t *defs,
                              int64_t prev_mask,
                              bool *result_out);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* KB_BIT_STRUCTURES_H */