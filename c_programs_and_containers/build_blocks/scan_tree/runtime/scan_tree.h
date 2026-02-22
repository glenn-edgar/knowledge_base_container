/* scan_tree.h
 *
 * Scan Tree Runtime Library
 *
 * Architecture:
 *   Code generator produces CONST DATA TABLES (st_system_desc_t).
 *   Engine allocates working storage and evaluates the graph.
 *   Handle (st_handle_t) is the single access point.
 */

 #ifndef SCAN_TREE_H
 #define SCAN_TREE_H
 
 #include <stdint.h>
 #include <string.h>
 
 #define ST_RAW_PTR(h, buf_id, type) \
     ((type *)st_raw_current_checked((h), (buf_id), sizeof(type)))
 
 typedef struct st_handle st_handle_t;
 typedef struct st_input_desc st_input_desc_t;
 
 /* VFT signature — all system and user VFTs use this */
 typedef uint8_t (*st_vft_func_t)(uint8_t *state, const st_handle_t *h,
                                   const st_input_desc_t *inputs, uint32_t n_in);
 
 /* =============================================================
  * Const descriptors (generated, never modified at runtime)
  * ============================================================= */
 
 /* Buffer descriptor */
 typedef struct {
     const char *path;
     uint32_t    key;            /* FNV-1a of path */
     uint16_t    size;           /* element count */
     uint16_t    elem_size;      /* sizeof one element */
     uint8_t     is_layer;       /* 1=layer, 0=raw */
     uint8_t     buf_index;      /* index in handle raw_bufs[] or layer_bufs[] */
     uint8_t     level;          /* level number (layers only, 0xFF for raw) */
 } st_buf_desc_t;
 
 /* Input binding descriptor */
 #define ST_ROLE_DEFAULT   0
 #define ST_ROLE_SET       1
 #define ST_ROLE_CLEAR     2
 #define ST_ROLE_THRESHOLD 3
 #define ST_ROLE_BITS      4
 #define ST_ROLE_A         5
 #define ST_ROLE_B         6
 #define ST_ROLE_INPUT     7
 
 struct st_input_desc {
     uint16_t buf_id;            /* index into buf_descs[] */
     uint16_t start;
     uint16_t count;
     uint8_t  role;
 };
 
 /* Graph node descriptor */
 #define ST_MAX_INPUTS 8
 
 typedef struct {
     st_vft_func_t   func;
     uint16_t        output_buf_id;
     uint16_t        output_pos;
     st_input_desc_t inputs[ST_MAX_INPUTS];
     uint8_t         n_inputs;
 } st_node_desc_t;
 
 /* Lookup entry (sorted by key) */
 typedef struct {
     uint32_t key;
     uint16_t buf_id;
 } st_lookup_entry_t;
 
 /* Fuse action callback */
 typedef void (*st_fuse_action_t)(void *user_handle);
 
 /* Fuse table entry (maps node_id to action) */
 typedef struct {
     uint16_t          node_id;
     st_fuse_action_t  action;
 } st_fuse_entry_t;
 
 /* System descriptor — the single const blob the codegen produces */
 typedef struct {
     const char              *name;
     const st_buf_desc_t     *buf_descs;
     uint16_t                 n_bufs;
     const st_node_desc_t    *node_descs;
     uint16_t                 n_nodes;
     const st_lookup_entry_t *lookup;
     uint16_t                 n_lookup;
     uint16_t                 n_raw;
     uint16_t                 n_layer;
     const st_fuse_entry_t   *fuse_table;
     uint16_t                 n_fuses;
 } st_system_desc_t;
 
 /* =============================================================
  * Runtime working storage (allocated by engine)
  * ============================================================= */
 
 typedef struct {
     uint8_t *value;
     uint8_t *not_active;
     uint8_t *shadow;
     int8_t  *states;            /* three-state view: 1=ACTIVE, 0=FAULT, -1=NOT_OP */
     uint16_t size;
 } st_layer_rt_t;
 
 typedef struct {
     void    *current;
     void    *previous;
     uint16_t size;
     uint16_t elem_size;
     uint8_t  changed;
 } st_raw_rt_t;
 
 /* =============================================================
  * Handle
  * ============================================================= */
 
 struct st_handle {
     const st_system_desc_t *desc;
     st_raw_rt_t    *raw_bufs;       /* [desc->n_raw] */
     st_layer_rt_t  *layer_bufs;     /* [desc->n_layer] */
     uint8_t        *node_state;     /* [desc->n_nodes] per-node persistent state */
     uint8_t        *node_dirty;     /* [desc->n_nodes] dirty flags */
     uint8_t        *mem;            /* single allocation block */
     void           *user_handle;    /* application context, passed to fuse callbacks */
 };
 
 /* =============================================================
  * API
  * ============================================================= */
 
 int      st_init(st_handle_t *h, const st_system_desc_t *desc);
 void     st_destroy(st_handle_t *h);
 
 void    *st_raw_current(const st_handle_t *h, uint16_t buf_id);
 void    *st_raw_current_checked(const st_handle_t *h, uint16_t buf_id, uint16_t expected_elem_size);
 int8_t   st_get_state(const st_handle_t *h, uint16_t buf_id, uint16_t pos);
 
 /* Get pointer to three-state array for a layer buffer.
  * Returns stable pointer valid for lifetime of handle.
  * Updated by engine after each st_evaluate/st_cycle.
  * size receives the array length (may be NULL). */
 const int8_t *st_layer_states(const st_handle_t *h, uint16_t buf_id, uint16_t *size);
 
 int32_t  st_lookup_path(const st_handle_t *h, const char *path);
 int32_t  st_lookup_key(const st_handle_t *h, uint32_t key);
 uint32_t st_fnv1a(const char *str);
 
 void     st_swap_raw(st_handle_t *h);
 void     st_mark_dirty(st_handle_t *h);
 void     st_evaluate(st_handle_t *h);
 void     st_cycle(st_handle_t *h);
 
 /* Convenience: get data pointer for any buf_id (raw->current or layer->value) */
 const uint8_t *st_buf_data(const st_handle_t *h, uint16_t buf_id);
 
 /* Fuse action lookup (used internally by st_vft_fuse) */
 st_fuse_action_t st_fuse_lookup(const st_handle_t *h, uint16_t node_id);
 
 /* Debug / diagnostic output */
 void st_print_info(const st_handle_t *h);
 void st_print_layers(const st_handle_t *h);
 
 #include "builtins.h"
 
 #endif