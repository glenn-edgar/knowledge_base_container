/**
 * @file cfl_hbit.h
 * @brief Hierarchical Bit Map Runtime
 * 
 * Memory Model:
 *   - Schema data (nodes, arena info) is const ROM (generated)
 *   - User provides static RAM array sized by generated constant
 *   - cfl_hbit_init() wires up the instance to use that RAM
 *   - No malloc for tree - only controllers use allocator
 */

 #ifndef CFL_HBIT_H
 #define CFL_HBIT_H
 
 #include <stdint.h>
 #include <stdbool.h>
 #include <stddef.h>
 #include <stdarg.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ============================================ */
 /* Buffer Types                                 */
 /* ============================================ */
 
 typedef enum {
     CFL_HBIT_BUF_OR_LATCH = 0,
     CFL_HBIT_BUF_OR_MASK  = 1,
     CFL_HBIT_BUF_AND      = 2,
 } cfl_hbit_buffer_type_t;
 
 /* ============================================ */
 /* Error Codes                                  */
 /* ============================================ */
 
 typedef enum {
     CFL_HBIT_OK = 0,
     CFL_HBIT_ERR_NULL_PTR,
     CFL_HBIT_ERR_INVALID_NODE,
     CFL_HBIT_ERR_INVALID_BUFFER,
     CFL_HBIT_ERR_NOT_LEAF,
     CFL_HBIT_ERR_OUT_OF_RANGE,
 } cfl_hbit_error_t;
 
 /* ============================================ */
 /* ROM Types (from generated code)              */
 /* ============================================ */
 
 typedef struct {
     uint32_t path_hash;
     uint16_t class_index;
     int16_t  parent_index;
     uint16_t child_count;
     uint16_t first_child;
     uint8_t  depth;
     uint8_t  is_leaf;
 } cfl_hbit_node_t;
 
 typedef struct {
     uint16_t offset;
     uint8_t  size;
 } cfl_hbit_arena_info_t;
 
 typedef struct {
     uint32_t hash;
     uint16_t tree_index;
 } cfl_hbit_hash_entry_t;
 
 typedef struct {
     uint8_t                      type;
     uint16_t                     arena_size;
     const cfl_hbit_arena_info_t* arena_info;
 } cfl_hbit_buffer_config_t;
 
 typedef struct {
     uint16_t                        node_count;
     uint16_t                        buffer_count;
     uint16_t                        root_count;
     uint16_t                        ram_size;
     const cfl_hbit_node_t*          nodes;
     const cfl_hbit_hash_entry_t*    nodes_by_hash;
     const uint16_t*                 roots;
     const cfl_hbit_buffer_config_t* buffer_configs;
 } cfl_hbit_config_t;
 
 /* ============================================ */
 /* Allocator                                    */
 /* ============================================ */
 
 typedef void* (*cfl_hbit_alloc_fn)(size_t size, void* ctx);
 typedef void  (*cfl_hbit_free_fn)(void* ptr, void* ctx);
 
 typedef struct {
     cfl_hbit_alloc_fn alloc;
     cfl_hbit_free_fn  free;
     void*             ctx;
 } cfl_hbit_allocator_t;
 
 /* ============================================ */
 /* Runtime Instance                             */
 /* ============================================ */
 
 typedef struct {
     const cfl_hbit_config_t* config;
     cfl_hbit_allocator_t allocator;
     
     /* Buffer pointers (into ram) */
     uint8_t** current;
     uint8_t** shadow;
     uint8_t** latched;
     uint8_t** mask;
     
     /* Dirty tracking */
     uint8_t*  dirty_nodes;
     uint16_t  dirty_count;
     
     /* Allocated RAM block */
     uint8_t*  ram;
 } cfl_hbit_instance_t;
 
 /* ============================================ */
 /* Walk Callback                                */
 /* ============================================ */
 
 typedef bool (*cfl_hbit_walk_fn)(
     cfl_hbit_instance_t* inst,
     uint16_t node_idx,
     void* user_data);
 
 /* ============================================ */
 /* Inline Helpers                               */
 /* ============================================ */
 
 static inline bool cfl_hbit_get_bit(const uint8_t* data, uint8_t bit_idx) {
     return (data[bit_idx >> 3] >> (bit_idx & 7)) & 1;
 }
 
 static inline void cfl_hbit_set_bit(uint8_t* data, uint8_t bit_idx) {
     data[bit_idx >> 3] |= (1 << (bit_idx & 7));
 }
 
 static inline void cfl_hbit_clear_bit(uint8_t* data, uint8_t bit_idx) {
     data[bit_idx >> 3] &= ~(1 << (bit_idx & 7));
 }
 
 static inline bool cfl_hbit_is_dirty(const cfl_hbit_instance_t* inst, uint16_t idx) {
     return (inst->dirty_nodes[idx >> 3] >> (idx & 7)) & 1;
 }
 
 static inline void cfl_hbit_mark_dirty(cfl_hbit_instance_t* inst, uint16_t idx) {
     if (!cfl_hbit_is_dirty(inst, idx)) {
         inst->dirty_nodes[idx >> 3] |= (1 << (idx & 7));
         inst->dirty_count++;
     }
 }
 
 static inline void cfl_hbit_clear_dirty(cfl_hbit_instance_t* inst, uint16_t idx) {
     if (cfl_hbit_is_dirty(inst, idx)) {
         inst->dirty_nodes[idx >> 3] &= ~(1 << (idx & 7));
         inst->dirty_count--;
     }
 }
 
 /* ============================================ */
 /* Core API                                     */
 /* ============================================ */
 
 /**
  * Create instance - allocates instance and RAM via allocator
  * @param allocator User-provided allocator
  * @param config Schema config (ROM, from generated code)
  * @return Instance pointer, or NULL on failure
  */
 cfl_hbit_instance_t* cfl_hbit_create(
     const cfl_hbit_allocator_t* allocator,
     const cfl_hbit_config_t* config);
 
 /**
  * Destroy instance - frees RAM and instance via stored allocator
  */
 void cfl_hbit_destroy(cfl_hbit_instance_t* inst);
 
 void cfl_hbit_reset(cfl_hbit_instance_t* inst);
 
 /* Shadow buffer operations - write to shadow, then sync to current */
 cfl_hbit_error_t cfl_hbit_shadow_set_bit(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit);
 cfl_hbit_error_t cfl_hbit_shadow_clear_bit(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit);
 cfl_hbit_error_t cfl_hbit_shadow_write(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, const uint8_t* data, uint8_t len);
 
 /* Sync shadow to current for all dirty nodes, then propagate */
 void cfl_hbit_sync(cfl_hbit_instance_t* inst);
 
 /* Sync shadow to current for specific buffer */
 void cfl_hbit_sync_buffer(cfl_hbit_instance_t* inst, uint16_t buf);
 
 /* Sync and propagate in one call */
 void cfl_hbit_sync_and_propagate(cfl_hbit_instance_t* inst);
 
 int16_t cfl_hbit_find_node(const cfl_hbit_instance_t* inst, uint32_t hash);
 
 /* Find node by path string (e.g., "VALVE_STATUS.STATION_1.BANK_1") */
 int16_t cfl_hbit_find_node_path(const cfl_hbit_instance_t* inst, const char* fmt, ...);
 
 /* Compute FNV-1a hash of string (for bit name lookup) */
 uint32_t cfl_hbit_hash_string(const char* str);
 
 /* Shadow buffer operations - all writes go through shadow */
 cfl_hbit_error_t cfl_hbit_shadow_set_bit(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit);
 cfl_hbit_error_t cfl_hbit_shadow_clear_bit(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit);
 cfl_hbit_error_t cfl_hbit_shadow_write(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, const uint8_t* data, uint8_t len);
 cfl_hbit_error_t cfl_hbit_shadow_clear(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node);
 cfl_hbit_error_t cfl_hbit_shadow_fill(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t value);
 
 /* Clear/fill all leaves in shadow buffer */
 void cfl_hbit_shadow_clear_all_leaves(cfl_hbit_instance_t* inst, uint16_t buf);
 void cfl_hbit_shadow_fill_all_leaves(cfl_hbit_instance_t* inst, uint16_t buf, uint8_t value);
 
 /* Read current buffer */
 bool cfl_hbit_read_bit(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit);
 uint8_t cfl_hbit_read_node(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t* data, uint8_t max_len);
 
 /* Read latched buffer */
 bool cfl_hbit_read_latched_bit(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit);
 uint8_t cfl_hbit_read_latched_node(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t* data, uint8_t max_len);
 
 /* Clear latched buffer */
 cfl_hbit_error_t cfl_hbit_clear_latch_bit(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit);
 cfl_hbit_error_t cfl_hbit_clear_latch_all(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node);
 
 /* Clear mask buffer */
cfl_hbit_error_t cfl_hbit_clear_mask_all(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node);
 /* Read mask buffer */
 bool cfl_hbit_read_mask_bit(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit);
 uint8_t cfl_hbit_read_mask_node(const cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t* data, uint8_t max_len);
 
 /* Set mask buffer */
 cfl_hbit_error_t cfl_hbit_set_mask(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, const uint8_t* mask, uint8_t len);
 cfl_hbit_error_t cfl_hbit_set_mask_bit(cfl_hbit_instance_t* inst, uint16_t buf, uint16_t node, uint8_t bit, bool enabled);
 
 void cfl_hbit_propagate(cfl_hbit_instance_t* inst);
 void cfl_hbit_propagate_tree(cfl_hbit_instance_t* inst, uint16_t root_idx);
 
 void cfl_hbit_walk_preorder(cfl_hbit_instance_t* inst, uint16_t root, cfl_hbit_walk_fn cb, void* ctx);
 void cfl_hbit_walk_postorder(cfl_hbit_instance_t* inst, uint16_t root, cfl_hbit_walk_fn cb, void* ctx);
 void cfl_hbit_foreach_child(cfl_hbit_instance_t* inst, uint16_t parent, cfl_hbit_walk_fn cb, void* ctx);
 
 /* ============================================ */
 /* Controller                                   */
 /* ============================================ */
 
 typedef struct {
     uint16_t node_idx;
     uint16_t leaf_start;
     uint16_t leaf_count;
     uint16_t bit_start;
     uint16_t bit_count;
 } cfl_hbit_child_t;
 
 typedef struct {
     cfl_hbit_instance_t* inst;
     uint16_t root_node;
     uint16_t buffer_idx;
     uint16_t child_count;
     cfl_hbit_child_t* children;
     uint16_t leaf_count;
     uint16_t* leaf_nodes;
     uint8_t bits_per_leaf;
     uint16_t total_bits;
     cfl_hbit_allocator_t allocator;
     uint8_t* ram;
 } cfl_hbit_controller_t;
 
 cfl_hbit_controller_t* cfl_hbit_controller_create(
     cfl_hbit_instance_t* inst, uint16_t root, uint16_t buf);
 
 void cfl_hbit_controller_destroy(cfl_hbit_controller_t* ctrl);
 
 int16_t cfl_hbit_controller_get_node_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t child_bit, uint8_t* bit);
 int16_t cfl_hbit_controller_get_bitmap_node(cfl_hbit_controller_t* ctrl, uint16_t flat_idx, uint8_t* bit);
 
 /* Shadow write operations (use sync_and_propagate after) */
 cfl_hbit_error_t cfl_hbit_controller_set_bit(cfl_hbit_controller_t* ctrl, uint16_t idx);
 cfl_hbit_error_t cfl_hbit_controller_clear_bit(cfl_hbit_controller_t* ctrl, uint16_t idx);
 cfl_hbit_error_t cfl_hbit_controller_set_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t bit);
 cfl_hbit_error_t cfl_hbit_controller_clear_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t bit);
 
 /* Clear/fill all leaves in shadow under controller */
 void cfl_hbit_controller_clear_all(cfl_hbit_controller_t* ctrl);
 void cfl_hbit_controller_fill_all(cfl_hbit_controller_t* ctrl, uint8_t value);
 
 /* Read current buffer */
 bool cfl_hbit_controller_read_bit(cfl_hbit_controller_t* ctrl, uint16_t idx);
 bool cfl_hbit_controller_read_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t bit);
 
 /* Read latched buffer */
 bool cfl_hbit_controller_read_latched_bit(cfl_hbit_controller_t* ctrl, uint16_t idx);
 bool cfl_hbit_controller_read_latched_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t bit);
 
 /* Clear latched buffer */
 cfl_hbit_error_t cfl_hbit_controller_clear_latch_bit(cfl_hbit_controller_t* ctrl, uint16_t idx);
 cfl_hbit_error_t cfl_hbit_controller_clear_latch_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t bit);
 void cfl_hbit_controller_clear_all_latches(cfl_hbit_controller_t* ctrl);
 
 /* Read mask buffer */
 bool cfl_hbit_controller_read_mask_bit(cfl_hbit_controller_t* ctrl, uint16_t idx);
 bool cfl_hbit_controller_read_mask_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t bit);
 
 /* Set mask buffer */
 cfl_hbit_error_t cfl_hbit_controller_set_mask_bit(cfl_hbit_controller_t* ctrl, uint16_t idx, bool enabled);
 cfl_hbit_error_t cfl_hbit_controller_set_mask_child_bit(cfl_hbit_controller_t* ctrl, uint16_t child, uint16_t bit, bool enabled);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* CFL_HBIT_H */