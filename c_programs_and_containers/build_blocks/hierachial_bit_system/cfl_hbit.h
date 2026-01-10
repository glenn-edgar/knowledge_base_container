/**
 * @file cfl_hbit2_api.h
 * @brief ChainTree Hierarchical Bit Map - Clean API v2
 *
 * Self-contained header - do NOT include cfl_hbit.h
 *
 * Design:
 *   1. cfl_hbit2_node() - ONE path lookup, returns node_id
 *   2. cfl_hbit2_bitspace() - lookup bitspace by name, returns bs_id  
 *   3. All operations use (tree, node, bs_id, ...) pattern
 *
 * Node types:
 *   - Leaf: actual devices, can read/write bits
 *   - Aggregate: virtual groupings, read-only (computed by propagate)
 */

 #ifndef CFL_HBIT2_API_H
 #define CFL_HBIT2_API_H
 
 #include <stdint.h>
 #include <stdbool.h>
 #include <stddef.h>
 #include <stdarg.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ============================================ */
 /* Configuration                                */
 /* ============================================ */
 
 #ifndef CFL_HBIT_MAX_PATH_LEN
 #define CFL_HBIT_MAX_PATH_LEN 128
 #endif
 
 #ifndef CFL_HBIT_DEFAULT_CALLBACK_CAPACITY
 #define CFL_HBIT_DEFAULT_CALLBACK_CAPACITY 8
 #endif
 
 /* ============================================ */
 /* Internal Types (needed for tree structure)   */
 /* ============================================ */
 
 /* Custom allocator */
 typedef void* (*cfl_hbit_alloc_fn)(void* handle, size_t size);
 typedef void  (*cfl_hbit_free_fn)(void* handle, void* ptr);
 
 /* Binary header */
 typedef struct __attribute__((packed)) {
     uint32_t magic;
     uint32_t version;
     uint16_t bitspace_count;
     uint16_t class_count;
     uint16_t node_count;
     uint16_t config_count;
     uint16_t max_depth;
     uint16_t reserved;
     uint32_t bitspaces_offset;
     uint32_t classes_offset;
     uint32_t nodes_offset;
     uint32_t arenas_offset;
     uint32_t config_offset;
     uint32_t strings_offset;
 } cfl_hbit_header_t;
 
 /* Bitspace descriptor */
 typedef struct __attribute__((packed)) {
     uint32_t name_hash;
     uint8_t  merge_type;
     uint8_t  base_merge_type;
     uint8_t  latch;
     uint8_t  clear_requires_inactive;
     uint8_t  priority_count;
     uint8_t  reserved[3];
     uint32_t priority_hashes[8];
 } cfl_hbit_bitspace_desc_t;
 
 /* Node descriptor */
 typedef struct __attribute__((packed)) {
     uint32_t path_hash;
     uint16_t class_index;
     uint16_t depth;
     int32_t  parent_index;
     uint16_t child_count;
     uint16_t reserved;
 } cfl_hbit_node_desc_t;
 
 /* Config entry */
 typedef struct __attribute__((packed)) {
     uint32_t path_hash;
     uint8_t  value_type;
     uint8_t  reserved[3];
     uint32_t value;
 } cfl_hbit_config_entry_t;
 
 /* Arena descriptor in binary */
 typedef struct __attribute__((packed)) {
     uint32_t size;
     uint32_t size_with_latch;
     /* Followed by: uint32_t node_offsets[node_count] */
 } cfl_hbit_arena_desc_t;
 
 /* Forward declarations */
 typedef struct cfl_hbit cfl_hbit_t;
 typedef struct cfl_hbit2_tree cfl_hbit2_tree_t;
 
 /* Internal callback type (not part of public API) */
 typedef void (*cfl_hbit_change_cb_t)(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t node_hash,
     const uint8_t* old_bits,
     const uint8_t* new_bits,
     uint16_t byte_count,
     void* user_data
 );
 
 typedef struct {
     cfl_hbit_change_cb_t callback;
     void* user_data;
     int16_t bitspace_id;
     uint16_t reserved;
 } cfl_hbit_callback_t;
 
 /* Per-bitspace runtime state */
 typedef struct {
     uint8_t* shadow;
     uint8_t* current;
     uint8_t* latch;
     uint8_t* live;
     uint8_t* prev;
     uint32_t size;
 } cfl_hbit_arena_t;
 
 /* Main internal runtime structure */
 struct cfl_hbit {
     void* alloc_handle;
     cfl_hbit_alloc_fn alloc_fn;
     cfl_hbit_free_fn free_fn;
     
     const uint8_t* descriptor;
     uint32_t descriptor_size;
     
     const cfl_hbit_header_t* header;
     const cfl_hbit_bitspace_desc_t* bitspaces;
     const uint8_t* classes_base;
     const uint8_t* nodes_base;
     const uint8_t* arenas_base;
     const cfl_hbit_config_entry_t* config;
     const char* strings;
     
     cfl_hbit_arena_t* arenas;
     
     cfl_hbit_callback_t* callbacks;
     uint8_t callback_count;
     uint8_t callback_capacity;
     
     uint8_t* leaf_shadow_masks;
     uint8_t* leaf_current_masks;
     uint32_t* leaf_mask_offsets;
     uint16_t* leaf_node_indices;
     uint16_t leaf_count;
     uint32_t leaf_mask_total_bytes;
     
     void* memory_block;
     uint32_t memory_block_size;
     
     uint8_t initialized;
     uint8_t dirty;
     uint8_t owns_descriptor;
 };
 
 /* ============================================ */
 /* Public API Types                             */
 /* ============================================ */
 
 /* Tree wraps internal cfl_hbit_t - can be stack allocated */
 typedef struct cfl_hbit2_tree {
     cfl_hbit_t impl;
 } cfl_hbit2_tree_t;
 
 /* Status codes */
 typedef enum {
     CFL_HBIT2_OK = 0,
     CFL_HBIT2_ERR_NULL,
     CFL_HBIT2_ERR_NOT_INIT,
     CFL_HBIT2_ERR_BAD_NODE,
     CFL_HBIT2_ERR_BAD_BITSPACE,
     CFL_HBIT2_ERR_BAD_BIT,
     CFL_HBIT2_ERR_NOT_LEAF,
     CFL_HBIT2_ERR_SIZE_MISMATCH,
     CFL_HBIT2_ERR_SOURCE_ACTIVE,
     CFL_HBIT2_ERR_NO_MEMORY,
     CFL_HBIT2_ERR_BAD_DESCRIPTOR,
 } cfl_hbit2_status_t;
 
 /* Memory info */
 typedef struct {
     uint32_t descriptor_size;
     uint32_t arena_size;
     uint32_t mask_size;
     uint32_t total_ram;
     uint16_t node_count;
     uint16_t leaf_count;
     uint16_t bitspace_count;
 } cfl_hbit2_mem_t;
 
 /* ============================================ */
 /* Initialization                               */
 /* ============================================ */
 
 /**
  * Initialize tree from descriptor.
  */
 cfl_hbit2_status_t cfl_hbit2_init(
     cfl_hbit2_tree_t* tree,
     const uint8_t* desc,
     uint32_t desc_size
 );
 
 /**
  * Initialize from file.
  */
 cfl_hbit2_status_t cfl_hbit2_init_file(
     cfl_hbit2_tree_t* tree,
     const char* path
 );
 
 /**
  * Destroy tree, free resources.
  */
 void cfl_hbit2_destroy(cfl_hbit2_tree_t* tree);
 
 /**
  * Reset all bits to zero.
  */
 void cfl_hbit2_reset(cfl_hbit2_tree_t* tree);
 
 /**
  * Get memory info.
  */
 void cfl_hbit2_mem(cfl_hbit2_tree_t* tree, cfl_hbit2_mem_t* info);
 
 /* ============================================ */
 /* Lookup                                       */
 /* ============================================ */
 
 /**
  * Find node by path. This is THE path lookup function.
  * @return node_id >= 0 on success, -1 if not found
  */
 int32_t cfl_hbit2_node(
     cfl_hbit2_tree_t* tree,
     const char* path_fmt,
     ...
 );
 
 /**
  * Find bitspace by name.
  * @return bs_id >= 0 on success, -1 if not found
  */
 int16_t cfl_hbit2_bitspace(
     cfl_hbit2_tree_t* tree,
     const char* name
 );
 
 /* ============================================ */
 /* Node Info                                    */
 /* ============================================ */
 
 bool cfl_hbit2_info_is_leaf(cfl_hbit2_tree_t* tree, int32_t node);
 int cfl_hbit2_info_bits(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id);
 int cfl_hbit2_info_bytes(cfl_hbit2_tree_t* tree, int32_t node, int16_t bs_id);
 int cfl_hbit2_info_node_count(cfl_hbit2_tree_t* tree);
 int cfl_hbit2_info_bitspace_count(cfl_hbit2_tree_t* tree);
 
 /* ============================================ */
 /* Tree Navigation                              */
 /* ============================================ */
 
 int32_t cfl_hbit2_nav_parent(cfl_hbit2_tree_t* tree, int32_t node);
 int cfl_hbit2_nav_children(cfl_hbit2_tree_t* tree, int32_t node, int32_t* out, int max);
 int cfl_hbit2_nav_child_count(cfl_hbit2_tree_t* tree, int32_t node);
 
 /* ============================================ */
 /* Bit-Level Access                             */
 /* ============================================ */
 
 /**
  * Set a single bit. LEAF ONLY.
  */
 cfl_hbit2_status_t cfl_hbit2_bit_set(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id,
     int bit,
     bool value
 );
 
 /**
  * Get a single bit. Works on any node.
  * @return 1=set, 0=clear, -1=error
  */
 int cfl_hbit2_bit_get(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id,
     int bit
 );
 
 /**
  * Get bit edge since last sync.
  * @return 1=rising, -1=falling, 0=no change
  */
 int cfl_hbit2_bit_edge(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id,
     int bit
 );
 
 /* ============================================ */
 /* Byte-Level Access                            */
 /* ============================================ */
 
 cfl_hbit2_status_t cfl_hbit2_bank_set(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id,
     const uint8_t* data,
     int len
 );
 
 const uint8_t* cfl_hbit2_bank_get(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id
 );
 
 cfl_hbit2_status_t cfl_hbit2_bank_clear(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id
 );
 
 /* ============================================ */
 /* Mask Operations (LEAF ONLY)                  */
 /* ============================================ */
 
 cfl_hbit2_status_t cfl_hbit2_mask_set(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id,
     const uint8_t* mask,
     int len
 );
 
 cfl_hbit2_status_t cfl_hbit2_mask_clear(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id
 );
 
 const uint8_t* cfl_hbit2_mask_get(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id
 );
 
 /* ============================================ */
 /* Latch Operations (LEAF ONLY)                 */
 /* ============================================ */
 
 cfl_hbit2_status_t cfl_hbit2_latch_clear(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id
 );
 
 cfl_hbit2_status_t cfl_hbit2_latch_clear_bits(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id,
     uint32_t bits
 );
 
 void cfl_hbit2_latch_clear_all(
     cfl_hbit2_tree_t* tree,
     int16_t bs_id
 );
 
 const uint8_t* cfl_hbit2_latch_get(
     cfl_hbit2_tree_t* tree,
     int32_t node,
     int16_t bs_id
 );
 
 /* ============================================ */
 /* Synchronization                              */
 /* ============================================ */
 
 void cfl_hbit2_sync(cfl_hbit2_tree_t* tree);
 void cfl_hbit2_swap(cfl_hbit2_tree_t* tree);
 void cfl_hbit2_propagate(cfl_hbit2_tree_t* tree);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* CFL_HBIT2_API_H */