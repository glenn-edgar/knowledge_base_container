/**
 * @file cfl_hbit.h
 * @brief ChainTree Hierarchical Bit Map Runtime
 * 
 * Zero-copy, deterministic bit propagation engine for embedded systems.
 * Supports OR, AND, PRIORITY, and MASK merge operations with optional
 * latching and shadow/current double-buffering.
 *
 * Usage:
 *   #include "cfl_hbit.h"
 *   #include "generated_MySchema.bin.h"
 *
 *   cfl_hbit_t tree;
 *   cfl_hbit_init(&tree, MySchema_descriptor, MySchema_descriptor_size);
 *   cfl_hbit_set_bit(&tree, BS_ALARM, 0, true, "Plant.Line1.Robot1");
 *   cfl_hbit_sync(&tree);
 */

 #ifndef CFL_HBIT_H
 #define CFL_HBIT_H
 
 #include <stdint.h>
 #include <stdbool.h>
 #include <stddef.h>
 #include <stdarg.h>
 #include "cfl_exception.h"
 
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
 /* Custom Allocator Types                       */
 /* ============================================ */
 
 typedef void* (*cfl_hbit_alloc_fn)(void* handle, size_t size);
 typedef void  (*cfl_hbit_free_fn)(void* handle, void* ptr);
 
 typedef struct {
     void* handle;
     cfl_hbit_alloc_fn alloc;
     cfl_hbit_free_fn free;
 } cfl_hbit_allocator_t;
 
 /* ============================================ */
 /* Status Codes                                 */
 /* ============================================ */
 
 typedef enum {
     CFL_HBIT_OK = 0,
     CFL_HBIT_ERR_NULL_PTR,
     CFL_HBIT_ERR_NOT_INITIALIZED,
     CFL_HBIT_ERR_INVALID_DESCRIPTOR,
     CFL_HBIT_ERR_PATH_NOT_FOUND,
     CFL_HBIT_ERR_BITSPACE_INVALID,
     CFL_HBIT_ERR_BIT_OUT_OF_RANGE,
     CFL_HBIT_ERR_NO_MEMORY,
     CFL_HBIT_ERR_SOURCE_ACTIVE,
     CFL_HBIT_ERR_CALLBACK_FULL,
 } cfl_hbit_status_t;
 
 /* ============================================ */
 /* Merge Types                                  */
 /* ============================================ */
 
 #ifndef CFL_HBIT_MERGE_DEFINED
 #define CFL_HBIT_MERGE_DEFINED
 typedef enum {
     CFL_HBIT_MERGE_OR = 0,
     CFL_HBIT_MERGE_AND = 1,
     CFL_HBIT_MERGE_PRIORITY = 2,
     CFL_HBIT_MERGE_MASK = 3,
 } cfl_hbit_merge_t;
 #endif
 
 /* ============================================ */
 /* Binary Format Structures (ROM)               */
 /* ============================================ */
 
 /* Binary header - first 48 bytes of descriptor */
 typedef struct __attribute__((packed)) {
     uint32_t magic;              /* 0x54494248 "HBIT" */
     uint32_t version;            /* Format version */
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
 
 /* Bitspace descriptor - 48 bytes each */
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
 
 /* Node descriptor in binary - variable size based on bitspace count */
 typedef struct __attribute__((packed)) {
     uint32_t path_hash;
     uint16_t class_index;
     uint16_t depth;
     int32_t  parent_index;       /* -1 if root */
     uint16_t child_count;        /* 0 = leaf node */
     uint16_t reserved;
     /* Note: masks removed from binary - now runtime-only for leaf nodes */
 } cfl_hbit_node_desc_t;
 
 /* Arena descriptor in binary */
 typedef struct __attribute__((packed)) {
     uint32_t size;
     uint32_t size_with_latch;
     /* Followed by: uint32_t node_offsets[node_count] */
 } cfl_hbit_arena_desc_t;
 
 /* Config entry in binary */
 typedef struct __attribute__((packed)) {
     uint32_t path_hash;
     uint8_t  value_type;
     uint8_t  reserved[3];
     uint32_t value;              /* int32, float bits, bool, or string offset */
 } cfl_hbit_config_entry_t;
 
 /* Config value types */
 typedef enum {
     CFL_HBIT_CONFIG_NULL = 0,
     CFL_HBIT_CONFIG_INT = 1,
     CFL_HBIT_CONFIG_FLOAT = 2,
     CFL_HBIT_CONFIG_BOOL = 3,
     CFL_HBIT_CONFIG_STRING = 4,
 } cfl_hbit_config_type_t;
 
 /* ============================================ */
 /* Runtime Structures (RAM)                     */
 /* ============================================ */
 
 /* Forward declaration */
 typedef struct cfl_hbit cfl_hbit_t;
 
 /* Change callback signature */
 typedef void (*cfl_hbit_change_cb_t)(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t node_hash,
     const uint8_t* old_bits,
     const uint8_t* new_bits,
     uint16_t byte_count,
     void* user_data
 );
 
 /* Callback registration */
 typedef struct {
     cfl_hbit_change_cb_t callback;
     void* user_data;
     int16_t bitspace_id;         /* -1 for all bitspaces */
     uint16_t reserved;
 } cfl_hbit_callback_t;
 
 /* Per-bitspace runtime state */
 typedef struct {
     uint8_t* shadow;             /* Write buffer */
     uint8_t* current;            /* Read buffer (stable) */
     uint8_t* latch;              /* Latched state (if latch enabled) */
     uint8_t* live;               /* Live state before latch (if latch enabled) */
     uint8_t* prev;               /* Previous state for change detection */
     uint32_t size;               /* Bytes per arena */
 } cfl_hbit_arena_t;
 
 /* Main runtime structure */
 struct cfl_hbit {
     /* Custom allocator (NULL = use stdlib malloc/free) */
     void* alloc_handle;
     cfl_hbit_alloc_fn alloc_fn;
     cfl_hbit_free_fn free_fn;
     
     /* Descriptor pointer (ROM) */
     const uint8_t* descriptor;
     uint32_t descriptor_size;
     
     /* Parsed header info */
     const cfl_hbit_header_t* header;
     const cfl_hbit_bitspace_desc_t* bitspaces;
     const uint8_t* classes_base;
     const uint8_t* nodes_base;
     const uint8_t* arenas_base;
     const cfl_hbit_config_entry_t* config;
     const char* strings;
     
     /* Runtime arenas (dynamically allocated, count = bitspace_count) */
     cfl_hbit_arena_t* arenas;
     
     /* Callbacks (dynamically allocated) */
     cfl_hbit_callback_t* callbacks;
     uint8_t callback_count;
     uint8_t callback_capacity;
     
     /* Leaf node masks (shadow/current, sized to bank, only for leaf nodes) */
     uint8_t* leaf_shadow_masks;   /* All leaf masks (shadow buffer) */
     uint8_t* leaf_current_masks;  /* All leaf masks (current buffer) */
     uint32_t* leaf_mask_offsets;  /* Offset into mask buffer per leaf node */
     uint16_t* leaf_node_indices;  /* Map leaf index -> node index */
     uint16_t leaf_count;          /* Number of leaf nodes */
     uint32_t leaf_mask_total_bytes; /* Total mask bytes per buffer */
     
     /* Memory block (single allocation for all runtime data) */
     void* memory_block;
     uint32_t memory_block_size;
     
     /* State flags */
     uint8_t initialized;
     uint8_t dirty;               /* Shadow modified since last sync */
     uint8_t owns_descriptor;     /* True if descriptor was loaded from file */
 };
 
 /* ============================================ */
 /* Initialization and Lifecycle                 */
 /* ============================================ */
 
 /**
  * Initialize with custom allocator.
  *
  * @param tree      Output tree instance
  * @param desc      Binary descriptor data (from generated .bin.h)
  * @param desc_size Size of descriptor in bytes
  * @param allocator Custom allocator (NULL = use stdlib malloc/free)
  * @return CFL_HBIT_OK on success
  */
 cfl_hbit_status_t cfl_hbit_init(
     cfl_hbit_t* tree,
     const uint8_t* desc,
     uint32_t desc_size,
     const cfl_hbit_allocator_t* allocator
 );
 
 /**
  * Initialize with external memory pool (no allocation).
  *
  * @param tree      Output tree instance
  * @param desc      Binary descriptor data
  * @param desc_size Size of descriptor
  * @param pool      External memory pool
  * @param pool_size Size of memory pool
  * @return CFL_HBIT_OK on success
  */
 cfl_hbit_status_t cfl_hbit_init_static(
     cfl_hbit_t* tree,
     const uint8_t* desc,
     uint32_t desc_size,
     uint8_t* pool,
     uint32_t pool_size
 );
 
 /**
  * Get required memory size for static initialization.
  *
  * @param desc      Binary descriptor data
  * @param desc_size Size of descriptor
  * @return Required bytes, or 0 on error
  */
 uint32_t cfl_hbit_required_memory(
     const uint8_t* desc,
     uint32_t desc_size
 );
 
 /**
  * Load binary descriptor from file.
  * Uses the tree's allocator if set, otherwise stdlib malloc.
  * Caller must free with cfl_hbit_free_descriptor().
  *
  * @param tree      Tree instance (for allocator, can be partially initialized)
  * @param path      File path to .bin file
  * @param out_data  Output pointer to loaded data
  * @param out_size  Output size in bytes
  * @return CFL_HBIT_OK on success
  */
 cfl_hbit_status_t cfl_hbit_load_descriptor(
     cfl_hbit_t* tree,
     const char* path,
     uint8_t** out_data,
     uint32_t* out_size
 );
 
 /**
  * Free descriptor loaded with cfl_hbit_load_descriptor().
  *
  * @param tree      Tree instance (for allocator)
  * @param data      Data pointer returned by cfl_hbit_load_descriptor()
  */
 void cfl_hbit_free_descriptor(
     cfl_hbit_t* tree,
     uint8_t* data
 );
 
 /**
  * Initialize from file path (convenience wrapper).
  * Loads descriptor and initializes tree in one call.
  * The descriptor is stored in memory_block and freed on destroy.
  *
  * @param tree      Output tree instance
  * @param path      File path to .bin file
  * @param allocator Custom allocator (NULL = use stdlib)
  * @return CFL_HBIT_OK on success
  */
 cfl_hbit_status_t cfl_hbit_init_from_file(
     cfl_hbit_t* tree,
     const char* path,
     const cfl_hbit_allocator_t* allocator
 );
 
 /**
  * Destroy tree and free memory.
  */
 void cfl_hbit_destroy(cfl_hbit_t* tree);
 
 /**
  * Reset all bits to zero.
  */
 void cfl_hbit_reset(cfl_hbit_t* tree);
 
 /**
  * Reset a specific bitspace only.
  */
 void cfl_hbit_reset_bitspace(cfl_hbit_t* tree, uint16_t bitspace_id);
 
 /* ============================================ */
 /* Node Lookup                                  */
 /* ============================================ */
 
 /**
  * Find node index by path string.
  * Use returned index with _n suffix functions for faster access.
  *
  * @param tree     Tree instance
  * @param path_fmt Printf-style path format
  * @return Node index (>= 0) on success, -1 if not found
  */
 int32_t cfl_hbit_find_node_path(
     cfl_hbit_t* tree,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get parent node index.
  *
  * @param tree     Tree instance
  * @param node_idx Node index
  * @return Parent node index (>= 0), -1 if root or error
  */
 int32_t cfl_hbit_get_parent_n(
     cfl_hbit_t* tree,
     int32_t node_idx
 );
 
 /**
  * Get children of a node.
  * Scans node list for nodes with this node as parent.
  *
  * @param tree         Tree instance
  * @param node_idx     Parent node index
  * @param children     OUT: array to fill with child indices
  * @param max_children Size of children array
  * @return Number of children found (may be less than actual if array too small)
  */
 int cfl_hbit_get_children_n(
     cfl_hbit_t* tree,
     int32_t node_idx,
     int32_t* children,
     int max_children
 );
 
 /**
  * Get child count for a node (from descriptor, no scan needed).
  *
  * @param tree     Tree instance
  * @param node_idx Node index
  * @return Number of children, 0 if leaf or error
  */
 int cfl_hbit_get_child_count_n(
     cfl_hbit_t* tree,
     int32_t node_idx
 );
 
 /* ============================================ */
 /* Bit Operations - Node-indexed (_n suffix)    */
 /* ============================================ */
 
 /**
  * Set a single bit using pre-resolved node index.
  */
 cfl_hbit_status_t cfl_hbit_set_bit_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     bool value,
     int32_t node_idx
 );
 
 /**
  * Get a single bit using pre-resolved node index.
  * @return 1 if set, 0 if clear, -1 on error
  */
 int cfl_hbit_get_bit_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     int32_t node_idx
 );
 
 /**
  * Get pointer to bit bank using pre-resolved node index.
  */
 const uint8_t* cfl_hbit_get_bits_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     int32_t node_idx
 );
 
 /**
  * Get edge transition for a single bit using pre-resolved node index.
  * @return 1 = rising, -1 = falling, 0 = no change
  */
 int cfl_hbit_get_bit_edge_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     int32_t node_idx
 );
 
 /**
  * Clear latch using pre-resolved node index (leaf nodes only).
  * EXCEPTION if called on non-leaf node.
  */
 cfl_hbit_status_t cfl_hbit_clear_latch_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     int32_t node_idx
 );
 
 /**
  * Clear latch bits using pre-resolved node index (leaf nodes only).
  * EXCEPTION if called on non-leaf node.
  */
 cfl_hbit_status_t cfl_hbit_clear_latch_bits_n(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t bit_mask,
     int32_t node_idx
 );
 
 /* ============================================ */
 /* Bit Operations - Path-based (Write to Shadow)*/
 /* ============================================ */
 
 /**
  * Set a single bit in the shadow buffer.
  * Changes become visible after cfl_hbit_sync().
  *
  * @param tree        Tree instance
  * @param bitspace_id Bitspace index
  * @param bit_index   Bit position within the bank
  * @param value       true=set, false=clear
  * @param path_fmt    Printf-style path format
  * @return CFL_HBIT_OK on success
  */
 cfl_hbit_status_t cfl_hbit_set_bit(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     bool value,
     const char* path_fmt,
     ...
 );
 
 /**
  * Clear a single bit (convenience wrapper).
  */
 cfl_hbit_status_t cfl_hbit_clear_bit(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     const char* path_fmt,
     ...
 );
 
 /**
  * Set multiple bits from a mask.
  */
 cfl_hbit_status_t cfl_hbit_set_bits_mask(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t mask,
     uint32_t value,
     const char* path_fmt,
     ...
 );
 
 /**
  * Clear all bits in a node's bank.
  */
 cfl_hbit_status_t cfl_hbit_clear_bank(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 );
 
 /**
  * Bulk clear entire bitspace (all nodes).
  */
 void cfl_hbit_clear_bitspace(cfl_hbit_t* tree, uint16_t bitspace_id);
 
 /* ============================================ */
 /* Bit Operations (Read from Current)           */
 /* ============================================ */
 
 /**
  * Get pointer to a node's bit bank (read-only).
  * Returns NULL if path not found.
  */
 const uint8_t* cfl_hbit_get_bits(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get a single bit value.
  * Returns -1 if path not found, 0 or 1 otherwise.
  */
 int cfl_hbit_get_bit(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get bank size in bytes for a node.
  */
 int cfl_hbit_get_bank_size(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 );
 
 /* ============================================ */
 /* Mask Operations (leaf nodes only)            */
 /* ============================================ */
 
 /**
  * Set mask for a leaf node (controls which bits propagate upward).
  * Bits set to 1 will propagate, bits set to 0 are blocked.
  * Writes to shadow buffer - takes effect after swap().
  * 
  * EXCEPTION if called on non-leaf node.
  *
  * @param tree        Tree instance
  * @param bitspace_id Bitspace to set mask for
  * @param mask        Byte array of mask bits (sized to bank)
  * @param mask_bytes  Size of mask array in bytes
  * @param path_fmt    Printf-style path to node
  */
 cfl_hbit_status_t cfl_hbit_set_mask(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const uint8_t* mask,
     uint16_t mask_bytes,
     const char* path_fmt,
     ...
 );
 
 /**
  * Clear mask (restore to all 0xFF, all bits propagate).
  * Writes to shadow buffer - takes effect after swap().
  *
  * EXCEPTION if called on non-leaf node.
  */
 cfl_hbit_status_t cfl_hbit_clear_mask(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get current mask for a leaf node.
  * Returns pointer to current mask buffer (read-only).
  *
  * @return Pointer to mask bytes, or NULL if not a leaf or not found
  */
 const uint8_t* cfl_hbit_get_mask(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t* out_mask_bytes,
     const char* path_fmt,
     ...
 );
 
 /**
  * Check if a node is a leaf (has no children).
  */
 bool cfl_hbit_is_leaf(
     cfl_hbit_t* tree,
     const char* path_fmt,
     ...
 );
 
 /* ============================================ */
 /* Latch Operations                             */
 /* ============================================ */
 
 /**
  * Clear latch for a leaf node.
  * If clear_requires_inactive is set, returns CFL_HBIT_ERR_SOURCE_ACTIVE
  * if the source bit is still active.
  *
  * EXCEPTION if called on non-leaf node.
  */
 cfl_hbit_status_t cfl_hbit_clear_latch(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 );
 
 /**
  * Clear latch for specific bits only (leaf nodes only).
  *
  * EXCEPTION if called on non-leaf node.
  */
 cfl_hbit_status_t cfl_hbit_clear_latch_bits(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint32_t bit_mask,
     const char* path_fmt,
     ...
 );
 
 /**
  * Clear all latches in entire bitspace (all nodes).
  */
 void cfl_hbit_clear_all_latches(cfl_hbit_t* tree, uint16_t bitspace_id);
 
 /**
  * Get latched state (bits that were set and held).
  */
 const uint8_t* cfl_hbit_get_latched(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get live state (current source bits before latching).
  */
 const uint8_t* cfl_hbit_get_live(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     const char* path_fmt,
     ...
 );
 
 /* ============================================ */
 /* Synchronization                              */
 /* ============================================ */
 
 /**
  * Swap shadow and current buffers.
  * O(1) pointer swap, interrupt-safe.
  */
 void cfl_hbit_swap(cfl_hbit_t* tree);
 
 /**
  * Run bubble-up propagation on current buffer.
  * Must be called after swap to update parent states.
  */
 void cfl_hbit_propagate(cfl_hbit_t* tree);
 
 /**
  * Combined swap + propagate (convenience).
  */
 void cfl_hbit_sync(cfl_hbit_t* tree);
 
 /* ============================================ */
 /* Change Detection                             */
 /* ============================================ */
 
 /**
  * Register a change callback.
  *
  * @param bitspace_id -1 for all bitspaces
  */
 cfl_hbit_status_t cfl_hbit_register_callback(
     cfl_hbit_t* tree,
     int16_t bitspace_id,
     cfl_hbit_change_cb_t callback,
     void* user_data
 );
 
 /**
  * Process changes and fire callbacks.
  * Call after cfl_hbit_propagate().
  */
 void cfl_hbit_notify_changes(cfl_hbit_t* tree);
 
 /**
  * Get edge transition for a single bit since last sync.
  *
  * @param tree        Tree instance
  * @param bitspace_id Bitspace to check
  * @param bit_index   Bit position within the bank
  * @param path_fmt    Printf-style path to node
  * @return 1 = rising edge (0→1), -1 = falling edge (1→0), 0 = no change
  */
 int cfl_hbit_get_bit_edge(
     cfl_hbit_t* tree,
     uint16_t bitspace_id,
     uint16_t bit_index,
     const char* path_fmt,
     ...
 );
 
 /* ============================================ */
 /* Configuration Access                         */
 /* ============================================ */
 
 /**
  * Get integer config value.
  */
 int32_t cfl_hbit_config_get_int(
     cfl_hbit_t* tree,
     int32_t default_val,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get float config value.
  */
 float cfl_hbit_config_get_float(
     cfl_hbit_t* tree,
     float default_val,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get boolean config value.
  */
 bool cfl_hbit_config_get_bool(
     cfl_hbit_t* tree,
     bool default_val,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get string config value (returns pointer to ROM).
  */
 const char* cfl_hbit_config_get_string(
     cfl_hbit_t* tree,
     const char* default_val,
     const char* path_fmt,
     ...
 );
 
 /* ============================================ */
 /* Introspection                                */
 /* ============================================ */
 
 /**
  * Get node count in tree.
  */
 uint16_t cfl_hbit_node_count(cfl_hbit_t* tree);
 
 /**
  * Get bitspace count.
  */
 uint16_t cfl_hbit_bitspace_count(cfl_hbit_t* tree);
 
 /**
  * Check if a path exists.
  */
 bool cfl_hbit_path_exists(
     cfl_hbit_t* tree,
     const char* path_fmt,
     ...
 );
 
 /**
  * Get node index from path hash.
  * Returns -1 if not found.
  */
 int32_t cfl_hbit_find_node(cfl_hbit_t* tree, uint32_t path_hash);
 
 /* ============================================ */
 /* Hash Utilities                               */
 /* ============================================ */
 
 /**
  * Compute FNV-1a hash of a string.
  */
 uint32_t cfl_hbit_hash_string(const char* str);
 
 /**
  * Compute FNV-1a hash with printf-style formatting.
  * No temporary buffer allocation.
  */
 uint32_t cfl_hbit_hash_vprintf(const char* fmt, va_list args);
 
 /**
  * Compute FNV-1a hash with printf-style formatting.
  */
 uint32_t cfl_hbit_hash_printf(const char* fmt, ...);
 
 /* ============================================ */
 /* Memory Info                                  */
 /* ============================================ */
 
 typedef struct {
     uint32_t descriptor_size;    /* ROM usage */
     uint32_t arena_size;         /* Total arena RAM */
     uint32_t leaf_masks_size;    /* Leaf mask buffers RAM (shadow + current) */
     uint32_t total_ram;          /* Total RAM usage */
     uint16_t node_count;
     uint16_t leaf_count;
     uint16_t bitspace_count;
     uint16_t max_depth;
 } cfl_hbit_mem_info_t;
 
 /**
  * Get memory usage information.
  */
 void cfl_hbit_get_mem_info(cfl_hbit_t* tree, cfl_hbit_mem_info_t* info);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* CFL_HBIT_H */