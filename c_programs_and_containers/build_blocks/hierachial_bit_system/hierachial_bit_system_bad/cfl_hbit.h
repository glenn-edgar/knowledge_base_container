/**
 * @file cfl_hbit.h
 * @brief Hierarchical Bit Map Runtime - Header
 * 
 * Three buffer types:
 *   OR_LATCH - OR merge, bits latch until cleared
 *   OR_MASK  - OR merge with mask for selective propagation
 *   AND      - AND merge, all children must set bit
 */

 #ifndef CFL_HBIT_H
 #define CFL_HBIT_H
 
 #include <stdint.h>
 #include <stdbool.h>
 #include <stddef.h>
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* ============================================ */
 /* Allocator Interface                          */
 /* ============================================ */
 
 typedef void* (*cfl_hbit_alloc_fn)(size_t size, void* ctx);
 typedef void  (*cfl_hbit_free_fn)(void* ptr, void* ctx);
 
 typedef struct {
     cfl_hbit_alloc_fn alloc;
     cfl_hbit_free_fn  free;
     void*             ctx;    /* User context passed to alloc/free */
 } cfl_hbit_allocator_t;
 
 /* ============================================ */
 /* Buffer Types                                 */
 /* ============================================ */
 
 typedef enum {
     CFL_HBIT_BUF_OR_LATCH = 0,  /* OR merge, bits latch until cleared */
     CFL_HBIT_BUF_OR_MASK  = 1,  /* OR merge with mask */
     CFL_HBIT_BUF_AND      = 2,  /* AND merge */
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
     CFL_HBIT_ERR_ALLOC_FAILED,
 } cfl_hbit_error_t;
 
 /* ============================================ */
 /* Node Descriptor (matches generated code)     */
 /* ============================================ */
 
 typedef struct {
     uint32_t path_hash;
     uint16_t class_index;
     int16_t  parent_index;   /* -1 if root */
     uint16_t child_count;
     uint16_t first_child;
     uint8_t  depth;
     uint8_t  is_leaf;
 } cfl_hbit_node_t;
 
 /* ============================================ */
 /* Arena Offset (matches generated code)        */
 /* ============================================ */
 
 typedef struct {
     uint16_t offset;  /* Byte offset in arena */
     uint8_t  size;    /* Size in bytes */
 } cfl_hbit_arena_info_t;
 
 /* ============================================ */
 /* Hash Lookup Entry (matches generated code)   */
 /* ============================================ */
 
 typedef struct {
     uint32_t hash;
     uint16_t tree_index;
 } cfl_hbit_hash_entry_t;
 
 /* ============================================ */
 /* Buffer Descriptor                            */
 /* ============================================ */
 
 typedef struct {
     cfl_hbit_buffer_type_t       type;
     uint16_t                     arena_size;      /* Size in bytes */
     const cfl_hbit_arena_info_t* arena_info;      /* Per-node offsets [node_count] */
     
     /* Runtime storage (allocated by init) */
     uint8_t*                     current;         /* Current values */
     uint8_t*                     shadow;          /* Shadow buffer for atomic swap */
     uint8_t*                     latched;         /* Latched values (OR_LATCH only) */
     uint8_t*                     mask;            /* Propagation mask (OR_MASK only) */
 } cfl_hbit_buffer_t;
 
 /* ============================================ */
 /* Static Configuration (from generated code)   */
 /* ============================================ */
 
 typedef struct {
     uint16_t                       node_count;
     uint16_t                       buffer_count;
     uint16_t                       class_count;
     uint16_t                       root_count;
     
     const cfl_hbit_node_t*         nodes;           /* Tree-order node table */
     const cfl_hbit_hash_entry_t*   nodes_by_hash;   /* Hash-sorted lookup */
     const uint16_t*                roots;           /* Root node indices */
     const uint8_t*                 bank_sizes;      /* [class_count][buffer_count] */
     
     /* Buffer configurations (type, arena_size, arena_info) */
     const cfl_hbit_buffer_t*       buffer_configs;  /* [buffer_count] */
 } cfl_hbit_config_t;
 
 /* ============================================ */
 /* Runtime Instance                             */
 /* ============================================ */
 
 typedef struct {
     /* Configuration (pointer to static data) */
     const cfl_hbit_config_t*     config;
     
     /* Allocator (for cleanup) */
     cfl_hbit_allocator_t         allocator;
     
     /* Runtime buffers (allocated) */
     cfl_hbit_buffer_t*           buffers;         /* [buffer_count] */
     
     /* Dirty tracking for propagation */
     uint8_t*                     dirty_nodes;     /* Bit array: node needs propagation */
     uint16_t                     dirty_count;
     
     /* Single allocation block */
     uint8_t*                     ram;             /* All runtime memory */
 } cfl_hbit_instance_t;
 
 /* ============================================ */
 /* Walk Callback                                */
 /* ============================================ */
 
 typedef bool (*cfl_hbit_walk_fn)(
     cfl_hbit_instance_t* inst,
     uint16_t node_idx,
     void* user_data);
 
 /* ============================================ */
 /* Inline Bit Helpers                           */
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
 
 static inline void cfl_hbit_toggle_bit(uint8_t* data, uint8_t bit_idx) {
     data[bit_idx >> 3] ^= (1 << (bit_idx & 7));
 }
 
 /* ============================================ */
 /* Inline Node Accessors                        */
 /* ============================================ */
 
 static inline uint8_t* cfl_hbit_node_ptr(
     const cfl_hbit_buffer_t* buf,
     uint16_t node_idx)
 {
     return buf->current + buf->arena_info[node_idx].offset;
 }
 
 static inline uint8_t cfl_hbit_node_size(
     const cfl_hbit_buffer_t* buf,
     uint16_t node_idx)
 {
     return buf->arena_info[node_idx].size;
 }
 
 /* ============================================ */
 /* Inline Dirty Tracking                        */
 /* ============================================ */
 
 static inline bool cfl_hbit_is_dirty(const cfl_hbit_instance_t* inst, uint16_t node_idx) {
     return (inst->dirty_nodes[node_idx >> 3] >> (node_idx & 7)) & 1;
 }
 
 static inline void cfl_hbit_mark_dirty(cfl_hbit_instance_t* inst, uint16_t node_idx) {
     if (!cfl_hbit_is_dirty(inst, node_idx)) {
         inst->dirty_nodes[node_idx >> 3] |= (1 << (node_idx & 7));
         inst->dirty_count++;
     }
 }
 
 static inline void cfl_hbit_clear_dirty(cfl_hbit_instance_t* inst, uint16_t node_idx) {
     if (cfl_hbit_is_dirty(inst, node_idx)) {
         inst->dirty_nodes[node_idx >> 3] &= ~(1 << (node_idx & 7));
         inst->dirty_count--;
     }
 }
 
 /* ============================================ */
 /* API Functions (implemented in cfl_hbit.c)    */
 /* ============================================ */
 
 /* Initialization / Destruction */
 size_t cfl_hbit_calc_ram_size(const cfl_hbit_config_t* config);
 
 cfl_hbit_instance_t* cfl_hbit_create(
     const cfl_hbit_config_t* config,
     const cfl_hbit_allocator_t* allocator);
 
 void cfl_hbit_destroy(cfl_hbit_instance_t* inst);
 
 void cfl_hbit_reset(cfl_hbit_instance_t* inst);
 
 /* Node Lookup */
 int16_t cfl_hbit_find_node(const cfl_hbit_instance_t* inst, uint32_t hash);
 
 /* Leaf Node Operations */
 cfl_hbit_error_t cfl_hbit_leaf_set_bit(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx);
 
 cfl_hbit_error_t cfl_hbit_leaf_clear_bit(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx);
 
 cfl_hbit_error_t cfl_hbit_leaf_write(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     const uint8_t* data,
     uint8_t len);
 
 /* Read Operations */
 bool cfl_hbit_read_bit(
     const cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx);
 
 bool cfl_hbit_read_latched_bit(
     const cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx);
 
 uint8_t cfl_hbit_read_node(
     const cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t* data,
     uint8_t max_len);
 
 /* Mask Operations (OR_MASK buffers) */
 cfl_hbit_error_t cfl_hbit_set_mask(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     const uint8_t* mask,
     uint8_t len);
 
 cfl_hbit_error_t cfl_hbit_set_mask_bit(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx,
     bool enabled);
 
 /* Latch Clear Operations */
 cfl_hbit_error_t cfl_hbit_clear_latch_bit(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx,
     uint8_t bit_idx);
 
 cfl_hbit_error_t cfl_hbit_clear_latch_all(
     cfl_hbit_instance_t* inst,
     uint16_t buffer_idx,
     uint16_t node_idx);
 
 /* Propagation */
 void cfl_hbit_propagate(cfl_hbit_instance_t* inst);
 void cfl_hbit_propagate_tree(cfl_hbit_instance_t* inst, uint16_t root_idx);
 
 /* Tree Walking */
 void cfl_hbit_walk_preorder(
     cfl_hbit_instance_t* inst,
     uint16_t root_idx,
     cfl_hbit_walk_fn callback,
     void* user_data);
 
 void cfl_hbit_walk_postorder(
     cfl_hbit_instance_t* inst,
     uint16_t root_idx,
     cfl_hbit_walk_fn callback,
     void* user_data);
 
 void cfl_hbit_foreach_child(
     cfl_hbit_instance_t* inst,
     uint16_t parent_idx,
     cfl_hbit_walk_fn callback,
     void* user_data);
 
 /* ============================================ */
 /* Controller - Subtree as Flat Bitmap          */
 /* ============================================ */
 
 /**
  * Child descriptor within a controller
  */
 typedef struct {
     uint16_t node_idx;        /* Node index of this child */
     uint16_t leaf_start;      /* First leaf index in flat array */
     uint16_t leaf_count;      /* Number of leaves under this child */
     uint16_t bit_start;       /* First bit index in flat bitmap */
     uint16_t bit_count;       /* Total bits under this child */
 } cfl_hbit_child_t;
 
 /**
  * Controller for a subtree - provides flat bitmap access
  */
 typedef struct {
     cfl_hbit_instance_t* inst;       /* Reference to instance */
     uint16_t root_node;              /* Root node of this controller */
     uint16_t buffer_idx;             /* Which buffer this controller operates on */
     
     uint16_t child_count;            /* Number of direct children */
     cfl_hbit_child_t* children;      /* Array of child descriptors */
     
     uint16_t leaf_count;             /* Total leaf count in subtree */
     uint16_t* leaf_nodes;            /* Array of leaf node indices */
     
     uint8_t bits_per_leaf;           /* Bits per leaf (from buffer arena) */
     uint16_t total_bits;             /* Total bits in flat bitmap */
     
     /* Allocator for cleanup */
     cfl_hbit_allocator_t allocator;
     uint8_t* ram;                    /* Single allocation block */
 } cfl_hbit_controller_t;
 
 /**
  * Create controller for a subtree.
  * Builds mapping of children and leaves for fast indexed access.
  * @param inst Instance
  * @param root_node Root node index of subtree
  * @param buffer_idx Which buffer to operate on
  * @param allocator Custom allocator
  * @return Allocated controller, or NULL on error
  */
 cfl_hbit_controller_t* cfl_hbit_controller_create(
     cfl_hbit_instance_t* inst,
     uint16_t root_node,
     uint16_t buffer_idx,
     const cfl_hbit_allocator_t* allocator);
 
 /**
  * Destroy controller, free all resources.
  */
 void cfl_hbit_controller_destroy(cfl_hbit_controller_t* ctrl);
 
 /**
  * Get leaf node and bit index from child index + bit within child.
  * @param ctrl Controller
  * @param child_index Index of child (0..child_count-1)
  * @param child_bit_index Bit index within that child's leaf range
  * @param bit_index Output: bit index within the leaf
  * @return Node index of leaf, or -1 on error
  */
 int16_t cfl_hbit_controller_get_node_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t child_index,
     uint16_t child_bit_index,
     uint8_t* bit_index);
 
 /**
  * Get leaf node and bit index from flat bitmap index.
  * @param ctrl Controller
  * @param bitmap_index Flat index (0..total_bits-1)
  * @param bit_index Output: bit index within the leaf
  * @return Node index of leaf, or -1 on error
  */
 int16_t cfl_hbit_controller_get_bitmap_node(
     cfl_hbit_controller_t* ctrl,
     uint16_t bitmap_index,
     uint8_t* bit_index);
 
 /**
  * Set bit using flat bitmap index.
  */
 cfl_hbit_error_t cfl_hbit_controller_set_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t bitmap_index);
 
 /**
  * Clear bit using flat bitmap index.
  */
 cfl_hbit_error_t cfl_hbit_controller_clear_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t bitmap_index);
 
 /**
  * Read bit using flat bitmap index.
  */
 bool cfl_hbit_controller_read_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t bitmap_index);
 
 /**
  * Set bit using child index + bit within child.
  */
 cfl_hbit_error_t cfl_hbit_controller_set_child_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t child_index,
     uint16_t child_bit_index);
 
 /**
  * Clear bit using child index + bit within child.
  */
 cfl_hbit_error_t cfl_hbit_controller_clear_child_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t child_index,
     uint16_t child_bit_index);
 
 /**
  * Read bit using child index + bit within child.
  */
 bool cfl_hbit_controller_read_child_bit(
     cfl_hbit_controller_t* ctrl,
     uint16_t child_index,
     uint16_t child_bit_index);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* CFL_HBIT_H */