/*
 * cfl_chaintree_support.h - ChainTree Runtime Support
 *
 * Core types and handle structure for ChainTree execution.
 *
 * Two frontend paths populate cfl_chaintree_handle_t:
 *   .h/.c path  - all pointers reference const arrays in flash
 *   binary path - function pointer arrays in RAM, everything else in flash
 *
 * The engine accesses function pointers directly:
 *   handle->main_functions[node->main_function_index](...)
 *
 * This works identically for both paths. The binary path additionally
 * uses cfl_func_registry_t with sorted hash arrays for registration.
 */

 #ifndef CFL_CHAINTREE_SUPPORT_H
 #define CFL_CHAINTREE_SUPPORT_H
 
 #include <stdint.h>
 #include <stdbool.h>
 #include <stddef.h>
 
 /* ===== Forward declaration for blackboard types ===== */
 struct cfl_bb_table;
 typedef struct cfl_bb_table cfl_bb_table_t;

 /* ===== Function Pointer Types ===== */
 
 typedef unsigned (*cfl_main_function_t)(
     void *handle, unsigned bool_function_index,
     unsigned node_index, unsigned event_type,
     unsigned event_id, void *event_data);
 
 typedef void (*cfl_one_shot_function_t)(
     void *handle, unsigned node_index);
 
 typedef bool (*cfl_boolean_function_t)(
     void *handle, unsigned node_index,
     unsigned event_type, unsigned event_id,
     void *event_data);
 
 /* ===== Node Structure ===== */
 
 typedef struct {
     uint16_t node_index;
     uint16_t parent_index;
     uint16_t depth;
     uint16_t link_start;
     uint16_t link_count;          /* Bits 0-14: count, Bit 15: auto_start flag */
     uint16_t main_function_index;
     uint16_t init_function_index;
     uint16_t aux_function_index;
     uint16_t term_function_index;
     uint16_t node_data_id;
 } cfl_chaintree_node_t;
 
 /* ===== Link Count Bit Packing ===== */
 
 #define CFL_LINK_COUNT_MASK     0x7FFF
 #define CFL_AUTO_START_BIT      0x8000
 
 #define CFL_GET_LINK_COUNT(node)     ((node)->link_count & CFL_LINK_COUNT_MASK)
 #define CFL_GET_AUTO_START(node)     (((node)->link_count & CFL_AUTO_START_BIT) != 0)
 #define CFL_PACK_LINK_COUNT(count, auto_start) \
     (((count) & CFL_LINK_COUNT_MASK) | ((auto_start) ? CFL_AUTO_START_BIT : 0))
 
 /* ===== Node Alias Structure ===== */
 
 typedef struct {
     const char *alias;
     uint16_t    node_index;
 } cfl_node_alias_t;
 
 /* ===== Knowledge Base Info ===== */
 
 typedef struct {
     const char             *kb_name;
     uint16_t                root_node_index;
     uint16_t                start_index;
     uint16_t                node_count;
     uint16_t                max_depth;
     uint16_t                memory_factor;
     const cfl_node_alias_t *aliases;
     uint16_t                alias_count;
 } cfl_chaintree_kb_info_t;
 
 /* ===== Node Data (JSON Records) ===== */
 
 typedef enum {
     CFL_JSON_TYPE_STRING  = 0,
     CFL_JSON_TYPE_INT32   = 1,
     CFL_JSON_TYPE_FLOAT32 = 2,
     CFL_JSON_TYPE_NULL    = 3,
     CFL_JSON_TYPE_BOOL    = 4,
     CFL_JSON_TYPE_ARRAY   = 5,
     CFL_JSON_TYPE_OBJECT  = 6
 } cfl_json_type_t;
 
 typedef struct {
     cfl_json_type_t object_type;
     union {
         uint32_t string_offset;
         int32_t  i32_value;
         float    f32_value;
         uint8_t  bool_value;
         uint32_t container_count;
     } value;
 } cfl_json_record_t;
 
 typedef struct {
     uint32_t start_position;
     uint32_t num_records;
 } cfl_record_control_t;
 
 /* =====================================================================
  * Function Registry (binary path only)
  *
  * Sorted uint32_t FNV-1a hash arrays residing in flash image.
  * Used for binary_search during function registration.
  * .h/.c path sets registry to NULL.
  * ===================================================================== */
 
 typedef struct {
     const uint32_t *main_hashes;
     const uint32_t *one_shot_hashes;
     const uint32_t *boolean_hashes;
 } cfl_func_registry_t;
 
 /* =====================================================================
  * Handle Structure
  *
  * The engine accesses function pointers directly:
  *   handle->main_functions[node->main_function_index](...)
  *
  * .h/.c path:  main_functions points to const array in flash (zero RAM)
  * binary path: main_functions points to RAM array filled by registration
  *
  * The engine does not know or care which path populated the handle.
  * ===================================================================== */
 
 typedef struct {
     const char *unique_id;
 
     /* ---- Tree structure (always flash) ---- */
     const cfl_chaintree_node_t *nodes;
     uint16_t                    node_count;
 
     /* ---- Function pointer arrays ---- */
     /* .h/.c: const in flash, linker-resolved                          */
     /* binary: RAM array, filled by hash registration                  */
     const cfl_main_function_t     *main_functions;
     uint16_t                       main_function_count;
     const cfl_one_shot_function_t *one_shot_functions;
     uint16_t                       one_shot_function_count;
     const cfl_boolean_function_t  *boolean_functions;
     uint16_t                       boolean_function_count;
 
     /* ---- Function name arrays (always flash) ---- */
     const char     **main_function_names;
     const char     **one_shot_function_names;
     const char     **boolean_function_names;
     const uint16_t  *main_function_usage_count;
 
     /* ---- Link table (always flash) ---- */
     const uint16_t *link_table;
     uint16_t        link_table_size;
 
     /* ---- Events (always flash) ---- */
     const char **event_strings;
     uint16_t    event_count;
 
     /* ---- Bitmasks (always flash) ---- */
     const char **bitmask_names;
     uint16_t    bitmask_count;
 
     /* ---- Knowledge bases (always flash) ---- */
     const cfl_chaintree_kb_info_t *kb_table;
     uint16_t                       kb_count;
 
     /* ---- Node data (always flash) ---- */
     const cfl_json_record_t    *node_data_records;
     uint16_t                    node_data_records_count;
     const char                 *node_data_strings;
     uint16_t                    node_data_strings_size;
     const cfl_record_control_t *node_data_controls;
     uint16_t                    node_data_controls_count;
 
     /* ---- Hash registration (binary path only, NULL for .h/.c) ---- */
     const cfl_func_registry_t *registry;

     /* ---- Blackboard and constant records (NULL if not defined) ---- */
     const cfl_bb_table_t *bb_table;

 } cfl_chaintree_handle_t;
 
 /* ===== Lookup Functions ===== */
 
 const char *cfl_get_main_function_name(
     const cfl_chaintree_handle_t *handle, uint16_t func_index);
 int cfl_get_main_function_index(
     const cfl_chaintree_handle_t *handle, const char *func_name);
 
 const char *cfl_get_one_shot_function_name(
     const cfl_chaintree_handle_t *handle, uint16_t func_index);
 int cfl_get_one_shot_function_index(
     const cfl_chaintree_handle_t *handle, const char *func_name);
 
 const char *cfl_get_boolean_function_name(
     const cfl_chaintree_handle_t *handle, uint16_t func_index);
 int cfl_get_boolean_function_index(
     const cfl_chaintree_handle_t *handle, const char *func_name);
 
 const char *cfl_get_event_name(
     const cfl_chaintree_handle_t *handle, uint16_t event_index);
 int cfl_get_event_index(
     const cfl_chaintree_handle_t *handle, const char *name);
 
 const char *cfl_get_bitmask_name(
     const cfl_chaintree_handle_t *handle, uint8_t bit_index);
 int cfl_get_bitmask_index(
     const cfl_chaintree_handle_t *handle, const char *name);
 
 uint16_t cfl_get_kb_count(
     const cfl_chaintree_handle_t *handle);
 
 /* =====================================================================
  * Backward Compatibility
  *
  * The engine and existing code use unprefixed type names.
  * These typedefs map old names to new CFL-prefixed types.
  * ===================================================================== */
 
 typedef cfl_main_function_t       main_function_t;
 typedef cfl_one_shot_function_t   one_shot_function_t;
 typedef cfl_boolean_function_t    boolean_function_t;
 typedef cfl_chaintree_node_t      chaintree_node_t;
 typedef cfl_node_alias_t          node_alias_t;
 typedef cfl_chaintree_kb_info_t   chaintree_kb_info_t;
 typedef cfl_json_type_t           json_type_t;
 typedef cfl_json_record_t         json_record_t;
 typedef cfl_record_control_t      record_control_t;
 typedef cfl_func_registry_t       func_registry_t;
 typedef cfl_chaintree_handle_t    chaintree_handle_t;
 
 /* Old enum value names */
 #define JSON_TYPE_STRING   CFL_JSON_TYPE_STRING
 #define JSON_TYPE_INT32    CFL_JSON_TYPE_INT32
 #define JSON_TYPE_FLOAT32  CFL_JSON_TYPE_FLOAT32
 #define JSON_TYPE_NULL     CFL_JSON_TYPE_NULL
 #define JSON_TYPE_BOOL     CFL_JSON_TYPE_BOOL
 #define JSON_TYPE_ARRAY    CFL_JSON_TYPE_ARRAY
 #define JSON_TYPE_OBJECT   CFL_JSON_TYPE_OBJECT
 
 /* Old macro names */
 #define LINK_COUNT_MASK    CFL_LINK_COUNT_MASK
 #define AUTO_START_BIT     CFL_AUTO_START_BIT
 #define GET_LINK_COUNT     CFL_GET_LINK_COUNT
 #define GET_AUTO_START     CFL_GET_AUTO_START
 #define PACK_LINK_COUNT    CFL_PACK_LINK_COUNT
 
 /* Old lookup function names */
 #define ct_get_main_function_name    cfl_get_main_function_name
 #define ct_get_main_function_index   cfl_get_main_function_index
 #define ct_get_one_shot_function_name  cfl_get_one_shot_function_name
 #define ct_get_one_shot_function_index cfl_get_one_shot_function_index
 #define ct_get_boolean_function_name   cfl_get_boolean_function_name
 #define ct_get_boolean_function_index  cfl_get_boolean_function_index
 #define ct_get_event_name            cfl_get_event_name
 #define ct_get_event_index           cfl_get_event_index
 #define ct_get_bitmask_name          cfl_get_bitmask_name
 #define ct_get_bitmask_index         cfl_get_bitmask_index
 #define ct_get_kb_count              cfl_get_kb_count
 
 #endif /* CFL_CHAINTREE_SUPPORT_H */