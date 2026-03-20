/*
 * cfl_image_loader.h - ChainTree Binary Image Loader
 *
 * Loads .ctb binary images produced by stage6_binary.lua.
 *
 * DESIGN: All arrays in original indexer order, matching .h/.c path.
 * Hash tables are {hash, slot_index} pairs sorted by hash — used
 * only for function registration, not for indexing.
 */

 #ifndef CFL_IMAGE_LOADER_H
 #define CFL_IMAGE_LOADER_H
 
 #include "cfl_chaintree_support.h"
 #include "cfl_fnv1a.h"
 
 /* ===== Error Codes ===== */
 
 #define CFL_IMAGE_OK                  0
 #define CFL_IMAGE_ERR_NULL_PTR       -1
 #define CFL_IMAGE_ERR_TOO_SMALL      -2
 #define CFL_IMAGE_ERR_BAD_MAGIC      -3
 #define CFL_IMAGE_ERR_BAD_VER        -4
 #define CFL_IMAGE_ERR_BAD_SIZE       -5
 #define CFL_IMAGE_ERR_BAD_CRC        -6
 #define CFL_IMAGE_ERR_SECT_OOB       -7
 #define CFL_IMAGE_ERR_ALLOC          -8
 #define CFL_IMAGE_ERR_HASH_NOT_FOUND -9
 
 /* ===== Binary Format Constants ===== */
 
 #define CFL_CTB_MAGIC           0x43544231u  /* "1BTC" little-endian */
 #define CFL_CTB_VERSION_MAJOR   1
 
 /* ===== Section Types ===== */
 
 #define CFL_CTB_SECT_NODE  0x0001
 #define CFL_CTB_SECT_LINK  0x0002
 #define CFL_CTB_SECT_MFHT  0x0003   /* {hash, slot_index}[] sorted by hash */
 #define CFL_CTB_SECT_OSHT  0x0004
 #define CFL_CTB_SECT_BFHT  0x0005
 #define CFL_CTB_SECT_FSTR  0x0006   /* KB names, original order            */
 #define CFL_CTB_SECT_JREC  0x0007
 #define CFL_CTB_SECT_JCTL  0x0008
 #define CFL_CTB_SECT_JSTR  0x0009
 #define CFL_CTB_SECT_EVNT  0x000A
 #define CFL_CTB_SECT_BMSK  0x000B
 #define CFL_CTB_SECT_KBIN  0x000C
 #define CFL_CTB_SECT_KBAL  0x000D
 #define CFL_CTB_SECT_GSTR  0x000E
 #define CFL_CTB_SECT_MUSG  0x000F   /* usage count, original order         */
 #define CFL_CTB_SECT_BBRD  0x0010   /* blackboard record + fields + defaults */
 #define CFL_CTB_SECT_CREC  0x0011   /* constant records                      */
 
 /* ===== Binary Header — 64 bytes ===== */
 
 typedef struct {
     uint32_t magic;                 /* 0x00 */
     uint16_t version_major;         /* 0x04 */
     uint16_t version_minor;         /* 0x06 */
     uint32_t flags;                 /* 0x08 */
     uint32_t total_image_size;      /* 0x0C */
     uint32_t checksum;              /* 0x10 */
     uint16_t section_count;         /* 0x14 */
     uint16_t node_total_count;      /* 0x16 */
     uint16_t node_active_count;     /* 0x18 */
     uint16_t link_table_size;       /* 0x1A */
     uint16_t main_func_count;       /* 0x1C */
     uint16_t one_shot_func_count;   /* 0x1E */
     uint16_t boolean_func_count;    /* 0x20 */
     uint16_t event_count;           /* 0x22 */
     uint16_t bitmask_count;         /* 0x24 */
     uint16_t kb_count;              /* 0x26 */
     uint16_t json_records_count;    /* 0x28 */
     uint16_t json_controls_count;   /* 0x2A */
     uint32_t json_strings_size;     /* 0x2C */
     uint8_t  reserved[16];          /* 0x30 */
 } __attribute__((packed)) cfl_ctb_header_t;
 
 /* ===== Section Directory Entry — 16 bytes ===== */
 
 typedef struct {
     uint32_t section_type;
     uint32_t offset;
     uint32_t size;
     uint16_t entry_count;
     uint16_t entry_size;
 } __attribute__((packed)) cfl_ctb_section_dir_t;
 
 /* ===== Hash Table Entry — 8 bytes =====
  * Sorted by hash. slot_index = original indexer position.
  * Registration: binary_search(hash) → slot_index → fn_ptrs[slot_index] = fn
  */
 
 typedef struct {
     uint32_t hash;
     uint16_t slot_index;
     uint16_t reserved;
 } __attribute__((packed)) cfl_ctb_hash_entry_t;
 
 /* ===== Bitmask Entry — 8 bytes ===== */
 
 typedef struct {
     uint32_t string_pool_offset;
     uint16_t bit_index;
     uint16_t reserved;
 } __attribute__((packed)) cfl_ctb_bitmask_entry_t;
 
 /* ===== KB Info Entry — 24 bytes ===== */
 
 typedef struct {
     uint32_t name_offset;
     uint16_t start_index;
     uint16_t root_node_index;
     uint16_t node_count;
     uint16_t max_depth;
     uint16_t memory_factor;
     uint16_t alias_start;
     uint16_t alias_count;
     uint8_t  reserved[6];
 } __attribute__((packed)) cfl_ctb_kb_info_entry_t;
 
 /* ===== KB Alias Entry — 8 bytes ===== */
 
 typedef struct {
     uint32_t name_offset;
     uint16_t node_index;
     uint16_t reserved;
 } __attribute__((packed)) cfl_ctb_kb_alias_entry_t;
 
 /* ===== Image Loader State ===== */
 
 typedef struct {
     cfl_chaintree_handle_t handle;
 
     /* Hash tables in flash (for registration lookup) */
     const cfl_ctb_hash_entry_t *main_hash_table;
     const cfl_ctb_hash_entry_t *one_shot_hash_table;
     const cfl_ctb_hash_entry_t *boolean_hash_table;
 
     /* Mutable function pointer arrays (RAM) */
     cfl_main_function_t     *main_fn_ptrs;
     cfl_one_shot_function_t *one_shot_fn_ptrs;
     cfl_boolean_function_t  *boolean_fn_ptrs;
 
     /* Flash image references */
     const void             *image_base;
     uint32_t                image_size;
     const cfl_ctb_header_t *header;
 
     /* String pools in flash */
     const char *string_pool;
     uint32_t    string_pool_size;
     const char *func_name_base;
     uint32_t    func_name_size;
 
     /* Counts */
     uint16_t main_func_count;
     uint16_t one_shot_func_count;
     uint16_t boolean_func_count;
 } cfl_image_loader_t;
 
 /* ===== API ===== */
 
 int cfl_image_load(const void *image_data, uint32_t image_size,
                    cfl_image_loader_t *out);
 void cfl_image_free(cfl_image_loader_t *img);
 const cfl_chaintree_handle_t *cfl_image_get_handle(
     const cfl_image_loader_t *img);
 
 int cfl_image_register_main(
     cfl_image_loader_t *img, const char *name, cfl_main_function_t fn);
 int cfl_image_register_one_shot(
     cfl_image_loader_t *img, const char *name, cfl_one_shot_function_t fn);
 int cfl_image_register_boolean(
     cfl_image_loader_t *img, const char *name, cfl_boolean_function_t fn);
 
 int cfl_image_validate(const cfl_image_loader_t *img);
 const char *cfl_image_get_func_name(
     const cfl_image_loader_t *img, uint16_t table_type, uint16_t slot);
 
 #endif /* CFL_IMAGE_LOADER_H */