/*
 * ct_runtime.c - ChainTree Binary Image Runtime
 *
 * Loads .ctb binary images, registers functions by FNV-1a hash,
 * and exposes a standard chaintree_handle_t.
 *
 * Build:
 *   gcc -O2 -c ct_runtime.c fnv1a.c
 *   ar rcs libct_runtime.a ct_runtime.o fnv1a.o
 */

 #include "ct_runtime.h"

 #include <stdlib.h>
 #include <string.h>
 #include <stdio.h>
 
 /* ===================================================================
  * CRC32 (ISO 3309 / zlib compatible, table-driven)
  * =================================================================== */
 
 static uint32_t crc32_table[256];
 static int crc32_table_init = 0;
 
 static void crc32_init_table(void)
 {
     for (uint32_t i = 0; i < 256; i++) {
         uint32_t c = i;
         for (int j = 0; j < 8; j++) {
             if (c & 1)
                 c = 0xEDB88320u ^ (c >> 1);
             else
                 c = c >> 1;
         }
         crc32_table[i] = c;
     }
     crc32_table_init = 1;
 }
 
 uint32_t ct_crc32(const void *data, uint32_t len)
 {
     if (!crc32_table_init) crc32_init_table();
 
     const uint8_t *p = (const uint8_t *)data;
     uint32_t crc = 0xFFFFFFFFu;
     for (uint32_t i = 0; i < len; i++) {
         crc = crc32_table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
     }
     return crc ^ 0xFFFFFFFFu;
 }
 
 /* ===================================================================
  * Binary search in sorted uint32_t array
  * Returns index if found, -1 if not found
  * =================================================================== */
 
 static int binary_search_u32(const uint32_t *arr, uint16_t count, uint32_t target)
 {
     int lo = 0;
     int hi = (int)count - 1;
 
     while (lo <= hi) {
         int mid = lo + (hi - lo) / 2;
         if (arr[mid] == target) return mid;
         if (arr[mid] < target)
             lo = mid + 1;
         else
             hi = mid - 1;
     }
     return -1;
 }
 
 /* ===================================================================
  * Section lookup helper
  * =================================================================== */
 
 typedef struct {
     const void *data;
     uint32_t    size;
     uint16_t    entry_count;
     uint16_t    entry_size;
 } section_info_t;
 
 static int find_section(const void *image_base, const ctb_header_t *hdr,
                         uint32_t image_size, uint32_t section_type,
                         section_info_t *out)
 {
     const ctb_section_dir_t *dir = (const ctb_section_dir_t *)
         ((const uint8_t *)image_base + sizeof(ctb_header_t));
 
     for (uint16_t i = 0; i < hdr->section_count; i++) {
         if (dir[i].section_type == section_type) {
             if (dir[i].offset + dir[i].size > image_size)
                 return CT_ERR_SECTION_OOB;
 
             out->data = (const uint8_t *)image_base + dir[i].offset;
             out->size = dir[i].size;
             out->entry_count = dir[i].entry_count;
             out->entry_size = dir[i].entry_size;
             return CT_OK;
         }
     }
 
     /* Section not found — not an error, just absent */
     out->data = NULL;
     out->size = 0;
     out->entry_count = 0;
     out->entry_size = 0;
     return CT_OK;
 }
 
 /* ===================================================================
  * Walk function name strings to find name at slot index
  * =================================================================== */
 
 static const char *walk_func_names(const char *base, uint32_t total_size,
                                    uint16_t offset_count, uint16_t slot)
 {
     if (!base || slot >= offset_count) return NULL;
 
     const char *p = base;
     const char *end = base + total_size;
     uint16_t idx = 0;
 
     while (p < end && idx < slot) {
         p += strlen(p) + 1;
         idx++;
     }
 
     if (p >= end) return NULL;
     return p;
 }
 
 /* ===================================================================
  * ct_image_load
  * =================================================================== */
 
 int ct_image_load(const void *image_data, uint32_t image_size, ct_image_t *out)
 {
     if (!image_data || !out) return CT_ERR_NULL_PTR;
     if (image_size < sizeof(ctb_header_t)) return CT_ERR_TOO_SMALL;
 
     memset(out, 0, sizeof(ct_image_t));
 
     const ctb_header_t *hdr = (const ctb_header_t *)image_data;
 
     /* Validate magic */
     if (hdr->magic != CTB_MAGIC) return CT_ERR_BAD_MAGIC;
 
     /* Validate version */
     if (hdr->version_major != CTB_VERSION_MAJOR) return CT_ERR_BAD_VERSION;
 
     /* Validate size */
     if (hdr->total_image_size != image_size) return CT_ERR_BAD_SIZE;
 
     /* Validate checksum */
     {
         /* Copy image, zero checksum field, compute CRC32 */
         uint32_t stored_crc = hdr->checksum;
         /* Compute CRC with checksum field as zero */
         uint32_t crc = 0xFFFFFFFFu;
         if (!crc32_table_init) crc32_init_table();
 
         const uint8_t *p = (const uint8_t *)image_data;
         for (uint32_t i = 0; i < image_size; i++) {
             uint8_t b = p[i];
             /* Zero out bytes 16-19 (checksum field) */
             if (i >= 16 && i < 20) b = 0;
             crc = crc32_table[(crc ^ b) & 0xFF] ^ (crc >> 8);
         }
         crc ^= 0xFFFFFFFFu;
 
         if (crc != stored_crc) return CT_ERR_BAD_CHECKSUM;
     }
 
     /* Verify section directory fits */
     uint32_t dir_end = (uint32_t)sizeof(ctb_header_t) +
                        (uint32_t)hdr->section_count * sizeof(ctb_section_dir_t);
     if (dir_end > image_size) return CT_ERR_TOO_SMALL;
 
     out->image_base = image_data;
     out->image_size = image_size;
     out->header = hdr;
     out->main_func_count = hdr->main_func_count;
     out->one_shot_func_count = hdr->one_shot_func_count;
     out->boolean_func_count = hdr->boolean_func_count;
 
     /* ---- Find all sections ---- */
 
     section_info_t sect;
     int rc;
 
     /* Node array */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_NODE, &sect);
     if (rc != CT_OK) return rc;
     out->handle.nodes = (const chaintree_node_t *)sect.data;
     out->handle.node_count = hdr->node_active_count;
 
     /* Link table */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_LINK, &sect);
     if (rc != CT_OK) return rc;
     out->handle.link_table = (const uint16_t *)sect.data;
     out->handle.link_table_size = hdr->link_table_size;
 
     /* Main function hash table */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_MFHT, &sect);
     if (rc != CT_OK) return rc;
     out->main_hashes = (const uint32_t *)sect.data;
 
     /* One-shot function hash table */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_OSHT, &sect);
     if (rc != CT_OK) return rc;
     out->one_shot_hashes = (const uint32_t *)sect.data;
 
     /* Boolean function hash table */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_BFHT, &sect);
     if (rc != CT_OK) return rc;
     out->boolean_hashes = (const uint32_t *)sect.data;
 
     /* Function name strings */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_FSTR, &sect);
     if (rc != CT_OK) return rc;
     out->func_name_base = (const char *)sect.data;
     out->func_name_size = sect.size;
 
     /* JSON records */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_JREC, &sect);
     if (rc != CT_OK) return rc;
     out->handle.node_data_records = (const json_record_t *)sect.data;
     out->handle.node_data_records_count = hdr->json_records_count;
 
     /* JSON controls */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_JCTL, &sect);
     if (rc != CT_OK) return rc;
     out->handle.node_data_controls = (const record_control_t *)sect.data;
     out->handle.node_data_controls_count = hdr->json_controls_count;
 
     /* JSON strings */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_JSTR, &sect);
     if (rc != CT_OK) return rc;
     out->handle.node_data_strings = (const char *)sect.data;
     out->handle.node_data_strings_size = hdr->json_strings_size;
 
     /* General string pool */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_GSTR, &sect);
     if (rc != CT_OK) return rc;
     out->string_pool = (const char *)sect.data;
     out->string_pool_size = sect.size;
 
     /* Event strings — build pointer array from offsets */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_EVNT, &sect);
     if (rc != CT_OK) return rc;
     if (sect.data && hdr->event_count > 0) {
         const uint32_t *offsets = (const uint32_t *)sect.data;
         const char **event_ptrs = (const char **)calloc(hdr->event_count, sizeof(char *));
         if (!event_ptrs) return CT_ERR_ALLOC;
         for (uint16_t i = 0; i < hdr->event_count; i++) {
             event_ptrs[i] = out->string_pool + offsets[i];
         }
         out->handle.event_strings = event_ptrs;
         out->handle.event_count = hdr->event_count;
     }
 
     /* Bitmask names — build pointer array from entries */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_BMSK, &sect);
     if (rc != CT_OK) return rc;
     if (sect.data && hdr->bitmask_count > 0) {
         const ctb_bitmask_entry_t *entries = (const ctb_bitmask_entry_t *)sect.data;
         const char **bmask_ptrs = (const char **)calloc(hdr->bitmask_count, sizeof(char *));
         if (!bmask_ptrs) return CT_ERR_ALLOC;
         for (uint16_t i = 0; i < hdr->bitmask_count; i++) {
             bmask_ptrs[i] = out->string_pool + entries[i].string_pool_offset;
         }
         out->handle.bitmask_names = bmask_ptrs;
         out->handle.bitmask_count = hdr->bitmask_count;
     }
 
     /* KB info — build chaintree_kb_info_t array */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_KBIN, &sect);
     if (rc != CT_OK) return rc;
 
     section_info_t alias_sect;
     rc = find_section(image_data, hdr, image_size, CTB_SECT_KBAL, &sect);
     if (rc != CT_OK) return rc;
     alias_sect = sect;
 
     /* Re-find KB info (we overwrote sect above) */
     rc = find_section(image_data, hdr, image_size, CTB_SECT_KBIN, &sect);
     if (rc != CT_OK) return rc;
 
     if (sect.data && hdr->kb_count > 0) {
         const ctb_kb_info_entry_t *kb_entries = (const ctb_kb_info_entry_t *)sect.data;
         const ctb_kb_alias_entry_t *alias_entries = (const ctb_kb_alias_entry_t *)alias_sect.data;
 
         chaintree_kb_info_t *kb_infos = (chaintree_kb_info_t *)calloc(
             hdr->kb_count, sizeof(chaintree_kb_info_t));
         if (!kb_infos) return CT_ERR_ALLOC;
 
         for (uint16_t i = 0; i < hdr->kb_count; i++) {
             kb_infos[i].kb_name = out->string_pool + kb_entries[i].name_offset;
             kb_infos[i].root_node_index = kb_entries[i].root_node_index;
             kb_infos[i].start_index = kb_entries[i].start_index;
             kb_infos[i].node_count = kb_entries[i].node_count;
             kb_infos[i].max_depth = kb_entries[i].max_depth;
             kb_infos[i].memory_factor = kb_entries[i].memory_factor;
             kb_infos[i].alias_count = kb_entries[i].alias_count;
 
             if (kb_entries[i].alias_count > 0 && alias_entries) {
                 node_alias_t *aliases = (node_alias_t *)calloc(
                     kb_entries[i].alias_count, sizeof(node_alias_t));
                 if (!aliases) { free(kb_infos); return CT_ERR_ALLOC; }
 
                 for (uint16_t j = 0; j < kb_entries[i].alias_count; j++) {
                     uint16_t ai = kb_entries[i].alias_start + j;
                     aliases[j].alias = out->string_pool + alias_entries[ai].name_offset;
                     aliases[j].node_index = alias_entries[ai].node_index;
                 }
                 kb_infos[i].aliases = aliases;
             } else {
                 kb_infos[i].aliases = NULL;
             }
         }
         out->handle.kb_table = kb_infos;
         out->handle.kb_count = hdr->kb_count;
     }
 
     /* ---- Allocate function pointer arrays ---- */
 
     out->main_fn_ptrs = (main_function_t *)calloc(
         hdr->main_func_count, sizeof(main_function_t));
     out->one_shot_fn_ptrs = (one_shot_function_t *)calloc(
         hdr->one_shot_func_count, sizeof(one_shot_function_t));
     out->boolean_fn_ptrs = (boolean_function_t *)calloc(
         hdr->boolean_func_count, sizeof(boolean_function_t));
 
     if (!out->main_fn_ptrs || !out->one_shot_fn_ptrs || !out->boolean_fn_ptrs)
         return CT_ERR_ALLOC;
 
     /* Wire into handle */
     out->handle.main_functions = out->main_fn_ptrs;
     out->handle.main_function_count = hdr->main_func_count;
     out->handle.one_shot_functions = out->one_shot_fn_ptrs;
     out->handle.one_shot_function_count = hdr->one_shot_func_count;
     out->handle.boolean_functions = out->boolean_fn_ptrs;
     out->handle.boolean_function_count = hdr->boolean_func_count;
 
     /* Build function name string arrays for the handle */
     {
         const char **main_names = (const char **)calloc(hdr->main_func_count, sizeof(char *));
         const char **os_names = (const char **)calloc(hdr->one_shot_func_count, sizeof(char *));
         const char **bool_names = (const char **)calloc(hdr->boolean_func_count, sizeof(char *));
         if (!main_names || !os_names || !bool_names) return CT_ERR_ALLOC;
 
         const char *p = out->func_name_base;
         for (uint16_t i = 0; i < hdr->main_func_count && p; i++) {
             main_names[i] = p;
             p += strlen(p) + 1;
         }
         for (uint16_t i = 0; i < hdr->one_shot_func_count && p; i++) {
             os_names[i] = p;
             p += strlen(p) + 1;
         }
         for (uint16_t i = 0; i < hdr->boolean_func_count && p; i++) {
             bool_names[i] = p;
             p += strlen(p) + 1;
         }
 
         out->handle.main_function_names = main_names;
         out->handle.one_shot_function_names = os_names;
         out->handle.boolean_function_names = bool_names;
     }
 
     /* Unique ID (not stored in binary — use "ctb_image") */
     out->handle.unique_id = "ctb_image";
 
     /* Usage count not stored in binary; set to NULL */
     out->handle.main_function_usage_count = NULL;
 
     return CT_OK;
 }
 
 /* ===================================================================
  * ct_image_free
  * =================================================================== */
 
 void ct_image_free(ct_image_t *img)
 {
     if (!img) return;
 
     free(img->main_fn_ptrs);
     free(img->one_shot_fn_ptrs);
     free(img->boolean_fn_ptrs);
 
     /* Free allocated string pointer arrays */
     free((void *)img->handle.main_function_names);
     free((void *)img->handle.one_shot_function_names);
     free((void *)img->handle.boolean_function_names);
     free((void *)img->handle.event_strings);
     free((void *)img->handle.bitmask_names);
 
     /* Free KB info and alias arrays */
     if (img->handle.kb_table) {
         for (uint16_t i = 0; i < img->handle.kb_count; i++) {
             free((void *)img->handle.kb_table[i].aliases);
         }
         free((void *)img->handle.kb_table);
     }
 
     memset(img, 0, sizeof(ct_image_t));
 }
 
 /* ===================================================================
  * ct_get_handle
  * =================================================================== */
 
 const chaintree_handle_t *ct_get_handle(const ct_image_t *img)
 {
     if (!img) return NULL;
     return &img->handle;
 }
 
 /* ===================================================================
  * Function registration
  * =================================================================== */
 
 int ct_register_main(ct_image_t *img, const char *name, main_function_t fn)
 {
     if (!img || !name || !fn) return CT_ERR_NULL_PTR;
     uint32_t hash = fnv1a_32(name);
     int slot = binary_search_u32(img->main_hashes, img->main_func_count, hash);
     if (slot < 0) return CT_ERR_HASH_NOT_FOUND;
     img->main_fn_ptrs[slot] = fn;
     return slot;
 }
 
 int ct_register_one_shot(ct_image_t *img, const char *name, one_shot_function_t fn)
 {
     if (!img || !name || !fn) return CT_ERR_NULL_PTR;
     uint32_t hash = fnv1a_32(name);
     int slot = binary_search_u32(img->one_shot_hashes, img->one_shot_func_count, hash);
     if (slot < 0) return CT_ERR_HASH_NOT_FOUND;
     img->one_shot_fn_ptrs[slot] = fn;
     return slot;
 }
 
 int ct_register_boolean(ct_image_t *img, const char *name, boolean_function_t fn)
 {
     if (!img || !name || !fn) return CT_ERR_NULL_PTR;
     uint32_t hash = fnv1a_32(name);
     int slot = binary_search_u32(img->boolean_hashes, img->boolean_func_count, hash);
     if (slot < 0) return CT_ERR_HASH_NOT_FOUND;
     img->boolean_fn_ptrs[slot] = fn;
     return slot;
 }
 
 /* ===================================================================
  * Validation
  * =================================================================== */
 
 int ct_validate(const ct_image_t *img)
 {
     if (!img) return -1;
 
     int missing = 0;
 
     /* Skip slot 0 (CFL_NULL) in each table */
     for (uint16_t i = 1; i < img->main_func_count; i++) {
         if (!img->main_fn_ptrs[i]) {
             const char *name = ct_get_func_name(img, CTB_SECT_MFHT, i);
             fprintf(stderr, "  UNREGISTERED main function [%u]: %s\n",
                     i, name ? name : "?");
             missing++;
         }
     }
     for (uint16_t i = 1; i < img->one_shot_func_count; i++) {
         if (!img->one_shot_fn_ptrs[i]) {
             const char *name = ct_get_func_name(img, CTB_SECT_OSHT, i);
             fprintf(stderr, "  UNREGISTERED one_shot function [%u]: %s\n",
                     i, name ? name : "?");
             missing++;
         }
     }
     for (uint16_t i = 1; i < img->boolean_func_count; i++) {
         if (!img->boolean_fn_ptrs[i]) {
             const char *name = ct_get_func_name(img, CTB_SECT_BFHT, i);
             fprintf(stderr, "  UNREGISTERED boolean function [%u]: %s\n",
                     i, name ? name : "?");
             missing++;
         }
     }
 
     return missing;
 }
 
 /* ===================================================================
  * Function name lookup
  * =================================================================== */
 
 const char *ct_get_func_name(const ct_image_t *img, uint16_t table_type, uint16_t slot)
 {
     if (!img || !img->func_name_base) return NULL;
 
     uint16_t offset = 0;
 
     if (table_type == CTB_SECT_MFHT) {
         offset = 0;
     } else if (table_type == CTB_SECT_OSHT) {
         offset = img->main_func_count;
     } else if (table_type == CTB_SECT_BFHT) {
         offset = img->main_func_count + img->one_shot_func_count;
     } else {
         return NULL;
     }
 
     uint16_t total = img->main_func_count + img->one_shot_func_count + img->boolean_func_count;
     return walk_func_names(img->func_name_base, img->func_name_size,
                           total, offset + slot);
 }