/*
 * cfl_image_loader.c - ChainTree Binary Image Loader
 *
 * All arrays in original indexer order. Hash tables contain
 * {hash, slot_index} pairs for registration lookup only.
 */

 #include "cfl_image_loader.h"

 #include <stdlib.h>
 #include <string.h>
 #include <stdio.h>
 
 /* ===================================================================
  * CRC32
  * =================================================================== */
 
 static uint32_t crc32_table[256];
 static int crc32_table_ready = 0;
 
 static void crc32_init_table(void)
 {
     for (uint32_t i = 0; i < 256; i++) {
         uint32_t c = i;
         for (int j = 0; j < 8; j++) {
             c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
         }
         crc32_table[i] = c;
     }
     crc32_table_ready = 1;
 }
 
 static uint32_t crc32_compute(const uint8_t *data, uint32_t len,
                               uint32_t skip_start, uint32_t skip_end)
 {
     if (!crc32_table_ready) crc32_init_table();
     uint32_t crc = 0xFFFFFFFFu;
     for (uint32_t i = 0; i < len; i++) {
         uint8_t b = (i >= skip_start && i < skip_end) ? 0 : data[i];
         crc = crc32_table[(crc ^ b) & 0xFF] ^ (crc >> 8);
     }
     return crc ^ 0xFFFFFFFFu;
 }
 
 /* ===================================================================
  * Binary search on cfl_ctb_hash_entry_t array (sorted by hash)
  * Returns slot_index if found, -1 if not found
  * =================================================================== */
 
 static int hash_table_lookup(const cfl_ctb_hash_entry_t *table,
                              uint16_t count, uint32_t target_hash)
 {
     int lo = 0;
     int hi = (int)count - 1;
 
     while (lo <= hi) {
         int mid = lo + (hi - lo) / 2;
         if (table[mid].hash == target_hash)
             return (int)table[mid].slot_index;
         if (table[mid].hash < target_hash)
             lo = mid + 1;
         else
             hi = mid - 1;
     }
     return -1;
 }
 
 /* ===================================================================
  * Section lookup
  * =================================================================== */
 
 typedef struct {
     const void *data;
     uint32_t    size;
     uint16_t    entry_count;
     uint16_t    entry_size;
 } section_info_t;
 
 static int find_section(const void *image_base,
                         const cfl_ctb_header_t *hdr,
                         uint32_t image_size,
                         uint32_t section_type,
                         section_info_t *out)
 {
     const cfl_ctb_section_dir_t *dir = (const cfl_ctb_section_dir_t *)
         ((const uint8_t *)image_base + sizeof(cfl_ctb_header_t));
 
     for (uint16_t i = 0; i < hdr->section_count; i++) {
         if (dir[i].section_type == section_type) {
             if (dir[i].offset + dir[i].size > image_size)
                 return CFL_IMAGE_ERR_SECT_OOB;
             out->data        = (const uint8_t *)image_base + dir[i].offset;
             out->size        = dir[i].size;
             out->entry_count = dir[i].entry_count;
             out->entry_size  = dir[i].entry_size;
             return CFL_IMAGE_OK;
         }
     }
     out->data = NULL;
     out->size = 0;
     out->entry_count = 0;
     out->entry_size = 0;
     return CFL_IMAGE_OK;
 }
 
 /* ===================================================================
  * Walk packed NUL-delimited strings
  * =================================================================== */
 
 static const char *walk_strings(const char *base, uint32_t total_size,
                                 uint16_t slot)
 {
     if (!base) return NULL;
     const char *p = base;
     const char *end = base + total_size;
     uint16_t idx = 0;
     while (p < end && idx < slot) {
         p += strlen(p) + 1;
         idx++;
     }
     return (p < end) ? p : NULL;
 }
 
 /* ===================================================================
  * cfl_image_load
  * =================================================================== */
 
 int cfl_image_load(const void *image_data, uint32_t image_size,
                    cfl_image_loader_t *out)
 {
     if (!image_data || !out) return CFL_IMAGE_ERR_NULL_PTR;
     if (image_size < sizeof(cfl_ctb_header_t)) return CFL_IMAGE_ERR_TOO_SMALL;
 
     memset(out, 0, sizeof(cfl_image_loader_t));
 
     const cfl_ctb_header_t *hdr = (const cfl_ctb_header_t *)image_data;
 
     if (hdr->magic != CFL_CTB_MAGIC)                return CFL_IMAGE_ERR_BAD_MAGIC;
     if (hdr->version_major != CFL_CTB_VERSION_MAJOR) return CFL_IMAGE_ERR_BAD_VER;
     if (hdr->total_image_size != image_size)          return CFL_IMAGE_ERR_BAD_SIZE;
 
     /* Checksum at offset 0x10..0x13 */
     {
         uint32_t stored = hdr->checksum;
         uint32_t computed = crc32_compute(
             (const uint8_t *)image_data, image_size, 0x10, 0x14);
         if (computed != stored) return CFL_IMAGE_ERR_BAD_CRC;
     }
 
     uint32_t dir_end = (uint32_t)sizeof(cfl_ctb_header_t) +
                        (uint32_t)hdr->section_count * sizeof(cfl_ctb_section_dir_t);
     if (dir_end > image_size) return CFL_IMAGE_ERR_TOO_SMALL;
 
     out->image_base          = image_data;
     out->image_size          = image_size;
     out->header              = hdr;
     out->main_func_count     = hdr->main_func_count;
     out->one_shot_func_count = hdr->one_shot_func_count;
     out->boolean_func_count  = hdr->boolean_func_count;
 
     section_info_t sect;
     int rc;
 
     /* ---- Flash-direct sections ---- */
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_NODE, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->handle.nodes      = (const cfl_chaintree_node_t *)sect.data;
     out->handle.node_count = hdr->node_active_count;
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_LINK, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->handle.link_table      = (const uint16_t *)sect.data;
     out->handle.link_table_size = hdr->link_table_size;
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_JREC, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->handle.node_data_records       = (const cfl_json_record_t *)sect.data;
     out->handle.node_data_records_count = hdr->json_records_count;
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_JCTL, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->handle.node_data_controls       = (const cfl_record_control_t *)sect.data;
     out->handle.node_data_controls_count = hdr->json_controls_count;
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_JSTR, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->handle.node_data_strings      = (const char *)sect.data;
     out->handle.node_data_strings_size = (uint16_t)hdr->json_strings_size;
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_GSTR, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->string_pool      = (const char *)sect.data;
     out->string_pool_size = sect.size;
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_FSTR, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->func_name_base = (const char *)sect.data;
     out->func_name_size = sect.size;
 
     /* Usage count (flash-direct, original order) */
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_MUSG, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->handle.main_function_usage_count = (const uint16_t *)sect.data;
 
     /* ---- Hash tables (flash, for registration) ---- */
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_MFHT, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->main_hash_table = (const cfl_ctb_hash_entry_t *)sect.data;
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_OSHT, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->one_shot_hash_table = (const cfl_ctb_hash_entry_t *)sect.data;
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_BFHT, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     out->boolean_hash_table = (const cfl_ctb_hash_entry_t *)sect.data;
 
     /* No registry pointer needed — hash tables stored directly */
     out->handle.registry = NULL;
 
     /* Unique ID — not in binary format */
     out->handle.unique_id = "ctb_image";
 
     /* ---- RAM: function pointer arrays ---- */
 
     out->main_fn_ptrs = (cfl_main_function_t *)calloc(
         hdr->main_func_count, sizeof(cfl_main_function_t));
     out->one_shot_fn_ptrs = (cfl_one_shot_function_t *)calloc(
         hdr->one_shot_func_count, sizeof(cfl_one_shot_function_t));
     out->boolean_fn_ptrs = (cfl_boolean_function_t *)calloc(
         hdr->boolean_func_count, sizeof(cfl_boolean_function_t));
 
     if (!out->main_fn_ptrs || !out->one_shot_fn_ptrs || !out->boolean_fn_ptrs)
         return CFL_IMAGE_ERR_ALLOC;
 
     /* Wire into handle — same field layout as .h/.c path */
     out->handle.main_functions          = out->main_fn_ptrs;
     out->handle.main_function_count     = hdr->main_func_count;
     out->handle.one_shot_functions      = out->one_shot_fn_ptrs;
     out->handle.one_shot_function_count = hdr->one_shot_func_count;
     out->handle.boolean_functions       = out->boolean_fn_ptrs;
     out->handle.boolean_function_count  = hdr->boolean_func_count;
 
     /* ---- RAM: function name pointer arrays (original order) ---- */
     {
         const char **main_names = (const char **)calloc(
             hdr->main_func_count, sizeof(char *));
         const char **os_names = (const char **)calloc(
             hdr->one_shot_func_count, sizeof(char *));
         const char **bool_names = (const char **)calloc(
             hdr->boolean_func_count, sizeof(char *));
 
         if (!main_names || !os_names || !bool_names)
             return CFL_IMAGE_ERR_ALLOC;
 
         /* FSTR contains KB names in original order:
          * main[0]\0 main[1]\0 ... os[0]\0 ... bool[0]\0 ... */
         const char *p   = out->func_name_base;
         const char *end = p ? (p + out->func_name_size) : NULL;
 
         for (uint16_t i = 0; i < hdr->main_func_count && p && p < end; i++) {
             main_names[i] = p;
             p += strlen(p) + 1;
         }
         for (uint16_t i = 0; i < hdr->one_shot_func_count && p && p < end; i++) {
             os_names[i] = p;
             p += strlen(p) + 1;
         }
         for (uint16_t i = 0; i < hdr->boolean_func_count && p && p < end; i++) {
             bool_names[i] = p;
             p += strlen(p) + 1;
         }
 
         out->handle.main_function_names     = main_names;
         out->handle.one_shot_function_names = os_names;
         out->handle.boolean_function_names  = bool_names;
     }
 
     /* ---- RAM: event string pointers ---- */
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_EVNT, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     if (sect.data && hdr->event_count > 0) {
         const uint32_t *offsets = (const uint32_t *)sect.data;
         const char **ptrs = (const char **)calloc(hdr->event_count, sizeof(char *));
         if (!ptrs) return CFL_IMAGE_ERR_ALLOC;
         for (uint16_t i = 0; i < hdr->event_count; i++)
             ptrs[i] = out->string_pool + offsets[i];
         out->handle.event_strings = ptrs;
         out->handle.event_count   = hdr->event_count;
     }
 
     /* ---- RAM: bitmask name pointers ---- */
 
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_BMSK, &sect);
     if (rc != CFL_IMAGE_OK) return rc;
     if (sect.data && hdr->bitmask_count > 0) {
         const cfl_ctb_bitmask_entry_t *entries =
             (const cfl_ctb_bitmask_entry_t *)sect.data;
         const char **ptrs = (const char **)calloc(hdr->bitmask_count, sizeof(char *));
         if (!ptrs) return CFL_IMAGE_ERR_ALLOC;
         for (uint16_t i = 0; i < hdr->bitmask_count; i++)
             ptrs[i] = out->string_pool + entries[i].string_pool_offset;
         out->handle.bitmask_names = ptrs;
         out->handle.bitmask_count = hdr->bitmask_count;
     }
 
     /* ---- RAM: KB info + aliases ---- */
 
     section_info_t kb_sect;
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_KBIN, &kb_sect);
     if (rc != CFL_IMAGE_OK) return rc;
     section_info_t alias_sect = {0};
     rc = find_section(image_data, hdr, image_size, CFL_CTB_SECT_KBAL, &alias_sect);
     if (rc != CFL_IMAGE_OK) return rc;
 
     if (kb_sect.data && hdr->kb_count > 0) {
         const cfl_ctb_kb_info_entry_t *kb_entries =
             (const cfl_ctb_kb_info_entry_t *)kb_sect.data;
         const cfl_ctb_kb_alias_entry_t *alias_entries =
             (const cfl_ctb_kb_alias_entry_t *)alias_sect.data;
 
         cfl_chaintree_kb_info_t *kb_infos = (cfl_chaintree_kb_info_t *)calloc(
             hdr->kb_count, sizeof(cfl_chaintree_kb_info_t));
         if (!kb_infos) return CFL_IMAGE_ERR_ALLOC;
 
         for (uint16_t i = 0; i < hdr->kb_count; i++) {
             kb_infos[i].kb_name         = out->string_pool + kb_entries[i].name_offset;
             kb_infos[i].root_node_index = kb_entries[i].root_node_index;
             kb_infos[i].start_index     = kb_entries[i].start_index;
             kb_infos[i].node_count      = kb_entries[i].node_count;
             kb_infos[i].max_depth       = kb_entries[i].max_depth;
             kb_infos[i].memory_factor   = kb_entries[i].memory_factor;
             kb_infos[i].alias_count     = kb_entries[i].alias_count;
 
             if (kb_entries[i].alias_count > 0 && alias_entries) {
                 cfl_node_alias_t *aliases = (cfl_node_alias_t *)calloc(
                     kb_entries[i].alias_count, sizeof(cfl_node_alias_t));
                 if (!aliases) {
                     for (uint16_t k = 0; k < i; k++)
                         free((void *)kb_infos[k].aliases);
                     free(kb_infos);
                     return CFL_IMAGE_ERR_ALLOC;
                 }
                 for (uint16_t j = 0; j < kb_entries[i].alias_count; j++) {
                     uint16_t ai = kb_entries[i].alias_start + j;
                     aliases[j].alias      = out->string_pool + alias_entries[ai].name_offset;
                     aliases[j].node_index = alias_entries[ai].node_index;
                 }
                 kb_infos[i].aliases = aliases;
             }
         }
         out->handle.kb_table = kb_infos;
         out->handle.kb_count = hdr->kb_count;
     }
 
     return CFL_IMAGE_OK;
 }
 
 /* ===================================================================
  * cfl_image_free
  * =================================================================== */
 
 void cfl_image_free(cfl_image_loader_t *img)
 {
     if (!img) return;
 
     free(img->main_fn_ptrs);
     free(img->one_shot_fn_ptrs);
     free(img->boolean_fn_ptrs);
 
     free((void *)img->handle.main_function_names);
     free((void *)img->handle.one_shot_function_names);
     free((void *)img->handle.boolean_function_names);
 
     free((void *)img->handle.event_strings);
     free((void *)img->handle.bitmask_names);
 
     if (img->handle.kb_table) {
         for (uint16_t i = 0; i < img->handle.kb_count; i++)
             free((void *)img->handle.kb_table[i].aliases);
         free((void *)img->handle.kb_table);
     }
 
     memset(img, 0, sizeof(cfl_image_loader_t));
 }
 
 /* ===================================================================
  * cfl_image_get_handle
  * =================================================================== */
 
 const cfl_chaintree_handle_t *cfl_image_get_handle(const cfl_image_loader_t *img)
 {
     return img ? &img->handle : NULL;
 }
 
 /* ===================================================================
  * Function registration
  *
  * Hash the typed name → binary search the hash table →
  * get slot_index → write fn ptr to that slot.
  * =================================================================== */
 
 int cfl_image_register_main(cfl_image_loader_t *img,
                             const char *name, cfl_main_function_t fn)
 {
     if (!img || !name || !fn) return CFL_IMAGE_ERR_NULL_PTR;
     if (!img->main_hash_table) return CFL_IMAGE_ERR_NULL_PTR;
 
     uint32_t hash = cfl_fnv1a_32(name);
     int slot = hash_table_lookup(img->main_hash_table,
                                  img->main_func_count, hash);
     if (slot < 0) return CFL_IMAGE_ERR_HASH_NOT_FOUND;
     img->main_fn_ptrs[slot] = fn;
     return slot;
 }
 
 int cfl_image_register_one_shot(cfl_image_loader_t *img,
                                 const char *name, cfl_one_shot_function_t fn)
 {
     if (!img || !name || !fn) return CFL_IMAGE_ERR_NULL_PTR;
     if (!img->one_shot_hash_table) return CFL_IMAGE_ERR_NULL_PTR;
 
     uint32_t hash = cfl_fnv1a_32(name);
     int slot = hash_table_lookup(img->one_shot_hash_table,
                                  img->one_shot_func_count, hash);
     if (slot < 0) return CFL_IMAGE_ERR_HASH_NOT_FOUND;
     img->one_shot_fn_ptrs[slot] = fn;
     return slot;
 }
 
 int cfl_image_register_boolean(cfl_image_loader_t *img,
                                const char *name, cfl_boolean_function_t fn)
 {
     if (!img || !name || !fn) return CFL_IMAGE_ERR_NULL_PTR;
     if (!img->boolean_hash_table) return CFL_IMAGE_ERR_NULL_PTR;
 
     uint32_t hash = cfl_fnv1a_32(name);
     int slot = hash_table_lookup(img->boolean_hash_table,
                                  img->boolean_func_count, hash);
     if (slot < 0) return CFL_IMAGE_ERR_HASH_NOT_FOUND;
     img->boolean_fn_ptrs[slot] = fn;
     return slot;
 }
 
 /* ===================================================================
  * Validation — check all slots filled (skip slot 0 = CFL_NULL)
  * =================================================================== */
 
 int cfl_image_validate(const cfl_image_loader_t *img)
 {
     if (!img) return -1;
     int missing = 0;
 
     for (uint16_t i = 1; i < img->main_func_count; i++) {
         if (!img->main_fn_ptrs[i]) {
             const char *name = cfl_image_get_func_name(img, CFL_CTB_SECT_MFHT, i);
             fprintf(stderr, "  MISSING main[%u]: %s\n", i, name ? name : "?");
             missing++;
         }
     }
     for (uint16_t i = 1; i < img->one_shot_func_count; i++) {
         if (!img->one_shot_fn_ptrs[i]) {
             const char *name = cfl_image_get_func_name(img, CFL_CTB_SECT_OSHT, i);
             fprintf(stderr, "  MISSING one_shot[%u]: %s\n", i, name ? name : "?");
             missing++;
         }
     }
     for (uint16_t i = 1; i < img->boolean_func_count; i++) {
         if (!img->boolean_fn_ptrs[i]) {
             const char *name = cfl_image_get_func_name(img, CFL_CTB_SECT_BFHT, i);
             fprintf(stderr, "  MISSING boolean[%u]: %s\n", i, name ? name : "?");
             missing++;
         }
     }
 
     return missing;
 }
 
 /* ===================================================================
  * Function name lookup — names are in ORIGINAL order in FSTR
  * =================================================================== */
 
 const char *cfl_image_get_func_name(const cfl_image_loader_t *img,
                                     uint16_t table_type, uint16_t slot)
 {
     if (!img || !img->func_name_base) return NULL;
 
     uint16_t offset;
     if (table_type == CFL_CTB_SECT_MFHT)
         offset = 0;
     else if (table_type == CFL_CTB_SECT_OSHT)
         offset = img->main_func_count;
     else if (table_type == CFL_CTB_SECT_BFHT)
         offset = img->main_func_count + img->one_shot_func_count;
     else
         return NULL;
 
     return walk_strings(img->func_name_base, img->func_name_size, offset + slot);
 }