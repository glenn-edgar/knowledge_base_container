/**
 * json_record_file.h
 * 
 * Load JSON records from binary file at runtime.
 * 
 * Binary file format (little-endian):
 *   [Header]
 *     uint32_t magic          = 0x4A534F4E ("JSON")
 *     uint32_t version        = 1
 *     uint32_t record_count
 *     uint32_t string_size
 *     uint32_t control_count
 *   [String table]
 *     char[string_size]       null-terminated strings
 *   [Records]
 *     json_record_t[record_count]
 *   [Controls]
 *     record_control_t[control_count]
 * 
 * Usage with static buffers (embedded):
 *   static char string_buf[1024];
 *   static json_record_t record_buf[256];
 *   static record_control_t control_buf[8];
 *   
 *   json_file_buffers_t bufs = {
 *       .strings = string_buf, .strings_size = sizeof(string_buf),
 *       .records = record_buf, .records_count = 256,
 *       .controls = control_buf, .controls_count = 8
 *   };
 *   
 *   json_reader_t reader;
 *   uint32_t num_controls;
 *   if (json_file_load("tree.bin", &bufs, &reader, &num_controls)) {
 *       // Use reader...
 *   }
 * 
 * Usage with dynamic allocation (Linux/RTOS):
 *   json_file_data_t* data = json_file_load_alloc("tree.bin");
 *   if (data) {
 *       // Use data->reader...
 *       json_file_free(data);
 *   }
 */

 #ifndef CFL_JSON_RECORD_FILE_H
 #define CFL_JSON_RECORD_FILE_H
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 #include "cfl_json_record_reader.h"
 #include "cfl_exception.h"
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 
 #define JSON_FILE_MAGIC   0x4A534F4E  /* "JSON" */
 #define JSON_FILE_VERSION 1
 
 //=============================================================================
 // File header
 //=============================================================================
 
 typedef struct {
     uint32_t magic;
     uint32_t version;
     uint32_t record_count;
     uint32_t string_size;
     uint32_t control_count;
     uint32_t node_count;    // Number of ChainTree nodes
 } json_file_header_t;
 
 //=============================================================================
 // Static buffer interface (no malloc)
 //=============================================================================
 
 typedef struct {
     char* strings;
     uint32_t strings_size;      // buffer capacity
     json_record_t* records;
     uint32_t records_count;     // buffer capacity (num elements)
     record_control_t* controls;
     uint32_t controls_count;    // buffer capacity (num elements)
 } json_file_buffers_t;
 
 /**
  * Load binary file into provided static buffers.
  * Returns true on success.
  * out_data is populated with pointers into the buffers.
  */
 static inline bool json_file_load(const char* path, 
                                    json_file_buffers_t* bufs,
                                    json_data_t* out_data,
                                    record_control_t** out_controls,
                                    uint32_t* out_control_count) {
     if (!path) { EXCEPTION("json_file_load: null path"); return false; }
     if (!bufs) { EXCEPTION("json_file_load: null buffers"); return false; }
     if (!out_data) { EXCEPTION("json_file_load: null out_data"); return false; }
     
     FILE* f = fopen(path, "rb");
     if (!f) {
         EXCEPTION("json_file_load: failed to open file");
         return false;
     }
     
     bool success = false;
     json_file_header_t hdr;
     
     // Read header
     if (fread(&hdr, sizeof(hdr), 1, f) != 1) {
         EXCEPTION("json_file_load: failed to read header");
         goto cleanup;
     }
     
     // Validate header
     if (hdr.magic != JSON_FILE_MAGIC) {
         EXCEPTION("json_file_load: invalid magic number");
         goto cleanup;
     }
     if (hdr.version != JSON_FILE_VERSION) {
         EXCEPTION("json_file_load: unsupported version");
         goto cleanup;
     }
     
     // Check buffer sizes
     if (hdr.string_size > bufs->strings_size) {
         EXCEPTION("json_file_load: string buffer too small");
         goto cleanup;
     }
     if (hdr.record_count > bufs->records_count) {
         EXCEPTION("json_file_load: record buffer too small");
         goto cleanup;
     }
     if (hdr.control_count > bufs->controls_count) {
         EXCEPTION("json_file_load: control buffer too small");
         goto cleanup;
     }
     
     // Read string table
     if (hdr.string_size > 0) {
         if (fread(bufs->strings, 1, hdr.string_size, f) != hdr.string_size) {
             EXCEPTION("json_file_load: failed to read strings");
             goto cleanup;
         }
     }
     
     // Read records
     if (hdr.record_count > 0) {
         size_t rec_bytes = hdr.record_count * sizeof(json_record_t);
         if (fread(bufs->records, 1, rec_bytes, f) != rec_bytes) {
             EXCEPTION("json_file_load: failed to read records");
             goto cleanup;
         }
     }
     
     // Read controls
     if (hdr.control_count > 0) {
         size_t ctrl_bytes = hdr.control_count * sizeof(record_control_t);
         if (fread(bufs->controls, 1, ctrl_bytes, f) != ctrl_bytes) {
             EXCEPTION("json_file_load: failed to read controls");
             goto cleanup;
         }
     }
     
     // Initialize output data descriptor
     out_data->records = bufs->records;
     out_data->record_count = hdr.record_count;
     out_data->strings = bufs->strings;
     out_data->string_size = hdr.string_size;
     out_data->node_count = hdr.node_count;
     
     if (out_controls) {
         *out_controls = bufs->controls;
     }
     if (out_control_count) {
         *out_control_count = hdr.control_count;
     }
     
     success = true;
     
 cleanup:
     fclose(f);
     return success;
 }
 
 //=============================================================================
 // Dynamic allocation interface (for systems with malloc)
 //=============================================================================
 
 typedef struct {
     json_data_t data;           // Use this with json_cursor_init_from_data()
     record_control_t* controls; // For multiple sub-objects (if loaded)
     uint32_t control_count;
     // Internal storage (freed together)
     char* _string_buf;
     json_record_t* _record_buf;
     record_control_t* _control_buf;
 } json_file_data_t;
 
 /**
  * Load binary file with dynamic allocation.
  * Returns pointer to data structure, or NULL on failure.
  * Caller must call json_file_free() when done.
  */
 static inline json_file_data_t* json_file_load_alloc(const char* path) {
     if (!path) { EXCEPTION("json_file_load_alloc: null path"); return NULL; }
     
     FILE* f = fopen(path, "rb");
     if (!f) {
         EXCEPTION("json_file_load_alloc: failed to open file");
         return NULL;
     }
     
     json_file_data_t* data = NULL;
     json_file_header_t hdr;
     char* string_buf = NULL;
     json_record_t* record_buf = NULL;
     record_control_t* control_buf = NULL;
     
     // Read header
     if (fread(&hdr, sizeof(hdr), 1, f) != 1) {
         EXCEPTION("json_file_load_alloc: failed to read header");
         goto error;
     }
     
     // Validate header
     if (hdr.magic != JSON_FILE_MAGIC) {
         EXCEPTION("json_file_load_alloc: invalid magic number");
         goto error;
     }
     if (hdr.version != JSON_FILE_VERSION) {
         EXCEPTION("json_file_load_alloc: unsupported version");
         goto error;
     }
     
     // Allocate buffers
     data = (json_file_data_t*)calloc(1, sizeof(json_file_data_t));
     if (!data) {
         EXCEPTION("json_file_load_alloc: failed to allocate data struct");
         goto error;
     }
     
     if (hdr.string_size > 0) {
         string_buf = (char*)malloc(hdr.string_size);
         if (!string_buf) {
             EXCEPTION("json_file_load_alloc: failed to allocate strings");
             goto error;
         }
     }
     
     if (hdr.record_count > 0) {
         record_buf = (json_record_t*)malloc(hdr.record_count * sizeof(json_record_t));
         if (!record_buf) {
             EXCEPTION("json_file_load_alloc: failed to allocate records");
             goto error;
         }
     }
     
     if (hdr.control_count > 0) {
         control_buf = (record_control_t*)malloc(hdr.control_count * sizeof(record_control_t));
         if (!control_buf) {
             EXCEPTION("json_file_load_alloc: failed to allocate controls");
             goto error;
         }
     }
     
     // Read data
     if (hdr.string_size > 0) {
         if (fread(string_buf, 1, hdr.string_size, f) != hdr.string_size) {
             EXCEPTION("json_file_load_alloc: failed to read strings");
             goto error;
         }
     }
     
     if (hdr.record_count > 0) {
         size_t rec_bytes = hdr.record_count * sizeof(json_record_t);
         if (fread(record_buf, 1, rec_bytes, f) != rec_bytes) {
             EXCEPTION("json_file_load_alloc: failed to read records");
             goto error;
         }
     }
     
     if (hdr.control_count > 0) {
         size_t ctrl_bytes = hdr.control_count * sizeof(record_control_t);
         if (fread(control_buf, 1, ctrl_bytes, f) != ctrl_bytes) {
             EXCEPTION("json_file_load_alloc: failed to read controls");
             goto error;
         }
     }
     
     // Setup data structure
     data->data.records = record_buf;
     data->data.record_count = hdr.record_count;
     data->data.strings = string_buf;
     data->data.string_size = hdr.string_size;
     data->data.node_count = hdr.node_count;
     data->controls = control_buf;
     data->control_count = hdr.control_count;
     data->_string_buf = string_buf;
     data->_record_buf = record_buf;
     data->_control_buf = control_buf;
     
     fclose(f);
     return data;
     
 error:
     if (f) fclose(f);
     if (string_buf) free(string_buf);
     if (record_buf) free(record_buf);
     if (control_buf) free(control_buf);
     if (data) free(data);
     return NULL;
 }
 
 /**
  * Free data loaded by json_file_load_alloc().
  */
 static inline void json_file_free(json_file_data_t* data) {
     if (!data) return;
     if (data->_string_buf) free(data->_string_buf);
     if (data->_record_buf) free(data->_record_buf);
     if (data->_control_buf) free(data->_control_buf);
     free(data);
 }
 
 //=============================================================================
 // Query file info without loading
 //=============================================================================
 
 typedef struct {
     uint32_t record_count;
     uint32_t string_size;
     uint32_t control_count;
     uint32_t node_count;
     bool valid;
 } json_file_info_t;
 
 static inline json_file_info_t json_file_get_info(const char* path) {
     json_file_info_t info = {0};
     
     if (!path) return info;
     
     FILE* f = fopen(path, "rb");
     if (!f) return info;
     
     json_file_header_t hdr;
     if (fread(&hdr, sizeof(hdr), 1, f) == 1 &&
         hdr.magic == JSON_FILE_MAGIC &&
         hdr.version == JSON_FILE_VERSION) {
         info.record_count = hdr.record_count;
         info.string_size = hdr.string_size;
         info.control_count = hdr.control_count;
         info.node_count = hdr.node_count;
         info.valid = true;
     }
     
     fclose(f);
     return info;
 }
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif // CFL_JSON_RECORD_FILE_H