/**
 * json_path.h
 * 
 * Path-based lookup for JSON records with dot notation and array indexing.
 * 
 * Path syntax:
 *   "key"              - object key
 *   "a.b.c"            - nested objects
 *   "arr[0]"           - array index
 *   "arr[0].key"       - array element then object
 *   "a.b[2].c[0].d"    - mixed paths
 * 
 * Two API styles:
 *   - Optional: json_path_string(c, path, default) - returns default on failure
 *   - Required: json_path_string_ex(c, path) - calls EXCEPTION on failure
 * 
 * Usage:
 *   // Optional - returns default if not found
 *   int32_t timeout = json_path_int(&c, "config.timeout", 1000);
 *   
 *   // Required - throws exception if not found
 *   const char* name = json_path_string_ex(&c, "name");
 */

 #ifndef CFL_JSON_PATH_H
 #define CFL_JSON_PATH_H

 #ifdef __cplusplus
 extern "C" {
 #endif
 #include "cfl_json_record_reader.h"
 #include "cfl_exception.h"
 #include <stdlib.h>
 #include <stdio.h>
 
 //=============================================================================
 // Exception message buffer
 //=============================================================================
 
 #ifndef JSON_EX_MSG_SIZE
 #define JSON_EX_MSG_SIZE 96
 #endif
 
 static char json_ex_msg_buf[JSON_EX_MSG_SIZE];
 
 static inline void json_ex_msg(const char* prefix, const char* path) {
     snprintf(json_ex_msg_buf, JSON_EX_MSG_SIZE, "%s: '%s'", prefix, path ? path : "(null)");
 }
 
 static inline void json_ex_msg2(const char* prefix, const char* path, const char* detail) {
     snprintf(json_ex_msg_buf, JSON_EX_MSG_SIZE, "%s: '%s' (%s)", prefix, path ? path : "(null)", detail);
 }
 
 //=============================================================================
 // Path result codes
 //=============================================================================
 
 typedef enum {
     JSON_PATH_OK = 0,
     JSON_PATH_NOT_FOUND,
     JSON_PATH_TYPE_MISMATCH,
     JSON_PATH_INDEX_OUT_OF_BOUNDS,
     JSON_PATH_INVALID_SYNTAX
 } json_path_result_t;
 
 //=============================================================================
 // Non-throwing path lookup
 //=============================================================================
 
 static inline json_path_result_t json_path_get(json_cursor_t* c, const char* path) {
     if (!c || !path) return JSON_PATH_INVALID_SYNTAX;
     
     const char* p = path;
     
     while (*p != '\0') {
         if (*p == '.') p++;
         
         // Array index access
         if (*p == '[') {
             if (json_cursor_type(c) != JSON_TYPE_ARRAY) {
                 return JSON_PATH_TYPE_MISMATCH;
             }
             
             p++;
             char* end;
             long idx = strtol(p, &end, 10);
             if (end == p || *end != ']') {
                 return JSON_PATH_INVALID_SYNTAX;
             }
             p = end + 1;
             
             uint32_t count = json_array_count(c);
             if (idx < 0 || (uint32_t)idx >= count) {
                 return JSON_PATH_INDEX_OUT_OF_BOUNDS;
             }
             
             c->pos++;
             for (long i = 0; i < idx; i++) {
                 json_cursor_skip(c);
             }
             continue;
         }
         
         // Object key access
         if (json_cursor_type(c) != JSON_TYPE_OBJECT) {
             return JSON_PATH_TYPE_MISMATCH;
         }
         
         const char* key_end = p;
         while (*key_end != '\0' && *key_end != '.' && *key_end != '[') {
             key_end++;
         }
         int key_len = key_end - p;
         
         const json_record_t* rec = &c->reader->records[c->pos];
         uint32_t pair_count = rec->value.container_count / 2;
         uint32_t pos = c->pos + 1;
         bool found = false;
         
         for (uint32_t i = 0; i < pair_count; i++) {
             const char* k = c->reader->string_table + c->reader->records[pos].value.string_offset;
             pos++;
             
             int k_len = strlen(k);
             if (k_len == key_len && memcmp(k, p, key_len) == 0) {
                 c->pos = pos;
                 found = true;
                 break;
             }
             pos = json_skip_value(c->reader, pos);
         }
         
         if (!found) return JSON_PATH_NOT_FOUND;
         p = key_end;
     }
     
     return JSON_PATH_OK;
 }
 
 //=============================================================================
 // Throwing path lookup
 //=============================================================================
 
 static inline void json_path_get_ex(json_cursor_t* c, const char* path) {
     if (!c) { 
         json_ex_msg("null cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return; 
     }
     if (!path) { 
         EXCEPTION("json_path: null path"); 
         return; 
     }
     
     json_path_result_t result = json_path_get(c, path);
     
     switch (result) {
         case JSON_PATH_OK:
             return;
         case JSON_PATH_NOT_FOUND:
             json_ex_msg("path not found", path);
             EXCEPTION(json_ex_msg_buf);
             return;
         case JSON_PATH_TYPE_MISMATCH:
             json_ex_msg("type mismatch at", path);
             EXCEPTION(json_ex_msg_buf);
             return;
         case JSON_PATH_INDEX_OUT_OF_BOUNDS:
             json_ex_msg("index out of bounds", path);
             EXCEPTION(json_ex_msg_buf);
             return;
         case JSON_PATH_INVALID_SYNTAX:
             json_ex_msg("invalid syntax", path);
             EXCEPTION(json_ex_msg_buf);
             return;
     }
 }
 
 //=============================================================================
 // Optional getters - return default, no exception
 //=============================================================================
 
 static inline const char* json_path_string(json_cursor_t* c, const char* path, const char* def) {
     if (!c) return def;
     json_cursor_t temp = *c;
     if (json_path_get(&temp, path) != JSON_PATH_OK) return def;
     if (json_cursor_type(&temp) != JSON_TYPE_STRING) return def;
     const char* s = json_get_string(&temp);
     return s ? s : def;
 }
 
 static inline int32_t json_path_int(json_cursor_t* c, const char* path, int32_t def) {
     if (!c) return def;
     json_cursor_t temp = *c;
     if (json_path_get(&temp, path) != JSON_PATH_OK) return def;
     if (json_cursor_type(&temp) != JSON_TYPE_INT32) return def;
     return json_get_int(&temp, def);
 }
 
 static inline float json_path_float(json_cursor_t* c, const char* path, float def) {
     if (!c) return def;
     json_cursor_t temp = *c;
     if (json_path_get(&temp, path) != JSON_PATH_OK) return def;
     json_type_t t = json_cursor_type(&temp);
     if (t != JSON_TYPE_FLOAT32 && t != JSON_TYPE_INT32) return def;
     return json_get_float(&temp, def);
 }
 
 static inline bool json_path_bool(json_cursor_t* c, const char* path, bool def) {
     if (!c) return def;
     json_cursor_t temp = *c;
     if (json_path_get(&temp, path) != JSON_PATH_OK) return def;
     if (json_cursor_type(&temp) != JSON_TYPE_BOOL) return def;
     return json_get_bool(&temp, def);
 }
 
 static inline bool json_path_exists(json_cursor_t* c, const char* path) {
     if (!c) return false;
     json_cursor_t temp = *c;
     return json_path_get(&temp, path) == JSON_PATH_OK;
 }
 
 static inline json_path_result_t json_path_cursor(json_cursor_t* c, const char* path, json_cursor_t* out) {
     if (!c || !out) return JSON_PATH_INVALID_SYNTAX;
     *out = *c;
     return json_path_get(out, path);
 }
 
 static inline uint32_t json_path_array_count(json_cursor_t* c, const char* path) {
     if (!c) return 0;
     json_cursor_t temp = *c;
     if (json_path_get(&temp, path) != JSON_PATH_OK) return 0;
     if (json_cursor_type(&temp) != JSON_TYPE_ARRAY) return 0;
     return json_array_count(&temp);
 }
 
 //=============================================================================
 // Required getters - call EXCEPTION with path on failure
 //=============================================================================
 
 static inline const char* json_path_string_ex(json_cursor_t* c, const char* path) {
     if (!c) { 
         json_ex_msg("null cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return NULL; 
     }
     
     json_cursor_t temp = *c;
     json_path_get_ex(&temp, path);
     
     if (json_cursor_type(&temp) != JSON_TYPE_STRING) {
         json_ex_msg2("expected string at", path, "got different type");
         EXCEPTION(json_ex_msg_buf);
         return NULL;
     }
     return json_get_string(&temp);
 }
 
 static inline int32_t json_path_int_ex(json_cursor_t* c, const char* path) {
     if (!c) { 
         json_ex_msg("null cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return 0; 
     }
     
     json_cursor_t temp = *c;
     json_path_get_ex(&temp, path);
     
     if (json_cursor_type(&temp) != JSON_TYPE_INT32) {
         json_ex_msg2("expected int at", path, "got different type");
         EXCEPTION(json_ex_msg_buf);
         return 0;
     }
     return json_get_int(&temp, 0);
 }
 
 static inline float json_path_float_ex(json_cursor_t* c, const char* path) {
     if (!c) { 
         json_ex_msg("null cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return 0.0f; 
     }
     
     json_cursor_t temp = *c;
     json_path_get_ex(&temp, path);
     
     json_type_t t = json_cursor_type(&temp);
     if (t != JSON_TYPE_FLOAT32 && t != JSON_TYPE_INT32) {
         json_ex_msg2("expected number at", path, "got different type");
         EXCEPTION(json_ex_msg_buf);
         return 0.0f;
     }
     return json_get_float(&temp, 0.0f);
 }
 
 static inline bool json_path_bool_ex(json_cursor_t* c, const char* path) {
     if (!c) { 
         json_ex_msg("null cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return false; 
     }
     
     json_cursor_t temp = *c;
     json_path_get_ex(&temp, path);
     
     if (json_cursor_type(&temp) != JSON_TYPE_BOOL) {
         json_ex_msg2("expected bool at", path, "got different type");
         EXCEPTION(json_ex_msg_buf);
         return false;
     }
     return json_get_bool(&temp, false);
 }
 
 static inline void json_path_cursor_ex(json_cursor_t* c, const char* path, json_cursor_t* out) {
     if (!c) { 
         json_ex_msg("null cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return; 
     }
     if (!out) { 
         json_ex_msg("null output cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return; 
     }
     
     *out = *c;
     json_path_get_ex(out, path);
 }
 
 static inline uint32_t json_path_array_ex(json_cursor_t* c, const char* path, json_cursor_t* out) {
     if (!c) { 
         json_ex_msg("null cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return 0; 
     }
     if (!out) { 
         json_ex_msg("null output cursor", path);
         EXCEPTION(json_ex_msg_buf); 
         return 0; 
     }
     
     *out = *c;
     json_path_get_ex(out, path);
     
     if (json_cursor_type(out) != JSON_TYPE_ARRAY) {
         json_ex_msg2("expected array at", path, "got different type");
         EXCEPTION(json_ex_msg_buf);
         return 0;
     }
     return json_array_count(out);
 }
 
 //=============================================================================
 // Path array iteration helper
 //=============================================================================
 
 typedef struct {
     json_array_iter_t iter;
     bool valid;
 } json_path_array_iter_t;
 
 static inline bool json_path_array_iter_init(json_path_array_iter_t* it, 
                                               json_cursor_t* c, 
                                               const char* path) {
     if (!it) { 
         EXCEPTION("json_path_array_iter_init: null iterator"); 
         return false; 
     }
     
     it->valid = false;
     if (!c) return false;
     
     json_cursor_t temp = *c;
     if (json_path_get(&temp, path) != JSON_PATH_OK) return false;
     
     it->valid = json_array_iter_init(&it->iter, &temp);
     return it->valid;
 }
 
 static inline bool json_path_array_iter_next(json_path_array_iter_t* it, json_cursor_t* out) {
     if (!it || !it->valid) return false;
     if (!out) { 
         EXCEPTION("json_path_array_iter_next: null output"); 
         return false; 
     }
     
     return json_array_iter_next(&it->iter, out);
 }
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif // CFL_JSON_PATH_H