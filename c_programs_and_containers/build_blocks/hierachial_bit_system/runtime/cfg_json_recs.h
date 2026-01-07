/* Auto-generated. Do not edit. */
#pragma once
#include <stdint.h>
#include <stddef.h>

#define FNV1A_32_INIT   0x811c9dc5U
#define FNV1A_32_PRIME  0x01000193U

typedef uint32_t json_hash32_t;

typedef enum {
  JSON_TYPE_STRING_HASH = 0,
  JSON_TYPE_INT32       = 1,
  JSON_TYPE_UINT32      = 2,
  JSON_TYPE_FLOAT32     = 3,
  JSON_TYPE_NULL        = 4,
  JSON_TYPE_BOOL        = 5,
  JSON_TYPE_ARRAY       = 6,
  JSON_TYPE_OBJECT      = 7
} json_type_t;

typedef struct {
  json_type_t object_type;
  union {
    json_hash32_t hash32;
    int32_t       i32_value;
    uint32_t      u32_value;
    float         f32_value;
    uint8_t       bool_value;
    uint32_t      container_count;
  } value;
} json_record_t;

typedef struct {
  json_hash32_t path_hash;
  uint32_t      rec_index;
} json_path_index_t;

extern const json_record_t g_cfg_recs[];
extern const uint32_t      g_cfg_recs_len;

extern const json_path_index_t g_cfg_index[];
extern const uint32_t          g_cfg_index_len;

