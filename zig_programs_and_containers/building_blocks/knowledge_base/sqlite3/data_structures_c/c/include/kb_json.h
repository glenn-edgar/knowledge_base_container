#ifndef KB_JSON_H
#define KB_JSON_H

#if __has_include(<cjson/cJSON.h>)
#include <cjson/cJSON.h>
#elif __has_include("cJSON.h")
#include "cJSON.h"
#else
#error "cJSON not found. Install libcjson-dev or place cJSON.h in third_party/cJSON/"
#endif

#include "kb_common.h"

#ifdef __cplusplus
extern "C" {
#endif

char  *kb_json_encode(const cJSON *json);
cJSON *kb_json_decode(const char *json_str);
bool   kb_json_has_key_value(const char *json_str, const char *key, const char *value);
bool   kb_json_has_key(const char *json_str, const char *key);

#ifdef __cplusplus
}
#endif
#endif
