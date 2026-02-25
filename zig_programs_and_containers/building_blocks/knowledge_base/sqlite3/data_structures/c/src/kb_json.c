#include "kb_json.h"
#include <string.h>

char *kb_json_encode(const cJSON *json) { return json ? cJSON_PrintUnformatted(json) : NULL; }
cJSON *kb_json_decode(const char *s) { return s ? cJSON_Parse(s) : NULL; }

bool kb_json_has_key_value(const char *json_str, const char *key, const char *value)
{
    if (!json_str || !key || !value) return false;
    cJSON *obj = cJSON_Parse(json_str);
    if (!obj) return false;
    cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, key);
    bool found = cJSON_IsString(item) && item->valuestring && strcmp(item->valuestring, value) == 0;
    cJSON_Delete(obj);
    return found;
}

bool kb_json_has_key(const char *json_str, const char *key)
{
    if (!json_str || !key) return false;
    cJSON *obj = cJSON_Parse(json_str);
    if (!obj) return false;
    bool found = cJSON_HasObjectItem(obj, key);
    cJSON_Delete(obj);
    return found;
}
