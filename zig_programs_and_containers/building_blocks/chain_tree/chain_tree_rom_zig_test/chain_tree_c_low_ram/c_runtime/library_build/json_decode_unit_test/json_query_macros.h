/* Helper macros for common queries */
#define JSON_QUERY_INT(decoder, ctrl, path) \
    ({ json_query_result_t _r; \
       json_query_path(decoder, ctrl, path, &_r) == 0 && _r.found ? \
       _r.value.i32_value : 0; })

#define JSON_QUERY_FLOAT(decoder, ctrl, path) \
    ({ json_query_result_t _r; \
       json_query_path(decoder, ctrl, path, &_r) == 0 && _r.found ? \
       _r.value.f32_value : 0.0f; })

#define JSON_QUERY_STRING(decoder, ctrl, path) \
    ({ json_query_result_t _r; \
       json_query_path(decoder, ctrl, path, &_r) == 0 && _r.found ? \
       _r.value.string_value : NULL; })

/* Usage */
float temp = JSON_QUERY_FLOAT(&decoder, 0, "temperature");
int timeout = JSON_QUERY_INT(&decoder, 1, "config.timeout");
const char *name = JSON_QUERY_STRING(&decoder, 2, "data[0].name");


