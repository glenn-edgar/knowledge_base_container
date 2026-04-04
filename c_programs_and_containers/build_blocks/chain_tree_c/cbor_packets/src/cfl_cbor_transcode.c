/* ========================================================================
 * cfl_cbor_transcode.c — JSON text <-> CBOR bytes (string keys)
 *
 * Uses cJSON for JSON parsing/building. CBOR uses string keys (no hash
 * mapping). Numbers that are integers in JSON become CBOR unsigned/negative
 * integers; fractional numbers become CBOR float64.
 *
 * EXCEPTION on all errors.
 * ======================================================================== */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "cfl_cbor_packets.h"
#include "cJSON.h"
#include "cfl_exception.h"

/* Reset cJSON hooks to use stdlib malloc/free.
 * The CFL JSON packet library sets global cJSON hooks to use the CFL heap.
 * The transcoder must use stdlib since it manages its own memory. */
static void reset_cjson_hooks(void) {
    cJSON_InitHooks(NULL);
}

/* ========================================================================
 * JSON -> CBOR: walk cJSON tree, encode to CBOR
 * ======================================================================== */

static void encode_cjson_value(cfl_cbor_encoder_t *enc, const cJSON *item, int depth) {
    if (depth > CFL_CBOR_MAX_NESTING) {
        EXCEPTION("cfl_json_to_cbor: nesting too deep");
    }

    if (cJSON_IsNull(item)) {
        cfl_cbor_encode_null(enc);
    } else if (cJSON_IsBool(item)) {
        cfl_cbor_encode_bool(enc, cJSON_IsTrue(item) ? true : false);
    } else if (cJSON_IsNumber(item)) {
        double val = item->valuedouble;
        /* Encode integers as CBOR integers, fractional as float64 */
        if (val == floor(val) && val >= -2147483648.0 && val <= 4294967295.0) {
            if (val >= 0) {
                cfl_cbor_encode_uint(enc, (uint64_t)val);
            } else {
                cfl_cbor_encode_int(enc, (int64_t)val);
            }
        } else {
            cfl_cbor_encode_float64(enc, val);
        }
    } else if (cJSON_IsString(item)) {
        cfl_cbor_encode_text(enc, item->valuestring);
    } else if (cJSON_IsArray(item)) {
        int count = cJSON_GetArraySize(item);
        cfl_cbor_encode_array_begin(enc, (unsigned)count);
        const cJSON *child = item->child;
        while (child) {
            encode_cjson_value(enc, child, depth + 1);
            child = child->next;
        }
    } else if (cJSON_IsObject(item)) {
        int count = cJSON_GetArraySize(item);
        cfl_cbor_encode_map_begin(enc, (unsigned)count);
        const cJSON *child = item->child;
        while (child) {
            cfl_cbor_encode_text(enc, child->string);
            encode_cjson_value(enc, child, depth + 1);
            child = child->next;
        }
    } else {
        EXCEPTION("cfl_json_to_cbor: unsupported cJSON type");
    }
}

size_t cfl_json_text_to_cbor(const char *json_text,
                              uint8_t *cbor_buf, size_t cbor_buflen) {
    if (!json_text) EXCEPTION("cfl_json_text_to_cbor: NULL json_text");
    if (!cbor_buf) EXCEPTION("cfl_json_text_to_cbor: NULL cbor_buf");

    reset_cjson_hooks();
    cJSON *root = cJSON_Parse(json_text);
    if (!root) {
        EXCEPTION("cfl_json_text_to_cbor: invalid JSON");
    }

    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, cbor_buf, cbor_buflen);

    encode_cjson_value(&enc, root, 0);

    cJSON_Delete(root);

    return cfl_cbor_encoder_finish(&enc);
}

/* ========================================================================
 * CBOR -> JSON: decode CBOR, build cJSON tree, serialize
 * ======================================================================== */

static cJSON *decode_cbor_to_cjson(cfl_cbor_decoder_t *dec, int depth) {
    if (depth > CFL_CBOR_MAX_NESTING) {
        EXCEPTION("cfl_cbor_to_json: nesting too deep");
    }

    cfl_cbor_type_t t = cfl_cbor_decode_next(dec);

    switch (t) {
    case CFL_CBOR_UINT:
        return cJSON_CreateNumber((double)cfl_cbor_decode_uint(dec));

    case CFL_CBOR_INT:
        return cJSON_CreateNumber((double)cfl_cbor_decode_int(dec));

    case CFL_CBOR_FLOAT64:
        return cJSON_CreateNumber(cfl_cbor_decode_float(dec));

    case CFL_CBOR_BOOL:
        return cJSON_CreateBool(cfl_cbor_decode_bool(dec));

    case CFL_CBOR_NULL:
        return cJSON_CreateNull();

    case CFL_CBOR_TEXT: {
        size_t len;
        const char *text = cfl_cbor_decode_text(dec, &len);
        /* cJSON needs NUL-terminated string — copy */
        char *tmp = (char *)malloc(len + 1);
        if (!tmp) EXCEPTION("cfl_cbor_to_json: malloc failed");
        memcpy(tmp, text, len);
        tmp[len] = '\0';
        cJSON *node = cJSON_CreateString(tmp);
        free(tmp);
        return node;
    }

    case CFL_CBOR_ARRAY: {
        unsigned count = cfl_cbor_decode_count(dec);
        cJSON *arr = cJSON_CreateArray();
        for (unsigned i = 0; i < count; i++) {
            cJSON *child = decode_cbor_to_cjson(dec, depth + 1);
            cJSON_AddItemToArray(arr, child);
        }
        return arr;
    }

    case CFL_CBOR_MAP: {
        unsigned count = cfl_cbor_decode_count(dec);
        cJSON *obj = cJSON_CreateObject();
        for (unsigned i = 0; i < count; i++) {
            /* Key must be text string */
            cfl_cbor_type_t kt = cfl_cbor_decode_next(dec);
            if (kt != CFL_CBOR_TEXT) {
                cJSON_Delete(obj);
                EXCEPTION("cfl_cbor_to_json: map key is not text string");
            }
            size_t klen;
            const char *ktext = cfl_cbor_decode_text(dec, &klen);
            char *key = (char *)malloc(klen + 1);
            if (!key) EXCEPTION("cfl_cbor_to_json: malloc failed");
            memcpy(key, ktext, klen);
            key[klen] = '\0';

            /* Value */
            cJSON *val = decode_cbor_to_cjson(dec, depth + 1);
            cJSON_AddItemToObject(obj, key, val);
            free(key);
        }
        return obj;
    }

    case CFL_CBOR_END:
        EXCEPTION("cfl_cbor_to_json: unexpected end of CBOR data");
    /* fall through */
    default:
        EXCEPTION("cfl_cbor_to_json: unsupported CBOR type");
    }
    return NULL; /* unreachable */
}

char *cfl_cbor_to_json_text(const uint8_t *cbor_buf, size_t cbor_len,
                             cfl_cbor_heap_interface_t *heap) {
    if (!cbor_buf) EXCEPTION("cfl_cbor_to_json_text: NULL cbor_buf");
    if (!heap) EXCEPTION("cfl_cbor_to_json_text: NULL heap");

    reset_cjson_hooks();

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, cbor_buf, cbor_len);

    cJSON *root = decode_cbor_to_cjson(&dec, 0);
    if (!root) {
        EXCEPTION("cfl_cbor_to_json_text: decode produced NULL");
    }

    char *json_str = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);

    if (!json_str) {
        EXCEPTION("cfl_cbor_to_json_text: cJSON_PrintUnformatted failed");
    }

    /* Copy into heap-allocated buffer so caller can free via heap interface */
    size_t len = strlen(json_str);
    char *result = (char *)heap->alloc(heap->ctx, (uint16_t)(len + 1));
    if (!result) {
        free(json_str);
        EXCEPTION("cfl_cbor_to_json_text: heap alloc failed");
    }
    memcpy(result, json_str, len + 1);
    free(json_str);

    return result;
}
