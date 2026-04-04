/* ========================================================================
 * cfl_json_packets.c — Core JSON packet operations
 *
 * Wraps cJSON with per-call allocator and EXCEPTION-on-error policy.
 * ======================================================================== */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "cfl_json_packets.h"
#include "cJSON.h"
#include "cfl_exception.h"

/* ========================================================================
 * INTERNAL: PACKET STRUCTURE
 * ======================================================================== */

struct cfl_json_packet {
    cJSON                   *root;
    cfl_json_heap_interface_t *heap;
};

/* ========================================================================
 * INTERNAL: ALLOCATOR BRIDGE
 *
 * cJSON uses global hooks. We set them before each cJSON call using a
 * module-local heap pointer. Single-threaded ChainTree engine, so this
 * is safe.
 * ======================================================================== */

static cfl_json_heap_interface_t *s_active_heap = NULL;

static void *cjson_alloc(size_t sz) {
    if (!s_active_heap) {
        EXCEPTION("cfl_json: cJSON alloc called with no active heap");
    }
    void *ptr = s_active_heap->alloc(s_active_heap->ctx, (uint16_t)sz);
    if (!ptr) {
        EXCEPTION("cfl_json: heap allocation failed");
    }
    return ptr;
}

static void cjson_free(void *ptr) {
    if (!ptr) return;
    if (!s_active_heap) {
        EXCEPTION("cfl_json: cJSON free called with no active heap");
    }
    s_active_heap->free(s_active_heap->ctx, ptr);
}

static void set_heap(cfl_json_heap_interface_t *heap) {
    s_active_heap = heap;
    cJSON_Hooks hooks = { .malloc_fn = cjson_alloc, .free_fn = cjson_free };
    cJSON_InitHooks(&hooks);
}

/* ========================================================================
 * INTERNAL: PATH NAVIGATION
 *
 * Supports dot-notation and array indexing:
 *   "payload.sensors[2].temp"
 * ======================================================================== */

static cJSON *navigate_path(cJSON *root, const char *path) {
    if (!root || !path) return NULL;

    char buf[256];
    int len = (int)strlen(path);
    if (len >= (int)sizeof(buf)) return NULL;
    memcpy(buf, path, (size_t)len + 1);

    cJSON *current = root;
    char *p = buf;

    while (*p && current) {
        /* Skip leading dots */
        if (*p == '.') { p++; continue; }

        /* Find end of this segment */
        char *seg = p;
        while (*p && *p != '.' && *p != '[') p++;

        /* Handle array index */
        if (*p == '[') {
            *p = '\0';
            if (*seg) {
                current = cJSON_GetObjectItemCaseSensitive(current, seg);
                if (!current) return NULL;
            }
            p++;
            int idx = 0;
            while (*p >= '0' && *p <= '9') {
                idx = idx * 10 + (*p - '0');
                p++;
            }
            if (*p == ']') p++;
            current = cJSON_GetArrayItem(current, idx);
        } else {
            char saved = *p;
            *p = '\0';
            if (*seg) {
                current = cJSON_GetObjectItemCaseSensitive(current, seg);
            }
            *p = saved;
        }
    }

    return current;
}

/* Navigate to parent, creating intermediate objects as needed for set operations.
 * Returns parent node and writes the leaf key name into leaf_buf. */
static cJSON *navigate_to_parent(cJSON *root, const char *path,
                                  char *leaf_buf, int leaf_buf_size) {
    /* Find last dot */
    const char *last_dot = strrchr(path, '.');
    if (!last_dot) {
        /* No dots — leaf is the path itself, parent is root */
        int len = (int)strlen(path);
        if (len >= leaf_buf_size) return NULL;
        memcpy(leaf_buf, path, (size_t)len + 1);
        return root;
    }

    /* Copy parent path */
    int parent_len = (int)(last_dot - path);
    char parent_path[256];
    if (parent_len >= (int)sizeof(parent_path)) return NULL;
    memcpy(parent_path, path, (size_t)parent_len);
    parent_path[parent_len] = '\0';

    /* Copy leaf name */
    const char *leaf = last_dot + 1;
    int leaf_len = (int)strlen(leaf);
    if (leaf_len >= leaf_buf_size) return NULL;
    memcpy(leaf_buf, leaf, (size_t)leaf_len + 1);

    /* Navigate to parent, creating objects along the way */
    cJSON *current = root;
    char buf[256];
    memcpy(buf, parent_path, (size_t)parent_len + 1);
    char *p = buf;

    while (*p && current) {
        if (*p == '.') { p++; continue; }
        char *seg = p;
        while (*p && *p != '.') p++;
        char saved = *p;
        *p = '\0';

        cJSON *child = cJSON_GetObjectItemCaseSensitive(current, seg);
        if (!child) {
            child = cJSON_CreateObject();
            if (!child) return NULL;
            cJSON_AddItemToObject(current, seg, child);
        }
        current = child;
        *p = saved;
    }

    return current;
}

/* ========================================================================
 * PARSE
 * ======================================================================== */

cfl_json_packet_t *cfl_json_parse(cfl_json_heap_interface_t *heap,
                                   const char *text, int len) {
    if (!heap) {
        EXCEPTION("cfl_json_parse: NULL heap");
    }
    if (!text) {
        EXCEPTION("cfl_json_parse: NULL text");
    }
    if (len == 0) {
        EXCEPTION("cfl_json_parse: empty text");
    }

    set_heap(heap);

    cJSON *root;
    if (len > 0) {
        root = cJSON_ParseWithLength(text, (size_t)len);
    } else {
        /* len == -1: NUL-terminated */
        root = cJSON_Parse(text);
    }

    if (!root) {
        EXCEPTION("cfl_json_parse: invalid JSON");
    }

    cfl_json_packet_t *pkt = (cfl_json_packet_t *)heap->alloc(heap->ctx,
                                                                (uint16_t)sizeof(cfl_json_packet_t));
    if (!pkt) {
        cJSON_Delete(root);
        EXCEPTION("cfl_json_parse: failed to allocate packet");
    }
    pkt->root = root;
    pkt->heap = heap;
    return pkt;
}

/* ========================================================================
 * EXTRACT — all EXCEPTION on failure
 * ======================================================================== */

void cfl_json_get_string(cfl_json_packet_t *pkt, const char *path, const char **out) {
    if (!pkt) EXCEPTION("cfl_json_get_string: NULL packet");
    if (!path) EXCEPTION("cfl_json_get_string: NULL path");
    if (!out) EXCEPTION("cfl_json_get_string: NULL out");

    cJSON *item = navigate_path(pkt->root, path);
    if (!item) EXCEPTION("cfl_json_get_string: path not found");
    if (!cJSON_IsString(item)) EXCEPTION("cfl_json_get_string: not a string");

    *out = item->valuestring;
}

void cfl_json_get_int(cfl_json_packet_t *pkt, const char *path, int32_t *out) {
    if (!pkt) EXCEPTION("cfl_json_get_int: NULL packet");
    if (!path) EXCEPTION("cfl_json_get_int: NULL path");
    if (!out) EXCEPTION("cfl_json_get_int: NULL out");

    cJSON *item = navigate_path(pkt->root, path);
    if (!item) EXCEPTION("cfl_json_get_int: path not found");
    if (!cJSON_IsNumber(item)) EXCEPTION("cfl_json_get_int: not a number");

    *out = (int32_t)item->valueint;
}

void cfl_json_get_float(cfl_json_packet_t *pkt, const char *path, double *out) {
    if (!pkt) EXCEPTION("cfl_json_get_float: NULL packet");
    if (!path) EXCEPTION("cfl_json_get_float: NULL path");
    if (!out) EXCEPTION("cfl_json_get_float: NULL out");

    cJSON *item = navigate_path(pkt->root, path);
    if (!item) EXCEPTION("cfl_json_get_float: path not found");
    if (!cJSON_IsNumber(item)) EXCEPTION("cfl_json_get_float: not a number");

    *out = item->valuedouble;
}

void cfl_json_get_bool(cfl_json_packet_t *pkt, const char *path, bool *out) {
    if (!pkt) EXCEPTION("cfl_json_get_bool: NULL packet");
    if (!path) EXCEPTION("cfl_json_get_bool: NULL path");
    if (!out) EXCEPTION("cfl_json_get_bool: NULL out");

    cJSON *item = navigate_path(pkt->root, path);
    if (!item) EXCEPTION("cfl_json_get_bool: path not found");
    if (!cJSON_IsBool(item)) EXCEPTION("cfl_json_get_bool: not a boolean");

    *out = cJSON_IsTrue(item) ? true : false;
}

int cfl_json_array_size(cfl_json_packet_t *pkt, const char *path) {
    if (!pkt) EXCEPTION("cfl_json_array_size: NULL packet");
    if (!path) EXCEPTION("cfl_json_array_size: NULL path");

    cJSON *item = navigate_path(pkt->root, path);
    if (!item) EXCEPTION("cfl_json_array_size: path not found");
    if (!cJSON_IsArray(item)) EXCEPTION("cfl_json_array_size: not an array");

    return cJSON_GetArraySize(item);
}

/* ========================================================================
 * PROBE — the only soft query
 * ======================================================================== */

bool cfl_json_has_field(cfl_json_packet_t *pkt, const char *path) {
    if (!pkt || !path) return false;
    return navigate_path(pkt->root, path) != NULL;
}

/* ========================================================================
 * BUILD
 * ======================================================================== */

cfl_json_packet_t *cfl_json_create(cfl_json_heap_interface_t *heap) {
    if (!heap) EXCEPTION("cfl_json_create: NULL heap");

    set_heap(heap);

    cJSON *root = cJSON_CreateObject();
    if (!root) EXCEPTION("cfl_json_create: failed to create root object");

    cfl_json_packet_t *pkt = (cfl_json_packet_t *)heap->alloc(heap->ctx,
                                                                (uint16_t)sizeof(cfl_json_packet_t));
    if (!pkt) {
        cJSON_Delete(root);
        EXCEPTION("cfl_json_create: failed to allocate packet");
    }
    pkt->root = root;
    pkt->heap = heap;
    return pkt;
}

void cfl_json_set_string(cfl_json_packet_t *pkt, const char *path, const char *val) {
    if (!pkt) EXCEPTION("cfl_json_set_string: NULL packet");
    if (!path) EXCEPTION("cfl_json_set_string: NULL path");
    if (!val) EXCEPTION("cfl_json_set_string: NULL value");

    set_heap(pkt->heap);

    char leaf[128];
    cJSON *parent = navigate_to_parent(pkt->root, path, leaf, (int)sizeof(leaf));
    if (!parent) EXCEPTION("cfl_json_set_string: cannot navigate path");

    cJSON *existing = cJSON_GetObjectItemCaseSensitive(parent, leaf);
    if (existing) {
        cJSON *replacement = cJSON_CreateString(val);
        if (!replacement) EXCEPTION("cfl_json_set_string: alloc failed");
        cJSON_ReplaceItemInObjectCaseSensitive(parent, leaf, replacement);
    } else {
        cJSON_AddStringToObject(parent, leaf, val);
    }
}

void cfl_json_set_int(cfl_json_packet_t *pkt, const char *path, int32_t val) {
    if (!pkt) EXCEPTION("cfl_json_set_int: NULL packet");
    if (!path) EXCEPTION("cfl_json_set_int: NULL path");

    set_heap(pkt->heap);

    char leaf[128];
    cJSON *parent = navigate_to_parent(pkt->root, path, leaf, (int)sizeof(leaf));
    if (!parent) EXCEPTION("cfl_json_set_int: cannot navigate path");

    cJSON *existing = cJSON_GetObjectItemCaseSensitive(parent, leaf);
    if (existing) {
        cJSON *replacement = cJSON_CreateNumber((double)val);
        if (!replacement) EXCEPTION("cfl_json_set_int: alloc failed");
        cJSON_ReplaceItemInObjectCaseSensitive(parent, leaf, replacement);
    } else {
        cJSON_AddNumberToObject(parent, leaf, (double)val);
    }
}

void cfl_json_set_float(cfl_json_packet_t *pkt, const char *path, double val) {
    if (!pkt) EXCEPTION("cfl_json_set_float: NULL packet");
    if (!path) EXCEPTION("cfl_json_set_float: NULL path");

    set_heap(pkt->heap);

    char leaf[128];
    cJSON *parent = navigate_to_parent(pkt->root, path, leaf, (int)sizeof(leaf));
    if (!parent) EXCEPTION("cfl_json_set_float: cannot navigate path");

    cJSON *existing = cJSON_GetObjectItemCaseSensitive(parent, leaf);
    if (existing) {
        cJSON *replacement = cJSON_CreateNumber(val);
        if (!replacement) EXCEPTION("cfl_json_set_float: alloc failed");
        cJSON_ReplaceItemInObjectCaseSensitive(parent, leaf, replacement);
    } else {
        cJSON_AddNumberToObject(parent, leaf, val);
    }
}

void cfl_json_set_bool(cfl_json_packet_t *pkt, const char *path, bool val) {
    if (!pkt) EXCEPTION("cfl_json_set_bool: NULL packet");
    if (!path) EXCEPTION("cfl_json_set_bool: NULL path");

    set_heap(pkt->heap);

    char leaf[128];
    cJSON *parent = navigate_to_parent(pkt->root, path, leaf, (int)sizeof(leaf));
    if (!parent) EXCEPTION("cfl_json_set_bool: cannot navigate path");

    cJSON *existing = cJSON_GetObjectItemCaseSensitive(parent, leaf);
    if (existing) {
        cJSON *replacement = cJSON_CreateBool(val);
        if (!replacement) EXCEPTION("cfl_json_set_bool: alloc failed");
        cJSON_ReplaceItemInObjectCaseSensitive(parent, leaf, replacement);
    } else {
        cJSON_AddBoolToObject(parent, leaf, val);
    }
}

void cfl_json_set_object(cfl_json_packet_t *pkt, const char *path) {
    if (!pkt) EXCEPTION("cfl_json_set_object: NULL packet");
    if (!path) EXCEPTION("cfl_json_set_object: NULL path");

    set_heap(pkt->heap);

    char leaf[128];
    cJSON *parent = navigate_to_parent(pkt->root, path, leaf, (int)sizeof(leaf));
    if (!parent) EXCEPTION("cfl_json_set_object: cannot navigate path");

    cJSON *obj = cJSON_CreateObject();
    if (!obj) EXCEPTION("cfl_json_set_object: alloc failed");
    cJSON_AddItemToObject(parent, leaf, obj);
}

void cfl_json_set_array(cfl_json_packet_t *pkt, const char *path) {
    if (!pkt) EXCEPTION("cfl_json_set_array: NULL packet");
    if (!path) EXCEPTION("cfl_json_set_array: NULL path");

    set_heap(pkt->heap);

    char leaf[128];
    cJSON *parent = navigate_to_parent(pkt->root, path, leaf, (int)sizeof(leaf));
    if (!parent) EXCEPTION("cfl_json_set_array: cannot navigate path");

    cJSON *arr = cJSON_CreateArray();
    if (!arr) EXCEPTION("cfl_json_set_array: alloc failed");
    cJSON_AddItemToObject(parent, leaf, arr);
}

/* ========================================================================
 * SERIALIZE
 * ======================================================================== */

char *cfl_json_to_string(cfl_json_heap_interface_t *heap, cfl_json_packet_t *pkt) {
    if (!heap) EXCEPTION("cfl_json_to_string: NULL heap");
    if (!pkt) EXCEPTION("cfl_json_to_string: NULL packet");

    set_heap(heap);

    char *str = cJSON_PrintUnformatted(pkt->root);
    if (!str) EXCEPTION("cfl_json_to_string: serialization failed");

    return str;
}

int cfl_json_to_buffer(cfl_json_packet_t *pkt, char *buf, int buflen) {
    if (!pkt) EXCEPTION("cfl_json_to_buffer: NULL packet");
    if (!buf) EXCEPTION("cfl_json_to_buffer: NULL buffer");
    if (buflen <= 0) EXCEPTION("cfl_json_to_buffer: invalid buffer length");

    /* Use cJSON_PrintPreallocated for zero-alloc serialization */
    set_heap(pkt->heap);
    cJSON_bool ok = cJSON_PrintPreallocated(pkt->root, buf, buflen, 0 /* unformatted */);
    if (!ok) EXCEPTION("cfl_json_to_buffer: buffer too small");

    return (int)strlen(buf);
}

/* ========================================================================
 * FREE
 * ======================================================================== */

void cfl_json_free(cfl_json_heap_interface_t *heap, cfl_json_packet_t *pkt) {
    if (!pkt) return;
    if (!heap) return;

    set_heap(heap);
    if (pkt->root) {
        cJSON_Delete(pkt->root);
    }
    heap->free(heap->ctx, pkt);
}
