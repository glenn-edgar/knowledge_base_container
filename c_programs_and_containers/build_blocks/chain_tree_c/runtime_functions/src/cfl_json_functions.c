/* ========================================================================
 * cfl_json_functions.c — ChainTree JSON packet node functions
 *
 * Naming convention (matches stage3 pipeline):
 *   DSL "CFL_JSON_EMIT"      → reg "cfl_json_emit_one_shot"     → C "cfl_json_emit_one_shot_fn"
 *   DSL "CFL_JSON_SINK"      → reg "cfl_json_sink_main"         → C "cfl_json_sink_main_fn"
 *   DSL "CFL_JSON_SINK_INIT" → reg "cfl_json_sink_init_one_shot"→ C "cfl_json_sink_init_one_shot_fn"
 *
 * All nodes use cfl_allocate_state / cfl_smart_arena_alloc — safe for CFL_RESET loops.
 * ======================================================================== */

#include <string.h>
#include <stdio.h>
#include "cfl_json_functions.h"
#include "cfl_exception.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"

/* ========================================================================
 * HEAP INTERFACE BRIDGE
 * ======================================================================== */

static void *heap_alloc_bridge(void *ctx, uint16_t size) {
    return cfl_heap_malloc_pointer((cfl_heap_t *)ctx, size);
}

static void heap_free_bridge(void *ctx, void *ptr) {
    cfl_heap_free_pointer((cfl_heap_t *)ctx, ptr);
}

void cfl_json_bind_heap(cfl_json_heap_interface_t *iface, cfl_heap_t *heap) {
    iface->alloc = heap_alloc_bridge;
    iface->free  = heap_free_bridge;
    iface->ctx   = heap;
}

/* ========================================================================
 * FNV-1a 32-bit hash
 * ======================================================================== */

static uint32_t fnv1a_hash(const char *str) {
    uint32_t hash = 0x811c9dc5u;
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= 0x01000193u;
    }
    return hash;
}

/* ========================================================================
 * EMIT ONE-SHOT (DSL: CFL_JSON_EMIT)
 *
 * Reads node_dict.packet_text, copies to arena buffer, queues on event queue.
 * The JSON string in the DSL IS the packet. No interpretation.
 * ======================================================================== */

void cfl_json_emit_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;

    bool already = cfl_allocate_state(rt, node_index);

    cfl_json_emit_node_t *node;

    if (!already) {
        node = (cfl_json_emit_node_t *)
            cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_json_emit_node_t));
        if (!node) EXCEPTION("cfl_json_emit: alloc failed");

        node->heap = rt->heap;
        node->text_buffer_size = CFL_JSON_MAX_PACKET_SIZE;
        node->text_buffer = (char *)cfl_additional_arena_alloc(
            rt, node_index, (uint16_t)node->text_buffer_size);
        if (!node->text_buffer) EXCEPTION("cfl_json_emit: text buffer alloc failed");

        json_decoder_init_from_runtime(rt, node_index);

        int32_t temp;
        json_extract_int32_runtime(rt, "node_dict.event_column_id", &temp);
        node->event_column_id = (unsigned)temp;

        json_extract_int32_runtime(rt, "node_dict.event_id", &temp);
        node->event_id = (unsigned)temp;
    } else {
        node = cfl_get_json_emit_node(rt, node_index);
        json_decoder_init_from_runtime(rt, node_index);
    }

    const char *packet_text = NULL;
    json_extract_string_runtime(rt, "node_dict.packet_text", &packet_text);
    if (!packet_text) EXCEPTION("cfl_json_emit: NULL packet_text");

    unsigned len = (unsigned)strlen(packet_text);
    if (len >= node->text_buffer_size) {
        EXCEPTION("cfl_json_emit: packet_text exceeds buffer size");
    }
    memcpy(node->text_buffer, packet_text, len + 1);

    cfl_send_streaming_data_event(
        rt->event_queue, CFL_EVENT_PRIORITY_LOW,
        node->event_column_id, node->event_id,
        node->text_buffer);
}

void cfl_json_emit_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
    /* text_buffer lives in local arena — freed when arena is destroyed */
}

/* ========================================================================
 * SINK (DSL: CFL_JSON_SINK / CFL_JSON_SINK_INIT / CFL_JSON_SINK_TERM)
 * ======================================================================== */

void cfl_json_sink_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;

    bool already = cfl_allocate_state(rt, node_index);
    if (already) return;

    cfl_json_sink_node_t *node = (cfl_json_sink_node_t *)
        cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_json_sink_node_t));
    if (!node) EXCEPTION("cfl_json_sink_init: alloc failed");

    cfl_json_bind_heap(&node->heap_iface, rt->heap);
    node->user_data = NULL;

    json_decoder_init_from_runtime(rt, node_index);

    int32_t temp;
    json_extract_int32_runtime(rt, "node_dict.event_id", &temp);
    node->event_id = (unsigned)temp;
}

void cfl_json_sink_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
}

unsigned cfl_json_sink_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_json_sink_node_t *node = cfl_get_json_sink_node(rt, node_index);

if (event_id != node->event_id) return CFL_CONTINUE;
    if (!event_data) return CFL_CONTINUE;

    cfl_json_packet_t *pkt = cfl_json_parse(&node->heap_iface,
                                             (const char *)event_data, -1);

    const cfl_boolean_function_t bool_fn =
        rt->flash_handle->boolean_functions[bool_function_index];
    bool_fn(rt, node_index, event_type, event_id, pkt);

    cfl_json_free(&node->heap_iface, pkt);

    return CFL_CONTINUE;
}

/* ========================================================================
 * TRANSFORM (DSL: CFL_JSON_TRANSFORM / CFL_JSON_TRANSFORM_INIT / _TERM)
 * ======================================================================== */

void cfl_json_transform_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;

    bool already = cfl_allocate_state(rt, node_index);
    if (already) return;

    cfl_json_transform_node_t *node = (cfl_json_transform_node_t *)
        cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_json_transform_node_t));
    if (!node) EXCEPTION("cfl_json_transform_init: alloc failed");

    cfl_json_bind_heap(&node->heap_iface, rt->heap);

    node->text_buffer_size = CFL_JSON_MAX_PACKET_SIZE;
    node->text_buffer = (char *)cfl_heap_malloc_pointer(
        rt->heap, (uint16_t)node->text_buffer_size);
    if (!node->text_buffer) EXCEPTION("cfl_json_transform_init: text buffer alloc failed");

    node->out_packet = cfl_json_create(&node->heap_iface);
    node->user_data = NULL;

    json_decoder_init_from_runtime(rt, node_index);

    int32_t temp;
    json_extract_int32_runtime(rt, "node_dict.input_event_id", &temp);
    node->input_event_id = (unsigned)temp;

    json_extract_int32_runtime(rt, "node_dict.output_event_id", &temp);
    node->output_event_id = (unsigned)temp;

    json_extract_int32_runtime(rt, "node_dict.output_event_column_id", &temp);
    node->output_event_column_id = (unsigned)temp;
}

void cfl_json_transform_term_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_json_transform_node_t *node = cfl_get_json_transform_node(rt, node_index);
    if (!node) return;

    if (node->text_buffer) {
        cfl_heap_free_pointer(rt->heap, node->text_buffer);
        node->text_buffer = NULL;
    }
    if (node->out_packet) {
        cfl_json_free(&node->heap_iface, node->out_packet);
        node->out_packet = NULL;
    }
}

unsigned cfl_json_transform_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_json_transform_node_t *node = cfl_get_json_transform_node(rt, node_index);

if (event_id != node->input_event_id) return CFL_CONTINUE;
    if (!event_data) return CFL_CONTINUE;

    cfl_json_packet_t *in_pkt = cfl_json_parse(&node->heap_iface,
                                                (const char *)event_data, -1);

    const cfl_boolean_function_t bool_fn =
        rt->flash_handle->boolean_functions[bool_function_index];
    bool emit = bool_fn(rt, node_index, event_type, event_id, in_pkt);

    cfl_json_free(&node->heap_iface, in_pkt);

    if (emit && node->out_packet) {
        int len = cfl_json_to_buffer(node->out_packet, node->text_buffer,
                                     (int)node->text_buffer_size);
        (void)len;

        cfl_send_streaming_data_event(
            rt->event_queue, CFL_EVENT_PRIORITY_LOW,
            node->output_event_column_id, node->output_event_id,
            node->text_buffer);
    }

    return CFL_CONTINUE;
}

/* ========================================================================
 * DISPATCH (DSL: CFL_JSON_DISPATCH / CFL_JSON_DISPATCH_INIT / _TERM)
 * ======================================================================== */

void cfl_json_dispatch_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;

    bool already = cfl_allocate_state(rt, node_index);
    if (already) return;

    cfl_json_dispatch_node_t *node = (cfl_json_dispatch_node_t *)
        cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_json_dispatch_node_t));
    if (!node) EXCEPTION("cfl_json_dispatch_init: alloc failed");

    cfl_json_bind_heap(&node->heap_iface, rt->heap);
    node->user_data = NULL;

    json_decoder_init_from_runtime(rt, node_index);

    int32_t temp;
    json_extract_int32_runtime(rt, "node_dict.event_id", &temp);
    node->event_id = (unsigned)temp;

    json_extract_int32_runtime(rt, "node_dict.output_event_id", &temp);
    node->output_event_id = (unsigned)temp;

    json_extract_int32_runtime(rt, "node_dict.default_column_id", &temp);
    node->default_column_id = (unsigned)temp;

    int32_t route_count;
    json_extract_int32_runtime(rt, "node_dict.route_count", &route_count);
    if (route_count <= 0) {
        EXCEPTION("cfl_json_dispatch_init: route_count must be > 0");
    }
    node->route_count = (unsigned)route_count;

    node->routes = (cfl_json_dispatch_route_entry_t *)cfl_additional_arena_alloc(
        rt, node_index,
        (uint16_t)(sizeof(cfl_json_dispatch_route_entry_t) * (unsigned)route_count));
    if (!node->routes) EXCEPTION("cfl_json_dispatch_init: routes alloc failed");

    for (unsigned i = 0; i < node->route_count; i++) {
        char path_buf[128];

        snprintf(path_buf, sizeof(path_buf), "node_dict.routes[%u].type_hash", i);
        json_extract_int32_runtime(rt, path_buf, &temp);
        node->routes[i].type_hash = (uint32_t)temp;

        snprintf(path_buf, sizeof(path_buf), "node_dict.routes[%u].column_id", i);
        json_extract_int32_runtime(rt, path_buf, &temp);
        node->routes[i].column_id = (unsigned)temp;
    }
}

void cfl_json_dispatch_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
}

unsigned cfl_json_dispatch_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)bool_function_index;
    (void)event_type;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_json_dispatch_node_t *node = cfl_get_json_dispatch_node(rt, node_index);

if (event_id != node->event_id) return CFL_CONTINUE;
    if (!event_data) return CFL_CONTINUE;

    cfl_json_packet_t *pkt = cfl_json_parse(&node->heap_iface,
                                             (const char *)event_data, -1);

    const char *type_str = NULL;
    cfl_json_get_string(pkt, "type", &type_str);

    uint32_t hash = fnv1a_hash(type_str);

    unsigned target_column = node->default_column_id;
    for (unsigned i = 0; i < node->route_count; i++) {
        if (node->routes[i].type_hash == hash) {
            target_column = node->routes[i].column_id;
            break;
        }
    }

    cfl_send_streaming_data_event(
        rt->event_queue, CFL_EVENT_PRIORITY_LOW,
        target_column, node->output_event_id,
        event_data);

    cfl_json_free(&node->heap_iface, pkt);

    return CFL_CONTINUE;
}
