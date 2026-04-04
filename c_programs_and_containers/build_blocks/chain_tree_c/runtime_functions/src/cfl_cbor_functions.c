/* ========================================================================
 * cfl_cbor_functions.c — ChainTree CBOR packet node functions
 *
 * Emit: reads JSON text from DSL node_dict, converts to CBOR bytes,
 *       queues cfl_cbor_buffer_t* on the event queue.
 * Sink: receives cfl_cbor_buffer_t*, decodes CBOR to JSON text, parses
 *       to cfl_json_packet_t, passes to user boolean. User code sees
 *       the same cfl_json_packet_t as the JSON pipeline.
 *
 * Memory: node structs on arena (via smart alloc), CBOR byte buffers
 * on heap (freed in term functions). Safe for CFL_RESET loops.
 *
 * Naming convention (matches stage3 pipeline):
 *   DSL "CFL_CBOR_EMIT"      -> reg "cfl_cbor_emit_one_shot"     -> C "cfl_cbor_emit_one_shot_fn"
 *   DSL "CFL_CBOR_SINK"      -> reg "cfl_cbor_sink_main"         -> C "cfl_cbor_sink_main_fn"
 *   DSL "CFL_CBOR_SINK_INIT" -> reg "cfl_cbor_sink_init_one_shot"-> C "cfl_cbor_sink_init_one_shot_fn"
 * ======================================================================== */

#include <string.h>
#include <stdio.h>
#include "cfl_cbor_functions.h"
#include "cfl_json_functions.h"
#include "cfl_exception.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"

/* ========================================================================
 * HEAP INTERFACE BRIDGE
 * ======================================================================== */

static void *cbor_heap_alloc_bridge(void *ctx, uint16_t size) {
    return cfl_heap_malloc_pointer((cfl_heap_t *)ctx, size);
}

static void cbor_heap_free_bridge(void *ctx, void *ptr) {
    cfl_heap_free_pointer((cfl_heap_t *)ctx, ptr);
}

void cfl_cbor_bind_heap(cfl_cbor_heap_interface_t *iface, cfl_heap_t *heap) {
    iface->alloc = cbor_heap_alloc_bridge;
    iface->free  = cbor_heap_free_bridge;
    iface->ctx   = heap;
}

/* ========================================================================
 * EMIT ONE-SHOT (DSL: CFL_CBOR_EMIT)
 *
 * Reads node_dict.packet_text (JSON string from DSL), converts to CBOR
 * bytes in heap buffer, queues cfl_cbor_buffer_t* on event queue.
 * Uses cfl_allocate_state/cfl_smart_arena_alloc — safe for CFL_RESET.
 * ======================================================================== */

void cfl_cbor_emit_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;

    bool already = cfl_allocate_state(rt, node_index);

    cfl_cbor_emit_node_t *node;

    if (!already) {
        node = (cfl_cbor_emit_node_t *)
            cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_cbor_emit_node_t));
        if (!node) EXCEPTION("cfl_cbor_emit: alloc failed");

        node->heap = rt->heap;
        node->cbor_buffer_size = CFL_CBOR_MAX_PACKET_SIZE;
        node->cbor_buffer = (uint8_t *)cfl_heap_malloc_pointer(
            rt->heap, (uint16_t)node->cbor_buffer_size);
        if (!node->cbor_buffer) EXCEPTION("cfl_cbor_emit: cbor buffer alloc failed");

        json_decoder_init_from_runtime(rt, node_index);

        int32_t temp;
        json_extract_int32_runtime(rt, "node_dict.event_column_id", &temp);
        node->event_column_id = (unsigned)temp;

        json_extract_int32_runtime(rt, "node_dict.event_id", &temp);
        node->event_id = (unsigned)temp;
    } else {
        node = cfl_get_cbor_emit_node(rt, node_index);
        json_decoder_init_from_runtime(rt, node_index);
    }

    /* Read JSON text from DSL */
    const char *packet_text = NULL;
    json_extract_string_runtime(rt, "node_dict.packet_text", &packet_text);
    if (!packet_text) EXCEPTION("cfl_cbor_emit: NULL packet_text");

    /* Convert JSON text -> CBOR bytes */
    size_t cbor_len = cfl_json_text_to_cbor(packet_text,
        node->cbor_buffer, node->cbor_buffer_size);

    /* Set up the buffer reference */
    node->cbor_ref.data = node->cbor_buffer;
    node->cbor_ref.len = cbor_len;

    /* Queue the CBOR buffer reference */
    cfl_send_streaming_data_event(
        rt->event_queue, CFL_EVENT_PRIORITY_LOW,
        node->event_column_id, node->event_id,
        &node->cbor_ref);
}

void cfl_cbor_emit_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
    /* CBOR buffer lives on heap for node lifetime — the event queue sink
     * may still hold a reference to cbor_ref.data after this term fires.
     * Buffer is reused across CFL_RESET cycles. Freed at runtime cleanup. */
}

/* ========================================================================
 * SINK (DSL: CFL_CBOR_SINK / CFL_CBOR_SINK_INIT / CFL_CBOR_SINK_TERM)
 *
 * Receives cfl_cbor_buffer_t*, decodes CBOR to JSON text, parses to
 * cfl_json_packet_t, passes to user boolean. User code sees exactly
 * the same interface as CFL_JSON_SINK.
 * ======================================================================== */

void cfl_cbor_sink_init_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;

    bool already = cfl_allocate_state(rt, node_index);
    if (already) return;

    cfl_cbor_sink_node_t *node = (cfl_cbor_sink_node_t *)
        cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_cbor_sink_node_t));
    if (!node) EXCEPTION("cfl_cbor_sink_init: alloc failed");

    cfl_json_bind_heap(&node->heap_iface, rt->heap);
    cfl_cbor_bind_heap(&node->cbor_heap_iface, rt->heap);

    json_decoder_init_from_runtime(rt, node_index);

    int32_t temp;
    json_extract_int32_runtime(rt, "node_dict.event_id", &temp);
    node->event_id = (unsigned)temp;
}

void cfl_cbor_sink_term_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    (void)node_index;
    /* Sink has no heap allocations — JSON text and packets are freed per-event */
}

unsigned cfl_cbor_sink_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_cbor_sink_node_t *node = cfl_get_cbor_sink_node(rt, node_index);

    if (event_id != node->event_id) return CFL_CONTINUE;
    if (!event_data) return CFL_CONTINUE;

    /* event_data is cfl_cbor_buffer_t* */
    cfl_cbor_buffer_t *cbor_buf = (cfl_cbor_buffer_t *)event_data;

    /* Decode CBOR -> JSON text */
    char *json_text = cfl_cbor_to_json_text(
        cbor_buf->data, cbor_buf->len, &node->cbor_heap_iface);

    /* Parse JSON text -> cfl_json_packet_t */
    cfl_json_packet_t *pkt = cfl_json_parse(&node->heap_iface, json_text, -1);

    /* Call user boolean with parsed packet — same as JSON sink */
    const cfl_boolean_function_t bool_fn =
        rt->flash_handle->boolean_functions[bool_function_index];
    bool_fn(rt, node_index, event_type, event_id, pkt);

    /* Cleanup */
    cfl_json_free(&node->heap_iface, pkt);
    node->cbor_heap_iface.free(node->cbor_heap_iface.ctx, json_text);

    return CFL_CONTINUE;
}
