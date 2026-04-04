/* ========================================================================
 * cfl_cbor_node_control.c — CBOR-based controlled node functions
 *
 * Same pattern as cfl_json_node_control.c but request/response travels
 * as CBOR bytes. User booleans receive JSON text (decoded from CBOR).
 *
 * Memory: node structs on arena (via smart alloc). CBOR byte buffers
 * on heap (freed in term). response_text on heap (freed after encode).
 * Safe for CFL_RESET loops.
 *
 * Flow:
 *   Client: JSON request text -> CBOR encode -> send CBOR buffer
 *   Server: receive CBOR buffer -> decode to JSON -> user boolean
 *   Server: user sets JSON response text -> CBOR encode -> send
 *   Client: receive CBOR response -> decode to JSON -> user boolean
 * ======================================================================== */

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include "cfl_runtime.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "cfl_engine.h"
#include "cfl_cbor_node_control.h"
#include "cfl_cbor_functions.h"
#include "cfl_json_functions.h"
#include "json_node_decoder.h"
#include "cfl_exception_support.h"

/* ========================================================================
 * SERVER INIT
 * ======================================================================== */

static void cfl_cbor_server_decode(
    cfl_runtime_handle_t *runtime,
    unsigned node_index,
    cfl_cbor_server_controlled_node_t *node_data)
{
    int32_t temp;

    json_decoder_init_from_runtime(runtime, node_index);

    json_extract_int32_runtime(runtime, "node_dict.column_data.request_port.event_id", &temp);
    node_data->request_port.event_id = (unsigned)temp;

    json_extract_int32_runtime(runtime, "node_dict.column_data.response_port.event_id", &temp);
    node_data->response_port.event_id = (unsigned)temp;

    node_data->client_node_index = 0xFFFF;
    node_data->response_text = NULL;
    node_data->aux_data = NULL;

    /* Allocate CBOR response buffer on heap */
    node_data->cbor_buffer = (uint8_t *)cfl_heap_malloc_pointer(
        runtime->heap, (uint16_t)CFL_CBOR_MAX_PACKET_SIZE);
    if (!node_data->cbor_buffer) {
        EXCEPTION("cfl_cbor_server_decode: cbor buffer alloc failed");
    }
}

void cfl_cbor_controlled_node_init_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_controlled_node_t *ptr = NULL;

    if (cfl_allocate_state(handle, node_index) == false) {
        ptr = (cfl_cbor_server_controlled_node_t *)cfl_smart_arena_alloc(
            handle, node_index, sizeof(cfl_cbor_server_controlled_node_t));

        cfl_cbor_server_decode(runtime, node_index, ptr);
        cfl_json_bind_heap(&ptr->heap_iface, runtime->heap);
        cfl_cbor_bind_heap(&ptr->cbor_heap_iface, runtime->heap);
    } else {
        ptr = cfl_get_cbor_server_node(runtime, node_index);
        ptr->response_text = NULL;
    }
}

/* ========================================================================
 * SERVER MAIN
 *
 * Receives CBOR request buffer, decodes to JSON text, passes to user
 * boolean monitor.
 * ======================================================================== */

unsigned cfl_cbor_controlled_node_main_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_controlled_node_t *ptr = cfl_get_cbor_server_node(runtime, node_index);

    if (event_id == ptr->request_port.event_id) {
        if (event_type != CFL_EVENT_TYPE_STREAMING_DATA) {
            EXCEPTION("cfl_cbor_controlled_node_main: event type is not STREAMING_DATA");
        }

        /* event_data is cfl_cbor_buffer_t* — decode to JSON text for user boolean */
        cfl_cbor_buffer_t *cbor_buf = (cfl_cbor_buffer_t *)event_data;
        char *json_text = cfl_cbor_to_json_text(
            cbor_buf->data, cbor_buf->len, &ptr->cbor_heap_iface);

        boolean_function_t boolean_function =
            runtime->flash_handle->boolean_functions[bool_function_index];
        boolean_function(runtime, node_index, event_type, event_id, json_text);

        /* Free decoded JSON text */
        ptr->cbor_heap_iface.free(ptr->cbor_heap_iface.ctx, json_text);

        cfl_enable_all_nodes(runtime, node_index);
        return CFL_HALT;
    }

    if (event_id == CFL_RAISE_EXCEPTION_EVENT) {
        if (event_type != CFL_EVENT_TYPE_JSON_RECORD) {
            EXCEPTION("cfl_cbor_controlled_node_main: event_type is not JSON_RECORD");
        }

        uint16_t original_node_id = (uint32_t)((size_t)event_data);
        cfl_forward_exception_event(runtime, node_index, ptr->client_node_index, original_node_id);
        return CFL_DISABLE;
    }

    return cfl_verify_active_children(runtime, node_index);
}

/* ========================================================================
 * SERVER TERM
 *
 * If response_text is set (JSON), encode to CBOR and send to client.
 * If NULL, send NULL event_data (no response).
 * Free heap-allocated CBOR buffer.
 * ======================================================================== */

void cfl_cbor_controlled_node_term_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_cbor_server_controlled_node_t *ptr = cfl_get_cbor_server_node(runtime, node_index);

    void *send_data = NULL;

    if (ptr->response_text && ptr->cbor_buffer) {
        /* Encode JSON response to CBOR */
        size_t cbor_len = cfl_json_text_to_cbor(
            ptr->response_text, ptr->cbor_buffer, CFL_CBOR_MAX_PACKET_SIZE);
        ptr->cbor_ref.data = ptr->cbor_buffer;
        ptr->cbor_ref.len = cbor_len;
        send_data = &ptr->cbor_ref;

        /* Free the JSON response text */
        cfl_heap_free_pointer(runtime->heap, ptr->response_text);
        ptr->response_text = NULL;
    }

    cfl_send_streaming_data_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
        ptr->client_node_index, ptr->response_port.event_id, send_data);

    /* Free the CBOR response buffer */
    if (ptr->cbor_buffer) {
        cfl_heap_free_pointer(runtime->heap, ptr->cbor_buffer);
        ptr->cbor_buffer = NULL;
    }
}

/* ========================================================================
 * CLIENT INIT
 *
 * Reads JSON request text from DSL, converts to CBOR on heap.
 * ======================================================================== */

static void cfl_cbor_client_decode(
    cfl_runtime_handle_t *runtime,
    unsigned node_index,
    cfl_cbor_client_controlled_node_t *node_data)
{
    int32_t temp;
    (void)node_index;

    json_decoder_init_from_runtime(runtime, node_index);

    json_extract_int32_runtime(runtime, "node_dict.request_port.event_id", &temp);
    node_data->request_port.event_id = (unsigned)temp;

    json_extract_int32_runtime(runtime, "node_dict.response_port.event_id", &temp);
    node_data->response_port.event_id = (unsigned)temp;

    json_extract_int32_runtime(runtime, "node_dict.server_node_index", &temp);
    node_data->server_node_index = (unsigned)temp;

    /* Read pre-built JSON request text from DSL, convert to CBOR on heap */
    const char *request_text = NULL;
    json_extract_string_runtime(runtime, "node_dict.request_text", &request_text);

    if (request_text) {
        /* Allocate CBOR buffer on heap and encode */
        uint16_t buf_size = (uint16_t)CFL_CBOR_MAX_PACKET_SIZE;
        node_data->cbor_request = (uint8_t *)cfl_heap_malloc_pointer(
            runtime->heap, buf_size);
        if (!node_data->cbor_request) {
            EXCEPTION("cfl_cbor_client_decode: cbor request alloc failed");
        }
        node_data->cbor_request_len = cfl_json_text_to_cbor(
            request_text, node_data->cbor_request, buf_size);
    } else {
        node_data->cbor_request = NULL;
        node_data->cbor_request_len = 0;
    }

    node_data->aux_data = NULL;
    node_data->node_is_active = false;
}

void cfl_cbor_client_controlled_node_init_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_cbor_client_controlled_node_t *ptr = NULL;

    if (cfl_allocate_state(handle, node_index) == false) {
        ptr = (cfl_cbor_client_controlled_node_t *)cfl_smart_arena_alloc(
            handle, node_index, sizeof(cfl_cbor_client_controlled_node_t));

        cfl_cbor_client_decode(runtime, node_index, ptr);
        cfl_json_bind_heap(&ptr->heap_iface, runtime->heap);
        cfl_cbor_bind_heap(&ptr->cbor_heap_iface, runtime->heap);
    } else {
        ptr = cfl_get_cbor_client_node(runtime, node_index);
        ptr->node_is_active = false;
    }
}

/* ========================================================================
 * CLIENT MAIN
 *
 * Sends CBOR-encoded request to server.
 * Receives CBOR response, decodes to JSON text for user boolean.
 * ======================================================================== */

unsigned cfl_cbor_client_controlled_node_main_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_cbor_client_controlled_node_t *ptr = cfl_get_cbor_client_node(runtime, node_index);

    if (ptr->node_is_active == false) {
        uint16_t server_node_id = ptr->server_node_index;

        if (cfl_engine_node_is_enabled(runtime, server_node_id) == true) {
            EXCEPTION("cfl_cbor_client_main: server node is already enabled");
        }

        /* Set server flags */
        runtime->flags[server_node_id] &= ~CT_FLAG_USER_MASK;
        runtime->flags[server_node_id] |= (CT_FLAG_USER3 | CT_FLAG_USER2);

        /* Call server init first — allocates arena state */
        const chaintree_node_t server_node = runtime->flash_handle->nodes[server_node_id];
        one_shot_function_t one_shot_function =
            runtime->flash_handle->one_shot_functions[server_node.init_function_index];
        one_shot_function(runtime, server_node_id);

        /* Set client_node_index on server (after init allocated state) */
        cfl_cbor_server_controlled_node_t *server_ptr =
            cfl_get_cbor_server_node(runtime, server_node_id);
        server_ptr->client_node_index = node_index;

        /* Call server boolean with INIT event */
        boolean_function_t boolean_function =
            runtime->flash_handle->boolean_functions[server_node.aux_function_index];
        boolean_function(runtime, server_node_id, CFL_EVENT_TYPE_NULL, CFL_INIT_EVENT, NULL);

        /* Set up CBOR request reference and send to server */
        ptr->cbor_ref.data = ptr->cbor_request;
        ptr->cbor_ref.len = ptr->cbor_request_len;

        main_function_t main_function =
            runtime->flash_handle->main_functions[server_node.main_function_index];
        main_function(runtime, server_node.aux_function_index, server_node_id,
            CFL_EVENT_TYPE_STREAMING_DATA, ptr->request_port.event_id, &ptr->cbor_ref);

        ptr->node_is_active = true;
        return CFL_HALT;
    }

    /* Active: check for response event */
    if (event_id == ptr->response_port.event_id) {
        if (event_type != CFL_EVENT_TYPE_STREAMING_DATA) {
            EXCEPTION("cfl_cbor_client_main: event type is not STREAMING_DATA");
        }

        /* Decode CBOR response to JSON text for user boolean */
        char *response_text = NULL;
        if (event_data) {
            cfl_cbor_buffer_t *cbor_buf = (cfl_cbor_buffer_t *)event_data;
            response_text = cfl_cbor_to_json_text(
                cbor_buf->data, cbor_buf->len, &ptr->cbor_heap_iface);
        }

        boolean_function_t boolean_function =
            runtime->flash_handle->boolean_functions[bool_function_index];
        if (boolean_function(runtime, node_index, event_type, event_id, response_text) == false) {
            if (response_text) {
                ptr->cbor_heap_iface.free(ptr->cbor_heap_iface.ctx, response_text);
            }
            return CFL_TERMINATE;
        }

        if (response_text) {
            ptr->cbor_heap_iface.free(ptr->cbor_heap_iface.ctx, response_text);
        }
        return CFL_DISABLE;
    }

    /* Exception forwarding */
    if (event_id == CFL_RAISE_EXCEPTION_EVENT) {
        if (event_type != CFL_EVENT_TYPE_JSON_RECORD) {
            EXCEPTION("cfl_cbor_client_main: event_type is not JSON_RECORD");
        }
        uint16_t original_node_id = (uint16_t)((size_t)event_data);
        cfl_forward_exception_event(runtime, node_index, 0xffff, original_node_id);
        return CFL_DISABLE;
    }

    return CFL_HALT;
}

/* ========================================================================
 * CLIENT TERM — free heap-allocated CBOR request buffer
 * ======================================================================== */

void cfl_cbor_client_controlled_node_term_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_cbor_client_controlled_node_t *ptr = cfl_get_cbor_client_node(runtime, node_index);
    if (!ptr) return;

    if (ptr->cbor_request) {
        cfl_heap_free_pointer(runtime->heap, ptr->cbor_request);
        ptr->cbor_request = NULL;
    }
}

/* ========================================================================
 * HELPER FUNCTIONS
 * ======================================================================== */

uint16_t cfl_cbor_server_get_server_node_index(cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint16_t test_index = node_index;
    const chaintree_node_t *node = &handle->flash_handle->nodes[test_index];
    uint16_t parent_index = node->parent_index;

    while (parent_index != 0xffff) {
        if (node->main_function_index ==
            handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_CBOR_CONTROLLED_NODE_MAIN]) {
            return test_index;
        }
        node = &handle->flash_handle->nodes[parent_index];
        test_index = parent_index;
        parent_index = node->parent_index;
    }

    EXCEPTION("cfl_cbor_server_get_server_node_index: server node not found");
    return 0xffff;
}

cfl_cbor_server_controlled_node_t *cfl_cbor_server_get_node(
    cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint16_t server_index = cfl_cbor_server_get_server_node_index(handle, node_index);
    return cfl_get_cbor_server_node(handle, server_index);
}

void cfl_cbor_server_set_response_text(cfl_runtime_handle_t *handle,
    unsigned node_index, const char *text)
{
    cfl_cbor_server_controlled_node_t *server = cfl_cbor_server_get_node(handle, node_index);

    /* Free previous response if any */
    if (server->response_text) {
        cfl_heap_free_pointer(handle->heap, server->response_text);
        server->response_text = NULL;
    }

    if (text) {
        unsigned len = (unsigned)strlen(text);
        server->response_text = (char *)cfl_heap_malloc_pointer(
            handle->heap, (uint16_t)(len + 1));
        if (!server->response_text) {
            EXCEPTION("cfl_cbor_server_set_response_text: alloc failed");
        }
        memcpy(server->response_text, text, len + 1);
    }
}
