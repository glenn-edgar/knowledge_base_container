/* ========================================================================
 * cfl_json_node_control.c — JSON-based controlled node functions
 *
 * Mirrors the Avro controlled node pattern (cfl_node_control_support.c)
 * but uses JSON text for request/response data instead of Avro packets.
 *
 * Key differences from Avro version:
 *   - No schema_hash / packet_pointer validation
 *   - client_node_index set by client at activation (not from wire header)
 *   - Event data is char* JSON text passed through event queue
 *   - Container functions are format-agnostic (reused as-is)
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
#include "cfl_json_node_control.h"
#include "cfl_json_functions.h"
#include "json_node_decoder.h"
#include "cfl_exception_support.h"

/* ========================================================================
 * SERVER INIT
 * ======================================================================== */

static void cfl_json_server_decode(
    cfl_runtime_handle_t *runtime,
    unsigned node_index,
    cfl_json_server_controlled_node_t *node_data)
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
}

void cfl_json_controlled_node_init_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_json_server_controlled_node_t *ptr = NULL;

    if (cfl_allocate_state(handle, node_index) == false) {
        ptr = (cfl_json_server_controlled_node_t *)cfl_smart_arena_alloc(
            handle, node_index, sizeof(cfl_json_server_controlled_node_t));

        cfl_json_server_decode(runtime, node_index, ptr);
        cfl_json_bind_heap(&ptr->heap_iface, runtime->heap);
    } else {
        ptr = cfl_get_json_server_node(runtime, node_index);
        ptr->response_text = NULL;
    }
}

/* ========================================================================
 * SERVER MAIN
 * ======================================================================== */

unsigned cfl_json_controlled_node_main_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_json_server_controlled_node_t *ptr = cfl_get_json_server_node(runtime, node_index);

    if (event_id == ptr->request_port.event_id) {
        if (event_type != CFL_EVENT_TYPE_STREAMING_DATA) {
            EXCEPTION("cfl_json_controlled_node_main: event type is not STREAMING_DATA");
        }

        /* client_node_index already set by client before this call */

        boolean_function_t boolean_function =
            runtime->flash_handle->boolean_functions[bool_function_index];
        boolean_function(runtime, node_index, event_type, event_id, event_data);

        cfl_enable_all_nodes(runtime, node_index);
        return CFL_HALT;
    }

    if (event_id == CFL_RAISE_EXCEPTION_EVENT) {
        if (event_type != CFL_EVENT_TYPE_JSON_RECORD) {
            EXCEPTION("cfl_json_controlled_node_main: event_type is not JSON_RECORD");
        }

        uint16_t original_node_id = (uint32_t)((size_t)event_data);
        cfl_forward_exception_event(runtime, node_index, ptr->client_node_index, original_node_id);
        return CFL_DISABLE;
    }

    return cfl_verify_active_children(runtime, node_index);
}

/* ========================================================================
 * SERVER TERM
 * ======================================================================== */

void cfl_json_controlled_node_term_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_json_server_controlled_node_t *ptr = cfl_get_json_server_node(runtime, node_index);

    /* Send response to client — response_text may be NULL (no response data) */
    cfl_send_streaming_data_event(runtime->event_queue, CFL_EVENT_PRIORITY_LOW,
        ptr->client_node_index, ptr->response_port.event_id, ptr->response_text);
}

/* ========================================================================
 * CLIENT INIT
 * ======================================================================== */

static void cfl_json_client_decode(
    cfl_runtime_handle_t *runtime,
    unsigned node_index,
    cfl_json_client_controlled_node_t *node_data)
{
    int32_t temp;

    json_decoder_init_from_runtime(runtime, node_index);

    json_extract_int32_runtime(runtime, "node_dict.request_port.event_id", &temp);
    node_data->request_port.event_id = (unsigned)temp;

    json_extract_int32_runtime(runtime, "node_dict.response_port.event_id", &temp);
    node_data->response_port.event_id = (unsigned)temp;

    json_extract_int32_runtime(runtime, "node_dict.server_node_index", &temp);
    node_data->server_node_index = (unsigned)temp;

    /* Read pre-built JSON request text from DSL */
    const char *request_text = NULL;
    json_extract_string_runtime(runtime, "node_dict.request_text", &request_text);

    if (request_text) {
        unsigned len = (unsigned)strlen(request_text);
        node_data->request_text_size = len + 1;
        node_data->request_text = (char *)cfl_heap_malloc_pointer(
            runtime->heap, (uint16_t)node_data->request_text_size);
        if (!node_data->request_text) {
            EXCEPTION("cfl_json_client_decode: request_text alloc failed");
        }
        memcpy(node_data->request_text, request_text, node_data->request_text_size);
    } else {
        node_data->request_text = NULL;
        node_data->request_text_size = 0;
    }

    node_data->aux_data = NULL;
    node_data->node_is_active = false;
}

void cfl_json_client_controlled_node_init_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_json_client_controlled_node_t *ptr = NULL;

    if (cfl_allocate_state(handle, node_index) == false) {
        ptr = (cfl_json_client_controlled_node_t *)cfl_smart_arena_alloc(
            handle, node_index, sizeof(cfl_json_client_controlled_node_t));

        cfl_json_client_decode(runtime, node_index, ptr);
        cfl_json_bind_heap(&ptr->heap_iface, runtime->heap);
    } else {
        ptr = cfl_get_json_client_node(runtime, node_index);
        ptr->node_is_active = false;
    }
}

/* ========================================================================
 * CLIENT MAIN
 * ======================================================================== */

unsigned cfl_json_client_controlled_node_main_main_fn(void *handle, unsigned bool_function_index,
    unsigned node_index, unsigned event_type, unsigned event_id, void *event_data)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_json_client_controlled_node_t *ptr = cfl_get_json_client_node(runtime, node_index);

    if (ptr->node_is_active == false) {
        uint16_t server_node_id = ptr->server_node_index;

        if (cfl_engine_node_is_enabled(runtime, server_node_id) == true) {
            EXCEPTION("cfl_json_client_main: server node is already enabled");
        }

        /* Set server flags */
        runtime->flags[server_node_id] &= ~CT_FLAG_USER_MASK;
        runtime->flags[server_node_id] |= (CT_FLAG_USER3 | CT_FLAG_USER2);

        /* Call server init first — allocates arena state */
        const chaintree_node_t server_node = runtime->flash_handle->nodes[server_node_id];
        one_shot_function_t one_shot_function =
            runtime->flash_handle->one_shot_functions[server_node.init_function_index];
        one_shot_function(runtime, server_node_id);

        /* Now set client_node_index on server (after init allocated state) */
        cfl_json_server_controlled_node_t *server_ptr =
            cfl_get_json_server_node(runtime, server_node_id);
        server_ptr->client_node_index = node_index;

        /* Call server boolean with INIT event */
        boolean_function_t boolean_function =
            runtime->flash_handle->boolean_functions[server_node.aux_function_index];
        boolean_function(runtime, server_node_id, CFL_EVENT_TYPE_NULL, CFL_INIT_EVENT, NULL);

        /* Call server main with request — pass request_text as event_data */
        main_function_t main_function =
            runtime->flash_handle->main_functions[server_node.main_function_index];
        main_function(runtime, server_node.aux_function_index, server_node_id,
            CFL_EVENT_TYPE_STREAMING_DATA, ptr->request_port.event_id, ptr->request_text);

        ptr->node_is_active = true;
        return CFL_HALT;
    }

    /* Active: check for response event */
    if (event_id == ptr->response_port.event_id) {
        if (event_type != CFL_EVENT_TYPE_STREAMING_DATA) {
            EXCEPTION("cfl_json_client_main: event type is not STREAMING_DATA");
        }

        boolean_function_t boolean_function =
            runtime->flash_handle->boolean_functions[bool_function_index];
        if (boolean_function(runtime, node_index, event_type, event_id, event_data) == false) {
            /* Free server's heap-allocated response_text after consumption */
            if (event_data) {
                cfl_heap_free_pointer(runtime->heap, event_data);
            }
            return CFL_TERMINATE;
        }

        /* Free server's heap-allocated response_text after consumption */
        if (event_data) {
            cfl_heap_free_pointer(runtime->heap, event_data);
        }
        return CFL_DISABLE;
    }

    /* Exception forwarding */
    if (event_id == CFL_RAISE_EXCEPTION_EVENT) {
        if (event_type != CFL_EVENT_TYPE_JSON_RECORD) {
            EXCEPTION("cfl_json_client_main: event_type is not JSON_RECORD");
        }
        uint16_t original_node_id = (uint16_t)((size_t)event_data);
        cfl_forward_exception_event(runtime, node_index, 0xffff, original_node_id);
        return CFL_DISABLE;
    }

    return CFL_HALT;
}

/* ========================================================================
 * CLIENT TERM
 * ======================================================================== */

void cfl_json_client_controlled_node_term_one_shot_fn(void *handle, unsigned node_index)
{
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_json_client_controlled_node_t *ptr = cfl_get_json_client_node(runtime, node_index);
    if (!ptr) return;

    if (ptr->request_text) {
        cfl_heap_free_pointer(runtime->heap, ptr->request_text);
        ptr->request_text = NULL;
    }
}

/* ========================================================================
 * HELPER FUNCTIONS
 * ======================================================================== */

uint16_t cfl_json_server_get_server_node_index(cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint16_t test_index = node_index;
    const chaintree_node_t *node = &handle->flash_handle->nodes[test_index];
    uint16_t parent_index = node->parent_index;

    while (parent_index != 0xffff) {
        if (node->main_function_index ==
            handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_JSON_CONTROLLED_NODE_MAIN]) {
            return test_index;
        }
        node = &handle->flash_handle->nodes[parent_index];
        test_index = parent_index;
        parent_index = node->parent_index;
    }

    EXCEPTION("cfl_json_server_get_server_node_index: server node not found");
    return 0xffff;
}

cfl_json_server_controlled_node_t *cfl_json_server_get_node(
    cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint16_t server_index = cfl_json_server_get_server_node_index(handle, node_index);
    return cfl_get_json_server_node(handle, server_index);
}

void cfl_json_server_set_response_text(cfl_runtime_handle_t *handle,
    unsigned node_index, const char *text)
{
    cfl_json_server_controlled_node_t *server = cfl_json_server_get_node(handle, node_index);

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
            EXCEPTION("cfl_json_server_set_response_text: alloc failed");
        }
        memcpy(server->response_text, text, len + 1);
    }
}
