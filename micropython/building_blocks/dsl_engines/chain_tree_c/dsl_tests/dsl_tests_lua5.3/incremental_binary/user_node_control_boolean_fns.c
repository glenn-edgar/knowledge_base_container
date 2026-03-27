#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>

#include "cfl_runtime.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "cfl_engine.h"
#include "cfl_node_control_support.h"
#include "json_node_decoder.h"
#include "avro_packets/drone_control.h"

bool on_fly_arc_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    (void)event_data;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_client_controlled_node_t *ptr = (cfl_client_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (ptr->aux_data != NULL) {
            return false;
        }

        ptr->request_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_arc_request_packet_t));
        ptr->request_port.data_pointer   = fly_arc_request_packet_init(
                                               (fly_arc_request_packet_t *)ptr->request_port.packet_pointer,
                                               (uint16_t)node_index);

        if (cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port) == false) {
            EXCEPTION("on_fly_arc_complete_boolean_fn: failed to match port");
            return false;
        }

        fly_arc_request_wire_t *req = (fly_arc_request_wire_t *)ptr->request_port.data_pointer;
    
        json_decoder_init_from_runtime(runtime, node_index);
        //json_print_node_data_runtime(runtime, node_index);
        const char *api_name = NULL;
        json_extract_string_runtime(runtime, "node_dict.api_name", &api_name);

        if (strcmp(api_name, "drone_control_fly_arc") != 0) {
            EXCEPTION("on_fly_arc_complete_boolean_fn: API name does not match");
            return true;
        }

        json_extract_float32_runtime(runtime, "node_dict.aux_data.distance",       &req->distance);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_altitude",  &req->final_altitude);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_speed",     &req->final_speed);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.heading",         &req->heading);

        const char *waypoint = NULL;
        json_extract_string_runtime(runtime, "node_dict.aux_data.finalize_data.waypoint", &waypoint);
        req->finalize.ptr = (void *)waypoint;

        cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port);
        return false;
    }

    if (event_id == ptr->response_port.event_id) {
        ptr->request_port.packet_pointer  = event_data;
        ptr->response_port.data_pointer   = (fly_arc_response_wire_t *)fly_arc_response_packet_verify(
                                                (const fly_arc_response_packet_t *)event_data);

        if (ptr->response_port.data_pointer == NULL) {
            EXCEPTION("on_fly_arc_complete_boolean_fn: response port packet is not correct");
            return false;
        }

        fly_arc_request_wire_t  *req  = (fly_arc_request_wire_t  *)ptr->request_port.data_pointer;
        fly_arc_response_wire_t *resp = (fly_arc_response_wire_t *)ptr->response_port.data_pointer;

        if (resp->success == false) {
            EXCEPTION("on_fly_arc_complete_boolean_fn: response failed");
            return false;
        }
        if (resp->final_altitude != req->final_altitude) {
            EXCEPTION("on_fly_arc_complete_boolean_fn: final altitude does not match");
            return false;
        }
        if (resp->final_speed != req->final_speed) {
            EXCEPTION("on_fly_arc_complete_boolean_fn: final speed does not match");
            return false;
        }
        if (resp->heading != req->heading) {
            EXCEPTION("on_fly_arc_complete_boolean_fn: heading does not match");
            return false;
        }

        printf("on_fly_arc_complete_boolean_fn: response successful\n");
        return true;
    }

    if (event_id == CFL_TERMINATE_EVENT) {
        return false;
    }

    return true;
}

bool on_fly_down_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    (void)event_data;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_client_controlled_node_t *ptr = (cfl_client_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (ptr->aux_data != NULL) {
            return false;
        }

        ptr->request_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_down_request_packet_t));
        ptr->request_port.data_pointer   = fly_down_request_packet_init(
                                               (fly_down_request_packet_t *)ptr->request_port.packet_pointer,
                                               (uint16_t)node_index);

        if (cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port) == false) {
            EXCEPTION("on_fly_down_complete_boolean_fn: failed to match port");
            return false;
        }

        fly_down_request_wire_t *req = (fly_down_request_wire_t *)ptr->request_port.data_pointer;
        
        json_decoder_init_from_runtime(runtime, node_index);
        //json_print_node_data_runtime(runtime, node_index);
        const char *api_name = NULL;
        json_extract_string_runtime(runtime, "node_dict.api_name", &api_name);

        if (strcmp(api_name, "drone_control_fly_down") != 0) {
            EXCEPTION("on_fly_down_complete_boolean_fn: API name does not match");
            return true;
        }

        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_altitude", &req->final_altitude);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_speed",    &req->final_speed);

        const char *target = NULL;
        json_extract_string_runtime(runtime, "node_dict.aux_data.finalize_data.target", &target);
        req->finalize.ptr = (void *)target;

        cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port);
        return false;
    }

    if (event_id == ptr->response_port.event_id) {
        ptr->request_port.packet_pointer  = event_data;
        ptr->response_port.data_pointer   = (fly_down_response_wire_t *)fly_down_response_packet_verify(
                                                (const fly_down_response_packet_t *)event_data);

        if (ptr->response_port.data_pointer == NULL) {
            EXCEPTION("on_fly_down_complete_boolean_fn: response port packet is not correct");
            return false;
        }

        fly_down_request_wire_t  *req  = (fly_down_request_wire_t  *)ptr->request_port.data_pointer;
        fly_down_response_wire_t *resp = (fly_down_response_wire_t *)ptr->response_port.data_pointer;

        if (resp->success == false) {
            EXCEPTION("on_fly_down_complete_boolean_fn: response failed");
            return false;
        }
        if (resp->final_altitude != req->final_altitude) {
            EXCEPTION("on_fly_down_complete_boolean_fn: final altitude does not match");
            return false;
        }
        if (resp->final_speed != req->final_speed) {
            EXCEPTION("on_fly_down_complete_boolean_fn: final speed does not match");
            return false;
        }

        return true;
    }

    if (event_id == CFL_TERMINATE_EVENT) {
        return false;
    }

    return true;
}

bool on_fly_straight_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    (void)event_data;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_client_controlled_node_t *ptr = (cfl_client_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (ptr->aux_data != NULL) {
            return false;
        }

        ptr->request_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_straight_request_packet_t));
        ptr->request_port.data_pointer   = fly_straight_request_packet_init(
                                               (fly_straight_request_packet_t *)ptr->request_port.packet_pointer,
                                               (uint16_t)node_index);

        if (cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port) == false) {
            EXCEPTION("on_fly_straight_complete_boolean_fn: failed to match port");
            return false;
        }

        fly_straight_request_wire_t *req = (fly_straight_request_wire_t *)ptr->request_port.data_pointer;
        json_decoder_init_from_runtime(runtime, node_index);
        //json_print_node_data_runtime(runtime, node_index);
        const char *api_name = NULL;
        json_extract_string_runtime(runtime, "node_dict.api_name", &api_name);

        if (strcmp(api_name, "drone_control_fly_straight") != 0) {
            EXCEPTION("on_fly_straight_complete_boolean_fn: API name does not match");
            return true;
        }

        json_extract_float32_runtime(runtime, "node_dict.aux_data.distance",       &req->distance);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_altitude",  &req->final_altitude);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_speed",     &req->final_speed);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.heading",         &req->heading);

        const char *waypoint = NULL;
        json_extract_string_runtime(runtime, "node_dict.aux_data.finalize_data.waypoint", &waypoint);
        req->finalize.ptr = (void *)waypoint;

        cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port);
        return false;
    }

    if (event_id == ptr->response_port.event_id) {
        ptr->request_port.packet_pointer  = event_data;
        ptr->response_port.data_pointer   = (fly_straight_response_wire_t *)fly_straight_response_packet_verify(
                                                (const fly_straight_response_packet_t *)event_data);

        if (ptr->response_port.data_pointer == NULL) {
            EXCEPTION("on_fly_straight_complete_boolean_fn: response port packet is not correct");
            return false;
        }

        fly_straight_request_wire_t  *req  = (fly_straight_request_wire_t  *)ptr->request_port.data_pointer;
        fly_straight_response_wire_t *resp = (fly_straight_response_wire_t *)ptr->response_port.data_pointer;

        if (resp->success == false) {
            EXCEPTION("on_fly_straight_complete_boolean_fn: response failed");
            return false;
        }
        if (resp->final_altitude != req->final_altitude) {
            EXCEPTION("on_fly_straight_complete_boolean_fn: final altitude does not match");
            return false;
        }
        if (resp->final_speed != req->final_speed) {
            EXCEPTION("on_fly_straight_complete_boolean_fn: final speed does not match");
            return false;
        }
        if (resp->heading != req->heading) {
            EXCEPTION("on_fly_straight_complete_boolean_fn: heading does not match");
            return false;
        }

        printf("on_fly_straight_complete_boolean_fn: response successful\n");
        return true;
    }

    if (event_id == CFL_TERMINATE_EVENT) {
        return false;
    }

    return true;
}

bool on_fly_up_complete_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    (void)event_data;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_client_controlled_node_t *ptr = (cfl_client_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (ptr->aux_data != NULL) {
            return false;
        }

        ptr->request_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_up_request_packet_t));
        ptr->request_port.data_pointer   = fly_up_request_packet_init(
                                               (fly_up_request_packet_t *)ptr->request_port.packet_pointer,
                                               (uint16_t)node_index);

        if (cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port) == false) {
            EXCEPTION("on_fly_up_complete_boolean_fn: failed to match port");
            return false;
        }

        fly_up_request_wire_t *req = (fly_up_request_wire_t *)ptr->request_port.data_pointer;
        json_decoder_init_from_runtime(runtime, node_index);
        //json_print_node_data_runtime(runtime, node_index);
        const char *api_name = NULL;
        json_extract_string_runtime(runtime, "node_dict.api_name", &api_name);

        if (strcmp(api_name, "drone_control_fly_up") != 0) {
            EXCEPTION("on_fly_up_complete_boolean_fn: API name does not match");
            return true;
        }

        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_altitude", &req->final_altitude);
        json_extract_float32_runtime(runtime, "node_dict.aux_data.final_speed",    &req->final_speed);

        const char *target = NULL;
        json_extract_string_runtime(runtime, "node_dict.aux_data.finalize_data.target", &target);
        req->finalize.ptr = (void *)target;

        cfl_packet_matches_port(ptr->request_port.packet_pointer, &ptr->request_port);
        return false;
    }

    if (event_id == ptr->response_port.event_id) {
        ptr->request_port.packet_pointer  = event_data;
        ptr->response_port.data_pointer   = (fly_up_response_wire_t *)fly_up_response_packet_verify(
                                                (const fly_up_response_packet_t *)event_data);

        if (ptr->response_port.data_pointer == NULL) {
            EXCEPTION("on_fly_up_complete_boolean_fn: response port packet is not correct");
            return false;
        }

        fly_up_request_wire_t  *req  = (fly_up_request_wire_t  *)ptr->request_port.data_pointer;
        fly_up_response_wire_t *resp = (fly_up_response_wire_t *)ptr->response_port.data_pointer;

        if (resp->success == false) {
            EXCEPTION("on_fly_up_complete_boolean_fn: response failed");
            return false;
        }
        if (resp->final_altitude != req->final_altitude) {
            EXCEPTION("on_fly_up_complete_boolean_fn: final altitude does not match");
            return false;
        }
        if (resp->final_speed != req->final_speed) {
            EXCEPTION("on_fly_up_complete_boolean_fn: final speed does not match");
            return false;
        }

        printf("on_fly_up_complete_boolean_fn: response successful\n");
        return true;
    }

    if (event_id == CFL_TERMINATE_EVENT) {
        return false;
    }

    return true;
}

// =========================================================================
// Server monitor functions
// =========================================================================

bool fly_arc_monitor_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_server_controlled_node_t *ptr = (cfl_server_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (ptr->response_port.packet_pointer != NULL) {
            return false;
        }

        ptr->response_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_arc_response_packet_t));
        ptr->response_port.data_pointer   = fly_arc_response_packet_init(
                                                (fly_arc_response_packet_t *)ptr->response_port.packet_pointer,
                                                (uint16_t)node_index);

        fly_arc_response_wire_t *resp = (fly_arc_response_wire_t *)ptr->response_port.data_pointer;
        resp->success    = false;
        resp->error_code = 0;
        return false;
    }

    if (event_id == CFL_TERMINATE_EVENT) {
        return false;
    }

    if (event_id == ptr->request_port.event_id) {
        const fly_arc_request_wire_t *req = fly_arc_request_packet_verify(
                                                (const fly_arc_request_packet_t *)event_data);
        ptr->request_port.packet_pointer = event_data;
        ptr->request_port.data_pointer   = (void *)req;
        return true;
    }

    return true;
}

bool fly_down_monitor_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_server_controlled_node_t *ptr = (cfl_server_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (ptr->response_port.packet_pointer != NULL) {
            return false;
        }

        ptr->response_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_down_response_packet_t));
        ptr->response_port.data_pointer   = fly_down_response_packet_init(
                                                (fly_down_response_packet_t *)ptr->response_port.packet_pointer,
                                                (uint16_t)node_index);

        fly_down_response_wire_t *resp = (fly_down_response_wire_t *)ptr->response_port.data_pointer;
        resp->success    = false;
        resp->error_code = 0;
        return false;
    }

    if (event_id == CFL_TERMINATE_EVENT) {
        return false;
    }

    if (event_id == ptr->request_port.event_id) {
        const fly_down_request_wire_t *req = fly_down_request_packet_verify(
                                                 (const fly_down_request_packet_t *)event_data);
        ptr->request_port.packet_pointer = event_data;
        ptr->request_port.data_pointer   = (void *)req;
        return true;
    }

    return true;
}

bool fly_straight_monitor_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_server_controlled_node_t *ptr = (cfl_server_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (ptr->response_port.packet_pointer != NULL) {
            return false;
        }

        ptr->response_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_straight_response_packet_t));
        ptr->response_port.data_pointer   = fly_straight_response_packet_init(
                                                (fly_straight_response_packet_t *)ptr->response_port.packet_pointer,
                                                (uint16_t)node_index);

        fly_straight_response_wire_t *resp = (fly_straight_response_wire_t *)ptr->response_port.data_pointer;
        resp->success    = false;
        resp->error_code = 0;
        return false;
    }

    if (event_id == CFL_TERMINATE_EVENT) {
        return false;
    }

    if (event_id == ptr->request_port.event_id) {
        const fly_straight_request_wire_t *req = fly_straight_request_packet_verify(
                                                     (const fly_straight_request_packet_t *)event_data);
        ptr->request_port.packet_pointer = event_data;
        ptr->request_port.data_pointer   = (void *)req;
        return true;
    }

    return true;
}

bool fly_up_monitor_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_server_controlled_node_t *ptr = (cfl_server_controlled_node_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);

    if (event_id == CFL_INIT_EVENT) {
        if (ptr->response_port.packet_pointer != NULL) {
            return false;
        }

        ptr->response_port.packet_pointer = cfl_additional_arena_alloc(runtime, node_index, sizeof(fly_up_response_packet_t));
        ptr->response_port.data_pointer   = fly_up_response_packet_init(
                                                (fly_up_response_packet_t *)ptr->response_port.packet_pointer,
                                                (uint16_t)node_index);

        fly_up_response_wire_t *resp = (fly_up_response_wire_t *)ptr->response_port.data_pointer;
        resp->success    = false;
        resp->error_code = 0;
        return false;
    }

    if (event_id == CFL_TERMINATE_EVENT) {
        return false;
    }

    if (event_id == ptr->request_port.event_id) {
        const fly_up_request_wire_t *req = fly_up_request_packet_verify(
                                               (const fly_up_request_packet_t *)event_data);
        ptr->request_port.packet_pointer = event_data;
        ptr->request_port.data_pointer   = (void *)req;
        return true;
    }

    return true;
}

// =========================================================================
// One-shot finalizers
// =========================================================================

void update_fly_arc_final_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *runtime    = (cfl_runtime_handle_t *)handle;
    cfl_port_t *request_port  = cfl_server_controlled_node_get_request_port(runtime, node_index);
    cfl_port_t *response_port = cfl_server_controlled_node_get_response_port(runtime, node_index);

    if (cfl_packet_matches_port(request_port->packet_pointer, request_port) == false) {
        EXCEPTION("update_fly_arc_final_one_shot_fn: failed to match request port");
        return;
    }
    if (cfl_packet_matches_port(response_port->packet_pointer, response_port) == false) {
        EXCEPTION("update_fly_arc_final_one_shot_fn: failed to match response port");
        return;
    }

    const fly_arc_request_wire_t *req  = (const fly_arc_request_wire_t *)request_port->data_pointer;
    fly_arc_response_wire_t      *resp = (fly_arc_response_wire_t *)response_port->data_pointer;

    resp->success        = true;
    resp->error_code     = 0;
    resp->distance       = req->distance;
    resp->final_altitude = req->final_altitude;
    resp->final_speed    = req->final_speed;
    resp->heading        = req->heading;
}

void update_fly_down_final_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *runtime    = (cfl_runtime_handle_t *)handle;
    cfl_port_t *request_port  = cfl_server_controlled_node_get_request_port(runtime, node_index);
    cfl_port_t *response_port = cfl_server_controlled_node_get_response_port(runtime, node_index);

    if (cfl_packet_matches_port(request_port->packet_pointer, request_port) == false) {
        EXCEPTION("update_fly_down_final_one_shot_fn: failed to match request port");
        return;
    }
    if (cfl_packet_matches_port(response_port->packet_pointer, response_port) == false) {
        EXCEPTION("update_fly_down_final_one_shot_fn: failed to match response port");
        return;
    }

    const fly_down_request_wire_t *req  = (const fly_down_request_wire_t *)request_port->data_pointer;
    fly_down_response_wire_t      *resp = (fly_down_response_wire_t *)response_port->data_pointer;

    resp->success        = true;
    resp->error_code     = 0;
    resp->final_altitude = req->final_altitude;
    resp->final_speed    = req->final_speed;
}

void update_fly_straight_final_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *runtime    = (cfl_runtime_handle_t *)handle;
    cfl_port_t *request_port  = cfl_server_controlled_node_get_request_port(runtime, node_index);
    cfl_port_t *response_port = cfl_server_controlled_node_get_response_port(runtime, node_index);

    if (cfl_packet_matches_port(request_port->packet_pointer, request_port) == false) {
        EXCEPTION("update_fly_straight_final_one_shot_fn: failed to match request port");
        return;
    }
    if (cfl_packet_matches_port(response_port->packet_pointer, response_port) == false) {
        EXCEPTION("update_fly_straight_final_one_shot_fn: failed to match response port");
        return;
    }

    const fly_straight_request_wire_t *req  = (const fly_straight_request_wire_t *)request_port->data_pointer;
    fly_straight_response_wire_t      *resp = (fly_straight_response_wire_t *)response_port->data_pointer;

    resp->success        = true;
    resp->error_code     = 0;
    resp->distance       = req->distance;
    resp->final_altitude = req->final_altitude;
    resp->final_speed    = req->final_speed;
    resp->heading        = req->heading;
}

void update_fly_up_final_one_shot_fn(void *handle, unsigned node_index) {
    cfl_runtime_handle_t *runtime    = (cfl_runtime_handle_t *)handle;
    cfl_port_t *request_port  = cfl_server_controlled_node_get_request_port(runtime, node_index);
    cfl_port_t *response_port = cfl_server_controlled_node_get_response_port(runtime, node_index);

    if (cfl_packet_matches_port(request_port->packet_pointer, request_port) == false) {
        EXCEPTION("update_fly_up_final_one_shot_fn: failed to match request port");
        return;
    }
    if (cfl_packet_matches_port(response_port->packet_pointer, response_port) == false) {
        EXCEPTION("update_fly_up_final_one_shot_fn: failed to match response port");
        return;
    }

    const fly_up_request_wire_t *req  = (const fly_up_request_wire_t *)request_port->data_pointer;
    fly_up_response_wire_t      *resp = (fly_up_response_wire_t *)response_port->data_pointer;

    resp->success        = true;
    resp->error_code     = 0;
    resp->final_altitude = req->final_altitude;
    resp->final_speed    = req->final_speed;
}

// =========================================================================
// Exception handler
// =========================================================================

static void parse_exception_record(cfl_runtime_handle_t *runtime, unsigned node_index) {
    json_decoder_init_from_runtime(runtime, node_index);
    //json_print_node_data_runtime(runtime, node_index);

    int32_t exception_id = 0;
    json_extract_int32_runtime(runtime, "node_dict.exception_id", &exception_id);
    printf("Exception ID: %d\n", exception_id);

    uint32_t exception_data_record = 0;
    json_navigate_path_runtime(runtime, "node_dict.exception_data", &exception_data_record);

    json_decoder_ctx_t *ctx = runtime->json_decoder_ctx;

    uint32_t child_count = 0;
    json_get_child_count(ctx, exception_data_record, &child_count);

    if (child_count >= 2) {
        uint32_t key_record   = exception_data_record + 1;
        uint32_t value_record = exception_data_record + 2;

        const char *exception_key = NULL;
        json_get_string_value(ctx, key_record, &exception_key);
        printf("Exception Key: %s\n", exception_key);

        float exception_value = 0.0f;
        json_get_float32(ctx, value_record, &exception_value);
        printf("Exception Value: %f\n", exception_value);
    }
}

bool drone_control_exception_catch_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    (void)node_index;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;

    switch (event_id) {
        case CFL_INIT_EVENT:
            return false;
        case CFL_TERMINATE_EVENT:
            return false;
        case CFL_RAISE_EXCEPTION_EVENT: {
            uint16_t original_node_id = (uint16_t)((size_t)event_data);
            printf("*********** catch_all_exception_boolean_fn ***********\n");
            printf("Raise exception event\n");
            printf("original node id: %d\n", original_node_id);
            parse_exception_record(runtime_handle, original_node_id);
            printf("raise the exception to parent node\n");
            printf("*********** catch_all_exception_boolean_fn ***********\n");
            return false;
        }
        default:
            EXCEPTION("Unexpected event in catch_all_exception_boolean_fn");
            return false;
    }
    return false;
}