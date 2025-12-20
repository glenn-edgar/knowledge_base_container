#ifndef CFL_NODE_CONTROL_SUPPORT_H
#define CFL_NODE_CONTROL_SUPPORT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "cfl_runtime.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "cfl_engine.h"
#include "cfl_node_control_support.h"
#include "json_node_decoder.h"
#include "avro_common.h"

typedef struct {
    avro_packet_header_t header;
    unsigned event_id;
    void *packet_pointer;
    void *data_pointer;
} cfl_port_t;

// Client controlled node data
typedef struct {
    cfl_port_t request_port;
    cfl_port_t response_port;
    unsigned server_node_index;
    void *aux_data;  // Application-specific data
    bool node_is_active;
} cfl_client_controlled_node_t;





#ifdef __cplusplus
}
#endif

#endif