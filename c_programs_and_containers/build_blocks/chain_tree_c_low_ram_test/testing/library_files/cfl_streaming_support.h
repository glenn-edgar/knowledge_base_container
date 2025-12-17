#ifndef CFL_STREAMING_SUPPORT_H
#define CFL_STREAMING_SUPPORT_H
#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cfl_runtime.h"
#include "cfl_exception.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"

typedef struct{
    const char *h_file_name;
    unsigned handler_id;
}cfl_streaming_port_data_t;

typedef struct{
    cfl_streaming_port_data_t port_data;
    unsigned event_id;
    unsigned event_column_id;
    void *aux_data;
    void *packet_data;
 }cfl_emit_setup_data_t;


typedef struct{
   cfl_streaming_port_data_t port_data;
   unsigned event_id;
   void *aux_data;

}cfl_one_port_monitor_data_t;


cfl_one_port_monitor_data_t * cfl_one_port_monitor_data_init(cfl_runtime_handle_t *runtime_handle, uint16_t node_index);

bool cfl_verify_avro_packet(void *data, cfl_streaming_port_data_t *port_data);

void cfl_emit_setup_data_init(cfl_emit_setup_data_t *emit_setup_data, cfl_runtime_handle_t *runtime_handle, uint16_t node_index);

void cfl_emit_packet_verify(void *data, cfl_emit_setup_data_t *emit_setup_data);

#ifdef __cplusplus
}
#endif
#endif