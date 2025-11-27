#ifndef CFL_COMMON_FUNCTIONS_H
#define CFL_COMMON_FUNCTIONS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

#include "cfl_engine.h"


void cfl_uint16_to_str(uint16_t value, char* buffer);

bool cfl_allocate_state(cfl_runtime_handle_t *handle, uint16_t node_index);
void *cfl_additional_arena_alloc(cfl_runtime_handle_t *handle, uint16_t node_index, uint16_t size);
void *cfl_smart_arena_alloc(cfl_runtime_handle_t *handle, uint16_t node_index, uint16_t size);
void cfl_change_state(cfl_runtime_handle_t *handle, uint16_t node_index, int32_t sm_node_id, const char *new_state, bool sync_flag, int32_t sync_event_id);
void cfl_terminate_state_machine(cfl_runtime_handle_t *handle, uint16_t node_index, int32_t sm_node_id);

void cfl_enable_all_nodes(cfl_runtime_handle_t *handle, uint16_t node_index);
#ifdef __cplusplus
}
#endif
#endif

