#ifndef CFL_COMMON_FUNCTIONS_H
#define CFL_COMMON_FUNCTIONS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

#include "cfl_engine.h"


void cfl_uint16_to_str(uint16_t value, char* buffer);


void *cfl_smart_arena_alloc(cfl_runtime_handle_t *handle, uint16_t node_index, uint16_t size);

#ifdef __cplusplus
}
#endif
#endif

