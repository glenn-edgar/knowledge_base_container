#ifndef CFL_S_ENGINE_INTERFACE_H
#define CFL_S_ENGINE_INTERFACE_H
#ifdef __cplusplus
extern "C" {
#endif
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "s_engine_types.h"
#include "s_engine_module.h"

void cfl_s_engine_module_check(cfl_runtime_handle_t *handle);

void cfl_initialize_s_engine(cfl_runtime_handle_t *handle, 
    const s_expr_module_def_t* const* registry,
    int registry_count);
    
#ifdef __cplusplus
}
#endif
#endif