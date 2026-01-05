#ifndef CFL_S_MAIN_FUNCTIONS_H
#define CFL_S_MAIN_FUNCTIONS_H

#ifdef __cplusplus
extern "C" {
#endif
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "json_node_decoder.h"

void cfl_load_main_s_functions(cfl_runtime_handle_t* handle);


#ifdef __cplusplus  
}
#endif

#endif