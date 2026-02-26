#ifndef CFL_S_ONE_SHOT_FUNCTIONS_H
#define CFL_S_ONE_SHOT_FUNCTIONS_H


#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "json_node_decoder.h"
#ifdef __cplusplus
extern "C" {
#endif

// System oneshot function table (exported for module loading)
void cfl_load_oneshot_s_functions(cfl_runtime_handle_t* handle);

#ifdef __cplusplus
}
#endif

#endif // CFL_S_ONE_SHOT_FUNCTIONS_H