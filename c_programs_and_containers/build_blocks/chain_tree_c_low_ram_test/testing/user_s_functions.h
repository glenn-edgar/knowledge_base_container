#ifndef USER_S_FUNCTIONS_H
#define USER_S_FUNCTIONS_H

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "cfl_common_function_headers.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Load user functions into module (call after cfl_initialize_s_engine, before cfl_s_engine_module_check)
void load_user_s_functions(cfl_runtime_handle_t* handle);

#ifdef __cplusplus
}
#endif

#endif // USER_S_FUNCTIONS_H