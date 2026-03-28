/*
 * cfl_mp_bridge.h - MicroPython bridge for ChainTree CFL engine
 *
 * Same trampoline architecture as cfl_lua53_bridge but using
 * MicroPython's mp_obj_t API. Exposes "cfl" module to Python.
 */

#ifndef CFL_MP_BRIDGE_H
#define CFL_MP_BRIDGE_H

#include "py/runtime.h"
#include "py/obj.h"
#include "cfl_engine.h"
#include "cfl_image_loader.h"

#define CFL_MP_MAX_MAIN     32
#define CFL_MP_MAX_ONESHOT  32
#define CFL_MP_MAX_BOOLEAN  32

/* Initialize bridge (called from C before running Python) */
void cfl_mp_bridge_init(void);

/* Store runtime handle for Python access */
void cfl_mp_bridge_set_runtime(cfl_runtime_handle_t *handle);

/* Register Python callable as CFL function. obj must be callable. */
int cfl_mp_bridge_register_main(
    cfl_image_loader_t *img, const char *name, mp_obj_t func);
int cfl_mp_bridge_register_one_shot(
    cfl_image_loader_t *img, const char *name, mp_obj_t func);
int cfl_mp_bridge_register_boolean(
    cfl_image_loader_t *img, const char *name, mp_obj_t func);

#endif /* CFL_MP_BRIDGE_H */
