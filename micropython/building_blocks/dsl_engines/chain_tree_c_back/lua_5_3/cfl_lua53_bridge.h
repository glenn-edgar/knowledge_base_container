/*
 * cfl_lua53_bridge.h - Lua 5.3+ bridge for ChainTree CFL engine
 *
 * Provides trampoline-based dispatch so that CFL user functions
 * (main, one-shot, boolean) can be implemented in Lua instead of C.
 *
 * Built-in CFL functions remain in C (runtime_functions library).
 * Only user/application functions go through the bridge.
 *
 * Pattern: at registration time each Lua function gets a unique
 * trampoline slot. The trampoline extracts lua_State* from the
 * runtime handle's user_handle, looks up the Lua function by
 * registry reference, and calls it.
 */

#ifndef CFL_LUA53_BRIDGE_H
#define CFL_LUA53_BRIDGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include "cfl_engine.h"
#include "cfl_image_loader.h"

/* Maximum number of Lua-implemented functions per type */
#define CFL_LUA_MAX_MAIN     32
#define CFL_LUA_MAX_ONESHOT  32
#define CFL_LUA_MAX_BOOLEAN  32

/* ========================================================================
 * Initialization / Teardown
 * ======================================================================== */

/*
 * Initialize the bridge. Creates Lua-side helper module "cfl" with
 * runtime accessor functions (json extraction, blackboard, events).
 * Call once after luaL_openlibs().
 */
void cfl_lua_bridge_init(lua_State *L);

/*
 * Attach lua_State to a runtime handle (stores in user_handle).
 * Must be called after cfl_runtime_create() and before cfl_runtime_run().
 */
void cfl_lua_bridge_attach(cfl_runtime_handle_t *handle, lua_State *L);

/* ========================================================================
 * Function Registration
 *
 * Each function expects the Lua function on top of the stack.
 * It pops the function and stores a registry reference.
 * Returns 0 on success, negative on error.
 * ======================================================================== */

int cfl_lua_bridge_register_main(
    cfl_image_loader_t *img, const char *name, lua_State *L);

int cfl_lua_bridge_register_one_shot(
    cfl_image_loader_t *img, const char *name, lua_State *L);

int cfl_lua_bridge_register_boolean(
    cfl_image_loader_t *img, const char *name, lua_State *L);

/* ========================================================================
 * Convenience: register by loading a named global function
 * ======================================================================== */

static inline int cfl_lua_bridge_register_main_global(
    cfl_image_loader_t *img, const char *cfl_name,
    const char *lua_name, lua_State *L)
{
    lua_getglobal(L, lua_name);
    if (!lua_isfunction(L, -1)) { lua_pop(L, 1); return -1; }
    return cfl_lua_bridge_register_main(img, cfl_name, L);
}

static inline int cfl_lua_bridge_register_oneshot_global(
    cfl_image_loader_t *img, const char *cfl_name,
    const char *lua_name, lua_State *L)
{
    lua_getglobal(L, lua_name);
    if (!lua_isfunction(L, -1)) { lua_pop(L, 1); return -1; }
    return cfl_lua_bridge_register_one_shot(img, cfl_name, L);
}

static inline int cfl_lua_bridge_register_boolean_global(
    cfl_image_loader_t *img, const char *cfl_name,
    const char *lua_name, lua_State *L)
{
    lua_getglobal(L, lua_name);
    if (!lua_isfunction(L, -1)) { lua_pop(L, 1); return -1; }
    return cfl_lua_bridge_register_boolean(img, cfl_name, L);
}

#ifdef __cplusplus
}
#endif

#endif /* CFL_LUA53_BRIDGE_H */
