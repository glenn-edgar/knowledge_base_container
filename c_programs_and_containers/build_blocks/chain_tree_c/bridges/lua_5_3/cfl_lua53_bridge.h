/*
 * cfl_lua53_bridge.h - Lua 5.3+ bridge for ChainTree CFL engine
 *
 * Trampoline-based dispatch so CFL user functions (main, one-shot,
 * boolean) can be implemented in Lua instead of C.
 *
 * Built-in CFL functions remain in C (runtime_functions library).
 * Only user/application functions go through the bridge.
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

/* Maximum Lua-implemented functions per type */
#define CFL_LUA_MAX_MAIN     32
#define CFL_LUA_MAX_ONESHOT  32
#define CFL_LUA_MAX_BOOLEAN  32

/* Initialize bridge. Creates "cfl" Lua module with runtime accessors. */
void cfl_lua_bridge_init(lua_State *L);

/* Store lua_State in runtime handle->user_handle. Call after cfl_runtime_create(). */
void cfl_lua_bridge_attach(cfl_runtime_handle_t *handle, lua_State *L);

/*
 * Register Lua function (on top of stack) with image loader.
 * Pops the function. Returns 0 on success, negative on error.
 */
int cfl_lua_bridge_register_main(
    cfl_image_loader_t *img, const char *name, lua_State *L);
int cfl_lua_bridge_register_one_shot(
    cfl_image_loader_t *img, const char *name, lua_State *L);
int cfl_lua_bridge_register_boolean(
    cfl_image_loader_t *img, const char *name, lua_State *L);

/* Convenience: register a named global Lua function */
static inline int cfl_lua_bridge_register_oneshot_global(
    cfl_image_loader_t *img, const char *cfl_name,
    const char *lua_name, lua_State *L)
{
    lua_getglobal(L, lua_name);
    if (!lua_isfunction(L, -1)) { lua_pop(L, 1); return -1; }
    return cfl_lua_bridge_register_one_shot(img, cfl_name, L);
}

static inline int cfl_lua_bridge_register_main_global(
    cfl_image_loader_t *img, const char *cfl_name,
    const char *lua_name, lua_State *L)
{
    lua_getglobal(L, lua_name);
    if (!lua_isfunction(L, -1)) { lua_pop(L, 1); return -1; }
    return cfl_lua_bridge_register_main(img, cfl_name, L);
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
