/*
 * cfl_mp_bridge.c - MicroPython bridge for ChainTree CFL engine
 *
 * Same trampoline-per-slot architecture as cfl_lua53_bridge.c.
 * Registers as MicroPython module "cfl" with:
 *   - Runtime accessor functions (json, blackboard, arena)
 *   - Constants (CONTINUE, HALT, event IDs)
 *   - register_one_shot/main/boolean for Python user functions
 */

#include "py/runtime.h"
#include "py/obj.h"
#include "py/objstr.h"
#include "py/misc.h"
#include "py/nlr.h"

#include "cfl_mp_bridge.h"
#include "cfl_runtime.h"
#include "cfl_file_loader.h"
#include "cfl_function_loader.h"
#include "cfl_common_functions.h"
#include "cfl_blackboard.h"
#include "cfl_exception_support.h"
#include "json_node_decoder.h"
#include <string.h>
#include <stdio.h>

/* ========================================================================
 * Bridge State
 * ======================================================================== */

static mp_obj_t g_main_funcs[CFL_MP_MAX_MAIN];
static int g_main_count = 0;

static mp_obj_t g_os_funcs[CFL_MP_MAX_ONESHOT];
static int g_os_count = 0;

static mp_obj_t g_bool_funcs[CFL_MP_MAX_BOOLEAN];
static int g_bool_count = 0;

static cfl_runtime_handle_t *g_runtime = NULL;

/* ========================================================================
 * Dispatch Functions
 * ======================================================================== */

static unsigned cfl_mp_main_dispatch(
    int slot, void *handle, unsigned bool_fn_idx,
    unsigned node_index, unsigned event_type,
    unsigned event_id, void *event_data)
{
    nlr_buf_t nlr;
    if (nlr_push(&nlr) == 0) {
        mp_obj_t args[6] = {
            mp_obj_new_int_from_uint((uintptr_t)handle),
            mp_obj_new_int(bool_fn_idx),
            mp_obj_new_int(node_index),
            mp_obj_new_int(event_type),
            mp_obj_new_int(event_id),
            mp_obj_new_int_from_uint((uintptr_t)event_data)
        };
        mp_obj_t result = mp_call_function_n_kw(g_main_funcs[slot], 6, 0, args);
        nlr_pop();
        return (unsigned)mp_obj_get_int(result);
    } else {
        mp_obj_print_exception(&mp_plat_print, MP_OBJ_FROM_PTR(nlr.ret_val));
        return CFL_HALT;
    }
}

static void cfl_mp_os_dispatch(int slot, void *handle, unsigned node_index)
{
    nlr_buf_t nlr;
    if (nlr_push(&nlr) == 0) {
        mp_obj_t args[2] = {
            mp_obj_new_int_from_uint((uintptr_t)handle),
            mp_obj_new_int(node_index)
        };
        mp_call_function_n_kw(g_os_funcs[slot], 2, 0, args);
        nlr_pop();
    } else {
        mp_obj_print_exception(&mp_plat_print, MP_OBJ_FROM_PTR(nlr.ret_val));
    }
}

static bool cfl_mp_bool_dispatch(
    int slot, void *handle, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    nlr_buf_t nlr;
    if (nlr_push(&nlr) == 0) {
        mp_obj_t args[5] = {
            mp_obj_new_int_from_uint((uintptr_t)handle),
            mp_obj_new_int(node_index),
            mp_obj_new_int(event_type),
            mp_obj_new_int(event_id),
            mp_obj_new_int_from_uint((uintptr_t)event_data)
        };
        mp_obj_t result = mp_call_function_n_kw(g_bool_funcs[slot], 5, 0, args);
        nlr_pop();
        return mp_obj_is_true(result);
    } else {
        mp_obj_print_exception(&mp_plat_print, MP_OBJ_FROM_PTR(nlr.ret_val));
        return false;
    }
}

/* ========================================================================
 * Trampoline Generation (same X-Macro as Lua bridge)
 * ======================================================================== */

#define SLOT_LIST \
    X(0)  X(1)  X(2)  X(3)  X(4)  X(5)  X(6)  X(7)  \
    X(8)  X(9)  X(10) X(11) X(12) X(13) X(14) X(15) \
    X(16) X(17) X(18) X(19) X(20) X(21) X(22) X(23) \
    X(24) X(25) X(26) X(27) X(28) X(29) X(30) X(31)

#define X(n) \
static unsigned main_tramp_##n(void *h, unsigned b, unsigned ni, \
    unsigned et, unsigned ei, void *ed) { \
    return cfl_mp_main_dispatch(n, h, b, ni, et, ei, ed); \
}
SLOT_LIST
#undef X
#define X(n) main_tramp_##n,
static cfl_main_function_t g_main_tramp_table[] = { SLOT_LIST };
#undef X

#define X(n) \
static void os_tramp_##n(void *h, unsigned ni) { \
    cfl_mp_os_dispatch(n, h, ni); \
}
SLOT_LIST
#undef X
#define X(n) os_tramp_##n,
static cfl_one_shot_function_t g_os_tramp_table[] = { SLOT_LIST };
#undef X

#define X(n) \
static bool bool_tramp_##n(void *h, unsigned ni, \
    unsigned et, unsigned ei, void *ed) { \
    return cfl_mp_bool_dispatch(n, h, ni, et, ei, ed); \
}
SLOT_LIST
#undef X
#define X(n) bool_tramp_##n,
static cfl_boolean_function_t g_bool_tramp_table[] = { SLOT_LIST };
#undef X

/* ========================================================================
 * Registration API (C-side, called from main.c before running Python)
 * ======================================================================== */

int cfl_mp_bridge_register_main(
    cfl_image_loader_t *img, const char *name, mp_obj_t func)
{
    if (g_main_count >= CFL_MP_MAX_MAIN) return -1;
    int slot = g_main_count++;
    g_main_funcs[slot] = func;
    return cfl_image_register_main(img, name, g_main_tramp_table[slot]);
}

int cfl_mp_bridge_register_one_shot(
    cfl_image_loader_t *img, const char *name, mp_obj_t func)
{
    if (g_os_count >= CFL_MP_MAX_ONESHOT) return -1;
    int slot = g_os_count++;
    g_os_funcs[slot] = func;
    return cfl_image_register_one_shot(img, name, g_os_tramp_table[slot]);
}

int cfl_mp_bridge_register_boolean(
    cfl_image_loader_t *img, const char *name, mp_obj_t func)
{
    if (g_bool_count >= CFL_MP_MAX_BOOLEAN) return -1;
    int slot = g_bool_count++;
    g_bool_funcs[slot] = func;
    return cfl_image_register_boolean(img, name, g_bool_tramp_table[slot]);
}

/* ========================================================================
 * Python-callable functions exposed as "cfl" module
 * ======================================================================== */

/* cfl.json_extract_string(handle_int, node_index, path) -> str */
static mp_obj_t mp_json_extract_string(mp_obj_t h, mp_obj_t ni, mp_obj_t path) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    unsigned node_index = (unsigned)mp_obj_get_int(ni);
    const char *p = mp_obj_str_get_str(path);
    json_decoder_init_from_runtime(rt, node_index);
    const char *result = NULL;
    json_extract_string_runtime(rt, p, &result);
    if (result) return mp_obj_new_str(result, strlen(result));
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_json_extract_string_obj, mp_json_extract_string);

/* cfl.json_extract_int32(handle_int, node_index, path) -> int */
static mp_obj_t mp_json_extract_int32(mp_obj_t h, mp_obj_t ni, mp_obj_t path) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    unsigned node_index = (unsigned)mp_obj_get_int(ni);
    json_decoder_init_from_runtime(rt, node_index);
    int32_t result = 0;
    json_extract_int32_runtime(rt, mp_obj_str_get_str(path), &result);
    return mp_obj_new_int(result);
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_json_extract_int32_obj, mp_json_extract_int32);

/* cfl.json_extract_float(handle_int, node_index, path) -> float */
static mp_obj_t mp_json_extract_float(mp_obj_t h, mp_obj_t ni, mp_obj_t path) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    unsigned node_index = (unsigned)mp_obj_get_int(ni);
    json_decoder_init_from_runtime(rt, node_index);
    float result = 0.0f;
    json_extract_float32_runtime(rt, mp_obj_str_get_str(path), &result);
    return mp_obj_new_float((mp_float_t)result);
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_json_extract_float_obj, mp_json_extract_float);

/* cfl.json_extract_bool(handle_int, node_index, path) -> bool */
static mp_obj_t mp_json_extract_bool(mp_obj_t h, mp_obj_t ni, mp_obj_t path) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    unsigned node_index = (unsigned)mp_obj_get_int(ni);
    json_decoder_init_from_runtime(rt, node_index);
    bool result = false;
    json_extract_bool_runtime(rt, mp_obj_str_get_str(path), &result);
    return mp_obj_new_bool(result);
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_json_extract_bool_obj, mp_json_extract_bool);

/* cfl.bb_get_int32(handle_int, field_name) -> int */
static mp_obj_t mp_bb_get_int32(mp_obj_t h, mp_obj_t field) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    return mp_obj_new_int(cfl_bb_get_int32(rt, cfl_bb_hash(mp_obj_str_get_str(field)), 0));
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_bb_get_int32_obj, mp_bb_get_int32);

/* cfl.bb_set_int32(handle_int, field_name, value) */
static mp_obj_t mp_bb_set_int32(mp_obj_t h, mp_obj_t field, mp_obj_t val) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    cfl_bb_set_int32(rt, cfl_bb_hash(mp_obj_str_get_str(field)), (int32_t)mp_obj_get_int(val));
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_bb_set_int32_obj, mp_bb_set_int32);

/* cfl.bb_get_float(handle_int, field_name) -> float */
static mp_obj_t mp_bb_get_float(mp_obj_t h, mp_obj_t field) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    return mp_obj_new_float(cfl_bb_get_float(rt, cfl_bb_hash(mp_obj_str_get_str(field)), 0.0f));
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_bb_get_float_obj, mp_bb_get_float);

/* cfl.bb_set_float(handle_int, field_name, value) */
static mp_obj_t mp_bb_set_float(mp_obj_t h, mp_obj_t field, mp_obj_t val) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    cfl_bb_set_float(rt, cfl_bb_hash(mp_obj_str_get_str(field)), (float)mp_obj_get_float(val));
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_bb_set_float_obj, mp_bb_set_float);

/* cfl.arena_get(handle_int, node_index) -> int (ptr as int) or None */
static mp_obj_t mp_arena_get(mp_obj_t h, mp_obj_t ni) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    void *ptr = cfl_heap_arena_get_node_ptr(rt->arena_system, (unsigned)mp_obj_get_int(ni));
    if (ptr) return mp_obj_new_int_from_uint((uintptr_t)ptr);
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_arena_get_obj, mp_arena_get);

/* cfl.smart_alloc(handle_int, node_index, size) -> int (ptr) */
static mp_obj_t mp_smart_alloc(mp_obj_t h, mp_obj_t ni, mp_obj_t sz) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    void *ptr = cfl_smart_arena_alloc(rt, (unsigned)mp_obj_get_int(ni), (uint16_t)mp_obj_get_int(sz));
    if (ptr) return mp_obj_new_int_from_uint((uintptr_t)ptr);
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_smart_alloc_obj, mp_smart_alloc);

/* cfl.heap_alloc(handle_int, size) -> int (ptr) */
static mp_obj_t mp_heap_alloc(mp_obj_t h, mp_obj_t sz) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    void *ptr = cfl_heap_malloc_pointer(rt->heap, (uint16_t)mp_obj_get_int(sz));
    if (ptr) return mp_obj_new_int_from_uint((uintptr_t)ptr);
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_heap_alloc_obj, mp_heap_alloc);

/* cfl.heap_free(handle_int, ptr_int) */
static mp_obj_t mp_heap_free(mp_obj_t h, mp_obj_t p) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    void *ptr = (void *)(uintptr_t)mp_obj_get_int(p);
    if (ptr) cfl_heap_free_pointer(rt->heap, ptr);
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_heap_free_obj, mp_heap_free);

/* cfl.read_i32(ptr_int, offset) -> int */
static mp_obj_t mp_read_i32(mp_obj_t p, mp_obj_t off) {
    void *ptr = (void *)(uintptr_t)mp_obj_get_int(p);
    int offset = mp_obj_get_int(off);
    int32_t val;
    memcpy(&val, (uint8_t *)ptr + offset, sizeof(int32_t));
    return mp_obj_new_int(val);
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_read_i32_obj, mp_read_i32);

/* cfl.write_i32(ptr_int, offset, value) */
static mp_obj_t mp_write_i32(mp_obj_t p, mp_obj_t off, mp_obj_t val) {
    void *ptr = (void *)(uintptr_t)mp_obj_get_int(p);
    int offset = mp_obj_get_int(off);
    int32_t v = (int32_t)mp_obj_get_int(val);
    memcpy((uint8_t *)ptr + offset, &v, sizeof(int32_t));
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_write_i32_obj, mp_write_i32);

/* cfl.read_u16(ptr_int, offset) -> int */
static mp_obj_t mp_read_u16(mp_obj_t p, mp_obj_t off) {
    void *ptr = (void *)(uintptr_t)mp_obj_get_int(p);
    int offset = mp_obj_get_int(off);
    uint16_t val;
    memcpy(&val, (uint8_t *)ptr + offset, sizeof(uint16_t));
    return mp_obj_new_int(val);
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_read_u16_obj, mp_read_u16);

/* cfl.read_ptr(ptr_int, offset) -> int (ptr) or None */
static mp_obj_t mp_read_ptr(mp_obj_t p, mp_obj_t off) {
    void *ptr = (void *)(uintptr_t)mp_obj_get_int(p);
    int offset = mp_obj_get_int(off);
    void *val;
    memcpy(&val, (uint8_t *)ptr + offset, sizeof(void *));
    if (val) return mp_obj_new_int_from_uint((uintptr_t)val);
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_read_ptr_obj, mp_read_ptr);

/* cfl.write_ptr(ptr_int, offset, value_ptr_int) */
static mp_obj_t mp_write_ptr(mp_obj_t p, mp_obj_t off, mp_obj_t val) {
    void *ptr = (void *)(uintptr_t)mp_obj_get_int(p);
    int offset = mp_obj_get_int(off);
    void *v = (void *)(uintptr_t)mp_obj_get_int(val);
    memcpy((uint8_t *)ptr + offset, &v, sizeof(void *));
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_3(mp_write_ptr_obj, mp_write_ptr);

/* cfl.get_event_index(handle_int, event_name) -> int */
static mp_obj_t mp_get_event_index(mp_obj_t h, mp_obj_t name) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    return mp_obj_new_int(ct_get_event_index(rt->flash_handle, mp_obj_str_get_str(name)));
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_get_event_index_obj, mp_get_event_index);

/* cfl.get_node_parent(handle_int, node_index) -> int */
static mp_obj_t mp_get_node_parent(mp_obj_t h, mp_obj_t ni) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    unsigned node_index = (unsigned)mp_obj_get_int(ni);
    return mp_obj_new_int(rt->flash_handle->nodes[node_index].parent_index);
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_get_node_parent_obj, mp_get_node_parent);

/* cfl.event_data_to_u16(event_data_int) -> int */
static mp_obj_t mp_event_data_to_u16(mp_obj_t ed) {
    return mp_obj_new_int((uint16_t)(uintptr_t)mp_obj_get_int(ed));
}
static MP_DEFINE_CONST_FUN_OBJ_1(mp_event_data_to_u16_obj, mp_event_data_to_u16);

/* ========================================================================
 * High-Level Python API: full lifecycle from Python
 * ======================================================================== */

static cfl_image_loader_t g_img;
static cfl_perm_t g_perm;
static char g_perm_buffer[0xffff];
static bool g_image_loaded = false;

/* cfl.load_embedded_image(bytes_obj) -> bool */
static mp_obj_t mp_load_embedded_image(mp_obj_t data_obj) {
    mp_buffer_info_t buf;
    mp_get_buffer_raise(data_obj, &buf, MP_BUFFER_READ);
    int rc = cfl_image_load(buf.buf, buf.len, &g_img);
    if (rc != CFL_IMAGE_OK) return mp_const_false;
    g_image_loaded = true;
    cfl_register_all_functions(&g_img);
    return mp_const_true;
}
static MP_DEFINE_CONST_FUN_OBJ_1(mp_load_embedded_image_obj, mp_load_embedded_image);

/* cfl.register_one_shot(name, func) */
static mp_obj_t mp_register_one_shot(mp_obj_t name_obj, mp_obj_t func_obj) {
    const char *name = mp_obj_str_get_str(name_obj);
    int rc = cfl_mp_bridge_register_one_shot(&g_img, name, func_obj);
    return mp_obj_new_int(rc);
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_register_one_shot_obj, mp_register_one_shot);

/* cfl.register_main(name, func) */
static mp_obj_t mp_register_main(mp_obj_t name_obj, mp_obj_t func_obj) {
    const char *name = mp_obj_str_get_str(name_obj);
    int rc = cfl_mp_bridge_register_main(&g_img, name, func_obj);
    return mp_obj_new_int(rc);
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_register_main_obj, mp_register_main);

/* cfl.register_boolean(name, func) */
static mp_obj_t mp_register_boolean(mp_obj_t name_obj, mp_obj_t func_obj) {
    const char *name = mp_obj_str_get_str(name_obj);
    int rc = cfl_mp_bridge_register_boolean(&g_img, name, func_obj);
    return mp_obj_new_int(rc);
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_register_boolean_obj, mp_register_boolean);

/* cfl.validate() -> int (missing count) */
static mp_obj_t mp_validate(void) {
    return mp_obj_new_int(cfl_image_validate(&g_img));
}
static MP_DEFINE_CONST_FUN_OBJ_0(mp_validate_obj, mp_validate);

/* cfl.create_runtime() -> handle_int */
static mp_obj_t mp_create_runtime(void) {
    const cfl_chaintree_handle_t *handle = cfl_image_get_handle(&g_img);
    if (!handle) return mp_const_none;

    cfl_runtime_create_params_t *params = cfl_runtime_create_params_create();
    params->perm = &g_perm;
    params->perm_buffer = g_perm_buffer;
    params->perm_buffer_size = (uint16_t)sizeof(g_perm_buffer);
    params->heap_size = 4096;
    params->max_allocator_count = cfl_calculate_arrena_number(handle);
    params->total_node_count = handle->node_count;
    params->allocator_0_size = 50;
    params->event_queue_high_priority_size = 8;
    params->event_queue_low_priority_size = 64;
    params->delta_time = 0.1;

    g_runtime = cfl_runtime_create(&g_perm, params, handle);
    cfl_runtime_create_params_destroy(params);
    if (!g_runtime) return mp_const_none;

    cfl_runtime_reset(g_runtime);
    return mp_obj_new_int_from_uint((uintptr_t)g_runtime);
}
static MP_DEFINE_CONST_FUN_OBJ_0(mp_create_runtime_obj, mp_create_runtime);

/* cfl.add_test(handle_int, test_index) */
static mp_obj_t mp_add_test(mp_obj_t h, mp_obj_t idx) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    cfl_add_test_by_index(rt, (unsigned)mp_obj_get_int(idx));
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_2(mp_add_test_obj, mp_add_test);

/* cfl.run(handle_int) -> bool */
static mp_obj_t mp_run(mp_obj_t h) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)(uintptr_t)mp_obj_get_int(h);
    bool result = cfl_runtime_run(rt);
    return mp_obj_new_bool(result);
}
static MP_DEFINE_CONST_FUN_OBJ_1(mp_run_obj, mp_run);

/* cfl.cleanup() */
static mp_obj_t mp_cleanup(void) {
    if (g_image_loaded) { cfl_image_free(&g_img); g_image_loaded = false; }
    g_runtime = NULL;
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_0(mp_cleanup_obj, mp_cleanup);

/* cfl.get_node_count() -> int */
static mp_obj_t mp_get_node_count(void) {
    const cfl_chaintree_handle_t *h = cfl_image_get_handle(&g_img);
    return mp_obj_new_int(h ? h->node_count : 0);
}
static MP_DEFINE_CONST_FUN_OBJ_0(mp_get_node_count_obj, mp_get_node_count);

/* cfl.get_kb_count() -> int */
static mp_obj_t mp_get_kb_count(void) {
    const cfl_chaintree_handle_t *h = cfl_image_get_handle(&g_img);
    return mp_obj_new_int(h ? h->kb_count : 0);
}
static MP_DEFINE_CONST_FUN_OBJ_0(mp_get_kb_count_obj, mp_get_kb_count);

/* ========================================================================
 * Module Definition
 * ======================================================================== */

static const mp_rom_map_elem_t cfl_module_globals_table[] = {
    { MP_ROM_QSTR(MP_QSTR___name__),            MP_ROM_QSTR(MP_QSTR_cfl) },

    /* JSON extraction */
    { MP_ROM_QSTR(MP_QSTR_json_extract_string),  MP_ROM_PTR(&mp_json_extract_string_obj) },
    { MP_ROM_QSTR(MP_QSTR_json_extract_int32),   MP_ROM_PTR(&mp_json_extract_int32_obj) },
    { MP_ROM_QSTR(MP_QSTR_json_extract_float),   MP_ROM_PTR(&mp_json_extract_float_obj) },
    { MP_ROM_QSTR(MP_QSTR_json_extract_bool),    MP_ROM_PTR(&mp_json_extract_bool_obj) },

    /* Blackboard */
    { MP_ROM_QSTR(MP_QSTR_bb_get_int32),         MP_ROM_PTR(&mp_bb_get_int32_obj) },
    { MP_ROM_QSTR(MP_QSTR_bb_set_int32),         MP_ROM_PTR(&mp_bb_set_int32_obj) },
    { MP_ROM_QSTR(MP_QSTR_bb_get_float),         MP_ROM_PTR(&mp_bb_get_float_obj) },
    { MP_ROM_QSTR(MP_QSTR_bb_set_float),         MP_ROM_PTR(&mp_bb_set_float_obj) },

    /* Arena / heap */
    { MP_ROM_QSTR(MP_QSTR_arena_get),            MP_ROM_PTR(&mp_arena_get_obj) },
    { MP_ROM_QSTR(MP_QSTR_smart_alloc),          MP_ROM_PTR(&mp_smart_alloc_obj) },
    { MP_ROM_QSTR(MP_QSTR_heap_alloc),           MP_ROM_PTR(&mp_heap_alloc_obj) },
    { MP_ROM_QSTR(MP_QSTR_heap_free),            MP_ROM_PTR(&mp_heap_free_obj) },

    /* Raw memory */
    { MP_ROM_QSTR(MP_QSTR_read_i32),             MP_ROM_PTR(&mp_read_i32_obj) },
    { MP_ROM_QSTR(MP_QSTR_write_i32),            MP_ROM_PTR(&mp_write_i32_obj) },
    { MP_ROM_QSTR(MP_QSTR_read_u16),             MP_ROM_PTR(&mp_read_u16_obj) },
    { MP_ROM_QSTR(MP_QSTR_read_ptr),             MP_ROM_PTR(&mp_read_ptr_obj) },
    { MP_ROM_QSTR(MP_QSTR_write_ptr),            MP_ROM_PTR(&mp_write_ptr_obj) },

    /* Runtime queries */
    { MP_ROM_QSTR(MP_QSTR_get_event_index),      MP_ROM_PTR(&mp_get_event_index_obj) },
    { MP_ROM_QSTR(MP_QSTR_get_node_parent),      MP_ROM_PTR(&mp_get_node_parent_obj) },
    { MP_ROM_QSTR(MP_QSTR_event_data_to_u16),    MP_ROM_PTR(&mp_event_data_to_u16_obj) },

    /* Lifecycle */
    { MP_ROM_QSTR(MP_QSTR_load_embedded_image), MP_ROM_PTR(&mp_load_embedded_image_obj) },
    { MP_ROM_QSTR(MP_QSTR_register_one_shot),   MP_ROM_PTR(&mp_register_one_shot_obj) },
    { MP_ROM_QSTR(MP_QSTR_register_main),       MP_ROM_PTR(&mp_register_main_obj) },
    { MP_ROM_QSTR(MP_QSTR_register_boolean),     MP_ROM_PTR(&mp_register_boolean_obj) },
    { MP_ROM_QSTR(MP_QSTR_validate),             MP_ROM_PTR(&mp_validate_obj) },
    { MP_ROM_QSTR(MP_QSTR_create_runtime),       MP_ROM_PTR(&mp_create_runtime_obj) },
    { MP_ROM_QSTR(MP_QSTR_add_test),             MP_ROM_PTR(&mp_add_test_obj) },
    { MP_ROM_QSTR(MP_QSTR_run),                  MP_ROM_PTR(&mp_run_obj) },
    { MP_ROM_QSTR(MP_QSTR_cleanup),              MP_ROM_PTR(&mp_cleanup_obj) },
    { MP_ROM_QSTR(MP_QSTR_get_node_count),       MP_ROM_PTR(&mp_get_node_count_obj) },
    { MP_ROM_QSTR(MP_QSTR_get_kb_count),         MP_ROM_PTR(&mp_get_kb_count_obj) },

    /* Return code constants */
    { MP_ROM_QSTR(MP_QSTR_CONTINUE),             MP_ROM_INT(CFL_CONTINUE) },
    { MP_ROM_QSTR(MP_QSTR_HALT),                 MP_ROM_INT(CFL_HALT) },
    { MP_ROM_QSTR(MP_QSTR_TERMINATE),            MP_ROM_INT(CFL_TERMINATE) },
    { MP_ROM_QSTR(MP_QSTR_RESET),                MP_ROM_INT(CFL_RESET) },
    { MP_ROM_QSTR(MP_QSTR_DISABLE),              MP_ROM_INT(CFL_DISABLE) },
    { MP_ROM_QSTR(MP_QSTR_SKIP_CONTINUE),        MP_ROM_INT(CFL_SKIP_CONTINUE) },

    /* Event constants */
    { MP_ROM_QSTR(MP_QSTR_INIT_EVENT),            MP_ROM_INT(CFL_INIT_EVENT) },
    { MP_ROM_QSTR(MP_QSTR_TERMINATE_EVENT),       MP_ROM_INT(CFL_TERMINATE_EVENT) },
    { MP_ROM_QSTR(MP_QSTR_START_TESTS),           MP_ROM_INT(CFL_START_TESTS) },
    { MP_ROM_QSTR(MP_QSTR_RAISE_EXCEPTION_EVENT), MP_ROM_INT(CFL_RAISE_EXCEPTION_EVENT) },
    { MP_ROM_QSTR(MP_QSTR_CHANGE_STATE_EVENT),    MP_ROM_INT(CFL_CHANGE_STATE_EVENT) },
    { MP_ROM_QSTR(MP_QSTR_RECOVERY_CHECK_EVENT),  MP_ROM_INT(CFL_RECOVERY_CHECK_EVENT) },
    { MP_ROM_QSTR(MP_QSTR_RECOVERY_SEQ_EVAL),     MP_ROM_INT(CFL_RECOVERY_SEQ_EVAL) },
};
static MP_DEFINE_CONST_DICT(cfl_module_globals, cfl_module_globals_table);

const mp_obj_module_t cfl_module = {
    .base = { &mp_type_module },
    .globals = (mp_obj_dict_t *)&cfl_module_globals,
};

MP_REGISTER_MODULE(MP_QSTR_cfl, cfl_module);

/* ========================================================================
 * C-side init
 * ======================================================================== */

void cfl_mp_bridge_init(void) {
    g_main_count = 0;
    g_os_count = 0;
    g_bool_count = 0;
    memset(g_main_funcs, 0, sizeof(g_main_funcs));
    memset(g_os_funcs, 0, sizeof(g_os_funcs));
    memset(g_bool_funcs, 0, sizeof(g_bool_funcs));
    g_runtime = NULL;
}

void cfl_mp_bridge_set_runtime(cfl_runtime_handle_t *handle) {
    g_runtime = handle;
}
