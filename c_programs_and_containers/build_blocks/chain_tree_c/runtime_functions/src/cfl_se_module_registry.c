#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include "cfl_runtime.h"
#include "cfl_blackboard.h"
#include "cfl_common_functions.h"
#include "cfl_fnv1a.h"
#include "json_node_decoder.h"
#include "cfl_se_module_registry.h"
#include "s_engine_builtins.h"
#include "cfl_se_oneshot_functions.h"
#include "cfl_se_main_functions.h"
#include "cfl_se_pred_functions.h"
#include "s_engine_event_queue.h"

/* ====================================================================
 * Allocator bridge: cfl_runtime_handle_t → s_expr_allocator_t
 *
 * ctx = cfl_runtime_handle_t* so that:
 *   - malloc/free use rt->heap
 *   - get_time uses rt->timer_handle
 *   - cfl bridge functions can retrieve rt via s_expr_tree_get_user_ctx()
 * ==================================================================== */

static void *se_heap_malloc(void *ctx, size_t size) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)ctx;
    return cfl_heap_malloc_pointer(rt->heap, (uint16_t)size);
}

static void se_heap_free(void *ctx, void *ptr) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)ctx;
    cfl_heap_free_pointer(rt->heap, ptr);
}

static double se_get_time(void *ctx) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)ctx;
    return cfl_timer_get_timestamp(rt->timer_handle);
}

static s_expr_allocator_t cfl_se_make_allocator(cfl_runtime_handle_t *rt) {
    return (s_expr_allocator_t){
        .malloc   = se_heap_malloc,
        .free     = se_heap_free,
        .ctx      = rt,
        .get_time = se_get_time
    };
}

/* ====================================================================
 * Registry lifecycle
 * ==================================================================== */

cfl_se_module_registry_t *cfl_se_registry_create(cfl_runtime_handle_t *rt) {
    cfl_se_module_registry_t *reg =
        cfl_heap_malloc_pointer(rt->heap, (uint16_t)sizeof(cfl_se_module_registry_t));
    if (!reg) {
        EXCEPTION("cfl_se_registry_create: failed to allocate registry");
        return NULL;
    }
    memset(reg, 0, sizeof(*reg));
    reg->rt = rt;
    return reg;
}

void cfl_se_registry_destroy(cfl_se_module_registry_t *reg) {
    if (!reg) return;
    for (uint8_t i = 0; i < CFL_SE_MAX_MODULES; i++) {
        if (reg->slots[i].name_hash != 0) {
            s_expr_module_free(&reg->slots[i].module);
            reg->slots[i].name_hash = 0;
        }
    }
    cfl_heap_free_pointer(reg->rt->heap, reg);
}

/* ====================================================================
 * Module definition registry (app registers before engine starts)
 * ==================================================================== */

void cfl_se_registry_register_def(
    cfl_se_module_registry_t *reg,
    const char *module_name,
    const uint8_t *binary_data,
    size_t binary_size)
{
    cfl_se_registry_register_def_with_user(reg, module_name,
        binary_data, binary_size, NULL, NULL);
}

void cfl_se_registry_register_def_with_user(
    cfl_se_module_registry_t *reg,
    const char *module_name,
    const uint8_t *binary_data,
    size_t binary_size,
    cfl_se_user_register_fn_t user_register,
    void *user_ctx)
{
    if (!reg || !module_name || !binary_data || binary_size == 0) {
        EXCEPTION("cfl_se_registry_register_def: NULL argument");
    }
    if (reg->def_count >= CFL_SE_MAX_MODULES) {
        EXCEPTION("cfl_se_registry_register_def: def table full");
    }
    uint32_t hash = cfl_fnv1a_32(module_name);
    reg->defs[reg->def_count].name_hash = hash;
    reg->defs[reg->def_count].binary_data = binary_data;
    reg->defs[reg->def_count].binary_size = binary_size;
    reg->defs[reg->def_count].user_register = user_register;
    reg->defs[reg->def_count].user_ctx = user_ctx;
    reg->def_count++;
}

const cfl_se_module_def_entry_t *cfl_se_registry_find_def(
    cfl_se_module_registry_t *reg, uint32_t name_hash)
{
    if (!reg) return NULL;
    for (uint8_t i = 0; i < reg->def_count; i++) {
        if (reg->defs[i].name_hash == name_hash)
            return &reg->defs[i];
    }
    return NULL;
}

/* ====================================================================
 * Module load / unload / lookup
 * ==================================================================== */

s_expr_module_t *cfl_se_registry_find(cfl_se_module_registry_t *reg, uint32_t name_hash) {
    if (!reg || name_hash == 0) return NULL;
    for (uint8_t i = 0; i < CFL_SE_MAX_MODULES; i++) {
        if (reg->slots[i].name_hash == name_hash)
            return &reg->slots[i].module;
    }
    return NULL;
}

s_expr_module_t *cfl_se_registry_load(
    cfl_se_module_registry_t *reg,
    const s_expr_module_def_t *def,
    const cfl_se_fn_table_set_t *layers,
    uint8_t layer_count,
    cfl_se_user_register_fn_t user_register,
    void *user_ctx)
{
    if (!reg || !def) return NULL;

    /* Reject duplicate */
    if (cfl_se_registry_find(reg, def->name_hash)) {
        EXCEPTION("cfl_se_registry_load: module already loaded");
        return NULL;
    }

    /* Find empty slot */
    cfl_se_module_slot_t *slot = NULL;
    for (uint8_t i = 0; i < CFL_SE_MAX_MODULES; i++) {
        if (reg->slots[i].name_hash == 0) {
            slot = &reg->slots[i];
            break;
        }
    }
    if (!slot) {
        EXCEPTION("cfl_se_registry_load: registry full");
        return NULL;
    }

    s_expr_allocator_t alloc = cfl_se_make_allocator(reg->rt);
    uint8_t err = s_expr_module_init(&slot->module, def, alloc);
    if (err != S_EXPR_ERR_OK) {
        EXCEPTION("cfl_se_registry_load: s_expr_module_init failed");
        return NULL;
    }

    /* Register function tables in layer order: builtins → cfl */
    for (uint8_t i = 0; i < layer_count; i++) {
        if (layers[i].main_tbl)
            s_expr_module_register_main(&slot->module, layers[i].main_tbl);
        if (layers[i].oneshot_tbl)
            s_expr_module_register_oneshot(&slot->module, layers[i].oneshot_tbl);
        if (layers[i].pred_tbl)
            s_expr_module_register_pred(&slot->module, layers[i].pred_tbl);
    }

    /* User registration callback — registers user function tables */
    if (user_register) {
        user_register(&slot->module, user_ctx);
    }

    err = s_expr_module_validate(&slot->module);
    if (err != S_EXPR_ERR_OK) {
        EXCEPTION("cfl_se_registry_load: FATAL module validation failed");
        printf("  error: %s, hash=0x%08X, index=%u\n",
               s_expr_error_str(err),
               (uint32_t)slot->module.error_hash,
               slot->module.error_index);
        s_expr_module_free(&slot->module);
        memset(slot, 0, sizeof(*slot));
        return NULL;
    }

    slot->name_hash = def->name_hash;
    reg->count++;
    return &slot->module;
}

void cfl_se_registry_unload(cfl_se_module_registry_t *reg, uint32_t name_hash) {
    if (!reg || name_hash == 0) return;
    for (uint8_t i = 0; i < CFL_SE_MAX_MODULES; i++) {
        if (reg->slots[i].name_hash == name_hash) {
            s_expr_module_free(&reg->slots[i].module);
            memset(&reg->slots[i], 0, sizeof(cfl_se_module_slot_t));
            reg->count--;
            return;
        }
    }
}

/* ====================================================================
 * ChainTree node functions for module load/unload
 * ==================================================================== */

void cfl_se_module_load_init_one_shot_fn(void *handle, uint16_t node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_se_module_registry_t *reg =
        (cfl_se_module_registry_t *)cfl_get_app_extensions(rt);

    if (!reg) {
        EXCEPTION("cfl_se_module_load_init: no registry in app_extensions");
    }

    /* Allocate arena for node data */
    bool already = cfl_allocate_state(rt, node_index);
    cfl_se_load_node_data_t *nd;
    if (already) {
        nd = (cfl_se_load_node_data_t *)
            cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    } else {
        nd = (cfl_se_load_node_data_t *)
            cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_se_load_node_data_t));
    }
    if (!nd) {
        EXCEPTION("cfl_se_module_load_init: arena alloc failed");
    }
    memset(nd, 0, sizeof(*nd));

    /* Decode module_name from JSON node data */
    const char *module_name = NULL;
    json_decoder_init_from_runtime(rt, node_index);
    json_extract_string_runtime(rt, "node_dict.module_name", &module_name);
    if (!module_name) {
        EXCEPTION("cfl_se_module_load_init: module_name not found in node data");
    }

    /* Resolve module name → binary via the def registry */
    uint32_t name_hash = cfl_fnv1a_32(module_name);
    const cfl_se_module_def_entry_t *def_entry = cfl_se_registry_find_def(reg, name_hash);
    if (!def_entry) {
        printf("  module_name='%s' hash=0x%08X\n", module_name, name_hash);
        EXCEPTION("cfl_se_module_load_init: module binary not registered");
    }

    /* Parse the binary into a loaded module (zero-copy from ROM) */
    s_expr_allocator_t alloc = cfl_se_make_allocator(reg->rt);
    s_expr_loaded_module_t *loaded = s_expr_load_from_rom(
        def_entry->binary_data, def_entry->binary_size, alloc);
    if (!loaded) {
        EXCEPTION("cfl_se_module_load_init: binary parse failed");
    }
    nd->loaded_module = loaded;

    /* Set up layers: builtins + cfl */
    cfl_se_fn_table_set_t layers[CFL_SE_MAX_FN_LAYERS];
    layers[0] = (cfl_se_fn_table_set_t){
        .main_tbl    = s_engine_builtin_main_table(),
        .oneshot_tbl = s_engine_builtin_oneshot_table(),
        .pred_tbl    = s_engine_builtin_pred_table(),
    };
    layers[1] = (cfl_se_fn_table_set_t){
        .main_tbl    = cfl_se_get_main_table(),
        .oneshot_tbl = cfl_se_get_oneshot_table(),
        .pred_tbl    = cfl_se_get_pred_table(),
    };

    /* Load the module into the registry */
    s_expr_module_t *mod = cfl_se_registry_load(
        reg, &loaded->def, layers, CFL_SE_MAX_FN_LAYERS, NULL, NULL);
    if (!mod) {
        EXCEPTION("cfl_se_module_load_init: module load failed");
    }

    nd->name_hash = loaded->def.name_hash;
    nd->loaded = true;
}

unsigned cfl_se_module_load_main_fn(
    void *handle, unsigned bool_function_index, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return CFL_CONTINUE;
}

void cfl_se_module_load_term_one_shot_fn(void *handle, uint16_t node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_se_module_registry_t *reg =
        (cfl_se_module_registry_t *)cfl_get_app_extensions(rt);

    if (!reg) {
        EXCEPTION("cfl_se_module_load_term: no registry");
    }

    cfl_se_load_node_data_t *nd = (cfl_se_load_node_data_t *)
        cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    if (!nd) {
        EXCEPTION("cfl_se_module_load_term: no node data");
    }

    if (nd->loaded) {
        cfl_se_registry_unload(reg, nd->name_hash);
        nd->loaded = false;
    }
}

/* ====================================================================
 * Tree load node functions
 * ==================================================================== */

void cfl_se_tree_load_init_one_shot_fn(void *handle, uint16_t node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_se_module_registry_t *reg =
        (cfl_se_module_registry_t *)cfl_get_app_extensions(rt);

    if (!reg) {
        EXCEPTION("cfl_se_tree_load_init: no registry in app_extensions");
    }

    /* Allocate arena for node data */
    bool already = cfl_allocate_state(rt, node_index);
    cfl_se_tree_load_node_data_t *nd;
    if (already) {
        nd = (cfl_se_tree_load_node_data_t *)
            cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    } else {
        nd = (cfl_se_tree_load_node_data_t *)
            cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_se_tree_load_node_data_t));
    }
    if (!nd) {
        EXCEPTION("cfl_se_tree_load_init: arena alloc failed");
    }
    memset(nd, 0, sizeof(*nd));

    /* Decode node data from JSON */
    const char *module_name = NULL;
    const char *tree_name = NULL;
    const char *bb_field_name = NULL;
    json_decoder_init_from_runtime(rt, node_index);
    json_extract_string_runtime(rt, "node_dict.module_name", &module_name);
    json_extract_string_runtime(rt, "node_dict.tree_name", &tree_name);
    json_extract_string_runtime(rt, "node_dict.bb_field_name", &bb_field_name);

    if (!module_name) { EXCEPTION("cfl_se_tree_load_init: module_name missing"); }
    if (!tree_name)   { EXCEPTION("cfl_se_tree_load_init: tree_name missing"); }
    if (!bb_field_name) { EXCEPTION("cfl_se_tree_load_init: bb_field_name missing"); }

    nd->module_hash = cfl_fnv1a_32(module_name);
    nd->tree_hash = cfl_fnv1a_32(tree_name);

    /* Resolve bb_field_name to offset via blackboard descriptor */
    void *field_ptr = cfl_bb_field_by_name(rt, bb_field_name);
    if (!field_ptr) {
        printf("  bb_field_name='%s'\n", bb_field_name);
        EXCEPTION("cfl_se_tree_load_init: blackboard field not found");
    }
    nd->bb_offset = (uint16_t)((uint8_t *)field_ptr - (uint8_t *)rt->blackboard);

    /* Find the module in the registry */
    s_expr_module_t *mod = cfl_se_registry_find(reg, nd->module_hash);
    if (!mod) {
        printf("  module_hash=0x%08X\n", nd->module_hash);
        EXCEPTION("cfl_se_tree_load_init: module not loaded in registry");
    }

    /* Create tree instance by hash */
    s_expr_tree_instance_t *inst =
        s_expr_tree_create_by_hash(mod, nd->tree_hash, (uint32_t)node_index);
    if (!inst) {
        printf("  tree_hash=0x%08X\n", nd->tree_hash);
        EXCEPTION("cfl_se_tree_load_init: tree create failed");
    }

    nd->inst = inst;

    /* Store instance pointer in blackboard uint64 slot */
    uint64_t *slot = CFL_BB_FIELD(rt, nd->bb_offset, uint64_t);
    *slot = (uint64_t)(uintptr_t)inst;
}

unsigned cfl_se_tree_load_main_fn(
    void *handle, unsigned bool_function_index, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return CFL_CONTINUE;
}

void cfl_se_tree_load_term_one_shot_fn(void *handle, uint16_t node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;

    cfl_se_tree_load_node_data_t *nd = (cfl_se_tree_load_node_data_t *)
        cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    if (!nd) {
        EXCEPTION("cfl_se_tree_load_term: no node data");
    }
    if (!nd->inst) return; /* tree was never created — nothing to clean up */

    /* Terminate the tree (backward walk, cleanup) */
    s_expr_node_terminate(nd->inst);

    /* Free the instance */
    s_expr_tree_free(nd->inst);

    /* Clear blackboard slot */
    uint64_t *slot = CFL_BB_FIELD(rt, nd->bb_offset, uint64_t);
    *slot = 0;

    nd->inst = NULL;
}

/* ====================================================================
 * SE tick node functions
 *
 * Maps SE result codes to CFL return codes:
 *   SE_CONTINUE..SE_SKIP_CONTINUE (0-5) → pass through (same values)
 *   SE_FUNCTION_CONTINUE (6)      → CFL_CONTINUE
 *   SE_FUNCTION_HALT (7)          → CFL_HALT
 *   SE_FUNCTION_TERMINATE (8)     → CFL_DISABLE
 *   SE_FUNCTION_RESET (9)         → CFL_CONTINUE + reset tree
 *   SE_FUNCTION_DISABLE (10)      → CFL_DISABLE
 *   SE_FUNCTION_SKIP_CONTINUE (11)→ CFL_CONTINUE
 *   SE_PIPELINE_CONTINUE (12)     → CFL_CONTINUE
 *   SE_PIPELINE_HALT (13)         → CFL_HALT
 *   SE_PIPELINE_TERMINATE (14)    → CFL_DISABLE
 *   SE_PIPELINE_RESET (15)        → CFL_CONTINUE
 *   SE_PIPELINE_DISABLE (16)      → CFL_DISABLE
 *   SE_PIPELINE_SKIP_CONTINUE (17)→ CFL_CONTINUE
 * ==================================================================== */

static unsigned se_result_to_cfl(s_expr_result_t result, bool *reset_tree) {
    *reset_tree = false;
    switch (result) {
        /* Application codes — pass through */
        case SE_CONTINUE:       return CFL_CONTINUE;
        case SE_HALT:           return CFL_HALT;
        case SE_TERMINATE:      return CFL_TERMINATE;
        case SE_RESET:          return CFL_RESET;
        case SE_DISABLE:        return CFL_DISABLE;
        case SE_SKIP_CONTINUE:  return CFL_SKIP_CONTINUE;

        /* Function codes */
        case SE_FUNCTION_CONTINUE:      return CFL_CONTINUE;
        case SE_FUNCTION_HALT:          return CFL_CONTINUE;
        case SE_FUNCTION_TERMINATE:     return CFL_DISABLE;
        case SE_FUNCTION_RESET:         *reset_tree = true; return CFL_CONTINUE;
        case SE_FUNCTION_DISABLE:       return CFL_DISABLE;
        case SE_FUNCTION_SKIP_CONTINUE: return CFL_CONTINUE;

        /* Pipeline codes — all CFL_CONTINUE for now */
        case SE_PIPELINE_CONTINUE:      return CFL_CONTINUE;
        case SE_PIPELINE_HALT:          return CFL_CONTINUE;
        case SE_PIPELINE_TERMINATE:     return CFL_CONTINUE;
        case SE_PIPELINE_RESET:         return CFL_CONTINUE;
        case SE_PIPELINE_DISABLE:       return CFL_CONTINUE;
        case SE_PIPELINE_SKIP_CONTINUE: return CFL_CONTINUE;

        default:                        return CFL_TERMINATE_SYSTEM;
    }
}

static bool se_result_is_complete(s_expr_result_t result) {
    return result != SE_PIPELINE_CONTINUE &&
           result != SE_PIPELINE_DISABLE;
}

 

void cfl_se_tick_init_one_shot_fn(void *handle, uint16_t node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    
    /* Allocate arena for node data */
    bool already = cfl_allocate_state(rt, node_index);
    cfl_se_tick_node_data_t *nd;
    if (already) {
        nd = (cfl_se_tick_node_data_t *)
            cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    } else {
        nd = (cfl_se_tick_node_data_t *)
            cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_se_tick_node_data_t));
    }
    if (!nd) {
        EXCEPTION("cfl_se_tick_init: arena alloc failed");
    }
    memset(nd, 0, sizeof(*nd));

    /* Decode tree_bb_field from JSON node data */
    const char *tree_bb_field = NULL;
    json_decoder_init_from_runtime(rt, node_index);
    json_extract_string_runtime(rt, "node_dict.tree_bb_field", &tree_bb_field);
    if (!tree_bb_field) {
        EXCEPTION("cfl_se_tick_init: tree_bb_field missing");
    }

    /* Resolve bb field name to offset */
    void *field_ptr = cfl_bb_field_by_name(rt, tree_bb_field);
    if (!field_ptr) {
        EXCEPTION("cfl_se_tick_init: blackboard field not found");
    }
    nd->bb_offset = (uint16_t)((uint8_t *)field_ptr - (uint8_t *)rt->blackboard);

    /* Get tree instance from blackboard */
    uint64_t *slot = CFL_BB_FIELD(rt, nd->bb_offset, uint64_t);
    nd->inst = (s_expr_tree_instance_t *)(uintptr_t)*slot;
    if (!nd->inst) {
        EXCEPTION("cfl_se_tick_init: tree instance is NULL in blackboard");
    }

    /* Set user context to runtime handle for cfl bridge functions */
    s_expr_tree_set_user_ctx(nd->inst, rt);
}

unsigned cfl_se_tick_main_fn(
    void *handle, unsigned bool_function_index, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)bool_function_index;
    (void)event_type;
    

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_se_tick_node_data_t *nd = (cfl_se_tick_node_data_t *)
        cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    if (!nd || !nd->inst) {
        EXCEPTION("cfl_se_tick_main: no tree instance");
    }

    s_expr_tree_instance_t *tree = nd->inst;

    /* Tick the tree — event_id passes through directly */
    s_expr_result_t result = s_expr_node_tick(tree, (uint16_t)event_id, event_data);
    //printf("[SE_TICK] tick_count=%u result=%d\n", tick_count, result);
    
    /* Process queued events */
    uint16_t event_count = s_expr_event_queue_count(tree);
    while (event_count > 0 && !se_result_is_complete(result)) {
        uint16_t tick_type;
        uint16_t ev_id;
        void *ev_data;

        s_expr_event_pop(tree, &tick_type, &ev_id, &ev_data);

        /* Save, set, execute, restore */
        uint16_t saved_tick_type = tree->tick_type;
        tree->tick_type = tick_type;

        s_expr_result_t event_result = s_expr_node_tick(tree, ev_id, ev_data);

        tree->tick_type = saved_tick_type;
        //printf("[SE_TICK] event_result=%d\n", event_result);
        if (se_result_is_complete(event_result)) {
            result = event_result;
            break;
        }

        event_count = s_expr_event_queue_count(tree);
    }

    /* Map SE result → CFL result.
     * se_tick is always a leaf node — use CFL_HALT to hold position
     * in the column. Only CFL_DISABLE lets the column advance. */
    bool reset_tree = false;
    unsigned cfl_result = se_result_to_cfl(result, &reset_tree);

    if (reset_tree) {
        s_expr_node_reset(tree);
    }

    /* Leaf override: CFL_CONTINUE → CFL_HALT (hold position in column) */
    if (cfl_result == CFL_CONTINUE) {
        cfl_result = CFL_HALT;
    }

    return cfl_result;
}

void cfl_se_tick_term_one_shot_fn(void *handle, uint16_t node_index) {
    (void)handle;
    (void)node_index;
    /* Tree lifecycle is owned by the se_tree_load node — nothing to do here */
}

/* ====================================================================
 * SE engine composite node functions
 *
 * Self-contained composite: init loads module + creates tree + stores
 * ptr in BB. Main ticks the s-engine tree. Term frees tree + unloads
 * module + terminates children.
 *
 * Children are ChainTree nodes controlled by the s-engine tree via
 * cfl_enable_child / cfl_disable_children (using ct_node_id).
 * Init does NOT enable children — the s-engine tree decides when.
 * ==================================================================== */

void cfl_se_engine_init_one_shot_fn(void *handle, uint16_t node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_se_module_registry_t *reg =
        (cfl_se_module_registry_t *)cfl_get_app_extensions(rt);

    if (!reg) {
        EXCEPTION("cfl_se_engine_init: no registry in app_extensions");
    }

    /* Allocate arena for node data */
    bool already = cfl_allocate_state(rt, node_index);
    cfl_se_engine_node_data_t *nd;
    if (already) {
        nd = (cfl_se_engine_node_data_t *)
            cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    } else {
        nd = (cfl_se_engine_node_data_t *)
            cfl_smart_arena_alloc(rt, node_index, sizeof(cfl_se_engine_node_data_t));
    }
    if (!nd) {
        EXCEPTION("cfl_se_engine_init: arena alloc failed");
    }
    memset(nd, 0, sizeof(*nd));

    /* Decode module_name, tree_name, tree_bb_field from JSON node data.
     * Both composite and leaf nodes store under node_dict.column_data.* */
    const char *module_name = NULL;
    const char *tree_name = NULL;
    const char *tree_bb_field = NULL;
    json_decoder_init_from_runtime(rt, node_index);
    json_extract_string_runtime(rt, "node_dict.column_data.module_name", &module_name);
    json_extract_string_runtime(rt, "node_dict.column_data.tree_name", &tree_name);
    json_extract_string_runtime(rt, "node_dict.column_data.tree_bb_field", &tree_bb_field);

    if (!module_name)   { EXCEPTION("cfl_se_engine_init: module_name missing"); }
    if (!tree_name)     { EXCEPTION("cfl_se_engine_init: tree_name missing"); }
    if (!tree_bb_field) { EXCEPTION("cfl_se_engine_init: tree_bb_field missing"); }

    /* Resolve bb field name to offset */
    void *field_ptr = cfl_bb_field_by_name(rt, tree_bb_field);
    if (!field_ptr) {
        EXCEPTION("cfl_se_engine_init: blackboard field not found");
    }
    nd->bb_offset = (uint16_t)((uint8_t *)field_ptr - (uint8_t *)rt->blackboard);

    /* ---- Module load ---- */
    uint32_t name_hash = cfl_fnv1a_32(module_name);
    nd->module_hash = name_hash;

    /* Check if module already loaded (shared across multiple se_engine nodes) */
    s_expr_module_t *mod = cfl_se_registry_find(reg, name_hash);
    if (!mod) {
        /* Not loaded yet — resolve binary and load */
        const cfl_se_module_def_entry_t *def_entry =
            cfl_se_registry_find_def(reg, name_hash);
        if (!def_entry) {
            printf("  module_name='%s' hash=0x%08X\n", module_name, name_hash);
            EXCEPTION("cfl_se_engine_init: module binary not registered");
        }

        s_expr_allocator_t alloc = cfl_se_make_allocator(reg->rt);
        s_expr_loaded_module_t *loaded = s_expr_load_from_rom(
            def_entry->binary_data, def_entry->binary_size, alloc);
        if (!loaded) {
            EXCEPTION("cfl_se_engine_init: binary parse failed");
        }
        nd->loaded_module = loaded;

        /* Register function tables: builtins + cfl */
        cfl_se_fn_table_set_t layers[CFL_SE_MAX_FN_LAYERS];
        layers[0] = (cfl_se_fn_table_set_t){
            .main_tbl    = s_engine_builtin_main_table(),
            .oneshot_tbl = s_engine_builtin_oneshot_table(),
            .pred_tbl    = s_engine_builtin_pred_table(),
        };
        layers[1] = (cfl_se_fn_table_set_t){
            .main_tbl    = cfl_se_get_main_table(),
            .oneshot_tbl = cfl_se_get_oneshot_table(),
            .pred_tbl    = cfl_se_get_pred_table(),
        };

        mod = cfl_se_registry_load(
            reg, &loaded->def, layers, CFL_SE_MAX_FN_LAYERS,
            def_entry->user_register, def_entry->user_ctx);
        if (!mod) {
            EXCEPTION("cfl_se_engine_init: module load failed");
        }
        nd->loaded = true;
    }

    /* ---- Tree create ---- */
    uint32_t tree_hash = cfl_fnv1a_32(tree_name);
    s_expr_tree_instance_t *inst =
        s_expr_tree_create_by_hash(mod, tree_hash, (uint32_t)node_index);
    if (!inst) {
        printf("  tree_hash=0x%08X\n", tree_hash);
        EXCEPTION("cfl_se_engine_init: tree create failed");
    }
    nd->inst = inst;

    /* Set user context to runtime handle for cfl bridge functions */
    s_expr_tree_set_user_ctx(inst, rt);

    /* Store instance pointer in blackboard uint64 slot */
    uint64_t *slot = CFL_BB_FIELD(rt, nd->bb_offset, uint64_t);
    *slot = (uint64_t)(uintptr_t)inst;
}

unsigned cfl_se_engine_main_fn(
    void *handle, unsigned bool_function_index, unsigned node_index,
    unsigned event_type, unsigned event_id, void *event_data)
{
    (void)bool_function_index;
    (void)event_type;

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_se_engine_node_data_t *nd = (cfl_se_engine_node_data_t *)
        cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    if (!nd || !nd->inst) {
        EXCEPTION("cfl_se_engine_main: no tree instance");
    }

    s_expr_tree_instance_t *tree = nd->inst;

    /* Tick the tree — event_id passes through directly */
    s_expr_result_t result = s_expr_node_tick(tree, (uint16_t)event_id, event_data);

    /* Process queued events */
    uint16_t event_count = s_expr_event_queue_count(tree);
    while (event_count > 0 && !se_result_is_complete(result)) {
        uint16_t tick_type;
        uint16_t ev_id;
        void *ev_data;

        s_expr_event_pop(tree, &tick_type, &ev_id, &ev_data);

        uint16_t saved_tick_type = tree->tick_type;
        tree->tick_type = tick_type;

        s_expr_result_t event_result = s_expr_node_tick(tree, ev_id, ev_data);

        tree->tick_type = saved_tick_type;

        if (se_result_is_complete(event_result)) {
            result = event_result;
            break;
        }

        event_count = s_expr_event_queue_count(tree);
    }

    /* Map SE result → CFL result */
    bool reset_tree = false;
    unsigned cfl_result = se_result_to_cfl(result, &reset_tree);
    //printf("[SE_ENGINE] node=%u se_result=%d cfl_result=%u\n", node_index, result, cfl_result);

    if (reset_tree) {
        s_expr_node_reset(tree);
    }

    return cfl_result;
}

void cfl_se_engine_term_one_shot_fn(void *handle, uint16_t node_index) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    cfl_se_module_registry_t *reg =
        (cfl_se_module_registry_t *)cfl_get_app_extensions(rt);

    cfl_se_engine_node_data_t *nd = (cfl_se_engine_node_data_t *)
        cfl_heap_arena_get_node_ptr(rt->arena_system, node_index);
    if (!nd) {
        EXCEPTION("cfl_se_engine_term: no node data");
    }

    /* Children are terminated by the ChainTree engine's own terminate walk.
     * We only clean up the s-engine tree and module here. */

    /* Free the tree instance */
    if (nd->inst) {
        s_expr_node_terminate(nd->inst);
        s_expr_tree_free(nd->inst);

        /* Clear blackboard slot */
        uint64_t *slot = CFL_BB_FIELD(rt, nd->bb_offset, uint64_t);
        *slot = 0;
        nd->inst = NULL;
    }

    /* Unload module if this node loaded it */
    if (nd->loaded && reg) {
        cfl_se_registry_unload(reg, nd->module_hash);
        nd->loaded = false;
    }
}
