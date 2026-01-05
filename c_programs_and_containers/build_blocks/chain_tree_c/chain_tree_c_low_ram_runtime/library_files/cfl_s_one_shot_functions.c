#include "cfl_s_one_shot_functions.h"
#include "cfl_common_function_headers.h"
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"
#include <stdio.h>
#include <stdlib.h>

// ============================================================================
// ONESHOT FUNCTION IMPLEMENTATIONS
// ============================================================================

// ============================================================================
// CFL_LOG: Print timestamp, node ID, and message hash
// Params: [0] = string hash (str_hash)
// ============================================================================

static void cfl_log_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("CFL_LOG: requires 1 parameter");
        return; 
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("CFL_LOG: param[0] must be STR_IDX");
        return;
    }
    
    const char* message = s_expr_get_string(inst, &params[0]);
    if (!message) {
        EXCEPTION("CFL_LOG: failed to get string");
        return;
    }
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_LOG: no runtime handle");
        return;
    }
    
    double timestamp = cfl_timer_get_timestamp(runtime_handle->timer_handle);
    printf("Timestamp: %f, Node ID: %u, Message: %s\n", 
           timestamp, inst->ct_node_id, message);
}

// ============================================================================
// CFL_ENABLE_CHILDREN: Enable all children of current node
// Params: none
// ============================================================================

static void cfl_enable_children_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_ENABLE_CHILDREN: no runtime handle");
        return;
    }
    
    cfl_enable_all_children(runtime_handle, inst->ct_node_id);
}

// ============================================================================
// CFL_ENABLE_CHILD: Enable a specific child by index
// Params: [0] = child_index (int/uint)
// ============================================================================

static void cfl_enable_child_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("CFL_ENABLE_CHILD: requires 1 parameter (child_index)");
        return;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_ENABLE_CHILD: param[0] must be INT or UINT");
        return;
    }
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_ENABLE_CHILD: no runtime handle");
        return;
    }
    
    cfl_enable_child(runtime_handle, inst->ct_node_id, (unsigned)s_expr_param_uint(&params[0]));
}

// ============================================================================
// CFL_DISABLE_CHILD: Disable a specific child by index
// Params: [0] = child_index (int/uint)
// ============================================================================

static void cfl_disable_child_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("CFL_DISABLE_CHILD: requires 1 parameter (child_index)");
        return;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_DISABLE_CHILD: param[0] must be INT or UINT");
        return;
    }
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_DISABLE_CHILD: no runtime handle");
        return;
    }
    
    cfl_disable_child(runtime_handle, inst->ct_node_id, (unsigned)s_expr_param_uint(&params[0]));
}

// ============================================================================
// CFL_DISABLE_CHILDREN: Disable all children of current node
// Params: none
// ============================================================================

static void cfl_disable_children_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_DISABLE_CHILDREN: no runtime handle");
        return;
    }
    
    cfl_disable_all_children(runtime_handle, inst->ct_node_id);
}

// ============================================================================
// CFL_INTERNAL_EVENT: Send an internal event to the event queue
// Params: [0] = event_type (int/uint)
//         [1] = event_data (int/uint)
// ============================================================================

static void cfl_internal_event_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // Skip on INIT and TERMINATE
    if (event_type != SE_EVENT_TICK) {
        return;
    }
    
    // Validate param count
    if (param_count != 2) {
        EXCEPTION("CFL_INTERNAL_EVENT requires 2 parameters: event_type, event_data");
        return;
    }
    
    // Validate param types
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t type1 = params[1].type & S_EXPR_OPCODE_MASK;
    
    if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_INTERNAL_EVENT param[0] must be INT or UINT");
        return;
    }
    
    if (type1 != S_EXPR_PARAM_INT && type1 != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_INTERNAL_EVENT param[1] must be INT or UINT");
        return;
    }
    
    // Get runtime handle from user context
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_INTERNAL_EVENT: no runtime handle");
        return;
    }
    
    // Send the event
    cfl_send_integer_event(
        runtime_handle->event_queue,
        CFL_EVENT_PRIORITY_LOW,
        inst->ct_node_id,
        (unsigned)s_expr_param_int(&params[0]),
        (cfl_int_t)s_expr_param_int(&params[1])
    );
}

// ============================================================================
// CFL_EXCEPTION: Raise an exception with a string hash
// Params: [0] = string hash (str_hash)
// ============================================================================

static void cfl_exception_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count != 1) {
        EXCEPTION("CFL_EXCEPTION: requires 1 parameter");
        return;
    }
    
    uint8_t type0 = params[0].type & S_EXPR_OPCODE_MASK;
    if (type0 != S_EXPR_PARAM_STR_HASH) {
        EXCEPTION("CFL_EXCEPTION: param[0] must be STR_HASH");
        return;
    }
    
    printf("CFL_EXCEPTION: 0x%llX\n", (unsigned long long)params[0].str_hash);
    EXCEPTION("CFL_EXCEPTION triggered");
}
static void cfl_set_bits_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("CFL_SET_BITS: requires at least 1 parameter");
        return;
    }
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_SET_BITS: no runtime handle");
        return;
    }
    
    for (uint16_t i = 0; i < param_count; i++) {
        uint8_t type_i = params[i].type & S_EXPR_OPCODE_MASK;
        if (type_i != S_EXPR_PARAM_INT && type_i != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_SET_BITS: param must be INT or UINT");
            return;
        }
        
        uint32_t bit_index = (uint32_t)s_expr_param_uint(&params[i]);
        if (bit_index >= 32) {
            EXCEPTION("CFL_SET_BITS: bit index out of range");
            return;
        }
        
        runtime_handle->bitmask |= (1U << bit_index);
    }
}

static void cfl_clear_bits_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("CFL_CLEAR_BITS: requires at least 1 parameter");
        return;
    }
    
    cfl_runtime_handle_t* runtime_handle = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime_handle) {
        EXCEPTION("CFL_CLEAR_BITS: no runtime handle");
        return;
    }
    
    for (uint16_t i = 0; i < param_count; i++) {
        uint8_t type_i = params[i].type & S_EXPR_OPCODE_MASK;
        if (type_i != S_EXPR_PARAM_INT && type_i != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_CLEAR_BITS: param must be INT or UINT");
            return;
        }
        
        uint32_t bit_index = (uint32_t)s_expr_param_uint(&params[i]);
        if (bit_index >= 32) {
            EXCEPTION("CFL_CLEAR_BITS: bit index out of range");
            return;
        }
        
        runtime_handle->bitmask &= ~(1U << bit_index);
    }
}



// ============================================================================
// CFL_JSON_READ_INT
// Params: field_ref, str_ptr(json_path)
// Field types: int8, int16, int32, int64
// ============================================================================

static void cfl_json_read_int_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("CFL_JSON_READ_INT: requires 2 parameters");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("CFL_JSON_READ_INT: param[0] must be FIELD");
        return;
    }
    
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("CFL_JSON_READ_INT: param[1] must be STR_IDX");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("CFL_JSON_READ_INT: no blackboard");
        return;
    }
    
    const char* json_path = s_expr_get_string(inst, &params[1]);
    if (!json_path) {
        EXCEPTION("CFL_JSON_READ_INT: failed to get json_path");
        return;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime ) {
        EXCEPTION("CFL_JSON_READ_INT: no JSON buffer");
        return;
    }
    
    void* field_ptr = (uint8_t*)bb + params[0].field_offset;
    uint16_t field_size = params[0].field_size;
    
    
    
    json_decoder_init_from_runtime(runtime, inst->ct_node_id);
   
    int32_t temp_value = 0;
    json_extract_int32_runtime(runtime, json_path, &temp_value);
    
    
    uint64_t value = (uint64_t)temp_value;
    switch (field_size) {
        case 1: *(int8_t*)field_ptr  = (int8_t)value;  break;
        case 2: *(int16_t*)field_ptr = (int16_t)value; break;
        case 4: *(int32_t*)field_ptr = (int32_t)value; break;
        case 8: *(int64_t*)field_ptr = (int64_t)value; break;
        default:
            printf("CFL_JSON_READ_INT: unsupported field_size %d\n", field_size);
            EXCEPTION("CFL_JSON_READ_INT: unsupported field_size \n");
            return;
    }
}

// ============================================================================
// CFL_JSON_READ_UINT
// Params: field_ref, str_ptr(json_path)
// Field types: uint8, uint16, uint32, uint64
// ============================================================================

static void cfl_json_read_uint_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("CFL_JSON_READ_UINT: requires 2 parameters");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("CFL_JSON_READ_UINT: param[0] must be FIELD");
        return;
    }
    
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("CFL_JSON_READ_UINT: param[1] must be STR_IDX");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("CFL_JSON_READ_UINT: no blackboard");
        return;
    }
    
    const char* json_path = s_expr_get_string(inst, &params[1]);
    if (!json_path) {
        EXCEPTION("CFL_JSON_READ_UINT: failed to get json_path");
        return;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime || !runtime) {
        EXCEPTION("CFL_JSON_READ_UINT: no JSON buffer");
        return;
    }
    
    void* field_ptr = (uint8_t*)bb + params[0].field_offset;
    uint16_t field_size = params[0].field_size;
        
    json_decoder_init_from_runtime(runtime, inst->ct_node_id);
   
    

    int32_t temp_value = 0;
    json_extract_int32_runtime(runtime, json_path, &temp_value);
    
    
    uint64_t value = (uint64_t)temp_value;
    // << END JSON PARSING >>
    
    switch (field_size) {
        case 1: *(uint8_t*)field_ptr  = (uint8_t)value;  break;
        case 2: *(uint16_t*)field_ptr = (uint16_t)value; break;
        case 4: *(uint32_t*)field_ptr = (uint32_t)value; break;
        case 8: *(uint64_t*)field_ptr = (uint64_t)value; break;
        default:
            printf("CFL_JSON_READ_UINT: unsupported field_size %d\n", field_size);
            EXCEPTION("CFL_JSON_READ_UINT: unsupported field_size \n");
            return;
    }
}

// ============================================================================
// CFL_JSON_READ_FLOAT
// Params: field_ref, str_ptr(json_path)
// Field type: float (32-bit)
// ============================================================================

static void cfl_json_read_float_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("CFL_JSON_READ_FLOAT: requires 2 parameters");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("CFL_JSON_READ_FLOAT: param[0] must be FIELD");
        return;
    }
    
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("CFL_JSON_READ_FLOAT: param[1] must be STR_IDX");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("CFL_JSON_READ_FLOAT: no blackboard");
        return;
    }
    
    const char* json_path = s_expr_get_string(inst, &params[1]);
    if (!json_path) {
        EXCEPTION("CFL_JSON_READ_FLOAT: failed to get json_path");
        return;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime ) {
        EXCEPTION("CFL_JSON_READ_FLOAT: no JSON buffer");
        return;
    }
    
    float* field_ptr = (float*)((uint8_t*)bb + params[0].field_offset);
    
    json_decoder_init_from_runtime(runtime, inst->ct_node_id);
    printf("CFL_JSON_READ_FLOAT: json_path: %s\n", json_path);
    float value = 0.0f;
    json_extract_float32_runtime(runtime, json_path, &value);
    
    // << END JSON PARSING >>
    *field_ptr = value;
    
}





// ============================================================================
// CFL_JSON_READ_BOOL
// Params: field_ref, str_ptr(json_path)
// Field type: bool
// ============================================================================

static void cfl_json_read_bool_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("CFL_JSON_READ_BOOL: requires 2 parameters");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("CFL_JSON_READ_BOOL: param[0] must be FIELD");
        return;
    }
    
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("CFL_JSON_READ_BOOL: param[1] must be STR_IDX");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("CFL_JSON_READ_BOOL: no blackboard");
        return;
    }
    
    const char* json_path = s_expr_get_string(inst, &params[1]);
    if (!json_path) {
        EXCEPTION("CFL_JSON_READ_BOOL: failed to get json_path");
        return;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime ) {
        EXCEPTION("CFL_JSON_READ_BOOL: no JSON buffer");
        return;
    }
    
    bool* field_ptr = (bool*)((uint8_t*)bb + params[0].field_offset);
    
    json_decoder_init_from_runtime(runtime, inst->ct_node_id);
    bool value = false;
    json_extract_bool_runtime(runtime, json_path, &value);

    
    
    *field_ptr = value;
}

// ============================================================================
// CFL_JSON_READ_STRING_PTR
// Params: field_ref, str_ptr(json_path)
// 
// ============================================================================

static void cfl_json_read_string_ptr_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("CFL_JSON_READ_STRING_PTR: requires 2 parameters");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("CFL_JSON_READ_STRING_PTR: param[0] must be FIELD");
        return;
    }
    
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("CFL_JSON_READ_STRING_PTR: param[1] must be STR_IDX");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("CFL_JSON_READ_STRING_PTR: no blackboard");
        return;
    }
    
    const char* json_path = s_expr_get_string(inst, &params[1]);
    if (!json_path) {
        EXCEPTION("CFL_JSON_READ_STRING_PTR: failed to get json_path");
        return;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime ) {
        EXCEPTION("CFL_JSON_READ_STRING_PTR: no JSON buffer");
        return;
    }
    
    char** field_ptr = (char**)((uint8_t*)bb + params[0].field_offset);
    
    
    json_decoder_init_from_runtime(runtime, inst->ct_node_id);
   
    const char* value = NULL;
    json_extract_string_runtime(runtime, json_path, &value);
   
    // << END JSON PARSING >>
    
    *field_ptr = (char*)value;
}

// ============================================================================
// CFL_JSON_READ_STRING_BUF
// Params: field_ref, str_ptr(json_path)
// Field type: char[N] (fixed buffer, uses field_size for max length)
// ============================================================================

static void cfl_json_read_string_buf_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("CFL_JSON_READ_STRING_BUF: requires 2 parameters");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("CFL_JSON_READ_STRING_BUF: param[0] must be FIELD");
        return;
    }
    
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("CFL_JSON_READ_STRING_BUF: param[1] must be STR_IDX");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("CFL_JSON_READ_STRING_BUF: no blackboard");
        return;
    }
    
    const char* json_path = s_expr_get_string(inst, &params[1]);
    if (!json_path) {
        EXCEPTION("CFL_JSON_READ_STRING_BUF: failed to get json_path");
        return;
    }
    
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)s_expr_tree_get_user_ctx(inst);
    if (!runtime ) {
        EXCEPTION("CFL_JSON_READ_STRING_BUF: no JSON buffer");
        return;
    }
    
    char* field_ptr = (char*)((uint8_t*)bb + params[0].field_offset);
    uint16_t buf_size = params[0].field_size;
    
    // Clear buffer first
    memset(field_ptr, 0, buf_size);
    const char* value = NULL;
    json_decoder_init_from_runtime(runtime, inst->ct_node_id);
    json_extract_string_runtime(runtime, json_path, &value);
   

    size_t value_len = strlen(value)+1;
    // << END JSON PARSING >>
    
    if (value && value_len > 0) {
        size_t copy_len = (value_len < (size_t)(buf_size - 1)) ? value_len : (size_t)(buf_size - 1);
        memcpy(field_ptr, value, copy_len);
        field_ptr[copy_len] = '\0';
    }
}

static void cfl_copy_const_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) {
        EXCEPTION("CFL_COPY_CONST: requires 2 parameters (field_ref, const_ref)");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("CFL_COPY_CONST: param[0] must be FIELD");
        return;
    }
    
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_CONST_REF) {
        EXCEPTION("CFL_COPY_CONST: param[1] must be CONST_REF");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("CFL_COPY_CONST: no blackboard");
        return;
    }
    
    // Get module to access constants table
    const s_expr_module_def_t* module_def = s_expr_tree_get_module_def(inst);
    if (!module_def) {
        EXCEPTION("CFL_COPY_CONST: no module definition");
        return;
    }
    
    // Validate const_index
    uint16_t const_index = params[1].const_index;
    if (const_index >= module_def->const_count) {
        printf("CFL_COPY_CONST: const_index %u out of range (max %u)\n",
                  const_index, module_def->const_count);
        EXCEPTION("CFL_COPY_CONST: const_index out of range");
        return;
    }
    
    // Destination in RAM blackboard
    void* dest = (uint8_t*)bb + params[0].field_offset;
    
    // Source is looked up from constants table (ROM)
    const void* src = module_def->constants[const_index];
    uint16_t size = params[1].const_size;
    
    if (!src) {
        printf("CFL_COPY_CONST: NULL constant at index %u\n", const_index);
        EXCEPTION("CFL_COPY_CONST: NULL constant at index \n");
        return;
    }
    
    memcpy(dest, src, size);
    
    printf("CFL_COPY_CONST: copied %u bytes from ROM[%u] to offset %u\n",
           size, const_index, params[0].field_offset);
}
// ============================================================================
// CFL_COPY_CONST_FULL
// Params: const_ref(source)
// Copies const struct to entire blackboard (must match record size)
// ============================================================================

static void cfl_copy_const_full_oneshot(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) {
        EXCEPTION("CFL_COPY_CONST_FULL: requires 1 parameter (const_ref)");
        return;
    }
    
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_CONST_REF) {
        EXCEPTION("CFL_COPY_CONST_FULL: param[0] must be CONST_REF");
        return;
    }
    
    void* bb = s_expr_tree_get_blackboard(inst);
    if (!bb) {
        EXCEPTION("CFL_COPY_CONST_FULL: no blackboard");
        return;
    }
    
    // Get module to access constants table
    const s_expr_module_def_t* module_def = s_expr_tree_get_module_def(inst);
    if (!module_def) {
        EXCEPTION("CFL_COPY_CONST_FULL: no module definition");
        return;
    }
    
    // Validate const_index
    uint16_t const_index = params[0].const_index;
    if (const_index >= module_def->const_count) {
        printf("CFL_COPY_CONST_FULL: const_index %u out of range (max %u)\n",
                  const_index, module_def->const_count);
        EXCEPTION("CFL_COPY_CONST_FULL: const_index out of range");
                  
        return;
    }
    
    uint16_t bb_size = s_expr_tree_get_blackboard_size(inst);
    
    // Source is looked up from constants table (ROM)
    const void* src = module_def->constants[const_index];
    uint16_t const_size = params[0].const_size;
    
    if (!src) {
        printf("CFL_COPY_CONST_FULL: NULL constant at index %u\n", const_index);
        EXCEPTION("CFL_COPY_CONST_FULL: NULL constant at index \n");
        return;
    }
    
    if (const_size != bb_size) {
        printf("CFL_COPY_CONST_FULL: size mismatch (const=%u, blackboard=%u)\n",
               const_size, bb_size);
        EXCEPTION("CFL_COPY_CONST_FULL: size mismatch");
        return;
    }
    
    memcpy(bb, src, const_size);
    
    printf("CFL_COPY_CONST_FULL: copied %u bytes from ROM[%u] to blackboard\n",
           const_size, const_index);
}
/*

*/
// ============================================================================
// SYSTEM ONESHOT ENTRIES (named for readability)
// ============================================================================

static const s_expr_fn_entry_named_t system_oneshot_entries_named[] = {
    { "CFL_LOG",              (void*)cfl_log_oneshot },
    { "CFL_ENABLE_CHILDREN",  (void*)cfl_enable_children_oneshot },
    { "CFL_DISABLE_CHILDREN", (void*)cfl_disable_children_oneshot },
    { "CFL_ENABLE_CHILD",     (void*)cfl_enable_child_oneshot },
    { "CFL_DISABLE_CHILD",    (void*)cfl_disable_child_oneshot },
    { "CFL_INTERNAL_EVENT",   (void*)cfl_internal_event_oneshot },
    { "CFL_EXCEPTION",        (void*)cfl_exception_oneshot },
    { "CFL_SET_BITS",         (void*)cfl_set_bits_oneshot },
    { "CFL_CLEAR_BITS",       (void*)cfl_clear_bits_oneshot },
    { "CFL_JSON_READ_INT",        (void*)cfl_json_read_int_oneshot },
    { "CFL_JSON_READ_UINT",       (void*)cfl_json_read_uint_oneshot },
    { "CFL_JSON_READ_FLOAT",      (void*)cfl_json_read_float_oneshot },
    { "CFL_JSON_READ_BOOL",       (void*)cfl_json_read_bool_oneshot },
    { "CFL_JSON_READ_STRING_PTR", (void*)cfl_json_read_string_ptr_oneshot },
    { "CFL_JSON_READ_STRING_BUF", (void*)cfl_json_read_string_buf_oneshot },
    { "CFL_COPY_CONST",           (void*)cfl_copy_const_oneshot },
    { "CFL_COPY_CONST_FULL",      (void*)cfl_copy_const_full_oneshot },
};

// ============================================================================
// HASH TABLE (populated at runtime)
// ============================================================================

#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))

static s_expr_fn_entry_t system_oneshot_entries[ARRAY_COUNT(system_oneshot_entries_named)];
static s_expr_fn_table_t system_oneshot_table;

// ============================================================================
// LOAD FUNCTION
// ============================================================================

void cfl_load_oneshot_s_functions(cfl_runtime_handle_t* handle) {
    if (!handle || !handle->s_expr_modules) {
        printf("ERROR: cfl_load_oneshot_s_functions called with invalid handle\n");
        return;
    }
    
    // Build hash table
    s_expr_build_fn_table(
        system_oneshot_entries_named,
        system_oneshot_entries,
        ARRAY_COUNT(system_oneshot_entries_named)
    );
    
    system_oneshot_table.entries = system_oneshot_entries;
    system_oneshot_table.count = ARRAY_COUNT(system_oneshot_entries);
    
    // Register to all modules
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        s_expr_module_register_oneshot(modules[i], &system_oneshot_table);
    }
}