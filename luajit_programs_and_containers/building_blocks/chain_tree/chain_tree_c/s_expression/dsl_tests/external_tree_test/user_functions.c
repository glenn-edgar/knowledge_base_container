#include "external_tree_user_functions.h"
#include "s_engine_types.h"
#include "s_engine_stack.h"

#include "s_engine_node.h"
#include <stdio.h>

// Oneshot functions
void write_register(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
){
    UNUSED(params);
    UNUSED(param_count);
    UNUSED(event_type);
    UNUSED(event_id);
    UNUSED(event_data);

    printf("write_register called\n");
    const s_expr_param_t* address_param = s_expr_stack_get_local(inst->stack, 0);
    printf("register address: 0x%08X\n", address_param->uint_val);
    const s_expr_param_t* value_param = s_expr_stack_get_local(inst->stack, 1);
    printf("register value: 0x%08X\n", value_param->uint_val);
    
   

}