#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "cfl_exception.h"
#include "s_engine_node.h"
#include "s_engine_list_dictionary_support.h"
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

// ============================================================================
// DICTIONARY SUPPORT FUNCTIONS
// ============================================================================

const s_expr_param_t* s_expr_key_contents(
    const s_expr_param_t* key_param,
    uint16_t* content_count
) {
    uint8_t opcode = key_param->type & S_EXPR_OPCODE_MASK;
    
    if (opcode != S_EXPR_PARAM_OPEN_KEY) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    // Content starts after OPEN_KEY
    const s_expr_param_t* content = key_param + 1;
    
    // Linear scan to find CLOSE_KEY (OPEN_KEY stores hash, not brace_idx)
    uint16_t count = 0;
    const s_expr_param_t* p = content;
    
    while (count < 10000) {
        opcode = p->type & S_EXPR_OPCODE_MASK;
        if (opcode == S_EXPR_PARAM_CLOSE_KEY) {
            break;
        }
        count++;
        p++;
    }
    
    if (content_count) *content_count = count;
    return content;
}

const s_expr_param_t* s_expr_dict_find_key(
    const s_expr_param_t* dict_param,
    uint32_t key_hash
) {
    uint8_t opcode = dict_param->type & S_EXPR_OPCODE_MASK;
    
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        return NULL;
    }
    
    uint16_t dict_size = dict_param->brace_idx;
    const s_expr_param_t* dict_end = dict_param + dict_size;
    const s_expr_param_t* p = dict_param + 1;
    
    while (p < dict_end) {
        opcode = p->type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN_KEY) {
            if (p->str_hash == key_hash) {
                return p + 1;
            }
            // Linear scan to skip past CLOSE_KEY (OPEN_KEY stores hash, not brace_idx)
            p++;
            while (p < dict_end) {
                if ((p->type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_CLOSE_KEY) {
                    p++;
                    break;
                }
                p++;
            }
        } else if (opcode == S_EXPR_PARAM_CLOSE_DICT) {
            break;
        } else {
            p++;
        }
    }
    
    return NULL;
}

const s_expr_param_t* s_expr_dict_find_int_key(
    const s_expr_param_t* dict_param,
    int32_t key_val
) {
    uint8_t opcode = dict_param->type & S_EXPR_OPCODE_MASK;
    
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        return NULL;
    }
    
    uint16_t dict_size = dict_param->brace_idx;
    const s_expr_param_t* dict_end = dict_param + dict_size;
    const s_expr_param_t* p = dict_param + 1;
    
    while (p < dict_end) {
        opcode = p->type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN_KEY) {
            const s_expr_param_t* key_param = p + 1;
            uint8_t key_opcode = key_param->type & S_EXPR_OPCODE_MASK;
            
            if (key_opcode == S_EXPR_PARAM_INT || key_opcode == S_EXPR_PARAM_UINT) {
                if ((int32_t)key_param->int_val == key_val) {
                    return key_param + 1;
                }
            }
            
            // Linear scan to skip past CLOSE_KEY
            p++;
            while (p < dict_end) {
                if ((p->type & S_EXPR_OPCODE_MASK) == S_EXPR_PARAM_CLOSE_KEY) {
                    p++;
                    break;
                }
                p++;
            }
        } else if (opcode == S_EXPR_PARAM_CLOSE_DICT) {
            break;
        } else {
            p++;
        }
    }
    
    return NULL;
}


// ============================================================================
// TUPLE/ARRAY/DICT CONTENT ACCESSORS
// ============================================================================

const s_expr_param_t* s_expr_tuple_contents(
    const s_expr_param_t* tuple_param,
    uint16_t* content_count
) {
    if (!S_EXPR_PARAM_IS_OPEN_TUPLE(tuple_param->type)) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    uint16_t brace_idx = tuple_param->brace_idx;
    if (brace_idx <= 1) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    if (content_count) *content_count = brace_idx - 1;
    return tuple_param + 1;
}

const s_expr_param_t* s_expr_array_contents(
    const s_expr_param_t* array_param,
    uint16_t* content_count
) {
    if (!S_EXPR_PARAM_IS_OPEN_ARRAY(array_param->type)) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    uint16_t brace_idx = array_param->brace_idx;
    if (brace_idx <= 1) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    if (content_count) *content_count = brace_idx - 1;
    return array_param + 1;
}

const s_expr_param_t* s_expr_dict_contents(
    const s_expr_param_t* dict_param,
    uint16_t* content_count
) {
    if (!S_EXPR_PARAM_IS_OPEN_DICT(dict_param->type)) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    uint16_t brace_idx = dict_param->brace_idx;
    if (brace_idx <= 1) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    if (content_count) *content_count = brace_idx - 1;
    return dict_param + 1;
}

const s_expr_param_t* s_expr_list_contents(
    const s_expr_param_t* list_param,
    uint16_t* content_count
) {
    if (!S_EXPR_PARAM_IS_OPEN(list_param->type)) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    uint16_t brace_idx = list_param->brace_idx;
    if (brace_idx <= 1) {
        if (content_count) *content_count = 0;
        return NULL;
    }
    
    if (content_count) *content_count = brace_idx - 1;
    return list_param + 1;
}