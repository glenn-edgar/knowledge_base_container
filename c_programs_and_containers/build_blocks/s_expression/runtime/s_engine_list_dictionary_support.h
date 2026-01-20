const s_expr_param_t* s_expr_dict_find_int_key(
    const s_expr_param_t* dict_param,
    int32_t key_val
);

const s_expr_param_t* s_expr_dict_find_key(
    const s_expr_param_t* dict_param,
    uint32_t key_hash
);

// ============================================================================
// STRUCTURE CONTENT ACCESSORS
// Returns pointer to first element inside, sets content_count
// Returns NULL if not valid or empty
// ============================================================================

const s_expr_param_t* s_expr_key_contents(
    const s_expr_param_t* key_param,
    uint16_t* content_count
);

const s_expr_param_t* s_expr_tuple_contents(
    const s_expr_param_t* tuple_param,
    uint16_t* content_count
);

const s_expr_param_t* s_expr_array_contents(
    const s_expr_param_t* array_param,
    uint16_t* content_count
);

const s_expr_param_t* s_expr_dict_contents(
    const s_expr_param_t* dict_param,
    uint16_t* content_count
);

const s_expr_param_t* s_expr_list_contents(
    const s_expr_param_t* list_param,
    uint16_t* content_count
);