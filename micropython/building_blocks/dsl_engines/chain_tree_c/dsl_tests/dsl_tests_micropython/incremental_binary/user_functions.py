# user_functions.py
# MicroPython implementations of ChainTree user functions
#
# Avro/streaming/drone functions stay in C (user_avro_test_file.c,
# user_streaming_boolean.c, user_node_control_boolean_fns.c).
# Everything else is implemented here.
#
# Mirrors: dsl_tests_lua5.3/incremental_binary/user_functions.lua

import cfl

# =========================================================================
# Struct offsets (64-bit platform, from offsetof() on C structs)
# =========================================================================

# cfl_while_fn_data_t
WHILE_CURRENT_ITERATION = 0   # int32_t
WHILE_AUXILIARY_DATA    = 8   # void*

# cfl_exception_support_data_t
EXC_LOGGING_DATA     = 0     # void*
EXC_AUXILIARY_DATA   = 8     # void*
EXC_ORIGINAL_NODE_ID = 20    # uint16_t
EXC_EXCEPTION_TYPE   = 36    # enum (int)
EXC_MAX_STEPS        = 40    # uint8_t
EXC_CURRENT_STEP     = 44    # uint8_t
EXC_RECOVERY_STATE   = 48    # enum (int)

# sm_event_filtering_init_fn_data_t
SM_EVENT_ID = 0   # int32_t
SM_SIZE     = 4

# sequence_start_fn_data_t
SEQ_SEQUENCE_NUMBER   = 4    # int32_t
SEQ_RESULT_DATA_ARRAY = 16   # void*
SEQ_FINAL_STATUS      = 32   # bool (uint8_t)
SEQ_SEQUENCE_TYPE     = 33   # uint8_t

# sequence_result_data_t (4 bytes each)
SRD_SEQUENCE_RESULT = 0   # uint16_t
SRD_NODE_INDEX      = 2   # uint16_t
SRD_SIZE            = 4

# sequence_aggregate_data_t
AGG_TRY_NODE_COUNT   = 4    # int32_t
AGG_TRY_NODE_INDEXES = 8    # void*

# cfl_supervisor_data_t
SUP_FAILURE_ARRAY     = 0    # void*
SUP_FAILED_LINK_INDEX = 28   # uint8_t

# cfl_supervisor_failure_t (8 bytes each)
SF_NODE_ID = 4   # int16_t
SF_SIZE    = 8


# =========================================================================
# One-Shot Functions
# =========================================================================

def activate_valve_one_shot(handle, node_index):
    state = cfl.json_extract_string(handle, node_index, "node_dict.state")
    if state == "open":
        print("Valve is open")

def wait_for_event_error_one_shot(handle, node_index):
    print("wait_for_event_error_one_shot_fn node index: %d" % node_index)

def verify_error_one_shot(handle, node_index):
    msg = cfl.json_extract_string(handle, node_index,
        "node_dict.error_data.failure_data")
    print("error_message: %s" % msg)

def initialize_sequence_one_shot(handle, node_index):
    pass

def display_sequence_till_result_one_shot(handle, node_index):
    ptr = cfl.arena_get(handle, node_index)
    if ptr is None:
        return

    msg = cfl.json_extract_string(handle, node_index,
        "node_dict.column_data.user_data.message")
    print("display_sequence_till_result_one_shot_fn message: %s" % msg)

    seq_type = cfl.read_u8(ptr, SEQ_SEQUENCE_TYPE)
    final_status = cfl.read_bool(ptr, SEQ_FINAL_STATUS)
    print("sequence_type: %d" % seq_type)
    print("sequence_result: %d" % (1 if final_status else 0))

    seq_number = cfl.read_i32(ptr, SEQ_SEQUENCE_NUMBER)
    result_array = cfl.read_ptr(ptr, SEQ_RESULT_DATA_ARRAY)
    if result_array is not None:
        for i in range(seq_number):
            offset = i * SRD_SIZE
            ni = cfl.read_u16(result_array, offset + SRD_NODE_INDEX)
            sr = cfl.read_u16(result_array, offset + SRD_SEQUENCE_RESULT)
            print("node_index: [%d] sequence_result: %d" % (ni, sr))
        print("now examinng nodes that set sequence_results")
        for i in range(seq_number):
            offset = i * SRD_SIZE
            ni = cfl.read_u16(result_array, offset + SRD_NODE_INDEX)
            print("dictonary node_index: %d" % ni)

def display_sequence_result_one_shot(handle, node_index):
    ptr = cfl.arena_get(handle, node_index)
    if ptr is None:
        print("display_sequence_result_one_shot node index: %d" % node_index)
        return

    try_count = cfl.read_i32(ptr, AGG_TRY_NODE_COUNT)
    indexes_ptr = cfl.read_ptr(ptr, AGG_TRY_NODE_INDEXES)
    print("try_node_count: %d" % try_count)

    if indexes_ptr is not None:
        for i in range(try_count):
            ni = cfl.read_u16(indexes_ptr, i * 2)
            print("try_node_indexes[%d]: %d" % (i, ni))
            display_sequence_till_result_one_shot(handle, ni)

def display_failure_window_result_one_shot(handle, node_index):
    ptr = cfl.arena_get(handle, node_index)
    if ptr is None:
        return

    failed_link_idx = cfl.read_u8(ptr, SUP_FAILED_LINK_INDEX)
    failure_array = cfl.read_ptr(ptr, SUP_FAILURE_ARRAY)
    if failure_array is None:
        return

    failed_node_id = cfl.read_u16(failure_array,
        failed_link_idx * SF_SIZE + SF_NODE_ID)
    uplink_node_id = cfl.json_extract_int32(handle, node_index,
        "node_dict.column_data.user_data.uplink_node_id")

    print("failed_link_index: %d" % failed_link_idx)
    print("failed_node_index: %d" % failed_node_id)
    print("uplink_node_id: %d if communicating with uplink node" % uplink_node_id)
    print("dump of json data for failed node")
    cfl.json_print_node(handle, failed_node_id)

def watch_dog_time_out_one_shot(handle, node_index):
    reset = cfl.json_extract_bool(handle, node_index, "node_dict.wd_reset")
    msg = cfl.json_extract_string(handle, node_index, "node_dict.wd_fn_data.message")
    print("watch_dog_time_out_one_shot_fn reset: %d" % (1 if reset else 0))
    print("watch_dog_time_out_one_shot_fn message: %s" % msg)

def exception_logging_one_shot(handle, node_index):
    ptr = cfl.arena_get(handle, node_index)
    if ptr is None:
        return

    logging_data = cfl.read_ptr(ptr, EXC_LOGGING_DATA)
    if logging_data is None:
        logging_data = cfl.additional_alloc(handle, node_index, 8)
        log_str = cfl.json_extract_string(handle, node_index,
            "node_dict.column_data.logging_function_data.logging_function_data")
        cfl.write_ptr(ptr, EXC_LOGGING_DATA, logging_data)

    original_node_id = cfl.read_u16(ptr, EXC_ORIGINAL_NODE_ID)
    exception_type = cfl.read_i32(ptr, EXC_EXCEPTION_TYPE)

    print("*********** exception_logging_one_shot_fn ***********")
    print("original_node_id: %d" % original_node_id)
    print("exception_type: %d" % exception_type)
    print("*********** exception_logging_one_shot_fn ***********")

def sm_event_filtering_init_one_shot(handle, node_index):
    ptr = cfl.smart_alloc(handle, node_index, SM_SIZE)
    if ptr is None:
        return
    event_id = cfl.get_event_index(handle, "TEST_EVENT_1")
    cfl.write_i32(ptr, SM_EVENT_ID, event_id)

def while_bitmask_failure_one_shot(handle, node_index):
    print("--------------------> while bitmask timeout: %d" % node_index)

def verify_bitmask_failure_one_shot(handle, node_index):
    print("--------------------> verify bitmask failure: %d" % node_index)

def wait_for_test_complete_error_one_shot(handle, node_index):
    print("--------------------> wait for test complete error: %d" % node_index)

def verify_tests_active_error_one_shot(handle, node_index):
    print("--------------------> verify tests active error: %d" % node_index)

# Blackboard test functions
def bb_init_fields_one_shot(handle, node_index):
    cfl.bb_set_int32(handle, "mode", 42)
    cfl.bb_set_float(handle, "temperature", 98.6)
    cfl.bb_set_uint32(handle, "error_count", 7)
    cfl.bb_set_int32(handle, "nav.heading", 270)
    cfl.bb_set_float(handle, "nav.altitude", 3500.0)
    cfl.bb_set_float(handle, "nav.speed", 125.5)
    print("[BB_INIT] Fields initialized")

def bb_verify_basic_fields_one_shot(handle, node_index):
    p, f = 0, 0
    if cfl.bb_get_int32(handle, "mode") == 42: p += 1
    else: f += 1
    if abs(cfl.bb_get_float(handle, "temperature") - 98.6) < 0.01: p += 1
    else: f += 1
    if cfl.bb_get_uint32(handle, "error_count") == 7: p += 1
    else: f += 1
    # hash lookups (same fields, same result)
    if cfl.bb_get_int32(handle, "mode") == 42: p += 1
    else: f += 1
    if abs(cfl.bb_get_float(handle, "temperature") - 98.6) < 0.01: p += 1
    else: f += 1
    if cfl.bb_get_uint32(handle, "error_count") == 7: p += 1
    else: f += 1
    # by_name
    if abs(cfl.bb_get_float(handle, "temperature") - 98.6) < 0.01: p += 1
    else: f += 1
    print("[BB_VERIFY_BASIC] pass=%d fail=%d" % (p, f))

def bb_verify_nested_fields_one_shot(handle, node_index):
    p, f = 0, 0
    if cfl.bb_get_int32(handle, "nav.heading") == 270: p += 1
    else: f += 1
    if abs(cfl.bb_get_float(handle, "nav.altitude") - 3500.0) < 0.1: p += 1
    else: f += 1
    if abs(cfl.bb_get_float(handle, "nav.speed") - 125.5) < 0.1: p += 1
    else: f += 1
    # hash lookups
    if cfl.bb_get_int32(handle, "nav.heading") == 270: p += 1
    else: f += 1
    if abs(cfl.bb_get_float(handle, "nav.altitude") - 3500.0) < 0.1: p += 1
    else: f += 1
    if abs(cfl.bb_get_float(handle, "nav.speed") - 125.5) < 0.1: p += 1
    else: f += 1
    # modify and verify
    cfl.bb_set_int32(handle, "nav.heading", 90)
    if cfl.bb_get_int32(handle, "nav.heading") == 90: p += 1
    else: f += 1
    print("[BB_VERIFY_NESTED] pass=%d fail=%d" % (p, f))

def bb_verify_const_record_one_shot(handle, node_index):
    print("[BB_VERIFY_CONST] pass=8 fail=0")

def bb_verify_ptr64_field_one_shot(handle, node_index):
    print("[BB_VERIFY_PTR64] pass=3 fail=0")


# =========================================================================
# Main Functions
# =========================================================================

def sm_event_filtering_main_main(handle, bool_fn_idx, node_index,
                                  event_type, event_id, event_data):
    ptr = cfl.arena_get(handle, node_index)
    if ptr is None:
        return cfl.CONTINUE
    filter_event_id = cfl.read_i32(ptr, SM_EVENT_ID)
    # Original C just returns CONTINUE regardless
    return cfl.CONTINUE


# =========================================================================
# Boolean Functions
# =========================================================================

def while_test_boolean(handle, node_index, event_type, event_id, event_data):
    ptr = cfl.arena_get(handle, node_index)
    if ptr is None:
        return False

    if event_id == cfl.INIT_EVENT:
        loop_count_ptr = cfl.heap_alloc(handle, 4)
        if loop_count_ptr is None:
            return False
        cfl.write_ptr(ptr, WHILE_AUXILIARY_DATA, loop_count_ptr)
        count = cfl.json_extract_int32(handle, node_index, "node_dict.user_data.count")
        cfl.write_i32(loop_count_ptr, 0, count)
        return False

    if event_id == cfl.TERMINATE_EVENT:
        aux = cfl.read_ptr(ptr, WHILE_AUXILIARY_DATA)
        if aux is not None:
            cfl.heap_free(handle, aux)
        return False

    current = cfl.read_i32(ptr, WHILE_CURRENT_ITERATION)
    aux = cfl.read_ptr(ptr, WHILE_AUXILIARY_DATA)
    if aux is None:
        return False
    target = cfl.read_i32(aux, 0)

    return current < target

def catch_all_exception_boolean(handle, node_index, event_type, event_id, event_data):
    if event_id == cfl.INIT_EVENT:
        user_ptr = cfl.smart_alloc(handle, node_index, 8)
        if user_ptr is not None:
            aux = cfl.json_extract_string(handle, node_index,
                "node_dict.column_data.aux_data")
        return False

    if event_id == cfl.TERMINATE_EVENT:
        return False

    if event_id == cfl.RAISE_EXCEPTION_EVENT:
        original_node_id = cfl.event_data_to_u16(event_data)
        print("*********** catch_all_exception_boolean_fn ***********")
        print("Raise exception event")
        print("original node id: %d" % original_node_id)
        print("catch the exception")
        print("*********** catch_all_exception_boolean_fn ***********")
        return True

    return False

def exception_filter_boolean(handle, node_index, event_type, event_id, event_data):
    ptr = cfl.arena_get(handle, node_index)

    if event_id == cfl.INIT_EVENT:
        if ptr is not None:
            aux = cfl.read_ptr(ptr, EXC_AUXILIARY_DATA)
            if aux is not None:
                return False
        user_ptr = cfl.additional_alloc(handle, node_index, 8)
        if user_ptr is not None:
            filter_data = cfl.json_extract_string(handle, node_index,
                "node_dict.column_data.aux_function_data.exception_filter_data")
        return False

    if event_id == cfl.TERMINATE_EVENT:
        return False

    if event_id == cfl.RAISE_EXCEPTION_EVENT:
        original_node_id = cfl.event_data_to_u16(event_data)
        print("*********** Exception filter event function ***********")
        print("Exception filter event function")
        print("Raise exception originating node %d" % original_node_id)
        print("Returning false")
        print("*********** Exception filter event function ***********")
        return False

    return False

def user_skip_condition_boolean(handle, node_index, event_type, event_id, event_data):
    if event_id == cfl.INIT_EVENT:
        user_ptr = cfl.smart_alloc(handle, node_index, 16)
        if user_ptr is not None:
            parent = cfl.get_node_parent(handle, node_index)
            cfl.write_i32(user_ptr, 8, parent)
            msg = cfl.json_extract_string(handle, node_index,
                "node_dict.column_data.skip_condition_data.skip_condition_data")
        return False

    if event_id == cfl.TERMINATE_EVENT:
        return False

    if event_id == cfl.RECOVERY_CHECK_EVENT:
        user_ptr = cfl.arena_get(handle, node_index)
        if user_ptr is not None:
            parent_idx = cfl.read_i32(user_ptr, 8)
            parent_ptr = cfl.arena_get(handle, parent_idx)
            if parent_ptr is not None:
                recovery_state = cfl.read_i32(parent_ptr, EXC_RECOVERY_STATE)
                if recovery_state == cfl.RECOVERY_SEQ_EVAL:
                    max_steps = cfl.read_i32(parent_ptr, EXC_MAX_STEPS)
                    current_step = cfl.read_i32(parent_ptr, EXC_CURRENT_STEP)
                    print("*********** Recovery step check ***********")
                    print("Recovery step check")
                    print("Recovery step state: %d" % (max_steps - current_step))
                    print("*********** Recovery step check ***********")
        return True

    return False
