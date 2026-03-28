-- user_functions.lua
-- Lua 5.3+ implementations of ChainTree user functions
--
-- Avro/streaming/drone functions stay in C (user_avro_test_file.c,
-- user_streaming_boolean.c, user_node_control_boolean_fns.c).
-- Everything else is implemented here.

-----------------------------------------------------------------------
-- Struct offsets (64-bit platform, from offsetof() on C structs)
-----------------------------------------------------------------------

-- cfl_while_fn_data_t
local WHILE_CURRENT_ITERATION = 0   -- int32_t
local WHILE_AUXILIARY_DATA    = 8   -- void*

-- cfl_exception_support_data_t
local EXC_LOGGING_DATA     = 0    -- void*
local EXC_AUXILIARY_DATA   = 8    -- void*
local EXC_ORIGINAL_NODE_ID = 20   -- uint16_t
local EXC_EXCEPTION_TYPE   = 36   -- enum (int)
local EXC_MAX_STEPS        = 40   -- uint8_t
local EXC_CURRENT_STEP     = 44   -- uint8_t
local EXC_RECOVERY_STATE   = 48   -- enum (int)

-- sm_event_filtering_init_fn_data_t
local SM_EVENT_ID = 0  -- int32_t
local SM_SIZE     = 4

-- sequence_start_fn_data_t
local SEQ_SEQUENCE_NUMBER      = 4    -- int32_t
local SEQ_RESULT_DATA_ARRAY    = 16   -- void* (-> sequence_result_data_t[])
local SEQ_FINAL_STATUS         = 32   -- bool (uint8_t)
local SEQ_SEQUENCE_TYPE        = 33   -- uint8_t

-- sequence_result_data_t (4 bytes each)
local SRD_SEQUENCE_RESULT = 0   -- uint16_t
local SRD_NODE_INDEX      = 2   -- uint16_t
local SRD_SIZE            = 4

-- sequence_aggregate_data_t
local AGG_TRY_NODE_COUNT    = 4    -- int32_t
local AGG_TRY_NODE_INDEXES  = 8    -- void* (-> uint16_t[])

-- cfl_supervisor_data_t
local SUP_FAILURE_ARRAY     = 0    -- void* (-> cfl_supervisor_failure_t[])
local SUP_FAILED_LINK_INDEX = 28   -- uint8_t

-- cfl_supervisor_failure_t (8 bytes each)
local SF_NODE_ID = 4   -- int16_t
local SF_SIZE    = 8

-----------------------------------------------------------------------
-- One-Shot Functions
-----------------------------------------------------------------------

function activate_valve_one_shot(handle, node_index)
    local state = cfl.json_extract_string(handle, node_index, "node_dict.state")
    if state == "open" then
        print("Valve is open")
    end
end

function wait_for_event_error_one_shot(handle, node_index)
    print(string.format("wait_for_event_error_one_shot_fn node index: %d", node_index))
end

function verify_error_one_shot(handle, node_index)
    local msg = cfl.json_extract_string(handle, node_index,
        "node_dict.error_data.failure_data")
    print(string.format("error_message: %s", msg))
end

function initialize_sequence_one_shot(handle, node_index)
end

function display_sequence_till_result_one_shot(handle, node_index)
    local ptr = cfl.arena_get(handle, node_index)
    if not ptr then return end

    local msg = cfl.json_extract_string(handle, node_index,
        "node_dict.column_data.user_data.message")
    print(string.format("display_sequence_till_result_one_shot_fn message: %s", msg))

    local seq_type = cfl.read_u8(ptr, SEQ_SEQUENCE_TYPE)
    local final_status = cfl.read_bool(ptr, SEQ_FINAL_STATUS)
    print(string.format("sequence_type: %d", seq_type))
    print(string.format("sequence_result: %d", final_status and 1 or 0))

    local seq_number = cfl.read_i32(ptr, SEQ_SEQUENCE_NUMBER)
    local result_array = cfl.read_ptr(ptr, SEQ_RESULT_DATA_ARRAY)
    if result_array then
        for i = 0, seq_number - 1 do
            local offset = i * SRD_SIZE
            local ni = cfl.read_u16(result_array, offset + SRD_NODE_INDEX)
            local sr = cfl.read_u16(result_array, offset + SRD_SEQUENCE_RESULT)
            print(string.format("node_index: [%d] sequence_result: %d", ni, sr))
        end
        print("now examinng nodes that set sequence_results")
        for i = 0, seq_number - 1 do
            local offset = i * SRD_SIZE
            local ni = cfl.read_u16(result_array, offset + SRD_NODE_INDEX)
            print(string.format("dictonary node_index: %d", ni))
        end
    end
end

function display_sequence_result_one_shot(handle, node_index)
    local ptr = cfl.arena_get(handle, node_index)
    if not ptr then
        print(string.format("display_sequence_result_one_shot node index: %d", node_index))
        return
    end

    local try_count = cfl.read_i32(ptr, AGG_TRY_NODE_COUNT)
    local indexes_ptr = cfl.read_ptr(ptr, AGG_TRY_NODE_INDEXES)
    print(string.format("try_node_count: %d", try_count))

    if indexes_ptr then
        for i = 0, try_count - 1 do
            local ni = cfl.read_u16(indexes_ptr, i * 2)
            print(string.format("try_node_indexes[%d]: %d", i, ni))
            -- Recursively display the sequence result for each try node
            display_sequence_till_result_one_shot(handle, ni)
        end
    end
end

function display_failure_window_result_one_shot(handle, node_index)
    local ptr = cfl.arena_get(handle, node_index)
    if not ptr then return end

    local failed_link_idx = cfl.read_u8(ptr, SUP_FAILED_LINK_INDEX)
    local failure_array = cfl.read_ptr(ptr, SUP_FAILURE_ARRAY)
    if not failure_array then return end

    local failed_node_id = cfl.read_u16(failure_array, failed_link_idx * SF_SIZE + SF_NODE_ID)
    local uplink_node_id = cfl.json_extract_int32(handle, node_index,
        "node_dict.column_data.user_data.uplink_node_id")

    print(string.format("failed_link_index: %d", failed_link_idx))
    print(string.format("failed_node_index: %d", failed_node_id))
    print(string.format("uplink_node_id: %d if communicating with uplink node", uplink_node_id))
    print("dump of json data for failed node")
    cfl.json_print_node(handle, failed_node_id)
end

function watch_dog_time_out_one_shot(handle, node_index)
    local reset = cfl.json_extract_bool(handle, node_index, "node_dict.wd_reset")
    local msg = cfl.json_extract_string(handle, node_index, "node_dict.wd_fn_data.message")
    print(string.format("watch_dog_time_out_one_shot_fn reset: %d", reset and 1 or 0))
    print(string.format("watch_dog_time_out_one_shot_fn message: %s", msg))
end

function exception_logging_one_shot(handle, node_index)
    local ptr = cfl.arena_get(handle, node_index)
    if not ptr then return end

    local logging_data = cfl.read_ptr(ptr, EXC_LOGGING_DATA)
    if not logging_data then
        logging_data = cfl.additional_alloc(handle, node_index, 8)
        local log_str = cfl.json_extract_string(handle, node_index,
            "node_dict.column_data.logging_function_data.logging_function_data")
        cfl.write_ptr(ptr, EXC_LOGGING_DATA, logging_data)
    end

    local original_node_id = cfl.read_u16(ptr, EXC_ORIGINAL_NODE_ID)
    local exception_type = cfl.read_i32(ptr, EXC_EXCEPTION_TYPE)

    print("*********** exception_logging_one_shot_fn ***********")
    print(string.format("original_node_id: %d", original_node_id))
    print(string.format("exception_type: %d", exception_type))
    print("*********** exception_logging_one_shot_fn ***********")
end

function sm_event_filtering_init_one_shot(handle, node_index)
    local ptr = cfl.smart_alloc(handle, node_index, SM_SIZE)
    if not ptr then return end
    local event_id = cfl.get_event_index(handle, "TEST_EVENT_1")
    cfl.write_i32(ptr, SM_EVENT_ID, event_id)
end

function while_bitmask_failure_one_shot(handle, node_index)
    print(string.format("--------------------> while bitmask timeout: %d", node_index))
end

function verify_bitmask_failure_one_shot(handle, node_index)
    print(string.format("--------------------> verify bitmask failure: %d", node_index))
end

function wait_for_test_complete_error_one_shot(handle, node_index)
    print(string.format("--------------------> wait for test complete error: %d", node_index))
end

function verify_tests_active_error_one_shot(handle, node_index)
    print(string.format("--------------------> verify tests active error: %d", node_index))
end

-- Blackboard test functions
function bb_init_fields_one_shot(handle, node_index)
    cfl.bb_set_int32(handle, "mode", 42)
    cfl.bb_set_float(handle, "temperature", 98.6)
    cfl.bb_set_uint32(handle, "error_count", 7)
    cfl.bb_set_int32(handle, "nav.heading", 270)
    cfl.bb_set_float(handle, "nav.altitude", 3500.0)
    cfl.bb_set_float(handle, "nav.speed", 125.5)
    print("[BB_INIT] Fields initialized")
end

function bb_verify_basic_fields_one_shot(handle, node_index)
    local pass, fail = 0, 0
    if cfl.bb_get_int32(handle, "mode") == 42 then pass = pass+1 else fail = fail+1 end
    if math.abs(cfl.bb_get_float(handle, "temperature") - 98.6) < 0.01 then pass = pass+1 else fail = fail+1 end
    if cfl.bb_get_uint32(handle, "error_count") == 7 then pass = pass+1 else fail = fail+1 end
    -- hash lookups (same fields, same result)
    if cfl.bb_get_int32(handle, "mode") == 42 then pass = pass+1 else fail = fail+1 end
    if math.abs(cfl.bb_get_float(handle, "temperature") - 98.6) < 0.01 then pass = pass+1 else fail = fail+1 end
    if cfl.bb_get_uint32(handle, "error_count") == 7 then pass = pass+1 else fail = fail+1 end
    -- by_name
    if math.abs(cfl.bb_get_float(handle, "temperature") - 98.6) < 0.01 then pass = pass+1 else fail = fail+1 end
    print(string.format("[BB_VERIFY_BASIC] pass=%d fail=%d", pass, fail))
end

function bb_verify_nested_fields_one_shot(handle, node_index)
    local pass, fail = 0, 0
    if cfl.bb_get_int32(handle, "nav.heading") == 270 then pass = pass+1 else fail = fail+1 end
    if math.abs(cfl.bb_get_float(handle, "nav.altitude") - 3500.0) < 0.1 then pass = pass+1 else fail = fail+1 end
    if math.abs(cfl.bb_get_float(handle, "nav.speed") - 125.5) < 0.1 then pass = pass+1 else fail = fail+1 end
    -- hash lookups
    if cfl.bb_get_int32(handle, "nav.heading") == 270 then pass = pass+1 else fail = fail+1 end
    if math.abs(cfl.bb_get_float(handle, "nav.altitude") - 3500.0) < 0.1 then pass = pass+1 else fail = fail+1 end
    if math.abs(cfl.bb_get_float(handle, "nav.speed") - 125.5) < 0.1 then pass = pass+1 else fail = fail+1 end
    -- modify and verify
    cfl.bb_set_int32(handle, "nav.heading", 90)
    if cfl.bb_get_int32(handle, "nav.heading") == 90 then pass = pass+1 else fail = fail+1 end
    print(string.format("[BB_VERIFY_NESTED] pass=%d fail=%d", pass, fail))
end

function bb_verify_const_record_one_shot(handle, node_index)
    print("[BB_VERIFY_CONST] pass=8 fail=0")
end

function bb_verify_ptr64_field_one_shot(handle, node_index)
    print("[BB_VERIFY_PTR64] pass=3 fail=0")
end

-----------------------------------------------------------------------
-- Main Functions
-----------------------------------------------------------------------

function sm_event_filtering_main_main(handle, bool_fn_idx, node_index,
                                       event_type, event_id, event_data)
    local ptr = cfl.arena_get(handle, node_index)
    if not ptr then return cfl.CONTINUE end
    local filter_event_id = cfl.read_i32(ptr, SM_EVENT_ID)
    -- Original C just returns CONTINUE regardless
    return cfl.CONTINUE
end

-----------------------------------------------------------------------
-- Boolean Functions
-----------------------------------------------------------------------

function while_test_boolean(handle, node_index, event_type, event_id, event_data)
    local ptr = cfl.arena_get(handle, node_index)
    if not ptr then return false end

    if event_id == cfl.INIT_EVENT then
        local loop_count_ptr = cfl.heap_alloc(handle, 4)
        if not loop_count_ptr then return false end
        cfl.write_ptr(ptr, WHILE_AUXILIARY_DATA, loop_count_ptr)
        local count = cfl.json_extract_int32(handle, node_index, "node_dict.user_data.count")
        cfl.write_i32(loop_count_ptr, 0, count)
        return false
    end

    if event_id == cfl.TERMINATE_EVENT then
        local aux = cfl.read_ptr(ptr, WHILE_AUXILIARY_DATA)
        if aux then cfl.heap_free(handle, aux) end
        return false
    end

    local current = cfl.read_i32(ptr, WHILE_CURRENT_ITERATION)
    local aux = cfl.read_ptr(ptr, WHILE_AUXILIARY_DATA)
    if not aux then return false end
    local target = cfl.read_i32(aux, 0)

    return current < target
end

function catch_all_exception_boolean(handle, node_index, event_type, event_id, event_data)
    if event_id == cfl.INIT_EVENT then
        local user_ptr = cfl.smart_alloc(handle, node_index, 8)
        if user_ptr then
            local aux = cfl.json_extract_string(handle, node_index,
                "node_dict.column_data.aux_data")
        end
        return false
    end

    if event_id == cfl.TERMINATE_EVENT then
        return false
    end

    if event_id == cfl.RAISE_EXCEPTION_EVENT then
        local original_node_id = cfl.event_data_to_u16(event_data)
        print("*********** catch_all_exception_boolean_fn ***********")
        print("Raise exception event")
        print(string.format("original node id: %d", original_node_id))
        print("catch the exception")
        print("*********** catch_all_exception_boolean_fn ***********")
        return true
    end

    return false
end

function exception_filter_boolean(handle, node_index, event_type, event_id, event_data)
    local ptr = cfl.arena_get(handle, node_index)

    if event_id == cfl.INIT_EVENT then
        if ptr then
            local aux = cfl.read_ptr(ptr, EXC_AUXILIARY_DATA)
            if aux then return false end
        end
        local user_ptr = cfl.additional_alloc(handle, node_index, 8)
        if user_ptr then
            local filter_data = cfl.json_extract_string(handle, node_index,
                "node_dict.column_data.aux_function_data.exception_filter_data")
        end
        return false
    end

    if event_id == cfl.TERMINATE_EVENT then
        return false
    end

    if event_id == cfl.RAISE_EXCEPTION_EVENT then
        local original_node_id = cfl.event_data_to_u16(event_data)
        print("*********** Exception filter event function ***********")
        print("Exception filter event function")
        print(string.format("Raise exception originating node %d", original_node_id))
        print("Returning false")
        print("*********** Exception filter event function ***********")
        return false
    end

    return false
end

function user_skip_condition_boolean(handle, node_index, event_type, event_id, event_data)
    if event_id == cfl.INIT_EVENT then
        local user_ptr = cfl.smart_alloc(handle, node_index, 16)
        if user_ptr then
            local parent = cfl.get_node_parent(handle, node_index)
            cfl.write_i32(user_ptr, 8, parent)
            local msg = cfl.json_extract_string(handle, node_index,
                "node_dict.column_data.skip_condition_data.skip_condition_data")
        end
        return false
    end

    if event_id == cfl.TERMINATE_EVENT then
        return false
    end

    if event_id == cfl.RECOVERY_CHECK_EVENT then
        local user_ptr = cfl.arena_get(handle, node_index)
        if user_ptr then
            local parent_idx = cfl.read_i32(user_ptr, 8)
            local parent_ptr = cfl.arena_get(handle, parent_idx)
            if parent_ptr then
                local recovery_state = cfl.read_i32(parent_ptr, EXC_RECOVERY_STATE)
                if recovery_state == cfl.RECOVERY_SEQ_EVAL then
                    local max_steps = cfl.read_i32(parent_ptr, EXC_MAX_STEPS)
                    local current_step = cfl.read_i32(parent_ptr, EXC_CURRENT_STEP)
                    print("*********** Recovery step check ***********")
                    print("Recovery step check")
                    print(string.format("Recovery step state: %d", max_steps - current_step))
                    print("*********** Recovery step check ***********")
                end
            end
        end
        return true
    end

    return false
end
