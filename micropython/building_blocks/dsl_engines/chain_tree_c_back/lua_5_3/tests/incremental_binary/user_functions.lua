-- user_functions.lua
-- Lua implementations of ChainTree user functions (via cfl_lua53_bridge)
--
-- For the first_test (index 3), only these are actually called:
--   activate_valve_one_shot
--   wait_for_event_error_one_shot
--   verify_error_one_shot
--
-- All other functions are stubs that will be fleshed out as we add tests.

-- The "cfl" module is pre-loaded by cfl_lua_bridge_init() in C.
-- It provides: cfl.json_extract_string(), cfl.json_extract_int32(), etc.
-- Return codes: cfl.CONTINUE, cfl.HALT, cfl.TERMINATE, cfl.RESET
-- Event IDs: cfl.INIT_EVENT, cfl.TERMINATE_EVENT, cfl.RAISE_EXCEPTION_EVENT

-- =========================================================================
-- One-Shot Functions (void return — called with handle, node_index)
-- =========================================================================

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
    local error_message = cfl.json_extract_string(handle, node_index,
        "node_dict.error_data.failure_data")
    print(string.format("error_message: %s", error_message))
end

function initialize_sequence_one_shot(handle, node_index)
    -- stub
end

function display_sequence_till_result_one_shot(handle, node_index)
    print(string.format("display_sequence_till_result_one_shot node index: %d", node_index))
end

function display_sequence_result_one_shot(handle, node_index)
    print(string.format("display_sequence_result_one_shot node index: %d", node_index))
end

function display_failure_window_result_one_shot(handle, node_index)
    print(string.format("display_failure_window_result_one_shot node index: %d", node_index))
end

function watch_dog_time_out_one_shot(handle, node_index)
    print(string.format("watch_dog_time_out_one_shot node index: %d", node_index))
end

function exception_logging_one_shot(handle, node_index)
    print("*********** exception_logging_one_shot ***********")
end

function sm_event_filtering_init_one_shot(handle, node_index)
    -- stub: would allocate arena data for event filtering
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

-- Avro/streaming stubs (not used in first_test)
function generate_avro_packet_one_shot(handle, node_index) end
function avro_verify_packet_init_one_shot(handle, node_index) end
function packet_generator_one_shot(handle, node_index) end
function generate_const_avro_packet_one_shot(handle, node_index) end
function avro_verify_const_packet_init_one_shot(handle, node_index) end
function update_fly_straight_final_one_shot(handle, node_index) end
function update_fly_arc_final_one_shot(handle, node_index) end
function update_fly_up_final_one_shot(handle, node_index) end
function update_fly_down_final_one_shot(handle, node_index) end

-- Blackboard test stubs (not used in first_test)
function bb_init_fields_one_shot(handle, node_index) end
function bb_verify_basic_fields_one_shot(handle, node_index) end
function bb_verify_nested_fields_one_shot(handle, node_index) end
function bb_verify_const_record_one_shot(handle, node_index) end
function bb_verify_ptr64_field_one_shot(handle, node_index) end

-- =========================================================================
-- Main Functions (return CFL code — called with handle, bool_fn_idx,
--   node_index, event_type, event_id, event_data)
-- =========================================================================

function sm_event_filtering_main_main(handle, bool_fn_idx, node_index,
                                       event_type, event_id, event_data)
    -- stub: event filtering main
    return cfl.CONTINUE
end

function avro_verify_const_packet_main(handle, bool_fn_idx, node_index,
                                        event_type, event_id, event_data)
    return cfl.CONTINUE
end

function avro_verify_packet_main(handle, bool_fn_idx, node_index,
                                  event_type, event_id, event_data)
    return cfl.CONTINUE
end

-- =========================================================================
-- Boolean Functions (return bool — called with handle, node_index,
--   event_type, event_id, event_data)
-- =========================================================================

function while_test_boolean(handle, node_index, event_type, event_id, event_data)
    return true
end

function catch_all_exception_boolean(handle, node_index, event_type, event_id, event_data)
    if event_id == cfl.INIT_EVENT then
        return false
    end
    if event_id == cfl.TERMINATE_EVENT then
        return false
    end
    if event_id == cfl.RAISE_EXCEPTION_EVENT then
        print("*********** catch_all_exception_boolean ***********")
        print("Raise exception event")
        print("catch the exception")
        print("*********** catch_all_exception_boolean ***********")
        return true
    end
    return false
end

function exception_filter_boolean(handle, node_index, event_type, event_id, event_data)
    if event_id == cfl.INIT_EVENT or event_id == cfl.TERMINATE_EVENT then
        return false
    end
    if event_id == cfl.RAISE_EXCEPTION_EVENT then
        print("*********** Exception filter event function ***********")
        print("Returning false")
        print("*********** Exception filter event function ***********")
        return false
    end
    return false
end

function user_skip_condition_boolean(handle, node_index, event_type, event_id, event_data)
    if event_id == cfl.INIT_EVENT or event_id == cfl.TERMINATE_EVENT then
        return false
    end
    return true
end

-- Streaming/drone stubs (not used in first_test)
function packet_filter_boolean(h, ni, et, ei, ed) return false end
function packet_sink_a_boolean(h, ni, et, ei, ed) return false end
function packet_sink_b_boolean(h, ni, et, ei, ed) return false end
function packet_tap_boolean(h, ni, et, ei, ed) return false end
function packet_transform_boolean(h, ni, et, ei, ed) return false end
function packet_collector_boolean(h, ni, et, ei, ed) return false end
function packet_collector_sink_boolean(h, ni, et, ei, ed) return false end
function packet_verify_x_range_boolean(h, ni, et, ei, ed) return false end
function packet_verified_sink_boolean(h, ni, et, ei, ed) return false end
function fly_straight_monitor_boolean(h, ni, et, ei, ed) return false end
function fly_arc_monitor_boolean(h, ni, et, ei, ed) return false end
function fly_up_monitor_boolean(h, ni, et, ei, ed) return false end
function fly_down_monitor_boolean(h, ni, et, ei, ed) return false end
function on_fly_straight_complete_boolean(h, ni, et, ei, ed) return false end
function on_fly_arc_complete_boolean(h, ni, et, ei, ed) return false end
function on_fly_up_complete_boolean(h, ni, et, ei, ed) return false end
function on_fly_down_complete_boolean(h, ni, et, ei, ed) return false end
function drone_control_exception_catch_boolean(h, ni, et, ei, ed) return false end
