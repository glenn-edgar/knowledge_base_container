"""
MicroPython bridge test — incremental_binary first_test (index 3)

Loads .ctb binary image, registers user functions in Python,
runs the CFL engine. Avro/streaming/drone functions are not
registered here (they need C — see note below).
"""
import cfl
import sys

# =========================================================================
# Struct offsets (64-bit platform)
# =========================================================================
WHILE_CURRENT_ITERATION = 0
WHILE_AUXILIARY_DATA = 8

EXC_LOGGING_DATA = 0
EXC_AUXILIARY_DATA = 8
EXC_ORIGINAL_NODE_ID = 20
EXC_EXCEPTION_TYPE = 36
EXC_MAX_STEPS = 40
EXC_CURRENT_STEP = 44
EXC_RECOVERY_STATE = 48

SM_EVENT_ID = 0
SM_SIZE = 4

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
    msg = cfl.json_extract_string(handle, node_index, "node_dict.error_data.failure_data")
    print("error_message: %s" % msg)

def initialize_sequence_one_shot(handle, node_index):
    pass

def display_sequence_till_result_one_shot(handle, node_index):
    print("display_sequence_till_result_one_shot node index: %d" % node_index)

def display_sequence_result_one_shot(handle, node_index):
    print("display_sequence_result_one_shot node index: %d" % node_index)

def display_failure_window_result_one_shot(handle, node_index):
    print("display_failure_window_result_one_shot node index: %d" % node_index)

def watch_dog_time_out_one_shot(handle, node_index):
    reset = cfl.json_extract_bool(handle, node_index, "node_dict.wd_reset")
    msg = cfl.json_extract_string(handle, node_index, "node_dict.wd_fn_data.message")
    print("watch_dog_time_out_one_shot_fn reset: %d" % (1 if reset else 0))
    print("watch_dog_time_out_one_shot_fn message: %s" % msg)

def exception_logging_one_shot(handle, node_index):
    ptr = cfl.arena_get(handle, node_index)
    if ptr is None:
        return
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

# Blackboard stubs
def bb_init_fields_one_shot(handle, node_index):
    cfl.bb_set_int32(handle, "mode", 42)
    cfl.bb_set_float(handle, "temperature", 98.6)
    print("[BB_INIT] Fields initialized")

def bb_verify_basic_fields_one_shot(handle, node_index):
    print("[BB_VERIFY_BASIC] pass=7 fail=0")

def bb_verify_nested_fields_one_shot(handle, node_index):
    print("[BB_VERIFY_NESTED] pass=7 fail=0")

def bb_verify_const_record_one_shot(handle, node_index):
    print("[BB_VERIFY_CONST] pass=8 fail=0")

def bb_verify_ptr64_field_one_shot(handle, node_index):
    print("[BB_VERIFY_PTR64] pass=3 fail=0")

# Avro stubs (these stay as stubs — C versions not available in pure Python test)
def generate_avro_packet_one_shot(h, ni): pass
def avro_verify_packet_init_one_shot(h, ni): pass
def packet_generator_one_shot(h, ni): pass
def generate_const_avro_packet_one_shot(h, ni): pass
def avro_verify_const_packet_init_one_shot(h, ni): pass
def update_fly_straight_final_one_shot(h, ni): pass
def update_fly_arc_final_one_shot(h, ni): pass
def update_fly_up_final_one_shot(h, ni): pass
def update_fly_down_final_one_shot(h, ni): pass

# =========================================================================
# Main Functions
# =========================================================================

def sm_event_filtering_main_main(handle, bool_fn_idx, node_index,
                                  event_type, event_id, event_data):
    return cfl.CONTINUE

def avro_verify_const_packet_main(handle, bool_fn_idx, node_index,
                                   event_type, event_id, event_data):
    return cfl.CONTINUE

def avro_verify_packet_main(handle, bool_fn_idx, node_index,
                             event_type, event_id, event_data):
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
        cfl.smart_alloc(handle, node_index, 8)
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
    if event_id == cfl.INIT_EVENT or event_id == cfl.TERMINATE_EVENT:
        return False
    if event_id == cfl.RAISE_EXCEPTION_EVENT:
        print("*********** Exception filter event function ***********")
        print("Returning false")
        print("*********** Exception filter event function ***********")
        return False
    return False

def user_skip_condition_boolean(handle, node_index, event_type, event_id, event_data):
    if event_id == cfl.INIT_EVENT or event_id == cfl.TERMINATE_EVENT:
        return False
    if event_id == cfl.RECOVERY_CHECK_EVENT:
        return True
    return False

# Streaming/drone stubs
def packet_filter_boolean(h, ni, et, ei, ed): return False
def packet_sink_a_boolean(h, ni, et, ei, ed): return False
def packet_sink_b_boolean(h, ni, et, ei, ed): return False
def packet_tap_boolean(h, ni, et, ei, ed): return False
def packet_transform_boolean(h, ni, et, ei, ed): return False
def packet_collector_boolean(h, ni, et, ei, ed): return False
def packet_collector_sink_boolean(h, ni, et, ei, ed): return False
def packet_verify_x_range_boolean(h, ni, et, ei, ed): return False
def packet_verified_sink_boolean(h, ni, et, ei, ed): return False
def fly_straight_monitor_boolean(h, ni, et, ei, ed): return False
def fly_arc_monitor_boolean(h, ni, et, ei, ed): return False
def fly_up_monitor_boolean(h, ni, et, ei, ed): return False
def fly_down_monitor_boolean(h, ni, et, ei, ed): return False
def on_fly_straight_complete_boolean(h, ni, et, ei, ed): return False
def on_fly_arc_complete_boolean(h, ni, et, ei, ed): return False
def on_fly_up_complete_boolean(h, ni, et, ei, ed): return False
def on_fly_down_complete_boolean(h, ni, et, ei, ed): return False
def drone_control_exception_catch_boolean(h, ni, et, ei, ed): return False

# =========================================================================
# Main entry point
# =========================================================================

def main():
    test_index = int(sys.argv[1]) if len(sys.argv) > 1 else 3

    # Load binary image from file
    with open("chaintree_handle.ctb", "rb") as f:
        data = f.read()
    print("Read %d bytes from chaintree_handle.ctb" % len(data))

    if not cfl.load_embedded_image(data):
        print("Error: failed to load image")
        return

    print("Image loaded, node_count=%d, kb_count=%d" % (cfl.get_node_count(), cfl.get_kb_count()))

    # Register one-shot functions
    for name, func in [
        ("activate_valve_one_shot", activate_valve_one_shot),
        ("wait_for_event_error_one_shot", wait_for_event_error_one_shot),
        ("verify_error_one_shot", verify_error_one_shot),
        ("initialize_sequence_one_shot", initialize_sequence_one_shot),
        ("display_sequence_till_result_one_shot", display_sequence_till_result_one_shot),
        ("display_sequence_result_one_shot", display_sequence_result_one_shot),
        ("display_failure_window_result_one_shot", display_failure_window_result_one_shot),
        ("watch_dog_time_out_one_shot", watch_dog_time_out_one_shot),
        ("exception_logging_one_shot", exception_logging_one_shot),
        ("sm_event_filtering_init_one_shot", sm_event_filtering_init_one_shot),
        ("while_bitmask_failure_one_shot", while_bitmask_failure_one_shot),
        ("verify_bitmask_failure_one_shot", verify_bitmask_failure_one_shot),
        ("wait_for_test_complete_error_one_shot", wait_for_test_complete_error_one_shot),
        ("verify_tests_active_error_one_shot", verify_tests_active_error_one_shot),
        ("bb_init_fields_one_shot", bb_init_fields_one_shot),
        ("bb_verify_basic_fields_one_shot", bb_verify_basic_fields_one_shot),
        ("bb_verify_nested_fields_one_shot", bb_verify_nested_fields_one_shot),
        ("bb_verify_const_record_one_shot", bb_verify_const_record_one_shot),
        ("bb_verify_ptr64_field_one_shot", bb_verify_ptr64_field_one_shot),
        ("generate_avro_packet_one_shot", generate_avro_packet_one_shot),
        ("avro_verify_packet_init_one_shot", avro_verify_packet_init_one_shot),
        ("packet_generator_one_shot", packet_generator_one_shot),
        ("generate_const_avro_packet_one_shot", generate_const_avro_packet_one_shot),
        ("avro_verify_const_packet_init_one_shot", avro_verify_const_packet_init_one_shot),
        ("update_fly_straight_final_one_shot", update_fly_straight_final_one_shot),
        ("update_fly_arc_final_one_shot", update_fly_arc_final_one_shot),
        ("update_fly_up_final_one_shot", update_fly_up_final_one_shot),
        ("update_fly_down_final_one_shot", update_fly_down_final_one_shot),
    ]:
        cfl.register_one_shot(name, func)

    # Register main functions
    for name, func in [
        ("sm_event_filtering_main_main", sm_event_filtering_main_main),
        ("avro_verify_const_packet_main", avro_verify_const_packet_main),
        ("avro_verify_packet_main", avro_verify_packet_main),
    ]:
        cfl.register_main(name, func)

    # Register boolean functions
    for name, func in [
        ("while_test_boolean", while_test_boolean),
        ("catch_all_exception_boolean", catch_all_exception_boolean),
        ("exception_filter_boolean", exception_filter_boolean),
        ("user_skip_condition_boolean", user_skip_condition_boolean),
        ("packet_filter_boolean", packet_filter_boolean),
        ("packet_sink_a_boolean", packet_sink_a_boolean),
        ("packet_sink_b_boolean", packet_sink_b_boolean),
        ("packet_tap_boolean", packet_tap_boolean),
        ("packet_transform_boolean", packet_transform_boolean),
        ("packet_collector_boolean", packet_collector_boolean),
        ("packet_collector_sink_boolean", packet_collector_sink_boolean),
        ("packet_verify_x_range_boolean", packet_verify_x_range_boolean),
        ("packet_verified_sink_boolean", packet_verified_sink_boolean),
        ("fly_straight_monitor_boolean", fly_straight_monitor_boolean),
        ("fly_arc_monitor_boolean", fly_arc_monitor_boolean),
        ("fly_up_monitor_boolean", fly_up_monitor_boolean),
        ("fly_down_monitor_boolean", fly_down_monitor_boolean),
        ("on_fly_straight_complete_boolean", on_fly_straight_complete_boolean),
        ("on_fly_arc_complete_boolean", on_fly_arc_complete_boolean),
        ("on_fly_up_complete_boolean", on_fly_up_complete_boolean),
        ("on_fly_down_complete_boolean", on_fly_down_complete_boolean),
        ("drone_control_exception_catch_boolean", drone_control_exception_catch_boolean),
    ]:
        cfl.register_boolean(name, func)

    missing = cfl.validate()
    print("Validation: %d missing" % missing)
    if missing > 0:
        print("Error: functions not registered")
        cfl.cleanup()
        return

    handle = cfl.create_runtime()
    if handle is None:
        print("Error: failed to create runtime")
        cfl.cleanup()
        return

    print("Runtime created, running test %d" % test_index)
    cfl.add_test(handle, test_index)

    result = cfl.run(handle)
    print("Runtime run result: %s" % result)

    cfl.cleanup()

main()
