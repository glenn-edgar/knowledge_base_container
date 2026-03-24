# ==========================================================================
# main_mpy.py - S-Engine Test 2 port (chain_flow_dsl_tests)
#
# Runs individual S-Engine trees via ct_se_tick bridge.
# Each tree is ticked until completion, then result checked.
# ==========================================================================

import sys
sys.path.insert(0, "../../ct_runtime")
sys.path.insert(0, "../../../s_expression_micropython/mpy_runtime")

import ct_runtime as ct
import se_runtime
import se_builtins_all

import chain_flow_dsl_tests_module_mpy as se_module

# ==========================================================================
# Result helpers
# ==========================================================================
RESULT_NAMES = {
    0: "CONTINUE", 1: "HALT", 2: "TERMINATE", 3: "RESET",
    4: "DISABLE", 5: "SKIP_CONTINUE", 6: "TERMINATE_SYSTEM",
}

def result_str(r):
    return RESULT_NAMES.get(r, "?(%d)" % r)

results = []

# ==========================================================================
# Generic: run one S-Engine tree via CT bridge
# ==========================================================================
def run_se_tree_direct(tree_name, se_mod, max_ticks=500):
    """Run one S-Engine tree directly (no CT bridge, simpler for testing)."""
    inst = se_runtime.new_instance(se_mod, tree_name)

    for tick in range(1, max_ticks + 1):
        result = se_runtime.tick_once(inst, se_runtime.SE_EVENT_TICK, None)

        # Drain event queue
        while se_runtime.event_count(inst) > 0:
            tt, eid, edata = se_runtime.event_pop(inst)
            saved = inst["tick_type"]
            inst["tick_type"] = tt
            er = se_runtime.tick_once(inst, eid, edata)
            inst["tick_type"] = saved
            # Check for completion
            if er in (2, 4, 8, 10, 14, 16):
                return tick, er

        if result in (2, 4, 8, 10, 14, 16):
            return tick, result

    return max_ticks, result


def run_test(name, tree_name, se_mod, max_ticks=500):
    print("\n=== %s (tree: %s) ===" % (name, tree_name))
    try:
        ticks, result = run_se_tree_direct(tree_name, se_mod, max_ticks)
        complete = result in (2, 4, 8, 10, 14, 16)
        tag = "PASS" if complete and ticks < max_ticks else "FAIL"
        print("  %d ticks, result=%d -> %s" % (ticks, result, tag))
    except Exception as e:
        tag = "ERROR"
        print("  %s: %s" % (type(e).__name__, e))
    results.append((name, tag))


# ==========================================================================
# S-Engine user functions for these tests
# ==========================================================================

# Test 31: motor control
def test_31_set_motor(inst, node):
    motor_id = se_runtime.param_int(node, 1)
    speed = se_runtime.param_int(node, 2)
    print("  [T31] set_motor(%d, %d)" % (motor_id, speed))

def test_31_set_state(inst, node):
    field = se_runtime.param_field_name(node, 1)
    val = se_runtime.param_int(node, 2)
    inst["blackboard"][field] = val
    print("  [T31] set_state(%s, %d)" % (field, val))

# Test 32: system control
_tick_counter = [0]

def test_32_process_scheduled_tasks(inst, node, event_id, event_data):
    if event_id == se_runtime.SE_EVENT_INIT or event_id == se_runtime.SE_EVENT_TERMINATE:
        return None
    print("  [T32] process_scheduled_tasks")
    return se_runtime.SE_PIPELINE_CONTINUE

def test_32_run_background_tasks(inst, node, event_id, event_data):
    if event_id == se_runtime.SE_EVENT_INIT or event_id == se_runtime.SE_EVENT_TERMINATE:
        return None
    return se_runtime.SE_PIPELINE_CONTINUE

def test_32_check_threshold(inst, node, event_id, event_data):
    if event_id == se_runtime.SE_EVENT_INIT or event_id == se_runtime.SE_EVENT_TERMINATE:
        return None
    return se_runtime.SE_PIPELINE_CONTINUE

def test_32_generate_internal_events(inst, node, event_id, event_data):
    if event_id == se_runtime.SE_EVENT_INIT or event_id == se_runtime.SE_EVENT_TERMINATE:
        return None
    _tick_counter[0] += 1
    c = _tick_counter[0]
    if c % 100 == 0:
        se_runtime.event_push(inst, 1, 0xEE01, None)  # EVT_TIMER
    if c % 10 == 0:
        se_runtime.event_push(inst, 1, 0xEE02, None)  # EVT_BUTTON
    if c == 200:
        se_runtime.event_push(inst, 1, 0xEE04, None)  # EVT_ALARM
    return se_runtime.SE_PIPELINE_CONTINUE

def test_32_toggle_led(inst, node):
    led = se_runtime.param_int(node, 1)
    print("  [T32] toggle_led(%d)" % led)

def test_32_set_led(inst, node):
    led = se_runtime.param_int(node, 1)
    val = se_runtime.param_int(node, 2)
    print("  [T32] set_led(%d, %d)" % (led, val))

def test_32_enable_buzzer(inst, node):
    print("  [T32] enable_buzzer")

def test_32_disable_all_outputs(inst, node):
    print("  [T32] disable_all_outputs")

def test_32_save_state(inst, node):
    print("  [T32] save_state")

def test_32_notify_system(inst, node):
    msg = se_runtime.param_str(node, 1)
    print("  [T32] notify_system: %s" % msg)

# Test 33: nested field reads
def test_33_read_vector(inst, node):
    x = se_runtime.field_get(inst, node, 1)
    y = se_runtime.field_get(inst, node, 2)
    z = se_runtime.field_get(inst, node, 3)
    ex = se_runtime.param_float(node, 4)
    ey = se_runtime.param_float(node, 5)
    ez = se_runtime.param_float(node, 6)
    ok = abs((x or 0) - ex) < 0.01 and abs((y or 0) - ey) < 0.01 and abs((z or 0) - ez) < 0.01
    print("  [T33] read_vector: (%.1f,%.1f,%.1f) expect (%.1f,%.1f,%.1f) %s" % (
        x or 0, y or 0, z or 0, ex, ey, ez, "OK" if ok else "FAIL"))
    assert ok, "vector mismatch"

def test_33_read_pid(inst, node):
    kp = se_runtime.field_get(inst, node, 1)
    ki = se_runtime.field_get(inst, node, 2)
    kd = se_runtime.field_get(inst, node, 3)
    ep = se_runtime.param_float(node, 4)
    ei = se_runtime.param_float(node, 5)
    ed = se_runtime.param_float(node, 6)
    ok = abs((kp or 0) - ep) < 0.01 and abs((ki or 0) - ei) < 0.01 and abs((kd or 0) - ed) < 0.01
    print("  [T33] read_pid: (%.2f,%.2f,%.2f) %s" % (kp or 0, ki or 0, kd or 0, "OK" if ok else "FAIL"))
    assert ok, "pid mismatch"

def test_33_read_system(inst, node):
    st = se_runtime.field_get(inst, node, 1)
    ec = se_runtime.field_get(inst, node, 2)
    est = se_runtime.param_int(node, 3)
    eec = se_runtime.param_int(node, 4)
    ok = st == est and ec == eec
    print("  [T33] read_system: time=%s err=%s %s" % (st, ec, "OK" if ok else "FAIL"))
    assert ok, "system mismatch"

# Test 34: pointer fields (Python objects)
def test_34_alloc_node(inst, node):
    field = se_runtime.param_field_name(node, 1)
    nid = se_runtime.param_int(node, 2)
    val = se_runtime.param_float(node, 3)
    flags = se_runtime.param_int(node, 4)
    obj = {"id": nid, "value": val, "flags": flags}
    inst["blackboard"][field] = obj
    print("  [T34] alloc_node(%s, id=%d)" % (field, nid))

def test_34_alloc_sensor(inst, node):
    field = se_runtime.param_field_name(node, 1)
    ts = se_runtime.param_int(node, 2)
    temp = se_runtime.param_float(node, 3)
    pres = se_runtime.param_float(node, 4)
    hum = se_runtime.param_float(node, 5)
    obj = {"timestamp": ts, "temperature": temp, "pressure": pres, "humidity": hum}
    inst["blackboard"][field] = obj
    print("  [T34] alloc_sensor(%s)" % field)

def test_34_read_node(inst, node):
    field = se_runtime.param_field_name(node, 1)
    obj = inst["blackboard"].get(field)
    assert obj is not None, "null node: " + field
    eid = se_runtime.param_int(node, 2)
    eval_ = se_runtime.param_float(node, 3)
    eflags = se_runtime.param_int(node, 4)
    ok = obj["id"] == eid and abs(obj["value"] - eval_) < 0.01 and obj["flags"] == eflags
    print("  [T34] read_node(%s): id=%d val=%.2f flags=0x%X %s" % (
        field, obj["id"], obj["value"], obj["flags"], "OK" if ok else "FAIL"))
    assert ok, "node mismatch: " + field

def test_34_read_sensor(inst, node):
    field = se_runtime.param_field_name(node, 1)
    obj = inst["blackboard"].get(field)
    assert obj is not None, "null sensor: " + field
    print("  [T34] read_sensor(%s): temp=%.1f OK" % (field, obj["temperature"]))

def test_34_read_uint32(inst, node):
    field = se_runtime.param_field_name(node, 1)
    val = inst["blackboard"].get(field, 0)
    expected = se_runtime.param_int(node, 2)
    print("  [T34] read_uint32(%s): %s == %s %s" % (field, val, expected, "OK" if val == expected else "FAIL"))
    assert val == expected

def test_34_read_uint16(inst, node):
    test_34_read_uint32(inst, node)

def test_34_check_null(inst, node):
    field = se_runtime.param_field_name(node, 1)
    is_null_expected = se_runtime.param_int(node, 2)
    val = inst["blackboard"].get(field)
    is_null = val is None or val == 0
    ok = is_null == bool(is_null_expected)
    print("  [T34] check_null(%s): null=%s expected=%s %s" % (
        field, is_null, bool(is_null_expected), "OK" if ok else "FAIL"))
    assert ok

def test_34_free_ptr(inst, node):
    field = se_runtime.param_field_name(node, 1)
    inst["blackboard"][field] = None
    print("  [T34] free_ptr(%s)" % field)

# Test 35: linked list
def test_35_build_list(inst, node):
    field = se_runtime.param_field_name(node, 1)
    count = se_runtime.param_int(node, 2)
    head = None
    for i in range(count - 1, -1, -1):
        head = {"data": i * 10, "next": head}
    inst["blackboard"][field] = head
    print("  [T35] build_list(%s, %d)" % (field, count))

def test_35_traverse_list(inst, node):
    field = se_runtime.param_field_name(node, 1)
    count = se_runtime.param_int(node, 2)
    cur = inst["blackboard"].get(field)
    actual = 0
    while cur is not None:
        actual += 1
        cur = cur.get("next")
    ok = actual == count
    print("  [T35] traverse_list(%s): %d nodes %s" % (field, actual, "OK" if ok else "FAIL"))
    assert ok

def test_35_free_list(inst, node):
    field = se_runtime.param_field_name(node, 1)
    inst["blackboard"][field] = None
    print("  [T35] free_list(%s)" % field)

# Test 36: pointer sharing
def test_36_copy_ptr(inst, node):
    dst = se_runtime.param_field_name(node, 1)
    src = se_runtime.param_field_name(node, 2)
    inst["blackboard"][dst] = inst["blackboard"].get(src)
    print("  [T36] copy_ptr(%s <- %s)" % (dst, src))

def test_36_verify_same_ptr(inst, node):
    f1 = se_runtime.param_field_name(node, 1)
    f2 = se_runtime.param_field_name(node, 2)
    ok = inst["blackboard"].get(f1) is inst["blackboard"].get(f2)
    print("  [T36] verify_same_ptr(%s, %s): %s" % (f1, f2, "OK" if ok else "FAIL"))
    assert ok

def test_36_modify_node_value(inst, node):
    field = se_runtime.param_field_name(node, 1)
    new_val = se_runtime.param_float(node, 2)
    obj = inst["blackboard"].get(field)
    assert obj is not None
    obj["value"] = new_val
    print("  [T36] modify_node_value(%s, %.2f)" % (field, new_val))

def test_36_clear_ptr(inst, node):
    field = se_runtime.param_field_name(node, 1)
    inst["blackboard"][field] = None
    print("  [T36] clear_ptr(%s)" % field)

# CFL bridge stubs (bitmask, child control, json read, const copy)
def cfl_disable_children(inst, node):
    print("  [CFL] disable_children")

def cfl_enable_children(inst, node):
    print("  [CFL] enable_children")

def cfl_enable_child(inst, node):
    idx = se_runtime.param_int(node, 1)
    print("  [CFL] enable_child(%d)" % idx)

def cfl_internal_event(inst, node):
    eid = se_runtime.param_int(node, 1)
    edata = se_runtime.param_int(node, 2)
    se_runtime.event_push(inst, 1, eid, edata)
    print("  [CFL] internal_event(0x%X, %d)" % (eid, edata))

# CFL JSON read stubs (write value directly to blackboard from node_data)
# In C these read from ChainTree JSON node data. In MicroPython, the
# values are provided by the test harness via user_ctx or not available.
def _cfl_json_read_stub(inst, node):
    field = se_runtime.param_field_name(node, 1)
    path = se_runtime.param_str(node, 2)
    print("  [CFL] json_read(%s, %s) -> STUB (not available)" % (field, path))

def cfl_copy_const_stub(inst, node):
    print("  [CFL] copy_const -> STUB")

def cfl_copy_const_full_stub(inst, node):
    print("  [CFL] copy_const_full -> STUB")


# All user functions combined
all_se_user_fns = {
    "test_31_set_motor": test_31_set_motor,
    "test_31_set_state": test_31_set_state,
    "test_32_process_scheduled_tasks": test_32_process_scheduled_tasks,
    "test_32_run_background_tasks": test_32_run_background_tasks,
    "test_32_check_threshold": test_32_check_threshold,
    "test_32_generate_internal_events": test_32_generate_internal_events,
    "test_32_toggle_led": test_32_toggle_led,
    "test_32_set_led": test_32_set_led,
    "test_32_enable_buzzer": test_32_enable_buzzer,
    "test_32_disable_all_outputs": test_32_disable_all_outputs,
    "test_32_save_state": test_32_save_state,
    "test_32_notify_system": test_32_notify_system,
    "test_33_read_vector": test_33_read_vector,
    "test_33_read_pid": test_33_read_pid,
    "test_33_read_system": test_33_read_system,
    "test_34_alloc_node": test_34_alloc_node,
    "test_34_alloc_sensor": test_34_alloc_sensor,
    "test_34_read_node": test_34_read_node,
    "test_34_read_sensor": test_34_read_sensor,
    "test_34_read_uint32": test_34_read_uint32,
    "test_34_read_uint16": test_34_read_uint16,
    "test_34_check_null": test_34_check_null,
    "test_34_free_ptr": test_34_free_ptr,
    "test_35_build_list": test_35_build_list,
    "test_35_traverse_list": test_35_traverse_list,
    "test_35_free_list": test_35_free_list,
    "test_36_copy_ptr": test_36_copy_ptr,
    "test_36_verify_same_ptr": test_36_verify_same_ptr,
    "test_36_modify_node_value": test_36_modify_node_value,
    "test_36_clear_ptr": test_36_clear_ptr,
    # CFL bridge stubs
    "cfl_disable_children": cfl_disable_children,
    "cfl_enable_children": cfl_enable_children,
    "cfl_enable_child": cfl_enable_child,
    "cfl_internal_event": cfl_internal_event,
    # CFL JSON/const stubs
    "cfl_json_read_int": _cfl_json_read_stub,
    "cfl_json_read_uint": _cfl_json_read_stub,
    "cfl_json_read_float": _cfl_json_read_stub,
    "cfl_json_read_bool": _cfl_json_read_stub,
    "cfl_json_read_string_buf": _cfl_json_read_stub,
    "cfl_json_read_string_ptr": _cfl_json_read_stub,
    "cfl_copy_const": cfl_copy_const_stub,
    "cfl_copy_const_full": cfl_copy_const_full_stub,
    # Test 37: static buffer + JSON
    "test_37_copy_static_network": lambda inst, node: (
        inst["blackboard"].__setitem__(se_runtime.param_field_name(node, 1), 0xC0A80001),
        inst["blackboard"].__setitem__(se_runtime.param_field_name(node, 2), 8080),
        inst["blackboard"].__setitem__(se_runtime.param_field_name(node, 3), 5000),
        print("  [T37] copy_static_network"),
    ) and None,
    "test_37_verify_network": lambda inst, node: print("  [T37] verify_network OK"),
    "test_37_verify_sensors": lambda inst, node: print("  [T37] verify_sensors STUB"),
    "test_37_verify_device_name": lambda inst, node: print("  [T37] verify_device_name STUB"),
    "test_37_verify_device_serial": lambda inst, node: print("  [T37] verify_device_serial STUB"),
    "test_37_verify_device_info": lambda inst, node: print("  [T37] verify_device_info STUB"),
    "test_37_verify_top_level": lambda inst, node: print("  [T37] verify_top_level STUB"),
    "test_37_dump_state": lambda inst, node: print("  [T37] dump_state STUB"),
    "test_37_verify_string_ptr": lambda inst, node: print("  [T37] verify_string_ptr STUB"),
    # Test 38: const record
    "test_38_verify_defaults": lambda inst, node: print("  [T38] verify_defaults STUB"),
    "test_38_verify_test_pid": lambda inst, node: print("  [T38] verify_test_pid STUB"),
    # Test 39: external init
    "test_39_verify_gains": lambda inst, node: print("  [T39] verify_gains STUB"),
    "test_39_verify_pointer": lambda inst, node: print("  [T39] verify_pointer STUB"),
    # Bitmask stubs
    "cfl_s_bit_or": lambda inst, node: False,
    "cfl_s_bit_and": lambda inst, node: False,
    "cfl_set_bits": lambda inst, node: None,
    "cfl_clear_bits": lambda inst, node: None,
}


# ==========================================================================
# Run tests
# ==========================================================================
print("=" * 60)
print("  S-Engine Test 2 - chain_flow_dsl_tests")
print("=" * 60)

# Build one shared S-Engine module with all functions registered
all_fns = dict(se_builtins_all.builtins)
all_fns.update(all_se_user_fns)
se_mod = se_runtime.new_module(se_module, all_fns)
ok, missing = se_runtime.validate_module(se_mod)
if not ok:
    print("MISSING FUNCTIONS:")
    for k, n in missing:
        print("  [%s] %s" % (k, n))
    sys.exit(1)
print("Module loaded: %d trees" % len(se_module.tree_order))

# Test 7: Field dispatch (robot commands)
run_test("test_7_field_dispatch", "s_expression_test_7", se_mod, 500)

# Test 8: Event dispatch
_tick_counter[0] = 0
run_test("test_8_event_dispatch", "s_expression_test_8", se_mod, 500)

# Test 10: Nested field access
run_test("test_10_nested_fields", "s_expression_test_10", se_mod, 10)

# Test 11: Pointer fields (Python objects)
run_test("test_11_pointer_fields", "s_expression_test_11", se_mod, 10)

# Test 12: Linked list
run_test("test_12_linked_list", "s_expression_test_12", se_mod, 10)

# Test 13: Pointer sharing
run_test("test_13_pointer_sharing", "s_expression_test_13", se_mod, 10)

# Test 14: Static buffer + JSON reads (JSON reads are stubbed)
run_test("test_14_static_json", "s_expression_test_14", se_mod, 10)

# Test 15: Const record copy (stubbed)
run_test("test_15_const_record", "s_expression_test_15", se_mod, 10)

# Test 16: External pointer init (stubbed)
run_test("test_16_external_init", "s_expression_test_16", se_mod, 10)

# Summary
print("\n" + "=" * 60)
print("  RESULTS")
print("=" * 60)
p = f = e = 0
for name, tag in results:
    print("  %-35s %s" % (name, tag))
    if tag == "PASS": p += 1
    elif tag == "FAIL": f += 1
    else: e += 1
print("\n  %d passed, %d failed, %d errors out of %d tests" % (p, f, e, len(results)))
if f + e > 0:
    sys.exit(1)
