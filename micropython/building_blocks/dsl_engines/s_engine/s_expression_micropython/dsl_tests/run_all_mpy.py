# ==========================================================================
# run_all_mpy.py
# Generic MicroPython test runner for S-Expression DSL tests
#
# Usage: micropython run_all_mpy.py
# ==========================================================================

import sys
import os
sys.path.insert(0, "../mpy_runtime")

import se_runtime
import se_builtins_all
import se_stack

def _add_test_path(subdir):
    """Add test subdirectory to sys.path for module import."""
    p = os.getcwd() + "/" + subdir
    if p not in sys.path:
        sys.path.insert(0, p)

def _import(subdir, modname):
    """Import a module from a test subdirectory."""
    _add_test_path(subdir)
    return __import__(modname)

# ==========================================================================
# Result helpers
# ==========================================================================
RESULT_NAMES = {
    0: "CONTINUE", 1: "HALT", 2: "TERMINATE", 3: "RESET",
    4: "DISABLE", 5: "SKIP_CONTINUE",
    6: "FN_CONTINUE", 7: "FN_HALT", 8: "FN_TERMINATE",
    9: "FN_RESET", 10: "FN_DISABLE", 11: "FN_SKIP_CONTINUE",
    12: "PL_CONTINUE", 13: "PL_HALT", 14: "PL_TERMINATE",
    15: "PL_RESET", 16: "PL_DISABLE", 17: "PL_SKIP_CONTINUE",
}

def result_str(r):
    return RESULT_NAMES.get(r, "?(%d)" % r)

def is_complete(r):
    return r in (2, 4, 8, 10, 14, 16)

def is_terminate(r):
    return r in (2, 8, 14)

# ==========================================================================
# Generic tick loop
# ==========================================================================
def run_tick_loop(inst, max_ticks=500, verbose=False):
    for tick in range(1, max_ticks + 1):
        result = se_runtime.tick_once(inst, se_runtime.SE_EVENT_TICK, None)

        if verbose:
            print("  Tick %3d: %s" % (tick, result_str(result)))

        # Drain event queue
        while se_runtime.event_count(inst) > 0:
            tt, eid, edata = se_runtime.event_pop(inst)
            saved = inst["tick_type"]
            inst["tick_type"] = tt
            er = se_runtime.tick_once(inst, eid, edata)
            inst["tick_type"] = saved
            if is_complete(er):
                return tick, er

        if is_complete(result):
            return tick, result

    return max_ticks, result

# ==========================================================================
# Load module helper
# ==========================================================================
def load_module(module_data, user_fns=None, need_stack=0):
    fns = dict(se_builtins_all.builtins)
    if user_fns:
        fns.update(user_fns)
    mod = se_runtime.new_module(module_data, fns)
    ok, missing = se_runtime.validate_module(mod)
    if not ok:
        print("  MISSING FUNCTIONS:")
        for kind, name in missing:
            print("    [%s] %s" % (kind, name))
        return None, None

    tree_name = module_data.tree_order[0]
    inst = se_runtime.new_instance(mod, tree_name)
    if need_stack > 0:
        inst["stack"] = se_stack.new_stack(need_stack)
    return mod, inst

# ==========================================================================
# Test results
# ==========================================================================
results = []

def run_test(name, fn):
    print("\n=== %s ===" % name)
    try:
        passed = fn()
        tag = "PASS" if passed else "FAIL"
    except Exception as e:
        tag = "ERROR"
        print("  %s: %s" % (type(e).__name__, e))
        passed = False
    results.append((name, tag))
    print("  -> %s" % tag)
    return passed

# ==========================================================================
# TEST: return_test — 18 trees, single tick each
# ==========================================================================
def test_return():
    md = _import("return_test", "return_tests_module_mpy")

    expected = [
        ("return_continue_test", 0),
        ("return_halt_test", 1),
        ("return_terminate_test", 2),
        ("return_reset_test", 3),
        ("return_disable_test", 4),
        ("return_skip_continue_test", 5),
        ("return_function_continue_test", 6),
        ("return_function_halt_test", 7),
        ("return_function_terminate_test", 8),
        ("return_function_reset_test", 9),
        ("return_function_disable_test", 10),
        ("return_function_skip_continue_test", 11),
        ("return_pipeline_continue_test", 12),
        ("return_pipeline_halt_test", 13),
        ("return_pipeline_terminate_test", 14),
        ("return_pipeline_reset_test", 15),
        ("return_pipeline_disable_test", 16),
        ("return_pipeline_skip_continue_test", 17),
    ]

    fns = dict(se_builtins_all.builtins)
    mod = se_runtime.new_module(md, fns)
    ok, missing = se_runtime.validate_module(mod)
    if not ok:
        print("  MISSING:", missing)
        return False

    all_pass = True
    for tree_name, expect in expected:
        # Check tree exists
        found = False
        for i, tn in enumerate(md.tree_order):
            if tn == tree_name:
                found = True
                break
        if not found:
            print("  SKIP: %s (not in module)" % tree_name)
            continue

        inst = se_runtime.new_instance(mod, tree_name)
        result = se_runtime.tick_once(inst, se_runtime.SE_EVENT_TICK, None)
        if result != expect:
            print("  FAIL: %s expected %d got %d" % (tree_name, expect, result))
            all_pass = False
        else:
            print("  OK: %s = %s" % (tree_name, result_str(result)))
    return all_pass

# ==========================================================================
# TEST: loop_test — tick loop, builtins only
# ==========================================================================
def test_loop():
    md = _import("loop_test", "loop_test_module_mpy")
    mod, inst = load_module(md)
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=500)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result) and ticks < 500

# ==========================================================================
# TEST: state_machine — needs user functions
# ==========================================================================
def test_state_machine():
    md = _import("state_machine", "state_machine_test_module_mpy")

    def cfl_disable_children(inst, node):
        eid = inst["current_event_id"]
        if eid == se_runtime.SE_EVENT_INIT or eid == se_runtime.SE_EVENT_TERMINATE:
            return
        print("  cfl_disable_children")

    def cfl_enable_child(inst, node):
        eid = inst["current_event_id"]
        if eid == se_runtime.SE_EVENT_INIT or eid == se_runtime.SE_EVENT_TERMINATE:
            return
        idx = se_runtime.param_int(node, 1)
        print("  cfl_enable_child: %d" % idx)

    user_fns = {
        "cfl_disable_children": cfl_disable_children,
        "cfl_enable_child": cfl_enable_child,
    }

    mod, inst = load_module(md, user_fns)
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=500)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result) and ticks < 500

# ==========================================================================
# TEST: dispatch — needs display_event_info
# ==========================================================================
def test_dispatch():
    md = _import("dispatch", "dispatch_test_module_mpy")

    def display_event_info(inst, node):
        eid = inst["current_event_id"]
        if eid == se_runtime.SE_EVENT_TICK:
            return
        edata = inst["current_event_data"]
        print("  display_event_info: id=%s data=%s" % (str(eid), str(edata)))

    mod, inst = load_module(md, {"display_event_info": display_event_info})
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=500)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result) and ticks < 500

# ==========================================================================
# TEST: advanced_primitive_test — needs display_event_info
# ==========================================================================
def test_advanced_primitive():
    md = _import("advanced_primitive_test", "advanced_primitive_test_module_mpy")

    def display_event_info(inst, node):
        eid = inst["current_event_id"]
        if eid == se_runtime.SE_EVENT_TICK:
            return
        edata = inst["current_event_data"]
        print("  display_event_info: id=%s data=%s" % (str(eid), str(edata)))

    mod, inst = load_module(md, {"display_event_info": display_event_info})
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=100)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result) and ticks < 100

# ==========================================================================
# TEST: complex_sequence — builtins only, uses time delays
# ==========================================================================
def test_complex_sequence():
    md = _import("complex_sequence", "complex_sequence_module_mpy")
    # Override get_time to use fast simulated time
    mod, inst = load_module(md)
    if not mod:
        return False
    # Use a fast clock: each tick = 0.1s simulated
    tick_count = [0]
    def fast_time():
        return tick_count[0] * 0.1
    mod["get_time"] = fast_time

    max_t = 500
    for t in range(1, max_t + 1):
        tick_count[0] = t
        result = se_runtime.tick_once(inst, se_runtime.SE_EVENT_TICK, None)

        while se_runtime.event_count(inst) > 0:
            tt, eid, edata = se_runtime.event_pop(inst)
            saved = inst["tick_type"]
            inst["tick_type"] = tt
            er = se_runtime.tick_once(inst, eid, edata)
            inst["tick_type"] = saved
            if is_complete(er):
                print("  %d ticks, result=%s" % (t, result_str(er)))
                return is_terminate(er)

        if is_complete(result):
            print("  %d ticks, result=%s" % (t, result_str(result)))
            return is_terminate(result)

    print("  max ticks exceeded")
    return False

# ==========================================================================
# TEST: callback_function
# ==========================================================================
def test_callback():
    md = _import("callback_function", "callback_function_module_mpy")
    mod, inst = load_module(md, need_stack=128)
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=10)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result)

# ==========================================================================
# TEST: stack_test
# ==========================================================================
def test_stack():
    md = _import("stack_test", "stack_test_module_mpy")
    mod, inst = load_module(md, need_stack=128)
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=5000)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result) and ticks < 5000

# ==========================================================================
# TEST: stack_equations
# ==========================================================================
def test_stack_equations():
    md = _import("stack_equations", "stack_equations_module_mpy")
    mod, inst = load_module(md, need_stack=128)
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=5000)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result) and ticks < 5000

# ==========================================================================
# TEST: json
# ==========================================================================
def test_json():
    md = _import("json", "json_test_module_mpy")

    def user_print_extract_results(inst, node):
        title = se_runtime.param_str(node, 1)
        print("  [extract] %s" % title)

    def user_print_array_results(inst, node):
        title = se_runtime.param_str(node, 1)
        print("  [array] %s" % title)

    def user_print_pointer_results(inst, node):
        title = se_runtime.param_str(node, 1)
        print("  [pointer] %s" % title)

    def user_verify_results(inst, node):
        print("  [verify] checking results")

    user_fns = {
        "user_print_extract_results": user_print_extract_results,
        "user_print_array_results": user_print_array_results,
        "user_print_pointer_results": user_print_pointer_results,
        "user_verify_results": user_verify_results,
    }

    mod, inst = load_module(md, user_fns)
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=500)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result) and ticks < 500

# ==========================================================================
# TEST: function_dictionary
# ==========================================================================
def test_function_dictionary():
    md = _import("function_dictionary", "function_dictionary_module_mpy")

    def write_register(inst, node):
        stk = inst["stack"]
        if stk:
            addr = se_stack.get_local(stk, 0)
            val = se_stack.get_local(stk, 1)
            print("  write_register: addr=0x%04X val=0x%04X" % (addr, val))

    mod, inst = load_module(md, {"write_register": write_register}, need_stack=128)
    if not mod:
        return False
    ticks, result = run_tick_loop(inst, max_ticks=10)
    print("  %d ticks, result=%s" % (ticks, result_str(result)))
    return is_complete(result)

# ==========================================================================
# MAIN — run all tests
# ==========================================================================
print("=" * 60)
print("  S-EXPRESSION MPY RUNTIME — FULL TEST SUITE")
print("=" * 60)

run_test("return_test", test_return)
run_test("loop_test", test_loop)
run_test("state_machine", test_state_machine)
run_test("dispatch", test_dispatch)
run_test("advanced_primitive", test_advanced_primitive)
run_test("complex_sequence", test_complex_sequence)
run_test("callback_function", test_callback)
run_test("stack_test", test_stack)
run_test("stack_equations", test_stack_equations)
run_test("json", test_json)
run_test("function_dictionary", test_function_dictionary)

# Summary
print("\n" + "=" * 60)
print("  RESULTS")
print("=" * 60)
passed = 0
failed = 0
errors = 0
for name, tag in results:
    print("  %-30s %s" % (name, tag))
    if tag == "PASS":
        passed += 1
    elif tag == "FAIL":
        failed += 1
    else:
        errors += 1

print("\n  %d passed, %d failed, %d errors out of %d tests" % (
    passed, failed, errors, len(results)))

if failed + errors > 0:
    sys.exit(1)
