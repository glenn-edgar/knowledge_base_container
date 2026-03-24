# ==========================================================================
# main_mpy.py
# MicroPython test harness for basic_primitive_test
#
# Port of main.lua (trigger_on_change test) to MicroPython runtime.
#
# Usage:
#   micropython main_mpy.py
# ==========================================================================

import sys
sys.path.insert(0, "../../mpy_runtime")

import se_runtime
import se_builtins_all
import basic_primitive_test_module_mpy as module_data
import user_functions_mpy as user_module

# ==========================================================================
# Shared state (mirrors C globals g_bitmap / g_trigger_events)
# ==========================================================================
ctx = {
    "bitmap": 0,
    "trigger_events": 0,
}

def reset_trigger_events():
    ctx["trigger_events"] = 0

def get_trigger_events():
    return ctx["trigger_events"]

# ==========================================================================
# Module load
# ==========================================================================
def load_module():
    mod = se_runtime.new_module(module_data, se_builtins_all.builtins)
    user_module.register(mod)

    ok, missing = se_runtime.validate_module(mod)
    if not ok:
        lines = []
        for kind, name in missing:
            lines.append("  [%s] %s" % (kind, name))
        return None, "missing functions:\n" + "\n".join(lines)

    return mod, None

# ==========================================================================
# Tick helper
# ==========================================================================
def do_tick(inst):
    return se_runtime.tick_once(inst, se_runtime.SE_EVENT_TICK, None)

# ==========================================================================
# Test: trigger_on_change
# ==========================================================================
def test_trigger_on_change(mod, tree_name):
    print("\n=== Test Trigger On Change ===")

    inst = se_runtime.new_instance(mod, tree_name)
    inst["user_ctx"] = ctx

    ctx["bitmap"] = 0
    ctx["trigger_events"] = 0

    test_pass = True

    def check(label, cond, msg):
        nonlocal test_pass
        if not cond:
            print("  FAIL %s" % msg)
            test_pass = False

    def check_not(label, cond, msg):
        nonlocal test_pass
        if cond:
            print("  FAIL %s" % msg)
            test_pass = False

    # --- Initial tick ---
    print("\n--- Initial tick (bitmap=0x%08X) ---" % ctx["bitmap"])
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())

    # --- Test 1: Set bit 0 -> BIT0_RISE ---
    prev = ctx["bitmap"]
    ctx["bitmap"] |= (1 << 0)
    print("\n--- Set bit 0 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t1", (get_trigger_events() & user_module.EVENT_BIT0_RISE) != 0, "Expected BIT0_RISE")

    # --- Test 2: Clear bit 0 -> BIT0_FALL ---
    prev = ctx["bitmap"]
    ctx["bitmap"] &= ~(1 << 0)
    print("\n--- Clear bit 0 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t2", (get_trigger_events() & user_module.EVENT_BIT0_FALL) != 0, "Expected BIT0_FALL")

    # --- Test 3: Set bit 1 only -> AND not yet true ---
    prev = ctx["bitmap"]
    ctx["bitmap"] |= (1 << 1)
    print("\n--- Set bit 1 only (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check_not("t3", (get_trigger_events() & user_module.EVENT_BITS12_RISE) != 0,
        "Unexpected BITS12_RISE (only bit1 set)")

    # --- Test 4: Set bit 2 -> AND now true, BITS12_RISE ---
    prev = ctx["bitmap"]
    ctx["bitmap"] |= (1 << 2)
    print("\n--- Set bit 2 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t4", (get_trigger_events() & user_module.EVENT_BITS12_RISE) != 0, "Expected BITS12_RISE")

    # --- Test 5: Clear bit 1 -> AND false, BITS12_FALL ---
    prev = ctx["bitmap"]
    ctx["bitmap"] &= ~(1 << 1)
    print("\n--- Clear bit 1 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t5", (get_trigger_events() & user_module.EVENT_BITS12_FALL) != 0, "Expected BITS12_FALL")

    # --- Test 6: Set bit 3 -> OR true, BITS34_RISE ---
    prev = ctx["bitmap"]
    ctx["bitmap"] |= (1 << 3)
    print("\n--- Set bit 3 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t6", (get_trigger_events() & user_module.EVENT_BITS34_RISE) != 0, "Expected BITS34_RISE")

    # --- Test 7: Set bit 4 also -> OR still true, no new edge ---
    prev = ctx["bitmap"]
    ctx["bitmap"] |= (1 << 4)
    print("\n--- Set bit 4 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check_not("t7",
        (get_trigger_events() & (user_module.EVENT_BITS34_RISE | user_module.EVENT_BITS34_FALL)) != 0,
        "Unexpected BITS34 event (OR still true)")

    # --- Test 8: Clear bit 3 -> OR still true via bit 4 ---
    prev = ctx["bitmap"]
    ctx["bitmap"] &= ~(1 << 3)
    print("\n--- Clear bit 3 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check_not("t8",
        (get_trigger_events() & (user_module.EVENT_BITS34_RISE | user_module.EVENT_BITS34_FALL)) != 0,
        "Unexpected BITS34 event (OR still true via bit4)")

    # --- Test 9: Clear bit 4 -> OR now false, BITS34_FALL ---
    prev = ctx["bitmap"]
    ctx["bitmap"] &= ~(1 << 4)
    print("\n--- Clear bit 4 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t9", (get_trigger_events() & user_module.EVENT_BITS34_FALL) != 0, "Expected BITS34_FALL")

    # --- Test 10: Set bit 5 -> NOT becomes false, BIT5_SET ---
    prev = ctx["bitmap"]
    ctx["bitmap"] |= (1 << 5)
    print("\n--- Set bit 5 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t10", (get_trigger_events() & user_module.EVENT_BIT5_SET) != 0,
        "Expected BIT5_SET (NOT became false)")

    # --- Test 11: Clear bit 5 -> NOT becomes true, BIT5_CLEAR ---
    prev = ctx["bitmap"]
    ctx["bitmap"] &= ~(1 << 5)
    print("\n--- Clear bit 5 (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t11", (get_trigger_events() & user_module.EVENT_BIT5_CLEAR) != 0,
        "Expected BIT5_CLEAR (NOT became true)")

    # --- Test 12: Set bit 0 AGAIN -> BIT0_RISE repeated ---
    prev = ctx["bitmap"]
    ctx["bitmap"] |= (1 << 0)
    print("\n--- Set bit 0 AGAIN (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t12", (get_trigger_events() & user_module.EVENT_BIT0_RISE) != 0,
        "Expected BIT0_RISE on repeated trigger")

    # --- Test 13: Clear bit 0 AGAIN -> BIT0_FALL repeated ---
    prev = ctx["bitmap"]
    ctx["bitmap"] &= ~(1 << 0)
    print("\n--- Clear bit 0 AGAIN (bitmap=0x%08X -> 0x%08X) ---" % (prev, ctx["bitmap"]))
    reset_trigger_events()
    do_tick(inst)
    print("  events fired: 0x%02X" % get_trigger_events())
    check("t13", (get_trigger_events() & user_module.EVENT_BIT0_FALL) != 0,
        "Expected BIT0_FALL on repeated trigger")

    # --- Test 14: Rapid toggle (3 cycles) ---
    print("\n--- Rapid toggle bit 0 (3 cycles) ---")
    for cycle in range(1, 4):
        # Rise
        ctx["bitmap"] |= (1 << 0)
        reset_trigger_events()
        do_tick(inst)
        check("t14-rise", (get_trigger_events() & user_module.EVENT_BIT0_RISE) != 0,
            "Cycle %d: Expected BIT0_RISE" % cycle)

        # Fall
        ctx["bitmap"] &= ~(1 << 0)
        reset_trigger_events()
        do_tick(inst)
        check("t14-fall", (get_trigger_events() & user_module.EVENT_BIT0_FALL) != 0,
            "Cycle %d: Expected BIT0_FALL" % cycle)
    print("  Completed 3 toggle cycles")

    # --- Summary ---
    if test_pass:
        print("\n  PASSED: All edge triggers working correctly")
    else:
        print("\n  FAILED: Some edge triggers failed")

    return test_pass

# ==========================================================================
# MAIN
# ==========================================================================
print()
print("================================================================")
print("    S-EXPRESSION ENGINE TRIGGER ON CHANGE TEST (MicroPython)")
print("================================================================")
print()

print("=== Loading module ===\n")

mod, err = load_module()
if mod is None:
    print("FATAL: Failed to load module: %s" % err)
    sys.exit(1)
print("Module loaded successfully")

tree_name = module_data.tree_order[0]
result = test_trigger_on_change(mod, tree_name)

print("\nAll tests completed!\n")
if not result:
    sys.exit(1)
