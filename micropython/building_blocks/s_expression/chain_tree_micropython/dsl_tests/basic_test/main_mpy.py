# ==========================================================================
# main_mpy.py - ChainTree MicroPython basic test
# ==========================================================================

import sys
sys.path.insert(0, "../../ct_runtime")

import ct_runtime as ct
import basic_test_module_mpy as module_data

# Result names for printing
RESULT_NAMES = {
    0: "CONTINUE", 1: "HALT", 2: "TERMINATE", 3: "RESET",
    4: "DISABLE", 5: "SKIP_CONTINUE", 6: "TERMINATE_SYSTEM",
}

def result_str(r):
    return RESULT_NAMES.get(r, "?(%d)" % r)

def is_complete(r):
    return r in (2, 4, 6)  # TERMINATE, DISABLE, TERMINATE_SYSTEM

# ==========================================================================
# Load and run
# ==========================================================================
print("=" * 50)
print("  ChainTree MicroPython - Basic Test")
print("=" * 50)
print()

# Create module with builtins
mod = ct.new_module(module_data, ct.builtins)

ok, missing = ct.validate_module(mod)
if not ok:
    print("MISSING FUNCTIONS:")
    for kind, name in missing:
        print("  [%s] %s" % (kind, name))
    sys.exit(1)

print("Module loaded: %d main functions" % len(module_data.main_funcs))

# Create instance
inst = ct.new_instance(mod, "basic_test")
print("Instance created: %d nodes" % inst["node_count"])
print("Blackboard: %s" % str(inst["blackboard"]))
print()

# Tick loop
MAX_TICKS = 50
for tick in range(1, MAX_TICKS + 1):
    result = ct.tick_once(inst, ct.CT_EVENT_TICK, None)
    bb_state = inst["blackboard"].get("state", "?")
    print("  Tick %2d: %-12s  bb.state=%s" % (tick, result_str(result), bb_state))

    # Drain event queue
    while ct.event_count(inst) > 0:
        tt, eid, edata = ct.event_pop(inst)
        saved = inst["tick_type"]
        inst["tick_type"] = tt
        er = ct.tick_once(inst, eid, edata)
        inst["tick_type"] = saved
        if is_complete(er):
            result = er
            break

    if is_complete(result):
        break

print()
if result == ct.CT_CONTINUE or result == ct.CT_DISABLE or result == ct.CT_TERMINATE:
    print("PASSED: Tree completed (%s) in %d ticks" % (result_str(result), tick))
    # Verify final state
    final_state = inst["blackboard"].get("state")
    if final_state == 3:
        print("PASSED: Final state = 3 (expected)")
    else:
        print("FAILED: Final state = %s (expected 3)" % final_state)
        sys.exit(1)
elif tick >= MAX_TICKS:
    print("FAILED: Max ticks exceeded")
    sys.exit(1)
else:
    print("FAILED: Unexpected result %s" % result_str(result))
    sys.exit(1)
