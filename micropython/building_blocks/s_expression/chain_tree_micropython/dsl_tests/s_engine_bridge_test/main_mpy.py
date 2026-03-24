# ==========================================================================
# main_mpy.py - S-Engine bridge test
# Port of s_test_binary/main.c: se_basic_load_test
#
# ChainTree loads and ticks the S-Engine state_machine_test module.
# The S-Engine state machine cycles 0->1->2->default->terminate.
# ChainTree sequence wraps: log -> se_tick -> log -> terminate_system.
# ==========================================================================

import sys

# ChainTree runtime
sys.path.insert(0, "../../ct_runtime")
import ct_runtime as ct

# S-Engine runtime (full, for the bridge)
sys.path.insert(0, "../../../s_expression_micropython/mpy_runtime")
import se_runtime
import se_builtins_all

# S-Engine module data (already validated in S-Engine test suite)
sys.path.insert(0, "../../../s_expression_micropython/dsl_tests/state_machine")
import state_machine_test_module_mpy as se_sm_module

# ChainTree module data
import se_bridge_test_module_mpy as ct_module

# S-Engine user functions for state_machine_test
def cfl_disable_children(inst, node):
    eid = inst["current_event_id"]
    if eid == se_runtime.SE_EVENT_INIT or eid == se_runtime.SE_EVENT_TERMINATE:
        return
    print("  [SE] cfl_disable_children")

def cfl_enable_child(inst, node):
    eid = inst["current_event_id"]
    if eid == se_runtime.SE_EVENT_INIT or eid == se_runtime.SE_EVENT_TERMINATE:
        return
    idx = se_runtime.param_int(node, 1)
    print("  [SE] cfl_enable_child: %d" % idx)

se_user_fns = {
    "cfl_disable_children": cfl_disable_children,
    "cfl_enable_child": cfl_enable_child,
}

# ==========================================================================
# Setup
# ==========================================================================
print("=" * 60)
print("  S-Engine Bridge Test (port of s_test_binary)")
print("=" * 60)
print()

# Create ChainTree module with builtins
ct_mod = ct.new_module(ct_module, ct.builtins)
ok, missing = ct.validate_module(ct_mod)
if not ok:
    print("MISSING CT FUNCTIONS:", missing)
    sys.exit(1)

# Create ChainTree instance
ct_inst = ct.new_instance(ct_mod, "se_basic_load_test")

# Wire up S-Engine modules via user_ctx
ct_inst["user_ctx"] = {
    "se_runtime": se_runtime,
    "se_modules": {
        "state_machine_test": {
            "module_data": se_sm_module,
            "builtins": se_builtins_all.builtins,
            "user_fns": se_user_fns,
        },
    },
}

print("ChainTree instance: %d nodes" % ct_inst["node_count"])
print("Blackboard: %s" % str(ct_inst["blackboard"]))
print()

# ==========================================================================
# Tick loop
# ==========================================================================
RESULT_NAMES = {
    0: "CONTINUE", 1: "HALT", 2: "TERMINATE", 3: "RESET",
    4: "DISABLE", 5: "SKIP_CONTINUE", 6: "TERMINATE_SYSTEM",
}

def result_str(r):
    return RESULT_NAMES.get(r, "?(%d)" % r)

MAX_TICKS = 500
for tick in range(1, MAX_TICKS + 1):
    result = ct.tick_once(ct_inst, ct.CT_EVENT_TICK, None)

    if tick <= 5 or tick % 50 == 0 or result != ct.CT_HALT:
        print("  CT Tick %3d: %s" % (tick, result_str(result)))

    # Drain CT event queue
    while ct.event_count(ct_inst) > 0:
        tt, eid, edata = ct.event_pop(ct_inst)
        saved = ct_inst["tick_type"]
        ct_inst["tick_type"] = tt
        er = ct.tick_once(ct_inst, eid, edata)
        ct_inst["tick_type"] = saved
        if er in (ct.CT_TERMINATE, ct.CT_DISABLE, ct.CT_TERMINATE_SYSTEM):
            result = er
            break

    if result in (ct.CT_TERMINATE, ct.CT_DISABLE, ct.CT_TERMINATE_SYSTEM):
        print("  CT Tick %3d: %s (final)" % (tick, result_str(result)))
        break

print()
if result == ct.CT_TERMINATE_SYSTEM:
    print("PASSED: ChainTree terminated system after S-Engine tree completed (%d ticks)" % tick)
elif result in (ct.CT_TERMINATE, ct.CT_DISABLE):
    print("PASSED: Tree completed (%s) in %d ticks" % (result_str(result), tick))
elif tick >= MAX_TICKS:
    print("FAILED: Max ticks exceeded")
    sys.exit(1)
else:
    print("FAILED: Unexpected result %s" % result_str(result))
    sys.exit(1)
