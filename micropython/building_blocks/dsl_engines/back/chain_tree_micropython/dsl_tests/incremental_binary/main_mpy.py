# ==========================================================================
# main_mpy.py - ChainTree MicroPython incremental_build test
#
# Runs selected trees from the incremental_build DSL through ct_runtime.
# Tick rate matches C runtime: 0.1s per tick with drift compensation.
# ==========================================================================

import sys
import time

# micropython.const shim for CPython
try:
    from micropython import const
except ImportError:
    import types
    _mp = types.ModuleType("micropython")
    _mp.const = lambda x: x
    sys.modules["micropython"] = _mp

sys.path.insert(0, "../../ct_runtime")

import ct_runtime as ct
from ct_builtins import builtins as ct_builtins
import incremental_build_module_mpy as module_data

DELTA_TIME = 0.1  # seconds per tick (matches C: params->delta_time)
MAX_TICKS = 2000  # safety limit


# ==========================================================================
# User-defined predicates
# ==========================================================================
def while_test_pred(inst, node):
    """WHILE_TEST — return True while iteration count < limit."""
    nd = node[ct.N_NODE_DATA]
    # node_data: ("user_data", ("count", 5))
    limit = 0
    if nd and len(nd) >= 2 and nd[0] == "user_data":
        ud = nd[1]
        if isinstance(ud, (list, tuple)):
            for i in range(0, len(ud) - 1, 2):
                if ud[i] == "count":
                    limit = int(ud[i + 1])
                    break
        else:
            limit = int(ud)
    ns = inst["node_states"][node[ct.N_NODE_INDEX]]
    return ns.get("state", 0) < limit

RESULT_NAMES = {
    0: "CONTINUE", 1: "HALT", 2: "TERMINATE", 3: "RESET",
    4: "DISABLE", 5: "SKIP_CONTINUE", 6: "TERMINATE_SYSTEM",
}

def result_str(r):
    return RESULT_NAMES.get(r, "?(%d)" % r)

def is_complete(r):
    return r in (2, 4, 6)

def run_tree(mod, tree_name, delta_time=DELTA_TIME, max_ticks=MAX_TICKS):
    print("--- Running tree: %s ---" % tree_name)
    try:
        inst = ct.new_instance(mod, tree_name)
    except RuntimeError as e:
        print("  SKIP: %s" % e)
        return None
    print("  Nodes: %d  Blackboard: %s" % (inst["node_count"], inst["blackboard"]))
    inst["delta_time"] = delta_time
    print("  Tick rate: %.3fs" % delta_time)

    t_start = time.time()
    future_time = t_start + delta_time
    result = ct.CT_HALT
    tick = 0

    for tick in range(1, max_ticks + 1):
        # Wait for next tick — compensate for processing time
        # Uncomment for real-time ticks:
        # now = time.time()
        # wait = future_time - now
        # if wait > 0:
        #     time.sleep(wait)

        # Tick
        inst["_tick_count"] = tick
        result = ct.tick_once(inst, ct.CT_EVENT_TICK, None)

        # Drain event queue
        while ct.event_count(inst) > 0:
            tt, eid, edata = ct.event_pop(inst)
            saved = inst["tick_type"]
            inst["tick_type"] = tt
            # Targeted event: deliver to specific node subtree
            if isinstance(edata, dict) and "target_node" in edata:
                target = edata["target_node"]
                er = ct.tick_targeted(inst, eid, edata, target)
            else:
                er = ct.tick_once(inst, eid, edata)
            inst["tick_type"] = saved
            if is_complete(er):
                result = er
                break

        # Advance timestamp (fixed rate, not wall-clock)
        future_time += delta_time

        if is_complete(result):
            break

    elapsed = time.time() - t_start
    status = "PASSED" if is_complete(result) else "TIMEOUT"
    print("  %s: %s in %d ticks (%.2fs elapsed)" % (
        status, result_str(result), tick, elapsed))
    print()
    return result

def select_tree(trees):
    print("Select a tree to run:")
    for i, name in enumerate(trees):
        print("  %2d. %s" % (i, name))
    print("   a. Run all")
    print("   q. Quit")
    choice = input("> ").strip()
    if choice == "q":
        sys.exit(0)
    if choice == "a":
        return list(trees)
    try:
        idx = int(choice)
        if 0 <= idx < len(trees):
            return [trees[idx]]
    except ValueError:
        # Try as name
        if choice in trees:
            return [choice]
    print("Invalid choice: %s" % choice)
    return None

# ==========================================================================
# Main
# ==========================================================================
print("=" * 60)
print("  ChainTree MicroPython - incremental_build test")
print("=" * 60)
print()
print("Main functions: %d" % len(module_data.main_funcs))

# Register builtins + user predicates
user_fns = dict(ct_builtins)
user_fns["WHILE_TEST"] = while_test_pred
mod = ct.new_module(module_data, user_fns)

# Parse command line or show selector
if len(sys.argv) > 1:
    names = []
    for a in sys.argv[1:]:
        if a in module_data.tree_order:
            names.append(a)
        else:
            try:
                idx = int(a)
                if 0 <= idx < len(module_data.tree_order):
                    names.append(module_data.tree_order[idx])
                else:
                    print("Index %d out of range (0..%d)" % (idx, len(module_data.tree_order) - 1))
                    sys.exit(1)
            except ValueError:
                print("Unknown tree: %s" % a)
                sys.exit(1)
else:
    names = select_tree(module_data.tree_order)

if names:
    print()
    for name in names:
        run_tree(mod, name)
