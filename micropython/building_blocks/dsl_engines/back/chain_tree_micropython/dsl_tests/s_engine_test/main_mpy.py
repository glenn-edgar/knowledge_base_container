# ==========================================================================
# main_mpy.py - ChainTree + S-Engine integration test
#
# Usage:
#   micropython main_mpy.py [tree_name_or_index ...]
#   micropython main_mpy.py                # interactive selector
#   micropython main_mpy.py 0              # run first tree by index
#   micropython main_mpy.py se_basic_load_test  # run by name
# ==========================================================================

import sys
import time
sys.path.insert(0, "../../ct_runtime")
sys.path.insert(0, "../../../s_expression_micropython/mpy_runtime")

import ct_runtime as ct
from ct_builtins import builtins as ct_builtins
import se_runtime as se
from se_builtins_all import builtins as se_builtins
import s_engine_test_module_mpy as ct_module_data

sys.path.insert(0, "s_engine")
import state_machine_test_module_mpy as se_module_data

DELTA_TIME = 0.1
MAX_TICKS = 2000

RESULT_NAMES = {
    0: "CONTINUE", 1: "HALT", 2: "TERMINATE", 3: "RESET",
    4: "DISABLE", 5: "SKIP_CONTINUE", 6: "TERMINATE_SYSTEM",
}

def result_str(r):
    return RESULT_NAMES.get(r, "?(%d)" % r)

def is_complete(r):
    return r in (2, 4, 6)

def run_tree(mod, user_ctx, tree_name, delta_time=DELTA_TIME, max_ticks=MAX_TICKS):
    print("--- Running tree: %s ---" % tree_name)
    inst = ct.new_instance(mod, tree_name)
    inst["user_ctx"] = user_ctx
    inst["delta_time"] = delta_time
    print("  CT nodes: %d" % inst["node_count"])
    print("  Tick rate: %.3fs" % delta_time)

    t_start = time.time()
    future_time = t_start + delta_time
    result = ct.CT_HALT
    tick = 0

    for tick in range(1, max_ticks + 1):
        now = time.time()
        wait = future_time - now
        if wait > 0:
            time.sleep(wait)

        inst["_tick_count"] = tick
        result = ct.tick_once(inst, ct.CT_EVENT_TICK, None)

        while ct.event_count(inst) > 0:
            tt, eid, edata = ct.event_pop(inst)
            saved = inst["tick_type"]
            inst["tick_type"] = tt
            er = ct.tick_once(inst, eid, edata)
            inst["tick_type"] = saved
            if is_complete(er):
                result = er
                break

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
        if choice in trees:
            return [choice]
    print("Invalid choice: %s" % choice)
    return None

# ==========================================================================
# Main
# ==========================================================================
print("=" * 60)
print("  ChainTree + S-Engine Integration Test")
print("=" * 60)
print()

# User stubs — C-oriented callbacks, no-op in MicroPython
user_fns = {
    "USER_REGISTER_S_FUNCTIONS": lambda inst, node: True,
}

# SE oneshot stubs — ChainTree bridge functions used by SE modules
se_user_fns = {
    "CFL_DISABLE_CHILDREN": lambda inst, node: None,
    "CFL_ENABLE_CHILD": lambda inst, node: None,
}

se_modules = {
    "state_machine_test": {
        "module_data": se_module_data,
        "builtins": se_builtins,
        "user_fns": se_user_fns,
    },
}

mod = ct.new_module(ct_module_data, ct.merge_fns(ct_builtins, user_fns))

user_ctx = {
    "se_runtime": se,
    "se_modules": se_modules,
}

# Parse command line or show selector
if len(sys.argv) > 1:
    names = []
    for a in sys.argv[1:]:
        if a in ct_module_data.tree_order:
            names.append(a)
        else:
            try:
                idx = int(a)
                if 0 <= idx < len(ct_module_data.tree_order):
                    names.append(ct_module_data.tree_order[idx])
                else:
                    print("Index %d out of range (0..%d)" % (idx, len(ct_module_data.tree_order) - 1))
                    sys.exit(1)
            except ValueError:
                print("Unknown tree: %s" % a)
                sys.exit(1)
else:
    names = select_tree(ct_module_data.tree_order)

if names:
    print()
    for name in names:
        run_tree(mod, user_ctx, name)
