# ==========================================================================
# measure_mpy.py
# Measure flash (bytecode) size and heap usage for basic_primitive_test
# ==========================================================================

import sys
sys.path.insert(0, "../../mpy_runtime")

import gc
import micropython

def report(label):
    gc.collect()
    print("--- %s ---" % label)
    micropython.mem_info()
    print()

# Baseline
gc.collect()
report("BASELINE (after imports: gc, micropython)")

# Phase 1: Import module data (would be ROM if frozen)
print("=" * 60)
print("PHASE 1: Import module data")
print("=" * 60)
gc.collect()
heap_before_module = gc.mem_alloc()

import basic_primitive_test_module_mpy as module_data

gc.collect()
heap_after_module = gc.mem_alloc()
print("Module data heap cost: %d bytes" % (heap_after_module - heap_before_module))
report("After module_data import")

# Phase 2: Import runtime + builtins
print("=" * 60)
print("PHASE 2: Import runtime + builtins")
print("=" * 60)
gc.collect()
heap_before_rt = gc.mem_alloc()

import se_runtime
import se_builtins_all

gc.collect()
heap_after_rt = gc.mem_alloc()
print("Runtime + builtins heap cost: %d bytes" % (heap_after_rt - heap_before_rt))
report("After runtime import")

# Phase 3: new_module (builds function lookup tables)
print("=" * 60)
print("PHASE 3: new_module()")
print("=" * 60)
gc.collect()
heap_before_mod = gc.mem_alloc()

import user_functions_mpy as user_module
mod = se_runtime.new_module(module_data, se_builtins_all.builtins)
user_module.register(mod)

gc.collect()
heap_after_mod = gc.mem_alloc()
print("new_module() heap cost: %d bytes" % (heap_after_mod - heap_before_mod))
report("After new_module()")

# Phase 4: new_instance (allocates node_states, blackboard, event queue)
print("=" * 60)
print("PHASE 4: new_instance()")
print("=" * 60)
gc.collect()
heap_before_inst = gc.mem_alloc()

tree_name = module_data.tree_order[0]
inst = se_runtime.new_instance(mod, tree_name)
inst["user_ctx"] = {"bitmap": 0, "trigger_events": 0}

gc.collect()
heap_after_inst = gc.mem_alloc()
print("new_instance() heap cost: %d bytes" % (heap_after_inst - heap_before_inst))
print("  node_count: %d" % inst["node_count"])
print("  pointer_count: %d" % inst["pointer_count"])
report("After new_instance()")

# Phase 5: Run one tick
print("=" * 60)
print("PHASE 5: tick_once()")
print("=" * 60)
gc.collect()
heap_before_tick = gc.mem_alloc()

se_runtime.tick_once(inst, se_runtime.SE_EVENT_TICK, None)

gc.collect()
heap_after_tick = gc.mem_alloc()
print("tick_once() heap delta: %d bytes" % (heap_after_tick - heap_before_tick))
report("After tick_once()")

# Summary
print("=" * 60)
print("SUMMARY")
print("=" * 60)
gc.collect()
total_heap = gc.mem_alloc()
print("Module data (ROM if frozen):  %d bytes heap" % (heap_after_module - heap_before_module))
print("Runtime + builtins:           %d bytes heap" % (heap_after_rt - heap_before_rt))
print("new_module() tables:          %d bytes heap" % (heap_after_mod - heap_before_mod))
print("new_instance() state:         %d bytes heap" % (heap_after_inst - heap_before_inst))
print("Total heap in use:            %d bytes" % total_heap)

# Bytecode size estimate
print()
print("--- Bytecode sizes (approx, from .mpy compile) ---")
import os
mpy_files = [
    ("module_data", "basic_primitive_test_module_mpy.py"),
    ("se_runtime", "../../mpy_runtime/se_runtime.py"),
    ("se_builtins_all", "../../mpy_runtime/se_builtins_all.py"),
    ("se_builtins_flow_control", "../../mpy_runtime/se_builtins_flow_control.py"),
    ("se_builtins_dispatch", "../../mpy_runtime/se_builtins_dispatch.py"),
    ("se_builtins_pred", "../../mpy_runtime/se_builtins_pred.py"),
    ("se_builtins_oneshot", "../../mpy_runtime/se_builtins_oneshot.py"),
    ("se_builtins_return_codes", "../../mpy_runtime/se_builtins_return_codes.py"),
    ("se_builtins_delays", "../../mpy_runtime/se_builtins_delays.py"),
    ("se_builtins_verify", "../../mpy_runtime/se_builtins_verify.py"),
    ("se_builtins_stack", "../../mpy_runtime/se_builtins_stack.py"),
    ("se_builtins_spawn", "../../mpy_runtime/se_builtins_spawn.py"),
    ("se_builtins_quads", "../../mpy_runtime/se_builtins_quads.py"),
    ("se_builtins_dict", "../../mpy_runtime/se_builtins_dict.py"),
    ("se_stack", "../../mpy_runtime/se_stack.py"),
]
total_src = 0
for label, path in mpy_files:
    try:
        st = os.stat(path)
        sz = st[6]  # st_size
        total_src += sz
        print("  %-30s %6d bytes (.py source)" % (label, sz))
    except:
        print("  %-30s  (not found)" % label)
print("  %-30s %6d bytes" % ("TOTAL source", total_src))
