import sys
sys.path.insert(0, "../../mpy_runtime")

import gc
import micropython
import os

def report(label):
    gc.collect()
    print("--- %s ---" % label)
    micropython.mem_info()
    print()

gc.collect()
report("BASELINE")

# Module data
gc.collect()
h0 = gc.mem_alloc()
import basic_primitive_test_module_mpy as module_data
gc.collect()
h1 = gc.mem_alloc()
print("Module data heap: %d bytes" % (h1 - h0))

# Slim runtime (single file)
gc.collect()
h0 = gc.mem_alloc()
import se_runtime_spike as se
gc.collect()
h1 = gc.mem_alloc()
print("Slim runtime heap: %d bytes" % (h1 - h0))
report("After imports")

# new_module
gc.collect()
h0 = gc.mem_alloc()
def test_bit(inst, node):
    ctx = inst["user_ctx"]
    bit_index = se.param_int(node, 1)
    return (ctx["bitmap"] & (1 << bit_index)) != 0

def _make_os(bit):
    def fn(inst, node):
        inst["user_ctx"]["trigger_events"] |= bit
    return fn

user_fns = {
    "on_bit0_rise": _make_os(1), "on_bit0_fall": _make_os(2),
    "on_bits_12_rise": _make_os(4), "on_bits_12_fall": _make_os(8),
    "on_bits_34_rise": _make_os(16), "on_bits_34_fall": _make_os(32),
    "on_bit5_clear": _make_os(64), "on_bit5_set": _make_os(128),
    "test_bit": test_bit,
}

mod = se.new_module(module_data, se.builtins)
se.register_fns(mod, user_fns)
gc.collect()
h1 = gc.mem_alloc()
print("new_module() heap: %d bytes" % (h1 - h0))

# new_instance
gc.collect()
h0 = gc.mem_alloc()
tree_name = module_data.tree_order[0]
inst = se.new_instance(mod, tree_name)
inst["user_ctx"] = {"bitmap": 0, "trigger_events": 0}
gc.collect()
h1 = gc.mem_alloc()
print("new_instance() heap: %d bytes (node_count=%d)" % (h1 - h0, inst["node_count"]))

# tick
gc.collect()
h0 = gc.mem_alloc()
se.tick_once(inst, se.SE_EVENT_TICK, None)
gc.collect()
h1 = gc.mem_alloc()
print("tick_once() heap delta: %d bytes" % (h1 - h0))

report("After tick")

# Summary
gc.collect()
print("=" * 50)
print("TOTAL heap in use: %d bytes" % gc.mem_alloc())
print()

# Source sizes
print("--- Source file sizes ---")
files = [
    ("se_runtime_spike.py", "../../mpy_runtime/se_runtime_spike.py"),
    ("module_data",         "basic_primitive_test_module_mpy.py"),
]
total = 0
for label, path in files:
    try:
        sz = os.stat(path)[6]
        total += sz
        print("  %-30s %6d bytes" % (label, sz))
    except:
        print("  %-30s  (not found)" % label)
print("  %-30s %6d bytes" % ("TOTAL", total))
print()

# Compare to full runtime
full_files = [
    "../../mpy_runtime/se_runtime.py",
    "../../mpy_runtime/se_builtins_all.py",
    "../../mpy_runtime/se_builtins_flow_control.py",
    "../../mpy_runtime/se_builtins_dispatch.py",
    "../../mpy_runtime/se_builtins_pred.py",
    "../../mpy_runtime/se_builtins_oneshot.py",
    "../../mpy_runtime/se_builtins_return_codes.py",
    "../../mpy_runtime/se_builtins_delays.py",
    "../../mpy_runtime/se_builtins_verify.py",
    "../../mpy_runtime/se_builtins_stack.py",
    "../../mpy_runtime/se_builtins_spawn.py",
    "../../mpy_runtime/se_builtins_quads.py",
    "../../mpy_runtime/se_builtins_dict.py",
    "../../mpy_runtime/se_stack.py",
]
full_total = 0
for f in full_files:
    try:
        full_total += os.stat(f)[6]
    except:
        pass
print("Full runtime source: %d bytes" % full_total)
print("Slim runtime source: %d bytes" % os.stat("../../mpy_runtime/se_runtime_spike.py")[6])
print("Reduction: %.0f%%" % (100.0 * (1.0 - 24454.0 / full_total)))
