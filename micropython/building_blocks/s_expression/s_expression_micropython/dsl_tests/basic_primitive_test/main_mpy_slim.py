# Test harness using se_runtime_spike.py (slim single-file runtime)

import sys
sys.path.insert(0, "../../mpy_runtime")

import se_runtime_spike as se

import basic_primitive_test_module_mpy as module_data

# User functions — rewritten to use se_runtime_spike
def test_bit(inst, node):
    ctx = inst["user_ctx"]
    bit_index = se.param_int(node, 1)
    return (ctx["bitmap"] & (1 << bit_index)) != 0

EVENT_BIT0_RISE   = 1 << 0
EVENT_BIT0_FALL   = 1 << 1
EVENT_BITS12_RISE = 1 << 2
EVENT_BITS12_FALL = 1 << 3
EVENT_BITS34_RISE = 1 << 4
EVENT_BITS34_FALL = 1 << 5
EVENT_BIT5_CLEAR  = 1 << 6
EVENT_BIT5_SET    = 1 << 7

def _make_oneshot(bit):
    def fn(inst, node):
        inst["user_ctx"]["trigger_events"] |= bit
    return fn

user_fns = {
    "on_bit0_rise":    _make_oneshot(EVENT_BIT0_RISE),
    "on_bit0_fall":    _make_oneshot(EVENT_BIT0_FALL),
    "on_bits_12_rise": _make_oneshot(EVENT_BITS12_RISE),
    "on_bits_12_fall": _make_oneshot(EVENT_BITS12_FALL),
    "on_bits_34_rise": _make_oneshot(EVENT_BITS34_RISE),
    "on_bits_34_fall": _make_oneshot(EVENT_BITS34_FALL),
    "on_bit5_clear":   _make_oneshot(EVENT_BIT5_CLEAR),
    "on_bit5_set":     _make_oneshot(EVENT_BIT5_SET),
    "test_bit":        test_bit,
}

# Load
mod = se.new_module(module_data, se.builtins)
se.register_fns(mod, user_fns)

ok, missing = se.validate_module(mod)
if not ok:
    print("MISSING:", missing)
    sys.exit(1)

tree_name = module_data.tree_order[0]
inst = se.new_instance(mod, tree_name)

ctx = {"bitmap": 0, "trigger_events": 0}
inst["user_ctx"] = ctx

def do_tick():
    return se.tick_once(inst, se.SE_EVENT_TICK, None)

test_pass = True

def check(cond, msg):
    global test_pass
    if not cond:
        print("  FAIL", msg)
        test_pass = False

def check_not(cond, msg):
    global test_pass
    if cond:
        print("  FAIL", msg)
        test_pass = False

print("=== Slim Runtime Test ===")

# Initial tick
ctx["trigger_events"] = 0
do_tick()

# T1: Set bit 0 -> RISE
ctx["bitmap"] |= 1; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BIT0_RISE, "T1: BIT0_RISE")

# T2: Clear bit 0 -> FALL
ctx["bitmap"] &= ~1; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BIT0_FALL, "T2: BIT0_FALL")

# T3: Set bit 1 only -> no AND
ctx["bitmap"] |= 2; ctx["trigger_events"] = 0; do_tick()
check_not(ctx["trigger_events"] & EVENT_BITS12_RISE, "T3: no BITS12_RISE")

# T4: Set bit 2 -> AND true
ctx["bitmap"] |= 4; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BITS12_RISE, "T4: BITS12_RISE")

# T5: Clear bit 1 -> AND false
ctx["bitmap"] &= ~2; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BITS12_FALL, "T5: BITS12_FALL")

# T6: Set bit 3 -> OR true
ctx["bitmap"] |= 8; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BITS34_RISE, "T6: BITS34_RISE")

# T7: Set bit 4 -> OR still true
ctx["bitmap"] |= 16; ctx["trigger_events"] = 0; do_tick()
check_not(ctx["trigger_events"] & (EVENT_BITS34_RISE | EVENT_BITS34_FALL), "T7: no edge")

# T8: Clear bit 3 -> OR still true via bit 4
ctx["bitmap"] &= ~8; ctx["trigger_events"] = 0; do_tick()
check_not(ctx["trigger_events"] & (EVENT_BITS34_RISE | EVENT_BITS34_FALL), "T8: no edge")

# T9: Clear bit 4 -> OR false
ctx["bitmap"] &= ~16; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BITS34_FALL, "T9: BITS34_FALL")

# T10: Set bit 5 -> NOT false
ctx["bitmap"] |= 32; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BIT5_SET, "T10: BIT5_SET")

# T11: Clear bit 5 -> NOT true
ctx["bitmap"] &= ~32; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BIT5_CLEAR, "T11: BIT5_CLEAR")

# T12-13: Repeat bit 0
ctx["bitmap"] |= 1; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BIT0_RISE, "T12: BIT0_RISE repeat")
ctx["bitmap"] &= ~1; ctx["trigger_events"] = 0; do_tick()
check(ctx["trigger_events"] & EVENT_BIT0_FALL, "T13: BIT0_FALL repeat")

# T14: Rapid toggle
for cycle in range(1, 4):
    ctx["bitmap"] |= 1; ctx["trigger_events"] = 0; do_tick()
    check(ctx["trigger_events"] & EVENT_BIT0_RISE, "T14 cycle %d rise" % cycle)
    ctx["bitmap"] &= ~1; ctx["trigger_events"] = 0; do_tick()
    check(ctx["trigger_events"] & EVENT_BIT0_FALL, "T14 cycle %d fall" % cycle)

if test_pass:
    print("PASSED: All 14 tests")
else:
    print("FAILED")
    sys.exit(1)
