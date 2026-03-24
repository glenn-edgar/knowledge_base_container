# ==========================================================================
# user_functions_mpy.py
# MicroPython translation of user_functions.lua
#
# Provides:
#   register(mod) - register all user functions with a module
#   EVENT_* constants for test assertions
#
# Shared state accessed via inst["user_ctx"], which must be a dict:
#   inst["user_ctx"] = {"bitmap": 0, "trigger_events": 0}
# ==========================================================================

import sys
sys.path.insert(0, "../../mpy_runtime")
import se_runtime

# ==========================================================================
# Event bit constants (mirrors C #define EVENT_BIT0_RISE etc.)
# ==========================================================================
EVENT_BIT0_RISE   = 1 << 0
EVENT_BIT0_FALL   = 1 << 1
EVENT_BITS12_RISE = 1 << 2
EVENT_BITS12_FALL = 1 << 3
EVENT_BITS34_RISE = 1 << 4
EVENT_BITS34_FALL = 1 << 5
EVENT_BIT5_CLEAR  = 1 << 6
EVENT_BIT5_SET    = 1 << 7

# ==========================================================================
# Oneshot actions (o_call signature: fn(inst, node))
# ==========================================================================
def on_bit0_rise(inst, node):
    print("  >> ON_BIT0_RISE")
    inst["user_ctx"]["trigger_events"] |= EVENT_BIT0_RISE

def on_bit0_fall(inst, node):
    print("  >> ON_BIT0_FALL")
    inst["user_ctx"]["trigger_events"] |= EVENT_BIT0_FALL

def on_bits_12_rise(inst, node):
    print("  >> ON_BITS_12_RISE (bit1 AND bit2)")
    inst["user_ctx"]["trigger_events"] |= EVENT_BITS12_RISE

def on_bits_12_fall(inst, node):
    print("  >> ON_BITS_12_FALL (bit1 AND bit2)")
    inst["user_ctx"]["trigger_events"] |= EVENT_BITS12_FALL

def on_bits_34_rise(inst, node):
    print("  >> ON_BITS_34_RISE (bit3 OR bit4)")
    inst["user_ctx"]["trigger_events"] |= EVENT_BITS34_RISE

def on_bits_34_fall(inst, node):
    print("  >> ON_BITS_34_FALL (bit3 OR bit4)")
    inst["user_ctx"]["trigger_events"] |= EVENT_BITS34_FALL

def on_bit5_clear(inst, node):
    print("  >> ON_BIT5_CLEAR (NOT bit5 went true)")
    inst["user_ctx"]["trigger_events"] |= EVENT_BIT5_CLEAR

def on_bit5_set(inst, node):
    print("  >> ON_BIT5_SET (NOT bit5 went false)")
    inst["user_ctx"]["trigger_events"] |= EVENT_BIT5_SET

# ==========================================================================
# Predicate: test_bit (p_call signature: fn(inst, node) -> bool)
# ==========================================================================
def test_bit(inst, node):
    ctx = inst["user_ctx"]
    assert ctx is not None, "test_bit: no user_ctx on inst"
    assert "bitmap" in ctx, "test_bit: no bitmap in user_ctx"

    bit_index = se_runtime.param_int(node, 1)
    assert 0 <= bit_index <= 31, "test_bit: bit index out of range: %d" % bit_index

    return (ctx["bitmap"] & (1 << bit_index)) != 0

# ==========================================================================
# Function table (keys match DSL func_name strings, uppercase)
# ==========================================================================
fns = {
    "on_bit0_rise":    on_bit0_rise,
    "on_bit0_fall":    on_bit0_fall,
    "on_bits_12_rise": on_bits_12_rise,
    "on_bits_12_fall": on_bits_12_fall,
    "on_bits_34_rise": on_bits_34_rise,
    "on_bits_34_fall": on_bits_34_fall,
    "on_bit5_clear":   on_bit5_clear,
    "on_bit5_set":     on_bit5_set,
    "test_bit":        test_bit,
}

def register(mod):
    se_runtime.register_fns(mod, fns)
