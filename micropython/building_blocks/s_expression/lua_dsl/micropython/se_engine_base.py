# ==========================================================================
# se_engine_base.py
# S-Expression Engine Base Class for MicroPython
#
# The param stream lives in flash as a class-level bytes constant.
# Builtins are methods. User functions are methods on the derived class.
# Dispatch is by index into a method tuple — no hash tables, no loader.
#
# RAM footprint: blackboard + pointer slots + stack only.
# ==========================================================================

import struct

# Param type tags (match s_expr_dsl.lua S_EXPR_PARAM)
_INT         = 0x00
_UINT        = 0x01
_FLOAT       = 0x02
_STR_HASH    = 0x03
_SLOT        = 0x04
_OPEN        = 0x05
_CLOSE       = 0x06
_OPEN_CALL   = 0x07
_ONESHOT     = 0x08
_MAIN        = 0x09
_PRED        = 0x0A
_FIELD       = 0x0B
_RESULT      = 0x0C
_STR_IDX     = 0x0D
_CONST_REF   = 0x0E
_OPEN_DICT   = 0x10
_CLOSE_DICT  = 0x11
_OPEN_KEY    = 0x12
_CLOSE_KEY   = 0x13
_OPEN_ARRAY  = 0x14
_CLOSE_ARRAY = 0x15
_OPEN_TUPLE  = 0x16
_CLOSE_TUPLE = 0x17
_STACK_TOS   = 0x18
_STACK_LOCAL = 0x19
_NULL_PARAM  = 0x1A
_STACK_PUSH  = 0x1B
_STACK_POP   = 0x1C

_FLAG_SURVIVES_RESET = 0x40
_FLAG_POINTER        = 0x80
_TYPE_MASK           = 0x3F

# Result codes
CONTINUE       = 0
HALT           = 1
TERMINATE      = 2
RESET          = 3
DISABLE        = 4
SKIP_CONTINUE  = 5

FN_CONTINUE       = 6
FN_HALT           = 7
FN_TERMINATE      = 8
FN_RESET          = 9
FN_DISABLE        = 10
FN_SKIP_CONTINUE  = 11

PIPE_CONTINUE      = 12
PIPE_HALT          = 13
PIPE_TERMINATE     = 14
PIPE_RESET         = 15
PIPE_DISABLE       = 16
PIPE_SKIP_CONTINUE = 17

# 32-bit param size
PARAM_SIZE = 8


class SeEngineBase:
    """
    Base class for generated S-Expression tree instances.

    Subclass constants (set by generated code):
        PARAMS      - bytes: the param stream (lives in flash)
        BB_SIZE     - int: blackboard byte count
        BB_ALIGN    - int: blackboard alignment
        PTR_COUNT   - int: pointer slot count
        STRINGS     - tuple of str: string table
        DEFAULTS    - bytes or None: default blackboard values

    Subclass must override:
        _build_dispatch() - returns tuple of methods indexed by func_index
    """

    def __init__(self):
        # Mutable state — lives in RAM
        if self.DEFAULTS:
            self.bb = bytearray(self.DEFAULTS)
        else:
            self.bb = bytearray(self.BB_SIZE)
        self.pointers = [None] * self.PTR_COUNT
        self.stack = []
        self._dispatch = self._build_dispatch()

        # Read-only view of param stream — no heap allocation
        self._params = memoryview(self.PARAMS)

    # ==================================================================
    # PARAM READERS
    # ==================================================================

    def _ptype(self, pos):
        """Read param type byte."""
        return self._params[pos]

    def _ptype_masked(self, pos):
        """Read param type with flags stripped."""
        return self._params[pos] & _TYPE_MASK

    def _pflags(self, pos):
        """Read param flags."""
        return self._params[pos] & 0xC0

    def _pindex(self, pos):
        """Read index_to_pointer byte."""
        return self._params[pos + 1]

    def _pu16a(self, pos):
        """Read u16_a field."""
        return struct.unpack_from('<H', self._params, pos + 4)[0]

    def _pu16b(self, pos):
        """Read u16_b field."""
        return struct.unpack_from('<H', self._params, pos + 6)[0]

    def _pi32(self, pos):
        """Read 32-bit signed int value."""
        return struct.unpack_from('<i', self._params, pos + 4)[0]

    def _pu32(self, pos):
        """Read 32-bit unsigned int value."""
        return struct.unpack_from('<I', self._params, pos + 4)[0]

    def _pf32(self, pos):
        """Read 32-bit float value."""
        return struct.unpack_from('<f', self._params, pos + 4)[0]

    # ==================================================================
    # BLACKBOARD ACCESS
    # ==================================================================

    def bb_i32(self, offset):
        return struct.unpack_from('<i', self.bb, offset)[0]

    def bb_set_i32(self, offset, value):
        struct.pack_into('<i', self.bb, offset, value)

    def bb_u32(self, offset):
        return struct.unpack_from('<I', self.bb, offset)[0]

    def bb_set_u32(self, offset, value):
        struct.pack_into('<I', self.bb, offset, value)

    def bb_f32(self, offset):
        return struct.unpack_from('<f', self.bb, offset)[0]

    def bb_set_f32(self, offset, value):
        struct.pack_into('<f', self.bb, offset, value)

    def bb_f64(self, offset):
        return struct.unpack_from('<d', self.bb, offset)[0]

    def bb_set_f64(self, offset, value):
        struct.pack_into('<d', self.bb, offset, value)

    # ==================================================================
    # FIELD PARAM HELPERS
    # ==================================================================

    def _field_offset(self, pos):
        """Read field offset from a FIELD param at pos."""
        return self._pu16a(pos)

    def _field_size(self, pos):
        """Read field size from a FIELD param at pos."""
        return self._pu16b(pos)

    def _read_field_value(self, pos):
        """Read the value at the blackboard location referenced by FIELD param."""
        offset = self._pu16a(pos)
        size = self._pu16b(pos)
        if size == 4:
            return self.bb_i32(offset)
        elif size == 8:
            return self.bb_f64(offset)
        return self.bb_i32(offset)

    def _write_field_value(self, field_pos, value):
        """Write value to the blackboard location referenced by FIELD param."""
        offset = self._pu16a(field_pos)
        size = self._pu16b(field_pos)
        if size == 4:
            if isinstance(value, float):
                self.bb_set_f32(offset, value)
            else:
                self.bb_set_i32(offset, value)
        elif size == 8:
            self.bb_set_f64(offset, value)

    def _read_typed_param(self, pos):
        """Read a typed parameter value (INT, UINT, FLOAT, STR_HASH, etc.)."""
        pt = self._ptype_masked(pos)
        if pt == _INT:
            return self._pi32(pos)
        elif pt == _UINT:
            return self._pu32(pos)
        elif pt == _FLOAT:
            return self._pf32(pos)
        elif pt == _STR_HASH:
            return self._pu32(pos)
        elif pt == _STR_IDX:
            idx = self._pu16a(pos)
            return self.STRINGS[idx]
        elif pt == _FIELD:
            return self._read_field_value(pos)
        elif pt == _NULL_PARAM:
            return 0
        return 0

    # ==================================================================
    # DISPATCH
    # ==================================================================

    def dispatch(self, pos, ev_type, ev_id, ev_data):
        """
        Read OPEN_CALL at pos, resolve function, call it.

        Param layout at pos:
            [OPEN_CALL] content_count=u16_a, parent_offset=u16_b
            [opcode]    node_index=u16_a, func_index=u16_b
            ... params and children ...
            [CLOSE]
        """
        content_count = self._pu16a(pos)
        opcode_pos = pos + PARAM_SIZE
        func_index = self._pu16b(opcode_pos)

        fn = self._dispatch[func_index]
        return fn(pos, content_count, ev_type, ev_id, ev_data)

    def run(self, ev_type=0, ev_id=0, ev_data=None):
        """Execute the tree root."""
        return self.dispatch(0, ev_type, ev_id, ev_data)

    # ==================================================================
    # CHILD ITERATION HELPERS
    # ==================================================================

    def _first_param_pos(self, call_pos):
        """Position of first param/child after OPEN_CALL + opcode."""
        return call_pos + 2 * PARAM_SIZE

    def _end_pos(self, call_pos, content_count):
        """Position of the CLOSE token."""
        return call_pos + content_count * PARAM_SIZE

    def _iter_children(self, call_pos, content_count):
        """
        Yield (pos, ptype) for each direct param and child OPEN_CALL.
        For OPEN_CALLs, yields the OPEN_CALL position; caller must
        skip using content_count.
        """
        cursor = self._first_param_pos(call_pos)
        end = self._end_pos(call_pos, content_count)
        while cursor < end:
            pt = self._ptype_masked(cursor)
            yield cursor, pt
            if pt == _OPEN_CALL:
                child_count = self._pu16a(cursor)
                cursor += (child_count + 1) * PARAM_SIZE
            else:
                cursor += PARAM_SIZE

    def _iter_child_calls(self, call_pos, content_count):
        """Yield pos for each direct child OPEN_CALL only."""
        for cursor, pt in self._iter_children(call_pos, content_count):
            if pt == _OPEN_CALL:
                yield cursor

    # ==================================================================
    # BUILTIN: SE_NOP
    # ==================================================================

    def _se_nop(self, pos, content_count, ev_type, ev_id, ev_data):
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_SEQUENCE
    # ==================================================================

    def _se_sequence(self, pos, content_count, ev_type, ev_id, ev_data):
        for child_pos in self._iter_child_calls(pos, content_count):
            result = self.dispatch(child_pos, ev_type, ev_id, ev_data)
            if result != CONTINUE:
                return result
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_FORK
    # ==================================================================

    def _se_fork(self, pos, content_count, ev_type, ev_id, ev_data):
        for child_pos in self._iter_child_calls(pos, content_count):
            self.dispatch(child_pos, ev_type, ev_id, ev_data)
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_IF_THEN_ELSE
    # ==================================================================

    def _se_if_then_else(self, pos, content_count, ev_type, ev_id, ev_data):
        children = list(self._iter_child_calls(pos, content_count))
        if len(children) < 3:
            return CONTINUE

        pred_result = self.dispatch(children[0], ev_type, ev_id, ev_data)
        if pred_result:
            return self.dispatch(children[1], ev_type, ev_id, ev_data)
        else:
            return self.dispatch(children[2], ev_type, ev_id, ev_data)

    # ==================================================================
    # BUILTIN: SE_CHAIN_FLOW
    # ==================================================================

    def _se_chain_flow(self, pos, content_count, ev_type, ev_id, ev_data):
        for child_pos in self._iter_child_calls(pos, content_count):
            result = self.dispatch(child_pos, ev_type, ev_id, ev_data)
            if result == PIPE_RESET:
                return CONTINUE
            elif result == PIPE_HALT:
                return HALT
            elif result != CONTINUE:
                return result
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_FUNCTION_INTERFACE
    # ==================================================================

    def _se_function_interface(self, pos, content_count, ev_type, ev_id, ev_data):
        for child_pos in self._iter_child_calls(pos, content_count):
            result = self.dispatch(child_pos, ev_type, ev_id, ev_data)
            if result == FN_RESET:
                return CONTINUE
            elif result == FN_HALT:
                return HALT
            elif result == FN_TERMINATE:
                return TERMINATE
            elif result != CONTINUE and result != FN_CONTINUE:
                return result
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_STATE_MACHINE
    # ==================================================================

    def _se_state_machine(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        end = self._end_pos(pos, content_count)

        # First param is field ref
        if cursor >= end:
            return CONTINUE
        state_val = self._read_field_value(cursor)
        cursor += PARAM_SIZE

        # Scan int/call pairs
        default_pos = None
        while cursor < end:
            pt = self._ptype_masked(cursor)
            if pt == _INT or pt == _UINT:
                case_val = self._pi32(cursor)
                cursor += PARAM_SIZE
                if cursor < end and self._ptype_masked(cursor) == _OPEN_CALL:
                    if case_val == -1:
                        default_pos = cursor
                    elif case_val == state_val:
                        return self.dispatch(cursor, ev_type, ev_id, ev_data)
                    child_count = self._pu16a(cursor)
                    cursor += (child_count + 1) * PARAM_SIZE
                else:
                    cursor += PARAM_SIZE
            elif pt == _OPEN_CALL:
                child_count = self._pu16a(cursor)
                cursor += (child_count + 1) * PARAM_SIZE
            else:
                cursor += PARAM_SIZE

        if default_pos is not None:
            return self.dispatch(default_pos, ev_type, ev_id, ev_data)
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_FIELD_DISPATCH
    # ==================================================================

    def _se_field_dispatch(self, pos, content_count, ev_type, ev_id, ev_data):
        # Same logic as state machine
        return self._se_state_machine(pos, content_count, ev_type, ev_id, ev_data)

    # ==================================================================
    # BUILTIN: SE_COND
    # ==================================================================

    def _se_cond(self, pos, content_count, ev_type, ev_id, ev_data):
        children = list(self._iter_child_calls(pos, content_count))
        i = 0
        while i + 1 < len(children):
            pred_result = self.dispatch(children[i], ev_type, ev_id, ev_data)
            if pred_result:
                return self.dispatch(children[i + 1], ev_type, ev_id, ev_data)
            i += 2
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_WHILE
    # ==================================================================

    def _se_while(self, pos, content_count, ev_type, ev_id, ev_data):
        children = list(self._iter_child_calls(pos, content_count))
        if len(children) < 2:
            return CONTINUE

        cond_pos = children[0]
        body_pos = children[1]

        while self.dispatch(cond_pos, ev_type, ev_id, ev_data):
            result = self.dispatch(body_pos, ev_type, ev_id, ev_data)
            if result != CONTINUE:
                return result
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_SET_FIELD
    # ==================================================================

    def _se_set_field(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        end = self._end_pos(pos, content_count)

        if cursor >= end:
            return CONTINUE
        field_pos = cursor
        cursor += PARAM_SIZE

        if cursor >= end:
            return CONTINUE
        value = self._read_typed_param(cursor)
        self._write_field_value(field_pos, value)
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_INC_FIELD / SE_DEC_FIELD
    # ==================================================================

    def _se_inc_field(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        field_offset = self._pu16a(cursor)
        cursor += PARAM_SIZE
        inc_val = self._pu32(cursor)
        current = self.bb_i32(field_offset)
        self.bb_set_i32(field_offset, current + inc_val)
        return CONTINUE

    def _se_dec_field(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        field_offset = self._pu16a(cursor)
        cursor += PARAM_SIZE
        dec_val = self._pu32(cursor)
        current = self.bb_i32(field_offset)
        self.bb_set_i32(field_offset, current - dec_val)
        return CONTINUE

    # ==================================================================
    # BUILTIN: SE_LOG
    # ==================================================================

    def _se_log(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        pt = self._ptype_masked(cursor)
        if pt == _STR_IDX:
            idx = self._pu16a(cursor)
            print(self.STRINGS[idx])
        return CONTINUE

    # ==================================================================
    # BUILTIN PREDICATES
    # ==================================================================

    def _se_true(self, pos, content_count, ev_type, ev_id, ev_data):
        return 1

    def _se_false(self, pos, content_count, ev_type, ev_id, ev_data):
        return 0

    def _se_field_eq(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        field_val = self._read_field_value(cursor)
        cursor += PARAM_SIZE
        test_val = self._read_typed_param(cursor)
        return 1 if field_val == test_val else 0

    def _se_field_ne(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        field_val = self._read_field_value(cursor)
        cursor += PARAM_SIZE
        test_val = self._read_typed_param(cursor)
        return 1 if field_val != test_val else 0

    def _se_field_gt(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        field_val = self._read_field_value(cursor)
        cursor += PARAM_SIZE
        test_val = self._read_typed_param(cursor)
        return 1 if field_val > test_val else 0

    def _se_field_ge(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        field_val = self._read_field_value(cursor)
        cursor += PARAM_SIZE
        test_val = self._read_typed_param(cursor)
        return 1 if field_val >= test_val else 0

    def _se_field_lt(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        field_val = self._read_field_value(cursor)
        cursor += PARAM_SIZE
        test_val = self._read_typed_param(cursor)
        return 1 if field_val < test_val else 0

    def _se_field_le(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        field_val = self._read_field_value(cursor)
        cursor += PARAM_SIZE
        test_val = self._read_typed_param(cursor)
        return 1 if field_val <= test_val else 0

    # ==================================================================
    # BUILTIN: COMPOSITE PREDICATES
    # ==================================================================

    def _se_pred_or(self, pos, content_count, ev_type, ev_id, ev_data):
        for child_pos in self._iter_child_calls(pos, content_count):
            if self.dispatch(child_pos, ev_type, ev_id, ev_data):
                return 1
        return 0

    def _se_pred_and(self, pos, content_count, ev_type, ev_id, ev_data):
        for child_pos in self._iter_child_calls(pos, content_count):
            if not self.dispatch(child_pos, ev_type, ev_id, ev_data):
                return 0
        return 1

    def _se_pred_not(self, pos, content_count, ev_type, ev_id, ev_data):
        for child_pos in self._iter_child_calls(pos, content_count):
            return 0 if self.dispatch(child_pos, ev_type, ev_id, ev_data) else 1
        return 1

    # ==================================================================
    # BUILTIN: SE_QUEUE_EVENT
    # ==================================================================

    def _se_queue_event(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        q_type = self._pu32(cursor)
        cursor += PARAM_SIZE
        q_id = self._pu32(cursor)
        # Subclass can override to route events
        self.on_event(q_type, q_id)
        return CONTINUE

    def on_event(self, ev_type, ev_id):
        """Override to handle queued events."""
        pass

    # ==================================================================
    # BUILTIN: SE_CHECK_EVENT
    # ==================================================================

    def _se_check_event(self, pos, content_count, ev_type, ev_id, ev_data):
        cursor = self._first_param_pos(pos)
        end = self._end_pos(pos, content_count)
        while cursor < end:
            pt = self._ptype_masked(cursor)
            if pt == _INT or pt == _UINT:
                check_id = self._pi32(cursor)
                if ev_id == check_id:
                    return 1
            cursor += PARAM_SIZE
        return 0

    # ==================================================================
    # SUBCLASS INTERFACE
    # ==================================================================

    def _build_dispatch(self):
        """
        Override in generated class. Returns a tuple of bound methods
        indexed by func_index. Example:

            return (
                self._se_function_interface,  # 0
                self._se_sequence,            # 1
                self._se_set_field,           # 2
                self.user_motor_start,        # 3  (user function)
                ...
            )
        """
        raise NotImplementedError
