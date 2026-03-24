# ==========================================================================
# basic_test_module_mpy.py
# Hand-written test module for ChainTree MicroPython runtime
#
# Tree: state machine cycles through 4 states with delays,
#       then terminates.
#
#   ct_state_machine("state")         <-- root, persistent
#     case 0: sequence(log, delay 3, set state=1)
#     case 1: sequence(log, delay 5, set state=2)
#     case 2: sequence(log, delay 2, set state=3)
#     case 3: sequence(log, TERMINATE)   <-- ends the tree
#     default: nop
# ==========================================================================

from micropython import const

# Node tuple: (func_name, func_index, node_index, guard_index, node_data, children)

main_funcs = (
    "CT_SEQUENCE",          # 0
    "CT_LOG",               # 1
    "CT_SET_FIELD",         # 2
    "CT_TICK_DELAY",        # 3
    "CT_STATE_MACHINE",     # 4
    "CT_NOP",               # 5
    "CT_RETURN_TERMINATE",  # 6
)

pred_funcs = ()

records = (
    (
        "test_blackboard",
        (
            ("state", "int32", 0),
        ),
    ),
)

tree_order = ("basic_test",)

trees = (
    (
        "basic_test",        # T_NAME
        0x12345678,          # T_NAME_HASH
        19,                  # T_NODE_COUNT
        "test_blackboard",   # T_RECORD
        # T_ROOT: state_machine on "state"
        (
            "CT_STATE_MACHINE", 4, 0, -1,
            ("state", 0, 1, 2, 3, -1),   # field, case0, case1, case2, case3, default
            (
                # case 0: sequence(log, delay 3, set state=1)
                (
                    "CT_SEQUENCE", 0, 1, -1, (),
                    (
                        ("CT_LOG", 1, 2, -1, ("state 0 - init",), ()),
                        ("CT_TICK_DELAY", 3, 3, -1, (3,), ()),
                        ("CT_SET_FIELD", 2, 4, -1, ("state", 1), ()),
                    ),
                ),
                # case 1: sequence(log, delay 5, set state=2)
                (
                    "CT_SEQUENCE", 0, 5, -1, (),
                    (
                        ("CT_LOG", 1, 6, -1, ("state 1 - running",), ()),
                        ("CT_TICK_DELAY", 3, 7, -1, (5,), ()),
                        ("CT_SET_FIELD", 2, 8, -1, ("state", 2), ()),
                    ),
                ),
                # case 2: sequence(log, delay 2, set state=3)
                (
                    "CT_SEQUENCE", 0, 9, -1, (),
                    (
                        ("CT_LOG", 1, 10, -1, ("state 2 - finishing",), ()),
                        ("CT_TICK_DELAY", 3, 11, -1, (2,), ()),
                        ("CT_SET_FIELD", 2, 12, -1, ("state", 3), ()),
                    ),
                ),
                # case 3: sequence(log, terminate)
                (
                    "CT_SEQUENCE", 0, 13, -1, (),
                    (
                        ("CT_LOG", 1, 14, -1, ("state 3 - done, terminating",), ()),
                        ("CT_RETURN_TERMINATE", 6, 15, -1, (), ()),
                    ),
                ),
                # default: nop
                ("CT_NOP", 5, 16, -1, (), ()),
            ),
        ),
    ),
)

string_table = ()
string_index = ()
events = ()
