# ==========================================================================
# se_bridge_test_module_mpy.py
# Port of s_test_binary: se_basic_load_test
# ==========================================================================

from micropython import const

main_funcs = (
    "CT_SEQUENCE",          # 0
    "CT_LOG",               # 1
    "CT_SE_TICK",           # 2
    "CT_TERMINATE_SYSTEM",  # 3
)

pred_funcs = ()

records = (
    (
        "se_test_state",
        (
            ("se_tree_ptr", "uint64", 0),
            ("tick_count", "int32", 0),
            ("test_result", "int32", 0),
            ("state", "int32", 0),
        ),
    ),
)

tree_order = ("se_basic_load_test",)

trees = (
    (
        "se_basic_load_test",  # T_NAME
        0xAAAABBBB,            # T_NAME_HASH
        5,                     # T_NODE_COUNT
        "se_test_state",       # T_RECORD
        # T_ROOT: sequence
        (
            "CT_SEQUENCE", 0, 0, -1, (),
            (
                ("CT_LOG", 1, 1, -1, ("SE bridge: loading state_machine_test",), ()),
                # CT_SE_TICK: node_data = (module_key, tree_name, bb_field)
                ("CT_SE_TICK", 2, 2, -1,
                    ("state_machine_test", "state_machine_test", "se_tree_ptr"), ()),
                ("CT_LOG", 1, 3, -1, ("SE bridge: tree complete, shutting down",), ()),
                ("CT_TERMINATE_SYSTEM", 3, 4, -1, (), ()),
            ),
        ),
    ),
)

string_table = ()
string_index = ()
events = ()
