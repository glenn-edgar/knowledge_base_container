# micropython.mk — CFL ChainTree bridge for MicroPython
#
# Used by: make USER_C_MODULES=.../chain_tree_c/bridges
# MicroPython scans USER_C_MODULES/*/micropython.mk

CFL_MOD_DIR := $(USERMOD_DIR)
CFL_ROOT    := $(CFL_MOD_DIR)/../..
CFL_MPY_TEST := $(CFL_ROOT)/dsl_tests/dsl_tests_micropython/incremental_binary

# Bridge source (processed for MP_QSTR_ and MP_REGISTER_MODULE)
SRC_USERMOD_C += $(CFL_MOD_DIR)/cfl_mp_bridge.c

# CFL runtime library sources (no MP macros, use LIB variant)
SRC_USERMOD_LIB_C += $(wildcard $(CFL_ROOT)/runtime_binary/src/*.c)
SRC_USERMOD_LIB_C += $(wildcard $(CFL_ROOT)/runtime_functions/src/*.c)
SRC_USERMOD_LIB_C += $(filter-out %/s_engine_exception.c,$(wildcard $(CFL_ROOT)/s_expression/runtime/*.c))

# C user functions (avro/streaming/drone — stay in C, not ported to Python)
# Use SRC_USERMOD_C so CFLAGS_USERMOD (-Wno-error) applies
SRC_USERMOD_C += $(CFL_MPY_TEST)/user_avro_test_file.c
SRC_USERMOD_C += $(CFL_MPY_TEST)/user_streaming_boolean.c
SRC_USERMOD_C += $(CFL_MPY_TEST)/user_node_control_boolean_fns.c
SRC_USERMOD_C += $(CFL_MPY_TEST)/cfl_c_user_functions.c

# Include paths
CFLAGS_USERMOD += -I$(CFL_MOD_DIR)
CFLAGS_USERMOD += -I$(CFL_ROOT)/runtime_binary/include
CFLAGS_USERMOD += -I$(CFL_ROOT)/runtime_functions/include
CFLAGS_USERMOD += -I$(CFL_ROOT)/s_expression/include
CFLAGS_USERMOD += -I$(CFL_ROOT)/s_expression/include/s_engine
CFLAGS_USERMOD += -I$(CFL_MPY_TEST)
CFLAGS_USERMOD += -DCFL_MICROPYTHON=1
CFLAGS_USERMOD += -Wno-double-promotion -Wno-float-conversion -Wno-error

# Relax warnings for CFL library sources (not processed for MP_QSTR_)
CFLAGS_USERMOD_LIB += -Wno-double-promotion -Wno-float-conversion -Wno-error
