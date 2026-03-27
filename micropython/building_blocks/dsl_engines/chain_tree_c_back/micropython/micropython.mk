# micropython.mk — MicroPython USER_C_MODULES integration for CFL
#
# Usage: make USER_C_MODULES=../cfl_micropython
#
# Status: stub — will be populated after Lua 5.3 bridge is validated.

CFL_MOD_DIR := $(USERMOD_DIR)

SRC_USERMOD += $(CFL_MOD_DIR)/cfl_mp_bridge.c
# SRC_USERMOD += $(CFL_MOD_DIR)/cfl_core/cfl_engine.c   (etc.)

CFLAGS_USERMOD += -I$(CFL_MOD_DIR)
CFLAGS_USERMOD += -DCFL_MICROPYTHON=1
