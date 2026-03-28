# ===========================================================================
# MicroPython Bridge Test — incremental_binary
#
# Two test modes:
#   make run       — C-only test (all user functions in C, no bridge)
#   make run-mpy   — MicroPython bridge test (user_functions.py via custom micropython)
#
# Layout:
#   chain_tree_c/
#     bridges/cfl_micropython/              cfl_mp_bridge.h/.c
#     runtime_binary/                       libcfl_binarycore.a
#     runtime_functions/                    libcfl_core_functions.a
#     s_expression/                         libs_s_engine.a
#     dsl_tests/
#       dsl_tests_c/incremental_binary/     generated .ctb, _image.h, _blackboard.h
#       dsl_tests_micropython/
#         incremental_binary/               <- you are here
#
# MicroPython bridge usage:
#   Build custom micropython unix port with CFL module:
#     cd micropython/ports/unix
#     make USER_C_MODULES=.../chain_tree_c/bridges
#   Then: make run-mpy MICROPYTHON=<path-to-custom-micropython>
# ===========================================================================

CC      ?= cc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=
MICROPYTHON ?= ./micropython

# -MMD: generate .d dependency files alongside .o files
# -MP:  add phony targets for headers (safe if a header is deleted)
DEPFLAGS = -MMD -MP

# --- Directories ---
BUILD_DIR       = build
ROOT            = ../../..
CORE_LIB_DIR    = $(ROOT)/runtime_binary
FUNC_LIB_DIR    = $(ROOT)/runtime_functions
SE_LIB_DIR      = $(ROOT)/s_expression
GENERATED_DIR   = ../../dsl_tests_c/incremental_binary
MPY_DIR         ?= $(ROOT)/../../../micropython/micropython_source
MPY_PORT_DIR    ?= $(MPY_DIR)/ports/unix
MPY_BUILD_DIR   ?= $(MPY_PORT_DIR)/build-standard

# --- Include paths: local + libraries + generated + MicroPython ---
CFLAGS += -I. -I$(GENERATED_DIR) -I$(CORE_LIB_DIR)/include -I$(FUNC_LIB_DIR)/include
CFLAGS += -I$(MPY_DIR) -I$(MPY_PORT_DIR) -I$(MPY_BUILD_DIR) -I$(MPY_PORT_DIR)/variants/standard

# --- Output binary ---
TARGET = main

# --- Static libraries ---
CORE_LIB = $(CORE_LIB_DIR)/libcfl_binarycore.a
FUNC_LIB = $(FUNC_LIB_DIR)/libcfl_core_functions.a
SE_LIB   = $(SE_LIB_DIR)/lib/libs_s_engine.a

# --- Library source/header files (for change detection) ---
CORE_LIB_SOURCES = $(wildcard $(CORE_LIB_DIR)/src/*.c) $(wildcard $(CORE_LIB_DIR)/include/*.h)
FUNC_LIB_SOURCES = $(wildcard $(FUNC_LIB_DIR)/src/*.c) $(wildcard $(FUNC_LIB_DIR)/include/*.h)

# --- Application sources ---
SRCS = $(wildcard *.c)
OBJS = $(patsubst %.c,$(BUILD_DIR)/%.o,$(SRCS))
DEPS = $(OBJS:.o=.d)

# ===========================================================================

.PHONY: all clean rebuild clean-libs clean-all run run-mpy

all: $(TARGET)

# Link order matters: functions depends on core and s_expression, so functions comes first
$(TARGET): $(OBJS) $(FUNC_LIB) $(CORE_LIB) $(SE_LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(FUNC_LIB) $(CORE_LIB) $(SE_LIB) -lm

# --- Rebuild libraries when any of their .c or .h files change ---
$(CORE_LIB): $(CORE_LIB_SOURCES)
	$(MAKE) -C $(CORE_LIB_DIR)

$(FUNC_LIB): $(FUNC_LIB_SOURCES)
	$(MAKE) -C $(FUNC_LIB_DIR)

# --- Source files ---
$(BUILD_DIR)/%.o: %.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

-include $(DEPS)

# --- C-only test ---
run: $(TARGET)
	./$(TARGET)

# --- MicroPython bridge test ---
# Requires custom micropython with CFL module compiled in.
# Set MICROPYTHON to the path of the custom binary.
run-mpy:
	$(MICROPYTHON) test_first.py

run-mpy-user:
	$(MICROPYTHON) -c "exec(open('user_functions.py').read()); print('user_functions.py loaded OK')"

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

clean-libs:
	$(MAKE) -C $(CORE_LIB_DIR) clean
	$(MAKE) -C $(FUNC_LIB_DIR) clean

clean-all: clean clean-libs

rebuild: clean-all all
