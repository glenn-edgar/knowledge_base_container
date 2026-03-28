# ===========================================================================
# Lua 5.3+ Bridge Test — incremental_binary
#
# Layout:
#   chain_tree_c/
#     bridges/lua_5_3/                    cfl_lua53_bridge.h/.c
#     runtime_binary/                     libcfl_binarycore.a
#     runtime_functions/                  libcfl_core_functions.a
#     s_expression/                       libs_s_engine.a
#     dsl_tests/
#       dsl_tests_c/incremental_binary/   generated .ctb, _image.h, _blackboard.h
#       dsl_tests_lua5.3/
#         incremental_binary/             <- you are here
# ===========================================================================

CC      ?= cc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=

DEPFLAGS = -MMD -MP

# --- Directories (relative to this Makefile) ---
BUILD_DIR       = build
ROOT            = ../../..
BRIDGE_DIR      = $(ROOT)/bridges/lua_5_3
CORE_LIB_DIR    = $(ROOT)/runtime_binary
FUNC_LIB_DIR    = $(ROOT)/runtime_functions
SE_LIB_DIR      = $(ROOT)/s_expression
GENERATED_DIR   = ../../dsl_tests_c/incremental_binary

# --- Lua 5.4 (API-compatible with 5.3) ---
LUA_CFLAGS  := $(shell pkg-config --cflags lua5.4)
LUA_LDFLAGS := $(shell pkg-config --libs lua5.4)

# --- Include paths ---
CFLAGS += -I. \
          -I$(BRIDGE_DIR) \
          -I$(GENERATED_DIR) \
          -I$(CORE_LIB_DIR)/include \
          -I$(FUNC_LIB_DIR)/include \
          -I$(SE_LIB_DIR)/include \
          $(LUA_CFLAGS)

# --- Output binary ---
TARGET = main

# --- Static libraries ---
CORE_LIB = $(CORE_LIB_DIR)/libcfl_binarycore.a
FUNC_LIB = $(FUNC_LIB_DIR)/libcfl_core_functions.a
SE_LIB   = $(SE_LIB_DIR)/lib/libs_s_engine.a

# --- Library sources (for change detection) ---
CORE_LIB_SOURCES = $(wildcard $(CORE_LIB_DIR)/src/*.c) $(wildcard $(CORE_LIB_DIR)/include/*.h)
FUNC_LIB_SOURCES = $(wildcard $(FUNC_LIB_DIR)/src/*.c) $(wildcard $(FUNC_LIB_DIR)/include/*.h)

# --- Application sources (main + bridge + avro/streaming/drone C files) ---
OBJS = $(BUILD_DIR)/main.o \
       $(BUILD_DIR)/cfl_lua53_bridge.o \
       $(BUILD_DIR)/user_avro_test_file.o \
       $(BUILD_DIR)/user_streaming_boolean.o \
       $(BUILD_DIR)/user_node_control_boolean_fns.o
DEPS = $(OBJS:.o=.d)

# ===========================================================================

.PHONY: all clean rebuild clean-libs clean-all run

all: $(TARGET)

$(TARGET): $(OBJS) $(FUNC_LIB) $(SE_LIB) $(CORE_LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(FUNC_LIB) $(SE_LIB) $(CORE_LIB) $(LUA_LDFLAGS) -lm

$(CORE_LIB): $(CORE_LIB_SOURCES)
	$(MAKE) -C $(CORE_LIB_DIR)

$(FUNC_LIB): $(FUNC_LIB_SOURCES)
	$(MAKE) -C $(FUNC_LIB_DIR)

$(BUILD_DIR)/main.o: main.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR)/cfl_lua53_bridge.o: $(BRIDGE_DIR)/cfl_lua53_bridge.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR)/%.o: %.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

-include $(DEPS)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

clean-libs:
	$(MAKE) -C $(CORE_LIB_DIR) clean
	$(MAKE) -C $(FUNC_LIB_DIR) clean

clean-all: clean clean-libs

rebuild: clean-all all
