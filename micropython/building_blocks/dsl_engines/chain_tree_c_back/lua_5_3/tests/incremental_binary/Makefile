# ===========================================================================
# Lua 5.3+ Bridge Test — incremental_binary
#
# Links: cfl_lua53_bridge + runtime_binary + runtime_functions + lua5.4
# ===========================================================================

CC      ?= cc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=

DEPFLAGS = -MMD -MP

# --- Directories ---
BUILD_DIR       = build
BRIDGE_DIR      = ../..
CORE_LIB_DIR    = ../../../runtime_binary
FUNC_LIB_DIR    = ../../../runtime_functions
SE_LIB_DIR      = ../../../s_expression

# Original test dir (for generated headers: image.h, blackboard.h)
ORIG_TEST_DIR   = ../../../dsl_tests/incremental_binary

# --- Lua 5.4 (API-compatible with 5.3) ---
LUA_CFLAGS  := $(shell pkg-config --cflags lua5.4)
LUA_LDFLAGS := $(shell pkg-config --libs lua5.4)

# --- Include paths ---
CFLAGS += -I. \
          -I$(BRIDGE_DIR) \
          -I$(ORIG_TEST_DIR) \
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

# --- Application sources ---
SRCS = main.c $(BRIDGE_DIR)/cfl_lua53_bridge.c
OBJS = $(BUILD_DIR)/main.o $(BUILD_DIR)/cfl_lua53_bridge.o
DEPS = $(OBJS:.o=.d)

# ===========================================================================

.PHONY: all clean rebuild clean-libs clean-all run

all: $(TARGET)

$(TARGET): $(OBJS) $(FUNC_LIB) $(SE_LIB) $(CORE_LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(FUNC_LIB) $(SE_LIB) $(CORE_LIB) $(LUA_LDFLAGS) -lm

# --- Rebuild libraries when their sources change ---
$(CORE_LIB): $(CORE_LIB_SOURCES)
	$(MAKE) -C $(CORE_LIB_DIR)

$(FUNC_LIB): $(FUNC_LIB_SOURCES)
	$(MAKE) -C $(FUNC_LIB_DIR)

# --- Compile ---
$(BUILD_DIR)/main.o: main.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR)/cfl_lua53_bridge.o: $(BRIDGE_DIR)/cfl_lua53_bridge.c | $(BUILD_DIR)
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
