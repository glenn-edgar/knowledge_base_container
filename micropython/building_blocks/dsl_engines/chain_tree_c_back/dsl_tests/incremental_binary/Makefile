# ===========================================================================
# Application Makefile (Binary Image Path)
#
# Directory layout:
#   *.c                     Source files (main.c, user functions, etc.)
#   *.h                     Local headers (generated blackboard offsets, etc.)
#   build/                  Object files and dependency files (generated)
#
# Libraries (rebuilt automatically when their .c or .h files change):
#   ../../runtime_binary/          libcfl_binarycore.a
#   ../../runtime_functions/       libcfl_core_functions.a
#
# Uses GCC -MMD -MP for automatic header dependency tracking.
# ===========================================================================

CC      ?= cc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=

# -MMD: generate .d dependency files alongside .o files
# -MP:  add phony targets for headers (safe if a header is deleted)
DEPFLAGS = -MMD -MP

# --- Directories ---
BUILD_DIR       = build
CORE_LIB_DIR    = ../../runtime_binary
FUNC_LIB_DIR    = ../../runtime_functions
SE_LIB_DIR      = ../../s_expression

# --- Include paths: local + both libraries ---
CFLAGS += -I. -I$(CORE_LIB_DIR)/include -I$(FUNC_LIB_DIR)/include -I$(SE_LIB_DIR)/include

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

.PHONY: all clean rebuild clean-libs clean-all

all: $(TARGET)

# Link order matters: functions depends on core, so functions comes first
$(TARGET): $(OBJS) $(FUNC_LIB) $(SE_LIB) $(CORE_LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(FUNC_LIB) $(SE_LIB) $(CORE_LIB) -lm

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

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

clean-libs:
	$(MAKE) -C $(CORE_LIB_DIR) clean
	$(MAKE) -C $(FUNC_LIB_DIR) clean

clean-all: clean clean-libs

rebuild: clean-all all
