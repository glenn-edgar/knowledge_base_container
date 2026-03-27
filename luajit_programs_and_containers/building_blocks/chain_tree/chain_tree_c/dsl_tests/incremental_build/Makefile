# ===========================================================================
# Application Makefile
#
# Directory layout:
#   *.c                     Top-level source files (main.c, etc.)
#   incr_build/             Incremental build subsystem (.c and .h)
#   build/                  Object files and dependency files (generated)
#
# Libraries:
#   ../../runtime_h/                 libcfl_core (engine, allocators, events)
#   ../../runtime_functions/            libcfl_core_functions (node types, SM, etc.)
#
# Uses GCC -MMD -MP for automatic header dependency tracking.
# ===========================================================================

CC      ?= gcc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=

# -MMD: generate .d dependency files alongside .o filescor
# -MP:  add phony targets for headers (safe if a header is deleted)
DEPFLAGS = -MMD -MP

# --- Directories ---
BUILD_DIR       = build
INCR_DIR        = incr
CORE_LIB_DIR    = ../../runtime_h
FUNC_LIB_DIR    = ../../runtime_functions

# --- Include paths: local + both libraries ---
CFLAGS += -I$(INCR_DIR) -I$(CORE_LIB_DIR)/include -I$(FUNC_LIB_DIR)/include

# --- Output binary ---
TARGET = main

# --- Static libraries (link order: functions first, then core) ---
CORE_LIB = $(CORE_LIB_DIR)/libcfl_core.a
FUNC_LIB = $(FUNC_LIB_DIR)/libcfl_core_functions.a

# --- Sources: all .c in top directory + all .c in incr_build/ ---
TOP_SRCS  = $(wildcard *.c)
INCR_SRCS = $(wildcard $(INCR_DIR)/*.c)

# --- Objects (all go into build/) ---
OBJS  = $(patsubst %.c,$(BUILD_DIR)/%.o,$(TOP_SRCS))
OBJS += $(patsubst $(INCR_DIR)/%.c,$(BUILD_DIR)/%.o,$(INCR_SRCS))

DEPS = $(OBJS:.o=.d)

# ===========================================================================

.PHONY: all clean rebuild

all: $(TARGET)

# Link order matters: functions depends on core, so functions comes first
$(TARGET): $(OBJS) $(FUNC_LIB) $(CORE_LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(FUNC_LIB) $(CORE_LIB) -lm

# --- Top-level .c files ---
$(BUILD_DIR)/%.o: %.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

# --- incr_build/ .c files ---
$(BUILD_DIR)/%.o: $(INCR_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

-include $(DEPS)

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

rebuild: clean all