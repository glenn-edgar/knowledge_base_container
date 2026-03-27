# ===========================================================================
# S-Engine Integration Test 2 — se_engine composite node
# ===========================================================================

CC      ?= cc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=

DEPFLAGS = -MMD -MP

# --- Directories ---
BUILD_DIR       = build
CORE_LIB_DIR    = ../../runtime_binary
FUNC_LIB_DIR    = ../../runtime_functions
SE_LIB_DIR      = ../../s_expression
SE_LOCAL_DIR    = s_engine

# --- Include paths ---
CFLAGS += -I. \
          -I$(SE_LOCAL_DIR) \
          -I$(CORE_LIB_DIR)/include \
          -I$(FUNC_LIB_DIR)/include \
          -I$(SE_LIB_DIR)/include/s_engine

# --- Output binary ---
TARGET = main

# --- Static libraries (link order: functions before core, s_engine last) ---
CORE_LIB = $(CORE_LIB_DIR)/libcfl_binarycore.a
FUNC_LIB = $(FUNC_LIB_DIR)/libcfl_core_functions.a
SE_LIB   = $(SE_LIB_DIR)/lib/libs_s_engine.a

# --- Library source/header files (for change detection) ---
CORE_LIB_SOURCES = $(wildcard $(CORE_LIB_DIR)/src/*.c) $(wildcard $(CORE_LIB_DIR)/include/*.h)
FUNC_LIB_SOURCES = $(wildcard $(FUNC_LIB_DIR)/src/*.c) $(wildcard $(FUNC_LIB_DIR)/include/*.h)
SE_LIB_SOURCES   = $(wildcard $(SE_LIB_DIR)/runtime/*.c) $(wildcard $(SE_LIB_DIR)/include/s_engine/*.h)

# --- Local headers (generated chaintree + s_engine module) ---
LOCAL_HEADERS = $(wildcard *.h) $(wildcard $(SE_LOCAL_DIR)/*.h)

# --- Application sources ---
SRCS = $(wildcard *.c) $(wildcard $(SE_LOCAL_DIR)/*.c)
OBJS = $(patsubst %.c,$(BUILD_DIR)/%.o,$(notdir $(SRCS)))
DEPS = $(OBJS:.o=.d)

# ===========================================================================

.PHONY: all clean rebuild libs

all: $(TARGET)

# --- Build libraries first (headers get installed during library build) ---
libs: $(FUNC_LIB) $(CORE_LIB) $(SE_LIB)

# Link order: functions, then core, then s_engine, then math
$(TARGET): $(OBJS) $(FUNC_LIB) $(CORE_LIB) $(SE_LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(FUNC_LIB) $(CORE_LIB) $(SE_LIB) -lm

# --- Rebuild libraries when their sources change ---
$(CORE_LIB): $(CORE_LIB_SOURCES)
	$(MAKE) -C $(CORE_LIB_DIR)

# s_expression must build before runtime_functions (installs headers it needs)
$(FUNC_LIB): $(FUNC_LIB_SOURCES) $(SE_LIB)
	$(MAKE) -C $(FUNC_LIB_DIR)

$(SE_LIB): $(SE_LIB_SOURCES)
	$(MAKE) -C $(SE_LIB_DIR)/runtime

# --- Compile .c from current dir (libraries must build first to install headers) ---
$(BUILD_DIR)/%.o: %.c $(LOCAL_HEADERS) | $(BUILD_DIR) libs
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

# --- Compile .c from s_engine/ subdir ---
$(BUILD_DIR)/%.o: $(SE_LOCAL_DIR)/%.c $(LOCAL_HEADERS) | $(BUILD_DIR) libs
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

-include $(DEPS)

clean:
	rm -rf $(BUILD_DIR) $(TARGET)
	$(MAKE) -C $(CORE_LIB_DIR) clean
	$(MAKE) -C $(FUNC_LIB_DIR) clean
	$(MAKE) -C $(SE_LIB_DIR)/runtime cleanall

rebuild: clean all
