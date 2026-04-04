# ===========================================================================
# CBOR Packet Pipeline Test Makefile (Binary Image Path)
#
# Mirrors json_packet_test Makefile but for CBOR packet streaming.
# ===========================================================================

CC      ?= cc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=

DEPFLAGS = -MMD -MP

# --- Directories ---
BUILD_DIR       = build
CORE_LIB_DIR    = ../../../runtime_binary
FUNC_LIB_DIR    = ../../../runtime_functions
JSON_LIB_DIR    = ../../../json_packets
CBOR_LIB_DIR    = ../../../cbor_packets
SE_LIB_DIR      = ../../../s_expression

# --- Include paths ---
CFLAGS += -I. \
          -I$(CORE_LIB_DIR)/include \
          -I$(FUNC_LIB_DIR)/include \
          -I$(JSON_LIB_DIR)/include \
          -I$(CBOR_LIB_DIR)/include

# --- Output binary ---
TARGET = main

# --- Static libraries (link order: functions, cbor, json, core, s_engine) ---
CORE_LIB = $(CORE_LIB_DIR)/libcfl_binarycore.a
FUNC_LIB = $(FUNC_LIB_DIR)/libcfl_core_functions.a
JSON_LIB = $(JSON_LIB_DIR)/lib/libcfl_json_packets.a
CBOR_LIB = $(CBOR_LIB_DIR)/lib/libcfl_cbor_packets.a
SE_LIB   = $(SE_LIB_DIR)/lib/libs_s_engine.a

# --- Library source/header files (for change detection) ---
CORE_LIB_SOURCES = $(wildcard $(CORE_LIB_DIR)/src/*.c) $(wildcard $(CORE_LIB_DIR)/include/*.h)
FUNC_LIB_SOURCES = $(wildcard $(FUNC_LIB_DIR)/src/*.c) $(wildcard $(FUNC_LIB_DIR)/include/*.h)
JSON_LIB_SOURCES = $(wildcard $(JSON_LIB_DIR)/src/*.c) $(wildcard $(JSON_LIB_DIR)/include/*.h)
CBOR_LIB_SOURCES = $(wildcard $(CBOR_LIB_DIR)/src/*.c) $(wildcard $(CBOR_LIB_DIR)/include/*.h)

# --- Application sources (all .c in this directory) ---
SRCS = $(wildcard *.c)
OBJS = $(patsubst %.c,$(BUILD_DIR)/%.o,$(SRCS))
DEPS = $(OBJS:.o=.d)

# ===========================================================================

.PHONY: all clean rebuild libs run clean-libs clean-all dsl

all: $(TARGET)

libs: $(CBOR_LIB) $(JSON_LIB) $(FUNC_LIB) $(CORE_LIB)

$(TARGET): $(OBJS) $(FUNC_LIB) $(CBOR_LIB) $(JSON_LIB) $(CORE_LIB) $(SE_LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(FUNC_LIB) $(CBOR_LIB) $(JSON_LIB) $(CORE_LIB) $(SE_LIB) -lm

$(CORE_LIB): $(CORE_LIB_SOURCES)
	$(MAKE) -C $(CORE_LIB_DIR)

$(FUNC_LIB): $(FUNC_LIB_SOURCES)
	$(MAKE) -C $(FUNC_LIB_DIR)

$(JSON_LIB): $(JSON_LIB_SOURCES)
	$(MAKE) -C $(JSON_LIB_DIR)

$(CBOR_LIB): $(CBOR_LIB_SOURCES)
	$(MAKE) -C $(CBOR_LIB_DIR)

$(BUILD_DIR)/%.o: %.c | $(BUILD_DIR) libs
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

-include $(DEPS)

# --- DSL compilation ---
dsl:
	cd ../../../ && ./s_build_json.sh dsl_tests/dsl_tests_c/cbor_packet_test/cbor_packet_test.lua dsl_tests/dsl_tests_c/cbor_packet_test
	cd ../../../ && ./s_build_headers_binary.sh dsl_tests/dsl_tests_c/cbor_packet_test/cbor_packet_test.json dsl_tests/dsl_tests_c/cbor_packet_test chaintree_handle

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

clean-libs:
	$(MAKE) -C $(CORE_LIB_DIR) clean
	$(MAKE) -C $(FUNC_LIB_DIR) clean
	$(MAKE) -C $(JSON_LIB_DIR) clean
	$(MAKE) -C $(CBOR_LIB_DIR) clean

clean-all: clean clean-libs

rebuild: clean-all all
