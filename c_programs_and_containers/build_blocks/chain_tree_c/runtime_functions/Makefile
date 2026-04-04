# ===========================================================================
# libcfl_core_functions Makefile
#
# Directory layout:
#   include/   Public headers
#   src/       Implementation files
#   build/     Object files and dependency files (generated)
#
# Depends on:
#   ../runtime_h/include/       libcfl_core headers
#   ../runtime_h/libcfl_core.a  libcfl_core static library
#
# Uses GCC -MMD -MP for automatic header dependency tracking.
# ===========================================================================

CC      ?= gcc
AR      ?= ar
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
CFLAGS  += -fPIC

# -MMD: generate .d dependency files alongside .o files
# -MP:  add phony targets for headers (safe if a header is deleted)
DEPFLAGS = -MMD -MP

# --- Directories ---
SRC_DIR     = src
INC_DIR     = include
BUILD_DIR   = build
LIB_DIR     = ../runtime_binary
LIB_INC_DIR = $(LIB_DIR)/include
SE_INC_DIR  = ../s_expression/include/s_engine
JSON_INC_DIR = ../json_packets/include
CBOR_INC_DIR = ../cbor_packets/include
CJSON_DIR    = ../json_packets/src

# --- Include paths: own headers + library headers + s_engine headers + json/cbor packets ---
CFLAGS += -I$(INC_DIR) -I$(LIB_INC_DIR) -I$(SE_INC_DIR) -I$(JSON_INC_DIR) -I$(CBOR_INC_DIR) -I$(CJSON_DIR)

# --- Outputs ---
STATIC_LIB = libcfl_core_functions.a
SHARED_LIB = libcfl_core_functions.so

# --- Sources, objects, dependency files ---
SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRCS))
DEPS = $(OBJS:.o=.d)

# ===========================================================================

.PHONY: all clean rebuild

all: $(STATIC_LIB) $(SHARED_LIB)

$(STATIC_LIB): $(OBJS)
	$(AR) rcs $@ $^

$(SHARED_LIB): $(OBJS)
	$(CC) -shared -o $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

-include $(DEPS)

clean:
	rm -rf $(BUILD_DIR) $(STATIC_LIB) $(SHARED_LIB)

rebuild: clean all

