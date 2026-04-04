# ===========================================================================
# libcfl_json_packets Makefile
#
# Directory layout:
#   include/   Public headers (cfl_json_packets.h)
#   src/       Implementation + vendored cJSON
#   build/     Object files and dependency files (generated)
#   lib/       Output library
#
# Depends on:
#   ../runtime_binary/include/   cfl_exception.h
#
# Uses GCC -MMD -MP for automatic header dependency tracking.
# ===========================================================================

CC      ?= gcc
AR      ?= ar
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
CFLAGS  += -fPIC

DEPFLAGS = -MMD -MP

# --- Directories ---
SRC_DIR     = src
INC_DIR     = include
BUILD_DIR   = build
LIB_DIR     = lib
EXC_INC_DIR = ../runtime_binary/include

# --- Include paths: own headers + own src (for cJSON.h) + exception header ---
CFLAGS += -I$(INC_DIR) -I$(SRC_DIR) -I$(EXC_INC_DIR)

# --- Outputs ---
STATIC_LIB = $(LIB_DIR)/libcfl_json_packets.a
SHARED_LIB = $(LIB_DIR)/libcfl_json_packets.so

# --- Sources, objects, dependency files ---
SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRCS))
DEPS = $(OBJS:.o=.d)

# ===========================================================================

.PHONY: all clean rebuild

all: $(STATIC_LIB) $(SHARED_LIB)

$(STATIC_LIB): $(OBJS) | $(LIB_DIR)
	$(AR) rcs $@ $^

$(SHARED_LIB): $(OBJS) | $(LIB_DIR)
	$(CC) -shared -o $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

-include $(DEPS)

clean:
	rm -rf $(BUILD_DIR) $(STATIC_LIB) $(SHARED_LIB)

rebuild: clean all
