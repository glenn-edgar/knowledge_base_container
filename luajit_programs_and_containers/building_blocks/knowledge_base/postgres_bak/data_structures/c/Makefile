# Makefile for Knowledge Base C Library (PostgreSQL)
#
# Prerequisites:
#   sudo apt-get install libpq-dev libcjson-dev
#
# Build:
#   make            — build static + shared libraries and test
#   make lib        — build both libraries
#   make static     — build static library only
#   make shared     — build shared library only
#   make test       — build test driver
#   make run-test   — build and run test
#   make clean      — remove build artifacts

CC       ?= gcc
AR       ?= ar
CFLAGS   ?= -Wall -Wextra -O2 -std=c11 -D_DEFAULT_SOURCE
INCLUDES  = -Iinclude -I/usr/include/postgresql

# Directories
OBJ_DIR   = obj
SRC_DIR   = src

# Source files
LIB_SRCS = $(SRC_DIR)/kb_common.c \
           $(SRC_DIR)/kb_search.c \
           $(SRC_DIR)/kb_status.c \
           $(SRC_DIR)/kb_job_queue.c \
           $(SRC_DIR)/kb_stream.c \
           $(SRC_DIR)/kb_rpc_server.c \
           $(SRC_DIR)/kb_rpc_client.c \
           $(SRC_DIR)/kb_bit_structures.c \
           $(SRC_DIR)/kb_link_table.c \
           $(SRC_DIR)/kb_document.c

# Object files in obj/ directory
LIB_OBJS     = $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.o,$(LIB_SRCS))
LIB_OBJS_PIC = $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.pic.o,$(LIB_SRCS))

# Library names
STATIC_LIB = libkb_data_structures.a
SHARED_LIB = libkb_data_structures.so

# Test
TEST_SRC  = tests/test_driver.c
TEST_BIN  = tests/test_driver

LDFLAGS   = -lpq -lcjson -lm

.PHONY: all lib static shared test run-test clean

all: lib test

lib: static shared

# --- Static library ---
static: $(STATIC_LIB)

$(STATIC_LIB): $(LIB_OBJS)
	$(AR) rcs $@ $^

# Compile to obj/ (no -fPIC, for static lib)
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# --- Shared library ---
shared: $(SHARED_LIB)

$(SHARED_LIB): $(LIB_OBJS_PIC)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

# Compile to obj/ with -fPIC (for shared lib)
$(OBJ_DIR)/%.pic.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -fPIC $(INCLUDES) -c $< -o $@

# Create obj directory
$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

# --- Test driver ---
test: $(TEST_BIN)

$(TEST_BIN): $(TEST_SRC) $(STATIC_LIB)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(STATIC_LIB) $(LDFLAGS)

# Build and run
run-test: test
	@echo "================================================"
	@echo "Running test_driver..."
	@echo "================================================"
	POSTGRES_PASSWORD=$${POSTGRES_PASSWORD} ./$(TEST_BIN)

clean:
	rm -rf $(OBJ_DIR) $(STATIC_LIB) $(SHARED_LIB) $(TEST_BIN)

# --- Header dependencies ---
$(OBJ_DIR)/kb_common.o $(OBJ_DIR)/kb_common.pic.o:                 include/kb_common.h
$(OBJ_DIR)/kb_search.o $(OBJ_DIR)/kb_search.pic.o:                 include/kb_common.h include/kb_search.h
$(OBJ_DIR)/kb_status.o $(OBJ_DIR)/kb_status.pic.o:                 include/kb_common.h include/kb_status.h
$(OBJ_DIR)/kb_job_queue.o $(OBJ_DIR)/kb_job_queue.pic.o:           include/kb_common.h include/kb_job_queue.h
$(OBJ_DIR)/kb_stream.o $(OBJ_DIR)/kb_stream.pic.o:                 include/kb_common.h include/kb_stream.h
$(OBJ_DIR)/kb_rpc_server.o $(OBJ_DIR)/kb_rpc_server.pic.o:         include/kb_common.h include/kb_rpc_server.h
$(OBJ_DIR)/kb_rpc_client.o $(OBJ_DIR)/kb_rpc_client.pic.o:         include/kb_common.h include/kb_rpc_client.h
$(OBJ_DIR)/kb_bit_structures.o $(OBJ_DIR)/kb_bit_structures.pic.o: include/kb_common.h include/kb_search.h include/kb_bit_structures.h
$(OBJ_DIR)/kb_link_table.o $(OBJ_DIR)/kb_link_table.pic.o:         include/kb_common.h include/kb_link_table.h
$(OBJ_DIR)/kb_document.o $(OBJ_DIR)/kb_document.pic.o:             include/kb_common.h include/kb_document.h