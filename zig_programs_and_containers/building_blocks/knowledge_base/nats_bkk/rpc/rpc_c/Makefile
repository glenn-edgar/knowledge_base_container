# Makefile for NATS RPC Library
#
# Prerequisites:
#   sudo apt-get install libcjson-dev
#   # nats.c: build from source (see README.md)
#
# Usage:
#   make                # build everything
#   make libs           # static + shared libraries only
#   make test           # build test binary
#   make run-test       # build and run tests
#   make run-demo       # build and run demo
#   make clean          # remove build artifacts
#   make install        # install to PREFIX (default /usr/local)

CC       ?= gcc
AR       ?= ar
CFLAGS   := -Wall -Wextra -std=c11 -O2 -fPIC
CPPFLAGS := -I./include
LDFLAGS  :=
LIBS     := -lnats -lcjson -lpthread

PREFIX   ?= /usr/local

# Directories
SRC_DIR   := src
INC_DIR   := include
TEST_DIR  := test
BUILD_DIR := build
OBJ_DIR   := $(BUILD_DIR)/obj

# Source / Object files
RPC_SRC   := $(SRC_DIR)/nats_rpc.c
RPC_OBJ   := $(OBJ_DIR)/nats_rpc.o
TEST_SRC  := $(TEST_DIR)/test_nats_rpc.c
TEST_OBJ  := $(OBJ_DIR)/test_nats_rpc.o

# Output
STATIC_LIB := $(BUILD_DIR)/libnats_rpc.a
SHARED_LIB := $(BUILD_DIR)/libnats_rpc.so
TEST_BIN   := $(BUILD_DIR)/test_nats_rpc

# ---------------------------------------------------------------

.PHONY: all libs test clean install dirs run-test run-demo

all: libs test

dirs:
	@mkdir -p $(OBJ_DIR)

# ---- Compile ----

$(RPC_OBJ): $(RPC_SRC) $(INC_DIR)/nats_rpc.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(TEST_OBJ): $(TEST_SRC) $(INC_DIR)/nats_rpc.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

# ---- Libraries ----

$(STATIC_LIB): $(RPC_OBJ)
	$(AR) rcs $@ $^

$(SHARED_LIB): $(RPC_OBJ)
	$(CC) -shared -o $@ $^ $(LIBS)

libs: $(STATIC_LIB) $(SHARED_LIB)

# ---- Test binary ----

test: $(TEST_BIN)

$(TEST_BIN): $(TEST_OBJ) $(STATIC_LIB)
	$(CC) $(CFLAGS) -o $@ $(TEST_OBJ) $(STATIC_LIB) $(LIBS) -lm

# ---- Run targets ----

run-test: $(TEST_BIN)
	@echo ""
	@echo "NOTE: Requires NATS server at 127.0.0.1:4222"
	@echo "  Start with:  docker run -p 4222:4222 nats:latest"
	@echo ""
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TEST_BIN) tests

run-demo: $(TEST_BIN)
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TEST_BIN) demo

# ---- Install ----

install: libs
	install -d $(PREFIX)/lib $(PREFIX)/include/nats_rpc
	install -m 644 $(STATIC_LIB) $(PREFIX)/lib/
	install -m 755 $(SHARED_LIB) $(PREFIX)/lib/
	install -m 644 $(INC_DIR)/nats_rpc.h $(PREFIX)/include/nats_rpc/

# ---- Clean ----

clean:
	rm -rf $(BUILD_DIR)

