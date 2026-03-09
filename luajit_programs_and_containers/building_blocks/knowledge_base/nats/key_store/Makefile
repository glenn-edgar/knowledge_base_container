# Makefile for NATS KeyStore + KbStore + JobQueue C Library
#
# Prerequisites:
#   sudo apt-get install libcjson-dev
#   # nats.c: build from source (see README.md)
#
# Usage:
#   make                # build everything
#   make libs           # static + shared libraries only
#   make test           # build all test binaries
#   make run-test       # build and run KV/KB tests
#   make run-test-jq    # build and run JobQueue tests
#   make run-demo       # run KV/KB demo
#   make run-demo-jq    # run JobQueue demo
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

# ---- Source files ----
KS_SRC     := $(SRC_DIR)/nats_key_store.c
KB_SRC     := $(SRC_DIR)/nats_kb_store.c
JQ_SRC     := $(SRC_DIR)/nats_job_queue.c
TEST_SRC   := $(TEST_DIR)/test_nats_kv.c
TESTJQ_SRC := $(TEST_DIR)/test_job_queue.c

# ---- Object files ----
KS_OBJ     := $(OBJ_DIR)/nats_key_store.o
KB_OBJ     := $(OBJ_DIR)/nats_kb_store.o
JQ_OBJ     := $(OBJ_DIR)/nats_job_queue.o
TEST_OBJ   := $(OBJ_DIR)/test_nats_kv.o
TESTJQ_OBJ := $(OBJ_DIR)/test_job_queue.o

# ---- Output libraries ----
STATIC_KS  := $(BUILD_DIR)/libnats_key_store.a
STATIC_KB  := $(BUILD_DIR)/libnats_kb_store.a
STATIC_JQ  := $(BUILD_DIR)/libnats_job_queue.a
SHARED_KS  := $(BUILD_DIR)/libnats_key_store.so
SHARED_KB  := $(BUILD_DIR)/libnats_kb_store.so
SHARED_JQ  := $(BUILD_DIR)/libnats_job_queue.so

# ---- Test binaries ----
TEST_BIN   := $(BUILD_DIR)/test_nats_kv
TESTJQ_BIN := $(BUILD_DIR)/test_job_queue

# ---------------------------------------------------------------

.PHONY: all libs test clean install dirs \
        run-test run-demo run-test-jq run-demo-jq run-all

all: libs test

dirs:
	@mkdir -p $(OBJ_DIR)

# ---- Compile objects ----

$(KS_OBJ): $(KS_SRC) $(INC_DIR)/nats_key_store.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(KB_OBJ): $(KB_SRC) $(INC_DIR)/nats_kb_store.h $(INC_DIR)/nats_key_store.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(JQ_OBJ): $(JQ_SRC) $(INC_DIR)/nats_job_queue.h $(INC_DIR)/nats_key_store.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(TEST_OBJ): $(TEST_SRC) $(INC_DIR)/nats_key_store.h $(INC_DIR)/nats_kb_store.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(TESTJQ_OBJ): $(TESTJQ_SRC) $(INC_DIR)/nats_job_queue.h $(INC_DIR)/nats_key_store.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

# ---- Static libraries ----

$(STATIC_KS): $(KS_OBJ)
	$(AR) rcs $@ $^

$(STATIC_KB): $(KB_OBJ)
	$(AR) rcs $@ $^

$(STATIC_JQ): $(JQ_OBJ)
	$(AR) rcs $@ $^

# ---- Shared libraries ----
# JobQueue and KbStore both depend on KeyStore, so link it in.

$(SHARED_KS): $(KS_OBJ)
	$(CC) -shared -o $@ $^ $(LIBS)

$(SHARED_KB): $(KB_OBJ) $(KS_OBJ)
	$(CC) -shared -o $@ $^ $(LIBS)

$(SHARED_JQ): $(JQ_OBJ) $(KS_OBJ)
	$(CC) -shared -o $@ $^ $(LIBS)

# ---- Aggregate targets ----

libs: $(STATIC_KS) $(STATIC_KB) $(STATIC_JQ) \
      $(SHARED_KS) $(SHARED_KB) $(SHARED_JQ)

test: $(TEST_BIN) $(TESTJQ_BIN)

# ---- Link test binaries ----

$(TEST_BIN): $(TEST_OBJ) $(STATIC_KB) $(STATIC_KS)
	$(CC) $(CFLAGS) -o $@ $(TEST_OBJ) $(STATIC_KB) $(STATIC_KS) $(LIBS)

$(TESTJQ_BIN): $(TESTJQ_OBJ) $(STATIC_JQ) $(STATIC_KS)
	$(CC) $(CFLAGS) -o $@ $(TESTJQ_OBJ) $(STATIC_JQ) $(STATIC_KS) $(LIBS)

# ---- Run targets ----

run-test: $(TEST_BIN)
	@echo ""
	@echo "NOTE: Requires NATS server at 127.0.0.1:4222 with JetStream"
	@echo ""
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TEST_BIN) all

run-demo: $(TEST_BIN)
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TEST_BIN) demo

run-test-jq: $(TESTJQ_BIN)
	@echo ""
	@echo "NOTE: Requires NATS server at 127.0.0.1:4222 with JetStream"
	@echo ""
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TESTJQ_BIN) tests

run-demo-jq: $(TESTJQ_BIN)
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TESTJQ_BIN) demo

run-all: $(TEST_BIN) $(TESTJQ_BIN)
	@echo ""
	@echo "NOTE: Requires NATS server at 127.0.0.1:4222 with JetStream"
	@echo ""
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TEST_BIN) all
	@echo ""
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TESTJQ_BIN) all

# ---- Install ----

install: libs
	install -d $(PREFIX)/lib $(PREFIX)/include/nats_kv
	install -m 644 $(STATIC_KS) $(STATIC_KB) $(STATIC_JQ) $(PREFIX)/lib/
	install -m 755 $(SHARED_KS) $(SHARED_KB) $(SHARED_JQ) $(PREFIX)/lib/
	install -m 644 $(INC_DIR)/nats_key_store.h $(PREFIX)/include/nats_kv/
	install -m 644 $(INC_DIR)/nats_kb_store.h  $(PREFIX)/include/nats_kv/
	install -m 644 $(INC_DIR)/nats_job_queue.h $(PREFIX)/include/nats_kv/

# ---- Clean ----

clean:
	rm -rf $(BUILD_DIR)