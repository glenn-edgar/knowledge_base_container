CC       ?= gcc
AR       ?= ar
CFLAGS   := -Wall -Wextra -std=c11 -O2 -fPIC -Wno-format-truncation
CPPFLAGS := -I./include
LIBS     := -lmosquitto -lcjson -lpthread
PREFIX   ?= /usr/local

SRC_DIR   := src
INC_DIR   := include
TEST_DIR  := test
BUILD_DIR := build
OBJ_DIR   := $(BUILD_DIR)/obj

LIB_SRC   := $(SRC_DIR)/rpc_pubsub.c
LIB_OBJ   := $(OBJ_DIR)/rpc_pubsub.o
TEST_SRC  := $(TEST_DIR)/rpc_pubsub_test.c
TEST_OBJ  := $(OBJ_DIR)/rpc_pubsub_test.o

STATIC_LIB := $(BUILD_DIR)/librpc_pubsub.a
SHARED_LIB := $(BUILD_DIR)/librpc_pubsub.so
TEST_BIN   := $(BUILD_DIR)/rpc_pubsub_test

.PHONY: all libs test clean install dirs run-test

all: libs test

dirs:
	@mkdir -p $(OBJ_DIR)

$(LIB_OBJ): $(LIB_SRC) $(INC_DIR)/rpc_pubsub.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(TEST_OBJ): $(TEST_SRC) $(INC_DIR)/rpc_pubsub.h | dirs
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(STATIC_LIB): $(LIB_OBJ)
	$(AR) rcs $@ $^

$(SHARED_LIB): $(LIB_OBJ)
	$(CC) -shared -o $@ $^ $(LIBS)

libs: $(STATIC_LIB) $(SHARED_LIB)

test: $(TEST_BIN)

$(TEST_BIN): $(TEST_OBJ) $(STATIC_LIB)
	$(CC) $(CFLAGS) -o $@ $(TEST_OBJ) $(STATIC_LIB) $(LIBS) -lm

run-test: $(TEST_BIN)
	LD_LIBRARY_PATH=$(BUILD_DIR) $(TEST_BIN)

install: libs
	install -d $(PREFIX)/lib $(PREFIX)/include/rpc_pubsub
	install -m 644 $(STATIC_LIB) $(PREFIX)/lib/
	install -m 755 $(SHARED_LIB) $(PREFIX)/lib/
	install -m 644 $(INC_DIR)/rpc_pubsub.h $(PREFIX)/include/rpc_pubsub/

clean:
	rm -rf $(BUILD_DIR)