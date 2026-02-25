# knowledge_base_c top-level Makefile
CC       = gcc
AR       = ar
CFLAGS   = -Wall -Wextra -O2 -Iinclude -I/usr/include/cjson
LDFLAGS  = -lsqlite3 -lcjson -lm

SRC_DIR  = src
OBJ_DIR  = obj
LIB_DIR  = lib

SRCS     = $(wildcard $(SRC_DIR)/*.c)
OBJS     = $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.o,$(SRCS))

STATIC   = $(LIB_DIR)/libkb.a
SHARED   = $(LIB_DIR)/libkb.so

all: dirs $(STATIC) $(SHARED)

dirs:
	@mkdir -p $(OBJ_DIR) $(LIB_DIR)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -fPIC -c $< -o $@

$(STATIC): $(OBJS)
	$(AR) rcs $@ $^

$(SHARED): $(OBJS)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

tests:
	$(MAKE) -C tests

run-tests:
	$(MAKE) -C tests run

clean:
	rm -rf $(OBJ_DIR) $(LIB_DIR)
	$(MAKE) -C tests clean

.PHONY: all dirs tests run-tests clean
