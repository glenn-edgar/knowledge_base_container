# tests/Makefile — Knowledge Base C Port
CC       = gcc
CFLAGS   = -Wall -Wextra -g -I../include -I/usr/include/cjson -I.
LDFLAGS  = -lsqlite3 -lcjson -lm

SRC_DIR  = ../src

LIB_SRCS = $(SRC_DIR)/kb_common.c \
           $(SRC_DIR)/kb_uuid.c \
           $(SRC_DIR)/kb_json.c \
           $(SRC_DIR)/kb_query_support.c \
           $(SRC_DIR)/bit_mask_rt_operations.c \
           $(SRC_DIR)/bit_s_expression.c \
           $(SRC_DIR)/kb_bit_structures.c \
           $(SRC_DIR)/kb_status_table.c \
           $(SRC_DIR)/kb_stream.c \
           $(SRC_DIR)/kb_job_queue.c \
           $(SRC_DIR)/kb_link_table.c \
           $(SRC_DIR)/kb_link_mount_table.c \
           $(SRC_DIR)/kb_rpc_server.c \
           $(SRC_DIR)/kb_rpc_client.c \
           $(SRC_DIR)/kb_data_structures.c

TESTS = test_kb_data_structures \
        test_kb_query_support \
        test_bit_s_expression

all: $(TESTS)

test_kb_data_structures: test_kb_data_structures.c $(LIB_SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_kb_query_support: test_kb_query_support.c $(LIB_SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_bit_s_expression: test_bit_s_expression.c $(LIB_SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

run: $(TESTS)
	@for t in $(TESTS); do \
		echo ""; \
		echo "=============================="; \
		echo "Running $$t"; \
		echo "=============================="; \
		./$$t; \
	done

clean:
	rm -f $(TESTS)

.PHONY: all run clean
