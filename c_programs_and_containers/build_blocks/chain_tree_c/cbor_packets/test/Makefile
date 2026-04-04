# ===========================================================================
# CBOR standalone test Makefile
# ===========================================================================

CC      ?= gcc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L

CBOR_DIR = ..
JSON_DIR = ../../json_packets

CFLAGS += -I$(CBOR_DIR)/include \
          -I$(JSON_DIR)/include \
          -I$(JSON_DIR)/src \
          -I../../runtime_binary/include

CBOR_LIB = $(CBOR_DIR)/lib/libcfl_cbor_packets.a
JSON_LIB = $(JSON_DIR)/lib/libcfl_json_packets.a

TARGET = test_cbor

.PHONY: all clean run

all: $(TARGET)

$(TARGET): test_cbor.c $(CBOR_LIB) $(JSON_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CBOR_LIB) $(JSON_LIB) -lm

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)
