# Makefile for avro_dsl test system

CC = gcc
CFLAGS = -Wall -Wextra -g -I.
LUA = luajit

TARGET = test_avro

# Generated files
SCHEMA_HEADER = sensor_msgs.h
DATA_HEADER = sensor_data_data.h
DATA_BIN = sensor_data.bin

.PHONY: all clean generate run

all: generate $(TARGET)

# Generate headers and binary from DSL
generate: $(SCHEMA_HEADER) $(DATA_HEADER)

$(SCHEMA_HEADER): sensor_msgs.lua avro_dsl.lua
	$(LUA) sensor_msgs.lua

$(DATA_HEADER) $(DATA_BIN): sensor_data.lua sensor_msgs.lua avro_dsl.lua
	$(LUA) sensor_data.lua

# Build test program
$(TARGET): main.c $(SCHEMA_HEADER) $(DATA_HEADER)
	$(CC) $(CFLAGS) -o $@ main.c

# Run the test
run: all
	./$(TARGET) $(DATA_BIN)

# Clean generated files
clean:
	rm -f $(TARGET) $(SCHEMA_HEADER) $(DATA_HEADER) $(DATA_BIN)

# Show generated files
show:
	@echo "=== $(SCHEMA_HEADER) ==="
	@cat $(SCHEMA_HEADER)
	@echo ""
	@echo "=== $(DATA_HEADER) ==="
	@cat $(DATA_HEADER)
	@echo ""
	@echo "=== $(DATA_BIN) (hex) ==="
	@xxd $(DATA_BIN)