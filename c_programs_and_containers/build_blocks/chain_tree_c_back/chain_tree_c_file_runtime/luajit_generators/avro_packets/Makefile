# Makefile for avro_dsl generated files
#
# Usage:
#   make generate  - Generate .h, _bin.h, .bin from schema
#   make test      - Build and run tests
#   make clean     - Remove generated files
#   make all       - Generate and test

CC = gcc
CFLAGS = -Wall -Wextra -O2
LUA = luajit

SCHEMA = test_schema.lua
NAME = sensor_data

.PHONY: all generate test clean

all: generate test

# Generate all outputs from schema
generate: $(NAME).h $(NAME)_bin.h $(NAME).bin

$(NAME).h $(NAME)_bin.h $(NAME).bin: $(SCHEMA) avro_dsl.lua
	$(LUA) $(SCHEMA)

# Build and run test program
test: test_sensor
	./test_sensor $(NAME).bin

test_sensor: main.c $(NAME).h $(NAME)_bin.h
	$(CC) $(CFLAGS) -o $@ main.c

# Clean generated files
clean:
	rm -f $(NAME).h $(NAME)_bin.h $(NAME).bin test_sensor

# Show generated sizes
sizes: $(NAME).h $(NAME)_bin.h $(NAME).bin
	@echo "Generated files:"
	@ls -la $(NAME).h $(NAME)_bin.h $(NAME).bin
	@echo ""
	@echo "Binary schema: $(shell wc -c < $(NAME).bin) bytes"
	@echo "C header:      $(shell wc -l < $(NAME).h) lines"

