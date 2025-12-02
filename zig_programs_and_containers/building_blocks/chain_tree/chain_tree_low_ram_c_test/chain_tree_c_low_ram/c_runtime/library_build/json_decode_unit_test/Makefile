# Makefile for JSON Decoder Unit Tests

CC = gcc
CFLAGS = -Wall -Wextra -Werror -std=c11 -g -O2
LDFLAGS = -lm

# Source files
SOURCES = main.c json_decoder.c json_query.c
HEADERS = json_decoder.h json_query.h
OBJECTS = $(SOURCES:.c=.o)

# Target executable
TARGET = json_test

# Default target
all: $(TARGET)

# Link
$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

# Compile
%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

# Run tests
test: $(TARGET)
	./$(TARGET)

# Run with valgrind for memory checks
valgrind: $(TARGET)
	valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes ./$(TARGET)

# Clean build artifacts
clean:
	rm -f $(OBJECTS) $(TARGET)

# Clean and rebuild
rebuild: clean all

# Show help
help:
	@echo "Available targets:"
	@echo "  all       - Build the test executable (default)"
	@echo "  test      - Build and run tests"
	@echo "  valgrind  - Run tests with valgrind memory checking"
	@echo "  clean     - Remove build artifacts"
	@echo "  rebuild   - Clean and rebuild"
	@echo "  help      - Show this help message"

.PHONY: all test valgrind clean rebuild help
