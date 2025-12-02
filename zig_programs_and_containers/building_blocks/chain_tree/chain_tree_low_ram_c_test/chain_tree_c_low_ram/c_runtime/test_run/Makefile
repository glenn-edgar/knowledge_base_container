# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -O2 -Ilibrary_files
AR = ar
ARFLAGS = rcs

# Directories
LIB_DIR = library_files
OBJ_DIR = obj
BIN_DIR = .

# Library name
LIBRARY = libcfl_runtime.a

# Source files
LIB_SOURCES = $(wildcard $(LIB_DIR)/*.c)
LIB_OBJECTS = $(patsubst $(LIB_DIR)/%.c,$(OBJ_DIR)/%.o,$(LIB_SOURCES))

# Top directory source files (all .c files)
TOP_SOURCES = $(wildcard *.c)
TOP_OBJECTS = $(patsubst %.c,$(OBJ_DIR)/%.o,$(TOP_SOURCES))

# Output executable
TARGET = $(BIN_DIR)/main

# Default target
all: $(TARGET)

# Create object directory if it doesn't exist
$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

# Compile library source files to object files
$(OBJ_DIR)/%.o: $(LIB_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Compile top directory .c files to object files
$(OBJ_DIR)/%.o: %.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Create static library
$(LIBRARY): $(LIB_OBJECTS)
	$(AR) $(ARFLAGS) $@ $^
	@echo "Static library $(LIBRARY) created successfully"

# Link all top-level objects with library
$(TARGET): $(TOP_OBJECTS) $(LIBRARY)
	$(CC) $(CFLAGS) $(TOP_OBJECTS) -L. -Wl,--whole-archive -lcfl_runtime -Wl,--no-whole-archive -o $@

# Clean build artifacts
clean:
	rm -rf $(OBJ_DIR) $(LIBRARY) $(TARGET)
	@echo "Cleaned all build artifacts"

# Clean and rebuild
rebuild: clean all

# Print variables (for debugging)
print:
	@echo "Library sources: $(LIB_SOURCES)"
	@echo "Library objects: $(LIB_OBJECTS)"
	@echo "Top sources: $(TOP_SOURCES)"
	@echo "Top objects: $(TOP_OBJECTS)"

.PHONY: all clean rebuild print