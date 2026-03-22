# Compiler and flags
CC = gcc
# Set to 0 or 1
MODULE_64BIT =0

CFLAGS = -Wall -Wextra -O2 -g -fsanitize=undefined -fno-omit-frame-pointer -Ilibrary_files -DMODULE_IS_64BIT=$(MODULE_64BIT)
LDFLAGS = -fsanitize=undefined -rdynamic
AR = ar
ARFLAGS = rcs

# Directories
LIB_DIR = library_files
GEN_DIR = generated_yaml_files
OBJ_DIR = obj
BIN_DIR = .

# Library name
LIBRARY = libcfl_runtime.a

# Header files tracking
ALL_HEADERS = $(wildcard $(LIB_DIR)/*.h) $(wildcard *.h) $(wildcard $(GEN_DIR)/*.h)
HEADER_MARKER = .header_timestamp

# Source files
LIB_SOURCES = $(wildcard $(LIB_DIR)/*.c)
LIB_OBJECTS = $(patsubst $(LIB_DIR)/%.c,$(OBJ_DIR)/%.o,$(LIB_SOURCES))

# Generated YAML files
GEN_SOURCES = $(wildcard $(GEN_DIR)/*.c)
GEN_OBJECTS = $(patsubst $(GEN_DIR)/%.c,$(OBJ_DIR)/gen_%.o,$(GEN_SOURCES))

# Top directory source files (all .c files)
TOP_SOURCES = $(wildcard *.c)
TOP_OBJECTS = $(patsubst %.c,$(OBJ_DIR)/%.o,$(TOP_SOURCES))

# Output executable
TARGET = $(BIN_DIR)/main

# Default target
all: $(TARGET)

# Header dependency tracking - if any header changes, clean and rebuild
$(HEADER_MARKER): $(ALL_HEADERS)
	@echo "Header files changed - cleaning build artifacts..."
	@$(MAKE) --no-print-directory clean-build
	@touch $(HEADER_MARKER)

# Create object directory if it doesn't exist
$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

# Compile library source files to object files
$(OBJ_DIR)/%.o: $(LIB_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Compile generated YAML files to object files
$(OBJ_DIR)/gen_%.o: $(GEN_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Compile top directory .c files to object files
$(OBJ_DIR)/%.o: %.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Create static library
$(LIBRARY): $(LIB_OBJECTS)
	$(AR) $(ARFLAGS) $@ $^
	@echo "Static library $(LIBRARY) created successfully"

# Link all top-level objects and generated objects with library
$(TARGET): $(HEADER_MARKER) $(TOP_OBJECTS) $(GEN_OBJECTS) $(LIBRARY)
	$(CC) $(CFLAGS) $(LDFLAGS) $(TOP_OBJECTS) $(GEN_OBJECTS) -L. -Wl,--whole-archive -lcfl_runtime -Wl,--no-whole-archive -o $@

# Clean build artifacts only (not header marker)
clean-build:
	@rm -rf $(OBJ_DIR) $(LIBRARY) $(TARGET)

# Clean everything including header marker
clean: clean-build
	@rm -f $(HEADER_MARKER)
	@echo "Cleaned all build artifacts"

# Clean and rebuild
rebuild: clean all

# Print variables (for debugging)
print:
	@echo "All headers: $(ALL_HEADERS)"
	@echo "Library sources: $(LIB_SOURCES)"
	@echo "Library objects: $(LIB_OBJECTS)"
	@echo "Generated sources: $(GEN_SOURCES)"
	@echo "Generated objects: $(GEN_OBJECTS)"
	@echo "Top sources: $(TOP_SOURCES)"
	@echo "Top objects: $(TOP_OBJECTS)"

.PHONY: all clean clean-build rebuild print