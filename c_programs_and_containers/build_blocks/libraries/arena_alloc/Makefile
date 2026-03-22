# Makefile for Arena Stack Memory Allocator

# Compiler and flags
CC = gcc
AR = ar
CFLAGS = -Wall -Wextra -std=c11 -O2 -fPIC
LDFLAGS = 
ARFLAGS = rcs

# Directories
PREFIX ?= /usr/local
LIBDIR = $(PREFIX)/lib
INCLUDEDIR = $(PREFIX)/include

# Library names
LIBRARY_NAME = arena_stack
STATIC_LIB = lib$(LIBRARY_NAME).a
SHARED_LIB = lib$(LIBRARY_NAME).so
VERSION = 1.0.0
SHARED_LIB_VERSIONED = $(SHARED_LIB).$(VERSION)

# Source files
LIB_SRC = arena_stack.c
LIB_OBJ = $(LIB_SRC:.c=.o)
EXAMPLE_SRC = example_arena_stack.c
EXAMPLE_BIN = arena_example

# Header files
HEADERS = arena_stack.h

# Debug build option
ifdef DEBUG
    CFLAGS += -g -O0 -DDEBUG
else
    CFLAGS += -DNDEBUG
endif

# Default target
.PHONY: all
all: $(STATIC_LIB) $(SHARED_LIB) $(EXAMPLE_BIN)

# Build static library
$(STATIC_LIB): $(LIB_OBJ)
	@echo "Creating static library: $@"
	$(AR) $(ARFLAGS) $@ $^
	@echo "Static library created successfully"

# Build shared library
$(SHARED_LIB): $(LIB_OBJ)
	@echo "Creating shared library: $@"
	$(CC) -shared -Wl,-soname,$(SHARED_LIB).1 -o $(SHARED_LIB_VERSIONED) $^ $(LDFLAGS)
	ln -sf $(SHARED_LIB_VERSIONED) $(SHARED_LIB)
	ln -sf $(SHARED_LIB_VERSIONED) $(SHARED_LIB).1
	@echo "Shared library created successfully"

# Compile library source to object file
$(LIB_OBJ): $(LIB_SRC) $(HEADERS)
	@echo "Compiling: $<"
	$(CC) $(CFLAGS) -c $< -o $@

# Build example program (linked with static library)
$(EXAMPLE_BIN): $(EXAMPLE_SRC) $(STATIC_LIB)
	@echo "Building example program: $@"
	$(CC) $(CFLAGS) $< -L. -l$(LIBRARY_NAME) -o $@
	@echo "Example program built successfully"

# Build example with shared library
.PHONY: example-shared
example-shared: $(EXAMPLE_SRC) $(SHARED_LIB)
	@echo "Building example program with shared library"
	$(CC) $(CFLAGS) $(EXAMPLE_SRC) -L. -l$(LIBRARY_NAME) -Wl,-rpath,. -o $(EXAMPLE_BIN)_shared
	@echo "Example program (shared) built successfully"

# Run the example program
.PHONY: run
run: $(EXAMPLE_BIN)
	@echo "Running example program..."
	@./$(EXAMPLE_BIN)

# Run example with shared library
.PHONY: run-shared
run-shared: example-shared
	@echo "Running example program (shared library)..."
	@LD_LIBRARY_PATH=. ./$(EXAMPLE_BIN)_shared

# Install libraries and headers
.PHONY: install
install: $(STATIC_LIB) $(SHARED_LIB)
	@echo "Installing libraries to $(LIBDIR)"
	install -d $(LIBDIR)
	install -m 644 $(STATIC_LIB) $(LIBDIR)
	install -m 755 $(SHARED_LIB_VERSIONED) $(LIBDIR)
	ln -sf $(SHARED_LIB_VERSIONED) $(LIBDIR)/$(SHARED_LIB)
	ln -sf $(SHARED_LIB_VERSIONED) $(LIBDIR)/$(SHARED_LIB).1
	@echo "Installing headers to $(INCLUDEDIR)"
	install -d $(INCLUDEDIR)
	install -m 644 $(HEADERS) $(INCLUDEDIR)
	@echo "Installation complete"
	@echo "Run 'ldconfig' as root to update library cache"

# Uninstall libraries and headers
.PHONY: uninstall
uninstall:
	@echo "Uninstalling from $(PREFIX)"
	rm -f $(LIBDIR)/$(STATIC_LIB)
	rm -f $(LIBDIR)/$(SHARED_LIB)*
	rm -f $(INCLUDEDIR)/$(HEADERS)
	@echo "Uninstallation complete"

# Debug build
.PHONY: debug
debug: clean
	@$(MAKE) DEBUG=1 all

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -f $(LIB_OBJ)
	rm -f $(STATIC_LIB)
	rm -f $(SHARED_LIB)*
	rm -f $(EXAMPLE_BIN)
	rm -f $(EXAMPLE_BIN)_shared
	@echo "Clean complete"

# Create a release tarball
.PHONY: dist
dist: clean
	@echo "Creating distribution tarball..."
	tar -czf arena_stack-$(VERSION).tar.gz \
		$(LIB_SRC) $(EXAMPLE_SRC) $(HEADERS) \
		Makefile README.md
	@echo "Distribution tarball created: arena_stack-$(VERSION).tar.gz"

# Run tests (if you add test files later)
.PHONY: test
test: $(EXAMPLE_BIN)
	@echo "Running tests..."
	@./$(EXAMPLE_BIN)
	@echo "All tests passed!"

# Check code with static analyzer
.PHONY: analyze
analyze:
	@echo "Running static analysis..."
	$(CC) $(CFLAGS) --analyze $(LIB_SRC)
	$(CC) $(CFLAGS) --analyze $(EXAMPLE_SRC)
	@echo "Analysis complete"

# Format code (requires clang-format)
.PHONY: format
format:
	@echo "Formatting code..."
	@if command -v clang-format >/dev/null 2>&1; then \
		clang-format -i $(LIB_SRC) $(EXAMPLE_SRC) $(HEADERS); \
		echo "Formatting complete"; \
	else \
		echo "clang-format not found, skipping"; \
	fi

# Show help
.PHONY: help
help:
	@echo "Arena Stack Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all              - Build static library, shared library, and example (default)"
	@echo "  $(STATIC_LIB)    - Build static library only"
	@echo "  $(SHARED_LIB)    - Build shared library only"
	@echo "  $(EXAMPLE_BIN)   - Build example program"
	@echo "  example-shared   - Build example with shared library"
	@echo "  run              - Build and run example program"
	@echo "  run-shared       - Build and run example with shared library"
	@echo "  install          - Install libraries and headers to $(PREFIX)"
	@echo "  uninstall        - Remove installed files"
	@echo "  debug            - Build with debug symbols"
	@echo "  clean            - Remove all build artifacts"
	@echo "  dist             - Create distribution tarball"
	@echo "  test             - Run tests"
	@echo "  analyze          - Run static code analysis"
	@echo "  format           - Format code with clang-format"
	@echo "  help             - Show this help message"
	@echo ""
	@echo "Variables:"
	@echo "  PREFIX           - Installation prefix (default: /usr/local)"
	@echo "  DEBUG            - Build with debug symbols (make DEBUG=1)"
	@echo ""
	@echo "Examples:"
	@echo "  make                    # Build everything"
	@echo "  make run                # Build and run example"
	@echo "  make DEBUG=1            # Build with debug symbols"
	@echo "  make install PREFIX=~/.local  # Install to home directory"

# Phony targets that don't represent files
.PHONY: all clean install uninstall run test help debug dist analyze format \
        example-shared run-shared