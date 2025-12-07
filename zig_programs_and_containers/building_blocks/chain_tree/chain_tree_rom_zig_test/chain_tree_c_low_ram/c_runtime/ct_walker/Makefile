# ============================================================================
# CT Tree Walker Makefile
# ============================================================================
# Builds static library, dynamic library, and example program
# No install targets - configure for your cross-compiler environment
# ============================================================================

# Compiler and tools
CC = gcc
AR = ar
RANLIB = ranlib

# Compiler flags
CFLAGS = -Wall -Wextra -std=c99 -O2 -I.
CFLAGS_DEBUG = -Wall -Wextra -std=c99 -g -I.
CFLAGS_PIC = -fPIC

# Linker flags
LDFLAGS =
LDFLAGS_SHARED = -shared

# Library version
VERSION_MAJOR = 1
VERSION_MINOR = 0
VERSION = $(VERSION_MAJOR).$(VERSION_MINOR)

# Target names
LIB_STATIC = libct_tree_walker.a
LIB_SHARED = libct_tree_walker.so
LIB_SHARED_VERSIONED = $(LIB_SHARED).$(VERSION)
EXAMPLE = example

# Source files
LIB_SRC = CT_Tree_Walker.c
LIB_OBJ = $(LIB_SRC:.c=.o)
LIB_OBJ_PIC = $(LIB_SRC:.c=.pic.o)

EXAMPLE_SRC = example.c
EXAMPLE_OBJ = $(EXAMPLE_SRC:.c=.o)

# ============================================================================
# PHONY TARGETS
# ============================================================================

.PHONY: all clean help info size libs example \
        static shared debug clean-libs clean-example

# ============================================================================
# DEFAULT TARGET
# ============================================================================

all: libs example

# ============================================================================
# LIBRARY TARGETS
# ============================================================================

libs: static shared

# Static library
static: $(LIB_STATIC)

$(LIB_STATIC): $(LIB_OBJ)
	@echo "Creating static library: $@"
	$(AR) rcs $@ $^
	$(RANLIB) $@

# Shared library
shared: $(LIB_SHARED)

$(LIB_SHARED): $(LIB_OBJ_PIC)
	@echo "Creating shared library: $@"
	$(CC) $(LDFLAGS_SHARED) -o $(LIB_SHARED_VERSIONED) $^
	ln -sf $(LIB_SHARED_VERSIONED) $(LIB_SHARED)

# ============================================================================
# EXAMPLE TARGET
# ============================================================================

example: $(EXAMPLE)

$(EXAMPLE): $(EXAMPLE_OBJ) $(LIB_STATIC)
	@echo "Linking example: $@"
	$(CC) $(LDFLAGS) -o $@ $(EXAMPLE_OBJ) -L. -lct_tree_walker

# ============================================================================
# OBJECT FILE RULES
# ============================================================================

# Regular object files (for static lib and example)
%.o: %.c CT_Tree_Walker.h
	@echo "Compiling: $<"
	$(CC) $(CFLAGS) -c -o $@ $<

# PIC object files (for shared lib)
%.pic.o: %.c CT_Tree_Walker.h
	@echo "Compiling (PIC): $<"
	$(CC) $(CFLAGS) $(CFLAGS_PIC) -c -o $@ $<

# ============================================================================
# DEBUG BUILD
# ============================================================================

debug: CFLAGS = $(CFLAGS_DEBUG)
debug: clean all
	@echo "Debug build complete"

# ============================================================================
# UTILITY TARGETS
# ============================================================================

# Show build information
info:
	@echo "========================================="
	@echo "CT Tree Walker Build Configuration"
	@echo "========================================="
	@echo "CC:              $(CC)"
	@echo "CFLAGS:          $(CFLAGS)"
	@echo "AR:              $(AR)"
	@echo "RANLIB:          $(RANLIB)"
	@echo ""
	@echo "Static Library:  $(LIB_STATIC)"
	@echo "Shared Library:  $(LIB_SHARED_VERSIONED)"
	@echo "Example:         $(EXAMPLE)"
	@echo "========================================="

# Show sizes of compiled binaries
size: all
	@echo "========================================="
	@echo "Binary Sizes"
	@echo "========================================="
	@if [ -f $(LIB_STATIC) ]; then \
		echo "Static Library:"; \
		ls -lh $(LIB_STATIC) | awk '{print "  " $$5 "  " $$9}'; \
	fi
	@if [ -f $(LIB_SHARED_VERSIONED) ]; then \
		echo "Shared Library:"; \
		ls -lh $(LIB_SHARED_VERSIONED) | awk '{print "  " $$5 "  " $$9}'; \
	fi
	@if [ -f $(EXAMPLE) ]; then \
		echo "Example Binary:"; \
		ls -lh $(EXAMPLE) | awk '{print "  " $$5 "  " $$9}'; \
	fi
	@echo "========================================="

# Help message
help:
	@echo "========================================="
	@echo "CT Tree Walker Makefile"
	@echo "========================================="
	@echo ""
	@echo "Targets:"
	@echo "  all           - Build libraries and example (default)"
	@echo "  libs          - Build both static and shared libraries"
	@echo "  static        - Build static library only"
	@echo "  shared        - Build shared library only"
	@echo "  example       - Build example program"
	@echo "  debug         - Build with debug symbols"
	@echo "  clean         - Remove all build artifacts"
	@echo "  clean-libs    - Remove library files only"
	@echo "  clean-example - Remove example files only"
	@echo "  info          - Show build configuration"
	@echo "  size          - Show sizes of compiled binaries"
	@echo "  help          - Show this message"
	@echo ""
	@echo "Cross-compilation examples:"
	@echo "  make CC=arm-none-eabi-gcc CFLAGS=\"-O2 -mcpu=cortex-m4\""
	@echo "  make CC=arm-linux-gnueabihf-gcc"
	@echo "  make CC=aarch64-linux-gnu-gcc"
	@echo ""
	@echo "Override variables:"
	@echo "  CC            - C compiler (default: gcc)"
	@echo "  CFLAGS        - Compiler flags (default: -Wall -Wextra -std=c99 -O2 -I.)"
	@echo "  AR            - Archiver (default: ar)"
	@echo "  RANLIB        - Library indexer (default: ranlib)"
	@echo "  LDFLAGS       - Linker flags"
	@echo "========================================="

# ============================================================================
# CLEAN TARGETS
# ============================================================================

clean: clean-libs clean-example
	@echo "Clean complete"

clean-libs:
	@echo "Cleaning libraries..."
	rm -f $(LIB_OBJ) $(LIB_OBJ_PIC)
	rm -f $(LIB_STATIC)
	rm -f $(LIB_SHARED) $(LIB_SHARED_VERSIONED)

clean-example:
	@echo "Cleaning example..."
	rm -f $(EXAMPLE_OBJ)
	rm -f $(EXAMPLE)

# ============================================================================
# DEPENDENCIES
# ============================================================================

CT_Tree_Walker.o: CT_Tree_Walker.c CT_Tree_Walker.h
CT_Tree_Walker.pic.o: CT_Tree_Walker.c CT_Tree_Walker.h
example.o: example.c CT_Tree_Walker.h