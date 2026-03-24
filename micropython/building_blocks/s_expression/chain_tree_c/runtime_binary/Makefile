# ===========================================================================
# libcfl_core Makefile
#
# Directory layout:
#   include/   Public headers
#   src/       Implementation files
#   build/     Object files and dependency files (generated)
#
# Uses GCC -MMD -MP to auto-generate .d dependency files.
# Any header change triggers recompilation of all files that include it.
# Produces both static (.a) and shared (.so) libraries.
# ===========================================================================

CC      ?= gcc
AR      ?= ar
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
CFLAGS  += -Iinclude -fPIC

# -MMD: generate .d file with non-system header dependencies
# -MP:  add phony targets for each header (prevents errors if header is deleted)
DEPFLAGS = -MMD -MP

# Cross-compile for 32-bit ARM:  make CC=arm-none-eabi-gcc CFLAGS+="-DCFL_32BIT"

# --- Directories ---
SRC_DIR   = src
INC_DIR   = include
BUILD_DIR = build

# --- Outputs ---
STATIC_LIB = libcfl_binarycore.a
SHARED_LIB = libcfl_binarycore.so

# --- Sources, objects, and dependency files ---
SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRCS))
DEPS = $(OBJS:.o=.d)

# --- Headers (for install target) ---
HDRS = $(wildcard $(INC_DIR)/*.h)

# ===========================================================================

.PHONY: all clean rebuild install uninstall

all: $(STATIC_LIB) $(SHARED_LIB)

$(STATIC_LIB): $(OBJS)
	$(AR) rcs $@ $^

$(SHARED_LIB): $(OBJS)
	$(CC) -shared -o $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Pull in the generated dependency files (first build they won't exist yet)
-include $(DEPS)

clean:
	rm -rf $(BUILD_DIR) $(STATIC_LIB) $(SHARED_LIB)

rebuild: clean all

# --- Install / uninstall (default: /usr/local) ---
PREFIX ?= /usr/local

install: all
	install -d $(PREFIX)/lib
	install -d $(PREFIX)/include
	install -m 644 $(STATIC_LIB) $(PREFIX)/lib/
	install -m 755 $(SHARED_LIB) $(PREFIX)/lib/
	install -m 644 $(INC_DIR)/*.h $(PREFIX)/include/

uninstall:
	rm -f $(PREFIX)/lib/$(STATIC_LIB) $(PREFIX)/lib/$(SHARED_LIB)
	rm -f $(addprefix $(PREFIX)/include/,$(notdir $(HDRS)))