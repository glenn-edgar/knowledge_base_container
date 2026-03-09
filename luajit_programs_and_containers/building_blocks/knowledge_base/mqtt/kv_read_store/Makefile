# ──────────────────────────────────────────────────────────────────────
#  MQTT KV Store Library - Makefile
# ──────────────────────────────────────────────────────────────────────

CC        = gcc
CFLAGS    = -Wall -Wextra -std=c11 -O2 -D_POSIX_C_SOURCE=199309L
LDFLAGS   = -lmosquitto -lpthread

INCDIR    = include
SRCDIR    = src
TESTDIR   = test
BUILDDIR  = build

CFLAGS   += -I$(INCDIR)

# ── Source files ────────────────────────────────────────────────────────

LIB_SRCS  = $(SRCDIR)/kv_store_reader.c $(SRCDIR)/kv_store_writer.c
LIB_OBJS  = $(BUILDDIR)/kv_store_reader.o $(BUILDDIR)/kv_store_writer.o

TEST_SRC  = $(TESTDIR)/kv_store_test.c
TEST_BIN  = $(BUILDDIR)/kv_store_test

# ── Static library ─────────────────────────────────────────────────────

LIB_NAME  = libmqtt_kv_store.a
LIB_OUT   = $(BUILDDIR)/$(LIB_NAME)

# ── Targets ─────────────────────────────────────────────────────────────

.PHONY: all clean test lib

all: lib test

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

# Library objects
$(BUILDDIR)/kv_store_reader.o: $(SRCDIR)/kv_store_reader.c $(INCDIR)/kv_store_reader.h | $(BUILDDIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILDDIR)/kv_store_writer.o: $(SRCDIR)/kv_store_writer.c $(INCDIR)/kv_store_writer.h | $(BUILDDIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Static library
lib: $(LIB_OUT)

$(LIB_OUT): $(LIB_OBJS)
	ar rcs $@ $^
	@echo ""
	@echo "Built: $(LIB_OUT)"

# Test binary
test: $(TEST_BIN)

$(TEST_BIN): $(TEST_SRC) $(LIB_OUT) | $(BUILDDIR)
	$(CC) $(CFLAGS) $< -L$(BUILDDIR) -lmqtt_kv_store $(LDFLAGS) -o $@
	@echo ""
	@echo "Built: $(TEST_BIN)"

# Run the test (requires mosquitto broker on localhost:1883)
run: $(TEST_BIN)
	./$(TEST_BIN)

clean:
	rm -rf $(BUILDDIR)

# ── Install (optional) ─────────────────────────────────────────────────

PREFIX ?= /usr/local

install: $(LIB_OUT)
	install -d $(DESTDIR)$(PREFIX)/lib
	install -d $(DESTDIR)$(PREFIX)/include
	install -m 644 $(LIB_OUT) $(DESTDIR)$(PREFIX)/lib/
	install -m 644 $(INCDIR)/kv_store_reader.h $(DESTDIR)$(PREFIX)/include/
	install -m 644 $(INCDIR)/kv_store_writer.h $(DESTDIR)$(PREFIX)/include/

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/lib/$(LIB_NAME)
	rm -f $(DESTDIR)$(PREFIX)/include/kv_store_reader.h
	rm -f $(DESTDIR)$(PREFIX)/include/kv_store_writer.h

