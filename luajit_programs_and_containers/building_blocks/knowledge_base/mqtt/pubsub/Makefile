# ──────────────────────────────────────────────────────────────────────
#  MQTT PUBSUB Library - Makefile
# ──────────────────────────────────────────────────────────────────────

CC        = gcc
CFLAGS    = -Wall -Wextra -std=c11 -O2
LDFLAGS   = -lmosquitto -lcjson -lpthread

INCDIR    = include
SRCDIR    = src
TESTDIR   = test
BUILDDIR  = build

CFLAGS   += -I$(INCDIR)

# ── Source files ────────────────────────────────────────────────────────

LIB_SRCS  = $(SRCDIR)/mqtt_pubsub.c
LIB_OBJS  = $(BUILDDIR)/mqtt_pubsub.o

TEST_SRC  = $(TESTDIR)/mqtt_pubsub_test.c
TEST_BIN  = $(BUILDDIR)/mqtt_pubsub_test

# ── Static library ─────────────────────────────────────────────────────

LIB_NAME  = libmqtt_pubsub.a
LIB_OUT   = $(BUILDDIR)/$(LIB_NAME)

# ── Targets ─────────────────────────────────────────────────────────────

.PHONY: all clean test lib run install uninstall

all: lib test

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(BUILDDIR)/mqtt_pubsub.o: $(SRCDIR)/mqtt_pubsub.c $(INCDIR)/mqtt_pubsub.h | $(BUILDDIR)
	$(CC) $(CFLAGS) -c $< -o $@

lib: $(LIB_OUT)

$(LIB_OUT): $(LIB_OBJS)
	ar rcs $@ $^
	@echo ""
	@echo "Built: $(LIB_OUT)"

test: $(TEST_BIN)

$(TEST_BIN): $(TEST_SRC) $(LIB_OUT) | $(BUILDDIR)
	$(CC) $(CFLAGS) $< -L$(BUILDDIR) -lmqtt_pubsub $(LDFLAGS) -o $@
	@echo ""
	@echo "Built: $(TEST_BIN)"

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
	install -m 644 $(INCDIR)/mqtt_pubsub.h $(DESTDIR)$(PREFIX)/include/

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/lib/$(LIB_NAME)
	rm -f $(DESTDIR)$(PREFIX)/include/mqtt_pubsub.h