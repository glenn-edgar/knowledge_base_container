# ──────────────────────────────────────────────────────────────────────
#  MQTT PUBSUB Library - Makefile
# ──────────────────────────────────────────────────────────────────────

CC        = gcc
CFLAGS    = -Wall -Wextra -std=c11 -O2 -fPIC
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

# ── Libraries ──────────────────────────────────────────────────────────

LIB_A     = $(BUILDDIR)/libmqtt_pubsub.a
LIB_SO    = $(BUILDDIR)/libmqtt_pubsub.so

# ── Targets ─────────────────────────────────────────────────────────────

.PHONY: all clean test lib run install uninstall

all: lib test

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(BUILDDIR)/mqtt_pubsub.o: $(SRCDIR)/mqtt_pubsub.c $(INCDIR)/mqtt_pubsub.h | $(BUILDDIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB_A): $(LIB_OBJS)
	ar rcs $@ $^

$(LIB_SO): $(LIB_OBJS)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

lib: $(LIB_A) $(LIB_SO)
	@echo ""
	@echo "Built: $(LIB_A) $(LIB_SO)"

test: $(TEST_BIN)

$(TEST_BIN): $(TEST_SRC) $(LIB_A) | $(BUILDDIR)
	$(CC) $(CFLAGS) $< -L$(BUILDDIR) -lmqtt_pubsub $(LDFLAGS) -o $@
	@echo ""
	@echo "Built: $(TEST_BIN)"

run: $(TEST_BIN)
	./$(TEST_BIN)

clean:
	rm -rf $(BUILDDIR)

# ── Install (optional) ─────────────────────────────────────────────────

PREFIX ?= /usr/local

install: $(LIB_A) $(LIB_SO)
	install -d $(DESTDIR)$(PREFIX)/lib
	install -d $(DESTDIR)$(PREFIX)/include
	install -m 644 $(LIB_A) $(DESTDIR)$(PREFIX)/lib/
	install -m 755 $(LIB_SO) $(DESTDIR)$(PREFIX)/lib/
	install -m 644 $(INCDIR)/mqtt_pubsub.h $(DESTDIR)$(PREFIX)/include/

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/lib/libmqtt_pubsub.a
	rm -f $(DESTDIR)$(PREFIX)/lib/libmqtt_pubsub.so
	rm -f $(DESTDIR)$(PREFIX)/include/mqtt_pubsub.h