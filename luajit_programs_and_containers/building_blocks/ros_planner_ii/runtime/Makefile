# Makefile -- Multi-VM runtime shared library

CC       = gcc
CFLAGS   = -Wall -Wextra -O2 -fPIC -I/usr/include/lua5.1
LDFLAGS  = -shared -lpthread
LJ_LIB   = /usr/lib/aarch64-linux-gnu/libluajit-5.1.so.2

TARGET   = libvmrt.so
SRCS     = vmrt.c
OBJS     = $(SRCS:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LJ_LIB)

%.o: %.c vmrt.h
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
