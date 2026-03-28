CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -O2
AR = ar

LIB = libscan_tree.a
SRCS = scan_tree.c builtins.c st_display.c
OBJS = $(SRCS:.c=.o)

all: $(LIB)

$(LIB): $(OBJS)
	$(AR) rcs $@ $^

%.o: %.c scan_tree.h builtins.h
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(OBJS) $(LIB)

.PHONY: all clean