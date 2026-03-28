# Makefile for water_plant_test
#
# Directory layout:
#   ../../runtime/        scan_tree.h, scan_tree.c, builtins.h, builtins.c
#   ../../scan_tree_dsl/  codegen_c.lua, scan_tree_dsl.lua, vft_helpers.lua
#   ./                    water_plant_test.c, water_plant.h (generated)

CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -O2
RUNTIME = ../../runtime

TARGET = vtf_fuse_test
SRCS = main.c user_functions.c
OBJS = $(SRCS:.c=.o)
LIB = $(RUNTIME)/libscan_tree.a

all: $(LIB) $(TARGET)

$(TARGET): $(OBJS) $(LIB)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LIB)

%.o: %.c vtf_fuse_test.h vtf_fuse_test_fuse_actions.h $(RUNTIME)/scan_tree.h
	$(CC) $(CFLAGS) -I$(RUNTIME) -c $<

$(LIB):
	$(MAKE) -C $(RUNTIME)

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean