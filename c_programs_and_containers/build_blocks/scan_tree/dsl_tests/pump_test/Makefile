CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -O2
RUNTIME = ../../runtime

TARGET = pump_test
SRCS = main.c user_functions.c
OBJS = $(SRCS:.c=.o)
LIB = $(RUNTIME)/libscan_tree.a

all: $(LIB) $(TARGET)

$(TARGET): $(OBJS) $(LIB)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LIB)

%.o: %.c pump_station.h pump_station_user_vft.h $(RUNTIME)/scan_tree.h
	$(CC) $(CFLAGS) -I$(RUNTIME) -c $<

$(LIB):
	$(MAKE) -C $(RUNTIME)

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean