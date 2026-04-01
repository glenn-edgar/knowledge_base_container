CC = gcc
CFLAGS = -O2 -fPIC -Wall -Wextra
TARGET = libfnv1a.so

all: $(TARGET)

$(TARGET): fnv1a.c fnv1a.h
	$(CC) $(CFLAGS) -shared -o $@ fnv1a.c

clean:
	rm -f $(TARGET)

.PHONY: all clean
