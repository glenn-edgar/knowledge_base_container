CC      := gcc
CFLAGS  := -Wall -Wextra -std=c11 -O2 -fPIC
INCLUDES := -I. -Iinclude
LIBS     := -L. -lmqtt_kv_store -lmqtt_queue -lmosquitto -lpthread
TARGET   := libmqtt_luajit_adapter.so

.PHONY: all clean

all: $(TARGET)

$(TARGET): mqtt_luajit_adapter.c include/mqtt_luajit_adapter.h
	$(CC) $(CFLAGS) -shared $(INCLUDES) -o $@ $< $(LIBS)

clean:
	rm -f $(TARGET)