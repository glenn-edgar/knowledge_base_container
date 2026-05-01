CC      := gcc
CFLAGS  := -Wall -Wextra -std=c11 -O2 -fPIC
INCLUDES := -I. -Iinclude
LIBS     := -L. -lmqtt_kv_store -lmqtt_queue -lmosquitto -lpthread
TARGET   := libmqtt_luajit_adapter.so
PUBSUB   := libmqtt_pubsub.so
CBOR     := liblua_cbor.so

.PHONY: all clean

all: $(TARGET) $(PUBSUB) $(CBOR)

$(TARGET): mqtt_luajit_adapter.c include/mqtt_luajit_adapter.h
	$(CC) $(CFLAGS) -shared $(INCLUDES) -o $@ $< $(LIBS)

$(PUBSUB): mqtt_pubsub.c include/mqtt_pubsub.h
	$(CC) $(CFLAGS) -shared $(INCLUDES) -o $@ $< -lmosquitto -lpthread

$(CBOR): lua_cbor.c include/lua_cbor.h
	$(CC) $(CFLAGS) -shared $(INCLUDES) -o $@ $< -lm

clean:
	rm -f $(TARGET) $(PUBSUB) $(CBOR)