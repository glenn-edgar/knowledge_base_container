# MQTT Queue Library

A C library for reliable queued messaging over MQTT v3.1.1 using persistent sessions. Wraps the Mosquitto C client library (libmosquitto) with thread-safe publisher and reader APIs.

The publisher supports single and batch message publishing with QoS 1/2. The reader uses persistent sessions (`clean_session=false`) so the broker queues messages while the consumer is offline, delivering them automatically on reconnect.

## Project Structure

```
mqtt_queue/
├── include/
│   └── mqtt_queue.h         # Public API header
├── src/
│   └── mqtt_queue.c         # Implementation
├── test/
│   └── mqtt_queue_test.c    # Test program
├── Makefile
└── README.md
```

## Prerequisites

### Ubuntu / Debian

```bash
# Install Mosquitto broker and C client development library
sudo apt update
sudo apt install -y mosquitto libmosquitto-dev build-essential

# Verify the broker is running
sudo systemctl status mosquitto

# Start if needed
sudo systemctl start mosquitto
sudo systemctl enable mosquitto
```

The broker listens on `localhost:1883` by default with no authentication.

### Fedora / RHEL

```bash
sudo dnf install mosquitto mosquitto-devel gcc make
sudo systemctl start mosquitto
```

### Arch Linux

```bash
sudo pacman -S mosquitto
sudo systemctl start mosquitto
```

## Building

```bash
make            # builds static library and test binary
make lib        # builds only libmqtt_queue.a
make test       # builds only the test binary
make clean      # removes build artifacts
```

Output in `build/`:

- `build/libmqtt_queue.a` — static library
- `build/mqtt_queue_test` — test binary

## Running the Test

```bash
make run
```

The test program runs two scenarios:

1. **Publisher test** — publishes individual messages, a batch, and a QoS 2 message
2. **Persistent queue test** — creates a persistent session with a subscription, disconnects, publishes messages while the consumer is offline, then reconnects and drains the queued messages

## Using the Library

### Linking

```bash
gcc -Iinclude -o my_app my_app.c -Lbuild -lmqtt_queue -lmosquitto -lpthread
```

### Publisher Example

```c
#include "mqtt_queue.h"

mqtt_queue_lib_init();

mqtt_queue_config_t cfg = {
    .host = "localhost", .port = 1883,
    .client_id = "my-publisher",
    .keepalive = 60, .clean_session = true,
    .username = NULL, .password = NULL,
};

mqtt_publisher_t pub;
mqtt_publisher_init(&pub, &cfg);
mqtt_publisher_connect(&pub, 5000);

mqtt_publisher_publish(&pub, "topic/foo", "{\"key\":\"value\"}", 1, false);

mqtt_publisher_disconnect(&pub);
mqtt_publisher_destroy(&pub);
mqtt_queue_lib_cleanup();
```

### Reader Example (Persistent Session)

```c
#include "mqtt_queue.h"

mqtt_queue_lib_init();

mqtt_queue_config_t cfg = {
    .host = "localhost", .port = 1883,
    .client_id = "my-reader",       // must be fixed for session persistence
    .keepalive = 60,
    .clean_session = false,          // persistent session
    .username = NULL, .password = NULL,
};

mqtt_reader_t rdr;
mqtt_reader_init(&rdr, &cfg);
mqtt_reader_connect(&rdr, 5000);

// Subscribe (first connect) or set rdr.session_present = true (reconnect)
mqtt_reader_subscribe(&rdr, "topic/foo", 1, 2000);

// Collect messages for 3 seconds
int count = 0;
mqtt_msg_t *msgs = mqtt_reader_read_queue(&rdr, "topic/foo", 1, 3000, &count);

for (mqtt_msg_t *m = msgs; m; m = m->next) {
    printf("%s: %s\n", m->topic, m->payload);
}
mqtt_msg_list_free(msgs);

mqtt_reader_disconnect(&rdr);
mqtt_reader_destroy(&rdr);
mqtt_queue_lib_cleanup();
```

## License

MIT

