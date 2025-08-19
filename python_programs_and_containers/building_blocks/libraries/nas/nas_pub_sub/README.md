# NasPubSub - NATS Client for Python

A lightweight, asynchronous Python client for NATS (Neural Autonomic Transport System) messaging system with publish/subscribe functionality.

## Features

- **Asynchronous Operations**: Built on Python's `asyncio` for non-blocking I/O
- **Auto-connection**: Connects automatically on initialization without authentication
- **Publish/Subscribe**: Full pub/sub messaging pattern support
- **Request/Reply**: Synchronous request-response pattern
- **Connection Sharing**: Create multiple client instances sharing a single connection
- **Wildcards**: Support for NATS wildcards (`*` and `>`) in subscriptions
- **Queue Groups**: Load balancing across subscribers
- **Keep-alive**: Automatic PING/PONG to maintain connection health
- **Context Manager**: Async context manager support for clean resource management

## Requirements

- Python 3.7+
- NATS Server (running on localhost:4222 by default)

## Installation

```bash
# Copy the nas_pub_sub.py file to your project
cp nas_pub_sub.py /path/to/your/project/

# No external dependencies required - uses Python standard library only
```

## Quick Start

### Basic Usage

```python
import asyncio
from nas_pub_sub import NasPubSub, Message

async def main():
    # Create client (auto-connects to localhost:4222)
    client = NasPubSub()
    
    # Wait for connection
    await asyncio.sleep(1)
    
    # Define message handler
    async def handler(msg: Message):
        print(f"Received: {msg.topic} -> {msg.payload.decode()}")
    
    # Subscribe to a topic
    await client.subscribe("sensors.temperature", handler)
    
    # Publish a message
    await client.publish("sensors.temperature", b"22.5 celsius")
    
    # Keep running briefly
    await asyncio.sleep(2)
    
    # Disconnect
    await client.disconnect()

asyncio.run(main())
```

### Using Context Manager

```python
async def main():
    async with NasPubSub("localhost", 4222) as client:
        await client.publish("events.startup", b"System started")
        # Connection automatically closed when exiting context

asyncio.run(main())
```

## Advanced Features

### Connection Sharing

Create multiple client instances that share the same underlying connection:

```python
# Create primary client
client1 = NasPubSub()
await asyncio.sleep(1)  # Wait for connection

# Create secondary client using existing connection
client2 = NasPubSub.from_connection(client1)

# Both clients can publish/subscribe independently
await client1.subscribe("topic1", handler1)
await client2.subscribe("topic2", handler2)
```

### Request/Reply Pattern

Send a request and wait for a response:

```python
# Set up responder
async def echo_handler(msg: Message):
    if msg.reply_to:
        await client.publish(msg.reply_to, msg.payload)

await client.subscribe("echo.service", echo_handler)

# Send request and wait for response
response = await client.request("echo.service", b"Hello", timeout=2.0)
if response:
    print(f"Response: {response.payload.decode()}")
```

### Wildcards

NATS supports two wildcard characters in subscriptions:

```python
# * matches a single token
await client.subscribe("sensors.*", handler)  # Matches sensors.temp, sensors.humidity

# > matches one or more tokens  
await client.subscribe("sensors.>", handler)  # Matches sensors.temp, sensors.temp.cpu, etc.
```

### Queue Groups

Distribute messages among subscribers in a queue group:

```python
# Multiple subscribers with same queue name share messages
await client1.subscribe("work.tasks", handler1, queue="workers")
await client2.subscribe("work.tasks", handler2, queue="workers")
# Messages to work.tasks will be distributed between handlers
```

## API Reference

### Class: `NasPubSub`

#### Constructor

```python
NasPubSub(host: str = "localhost", port: int = 4222, client_id: Optional[str] = None)
```

- `host`: NATS server hostname (default: "localhost")
- `port`: NATS server port (default: 4222)
- `client_id`: Optional client identifier (auto-generated if not provided)

#### Methods

##### `from_connection(existing_client: NasPubSub) -> NasPubSub`
Create a new client instance sharing an existing connection.

##### `async publish(subject: str, payload: bytes, reply_to: Optional[str] = None)`
Publish a message to a subject.

##### `async subscribe(subject: str, callback: Callable, queue: Optional[str] = None) -> str`
Subscribe to a subject. Returns subscription ID.

##### `async unsubscribe(sid: str = None, subject: str = None, max_msgs: Optional[int] = None)`
Unsubscribe from a subscription.

##### `async request(subject: str, payload: bytes, timeout: float = 1.0) -> Optional[Message]`
Send request and wait for response.

##### `async disconnect()`
Disconnect from NATS server.

##### `is_connected() -> bool`
Check if client is connected.

### Class: `Message`

Message dataclass with the following attributes:
- `topic`: Subject name
- `payload`: Message data (bytes)
- `timestamp`: Message timestamp
- `reply_to`: Optional reply subject
- `sid`: Subscription ID

## Docker Setup

If you're running NATS in Docker:

```bash
# Run NATS server
docker run -d \
  --name nats-server \
  -p 4222:4222 \
  -p 8222:8222 \
  nats:latest

# With JetStream in RAM-only mode
docker run -d \
  --name nats-ram \
  -p 4222:4222 \
  -p 8222:8222 \
  --tmpfs /js:rw,size=256m \
  nats:latest \
  -js -sd /js
```

## Connection States

The client tracks connection state through the `ConnectionState` enum:
- `DISCONNECTED`: Not connected
- `CONNECTING`: Connection in progress
- `CONNECTED`: Successfully connected
- `ERROR`: Connection error occurred

## Error Handling

The client handles errors gracefully:
- Connection errors raise exceptions during connect
- Callback errors are silently ignored to prevent disrupting message flow
- Disconnection errors during cleanup are ignored
- Network errors trigger reconnection attempts

## Performance Considerations

- Uses async I/O for non-blocking operations
- Minimal overhead with no external dependencies
- Efficient binary protocol communication
- Connection sharing reduces resource usage
- Keep-alive prevents stale connections

## Limitations

- No built-in reconnection logic (must be implemented by user)
- No TLS/authentication support (designed for trusted networks)
- No clustering support
- Basic error handling (errors in callbacks are silently ignored)

## Examples

See the example at the bottom of `nas_pub_sub.py` for a complete working demonstration including:
- Basic pub/sub
- Connection sharing
- Request/reply pattern
- Multiple subscribers

## License

This implementation is provided as-is for use with NATS messaging systems.

## Contributing

Feel free to extend this client with additional features such as:
- Automatic reconnection
- TLS support
- Authentication mechanisms
- Connection pooling
- Message persistence
- Enhanced error handling

