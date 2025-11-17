# NatsPubSub

A lightweight Python client for NATS messaging with both synchronous and asynchronous API support, namespace organization, and request/response patterns.

## Features

- **Dual API Support**: Both async/await and synchronous programming patterns
- **Namespace Support**: Automatic topic prefixing for multi-tenant environments
- **Request/Response**: Built-in request-reply messaging patterns
- **Wildcards**: Support for NATS wildcard subscriptions (* and >)
- **Queue Groups**: Load balancing across multiple subscribers
- **Thread-Safe**: Safe synchronous wrappers for multi-threaded applications
- **Context Managers**: Automatic connection cleanup
- **Connection Management**: Automatic reconnection and health monitoring

## Requirements

- Python 3.7+
- NATS Server running locally or remotely
- No external dependencies (uses only Python standard library)

## Quick Start

### Basic Publish/Subscribe

```python
import asyncio
from natspubsub import NatsPubSub, Message

# Async example
async def async_pubsub():
    client = NatsPubSub("localhost", 4222, namespace="myapp")
    await client.connect()
    
    # Subscribe to messages
    async def message_handler(msg: Message):
        print(f"Received: {msg.payload.decode()}")
        print(f"Topic: {msg.original_topic}")  # Without namespace
    
    await client.subscribe("notifications", message_handler)
    
    # Publish messages
    await client.publish("notifications", b"Hello World!")
    
    await asyncio.sleep(1)  # Let message process
    await client.disconnect()

# Synchronous example
def sync_pubsub():
    client = NatsPubSub("localhost", 4222, namespace="myapp", auto_connect=False)
    client.connect_sync()
    
    def message_handler(msg: Message):
        print(f"Received: {msg.payload.decode()}")
    
    client.subscribe_sync("notifications", message_handler)
    client.publish_sync("notifications", "Hello World!")
    
    import time
    time.sleep(1)  # Let message process
    client.disconnect_sync()
```

### Request/Response Pattern

```python
# Server side - responding to requests
async def setup_responder():
    server = NatsPubSub("localhost", 4222, namespace="api")
    await server.connect()
    
    async def handle_request(msg: Message):
        if msg.reply_to:
            # Process request and send response
            response = f"Processed: {msg.payload.decode()}"
            await server.publish(msg.reply_to, response.encode(), use_namespace=False)
    
    await server.subscribe("calculate", handle_request)
    return server

# Client side - making requests
async def make_request():
    client = NatsPubSub("localhost", 4222, namespace="api")
    await client.connect()
    
    # Send request and wait for response
    response = await client.request("calculate", b"2 + 2", timeout=5.0)
    if response:
        print(f"Response: {response.payload.decode()}")
    
    await client.disconnect()
```

### Using Context Managers

```python
# Async context manager
async def with_async_context():
    async with NatsPubSub("localhost", 4222, namespace="app") as client:
        await client.publish("events", b"Application started")
        # Connection automatically closed

# Sync context manager
def with_sync_context():
    with NatsPubSub("localhost", 4222, namespace="app", auto_connect=False) as client:
        client.connect_sync()
        client.publish_sync("events", "Application started")
        # Connection automatically closed
```

## Advanced Features

### Wildcard Subscriptions

```python
async def wildcard_example():
    client = NatsPubSub("localhost", 4222, namespace="sensors")
    await client.connect()
    
    # Subscribe to all temperature sensors
    async def temp_handler(msg: Message):
        print(f"Temperature data from {msg.topic}: {msg.payload.decode()}")
    
    await client.subscribe("temperature.*", temp_handler)
    
    # Subscribe to all sensor data
    async def all_sensors_handler(msg: Message):
        print(f"Sensor data: {msg.payload.decode()}")
    
    await client.subscribe("sensors.>", all_sensors_handler)
    
    # Publish to specific sensors
    await client.publish("temperature.room1", b"22.5")
    await client.publish("temperature.room2", b"23.1")
    await client.publish("sensors.humidity.room1", b"45%")
```

### Queue Groups (Load Balancing)

```python
async def queue_group_example():
    # Multiple workers processing the same queue
    worker1 = NatsPubSub("localhost", 4222, namespace="work")
    worker2 = NatsPubSub("localhost", 4222, namespace="work")
    
    await worker1.connect()
    await worker2.connect()
    
    async def process_job(msg: Message):
        job_id = msg.payload.decode()
        print(f"Worker processing job: {job_id}")
        # Simulate work
        await asyncio.sleep(1)
        print(f"Job {job_id} completed")
    
    # Both workers join the same queue group
    await worker1.subscribe("jobs", process_job, queue="job_processors")
    await worker2.subscribe("jobs", process_job, queue="job_processors")
    
    # Publisher sends jobs
    publisher = NatsPubSub("localhost", 4222, namespace="work")
    await publisher.connect()
    
    # Jobs will be distributed between worker1 and worker2
    for i in range(10):
        await publisher.publish("jobs", f"job_{i}".encode())
```

### Synchronous Threading Example

```python
import threading
import time

def worker_thread(worker_id: int):
    """Each thread gets its own NatsPubSub instance"""
    client = NatsPubSub("localhost", 4222, namespace="workers", auto_connect=False)
    client.connect_sync()
    
    def handle_work(msg: Message):
        work_item = msg.payload.decode()
        print(f"Worker {worker_id} processing: {work_item}")
        time.sleep(0.5)  # Simulate work
        print(f"Worker {worker_id} completed: {work_item}")
    
    # Subscribe to work queue
    client.subscribe_sync("tasks", handle_work, queue="task_processors")
    
    # Keep thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect_sync()

# Start multiple worker threads
threads = []
for i in range(3):
    thread = threading.Thread(target=worker_thread, args=(i,))
    thread.start()
    threads.append(thread)

# Publisher thread
def publisher():
    client = NatsPubSub("localhost", 4222, namespace="workers", auto_connect=False)
    client.connect_sync()
    
    for i in range(20):
        client.publish_sync("tasks", f"task_{i}")
        time.sleep(0.1)
    
    client.disconnect_sync()

publisher_thread = threading.Thread(target=publisher)
publisher_thread.start()
```

## API Reference

### NatsPubSub Class

#### Constructor
```python
NatsPubSub(host="localhost", port=4222, namespace="default", auto_connect=True)
```

- `host`: NATS server hostname or IP address
- `port`: NATS server port number
- `namespace`: Prefix added to all topics (except those starting with '_')
- `auto_connect`: Automatically connect on instantiation

#### Connection Methods

**Async:**
```python
await client.connect()                    # Connect to server
await client.wait_connected(timeout=5.0) # Wait for connection
await client.disconnect()                 # Disconnect from server
```

**Sync:**
```python
client.connect_sync(timeout=5.0)         # Connect to server
client.wait_connected_sync(timeout=5.0)  # Wait for connection  
client.disconnect_sync(timeout=5.0)      # Disconnect from server
```

#### Publishing Methods

**Async:**
```python
await client.publish(subject, payload, reply_to=None)
```

**Sync:**
```python
client.publish_sync(subject, payload, reply_to=None, timeout=5.0)
```

- `subject`: Topic name (namespace automatically added)
- `payload`: Message data as bytes or string
- `reply_to`: Optional reply subject for request/response

#### Subscription Methods

**Async:**
```python
sid = await client.subscribe(subject, callback, queue=None, use_namespace=True)
sid = await client.subscribe_pattern(pattern, callback, queue=None)
```

**Sync:**
```python
sid = client.subscribe_sync(subject, callback, queue=None, use_namespace=True, timeout=5.0)
sid = client.subscribe_pattern_sync(pattern, callback, queue=None, timeout=5.0)
```

- `subject`/`pattern`: Topic name or pattern with wildcards
- `callback`: Function called when message received (can be sync or async)
- `queue`: Optional queue group name for load balancing
- `use_namespace`: Whether to add namespace prefix

#### Unsubscription Methods

**Async:**
```python
await client.unsubscribe(sid=subscription_id)           # Unsubscribe by ID
await client.unsubscribe(subject=topic_name)            # Unsubscribe by topic
await client.unsubscribe(sid=subscription_id, max_msgs=10)  # Auto-unsub after N messages
```

**Sync:**
```python
client.unsubscribe_sync(sid=subscription_id, timeout=5.0)
client.unsubscribe_sync(subject=topic_name, timeout=5.0)
```

#### Request/Response Methods

**Async:**
```python
response = await client.request(subject, payload, timeout=1.0)
```

**Sync:**
```python
response = client.request_sync(subject, payload, timeout=1.0)
```

Returns a `Message` object or `None` if timeout.

#### Utility Methods
```python
client.is_connected()        # Check connection status
client.get_namespace()       # Get current namespace
```

### Message Class

```python
@dataclass
class Message:
    topic: str                    # Full topic name (with namespace)
    payload: bytes               # Message data
    timestamp: datetime          # When message was received
    reply_to: Optional[str]      # Reply subject (if any)
    sid: Optional[str]           # Subscription ID
    original_topic: Optional[str] # Topic without namespace prefix
```

## Configuration

### NATS Server Setup

**Using Docker:**
```bash
docker run -p 4222:4222 -p 8222:8222 nats:latest
```

**Local Installation:**
Follow instructions at: https://docs.nats.io/running-a-nats-service/introduction/installation

### Namespace Usage

Namespaces help organize topics in multi-tenant or multi-application environments:

```python
# Without namespace
client = NatsPubSub("localhost", 4222, namespace="")
await client.publish("events", b"data")  # Publishes to "events"

# With namespace  
client = NatsPubSub("localhost", 4222, namespace="myapp")
await client.publish("events", b"data")  # Publishes to "myapp.events"
```

Internal NATS subjects (starting with '_') ignore namespaces:
```python
# These always work regardless of namespace
await client.subscribe("_INBOX.12345", handler, use_namespace=False)
```

## Error Handling

### Connection Errors
```python
try:
    client = NatsPubSub("invalid-host", 4222, auto_connect=False)
    connected = await client.connect()
    if not connected:
        print("Failed to connect to NATS server")
except ConnectionError as e:
    print(f"Connection error: {e}")
```

### Publish Errors
```python
try:
    await client.publish("topic", b"data")
except ConnectionError:
    print("Not connected to NATS server")
except Exception as e:
    print(f"Publish failed: {e}")
```

### Callback Errors
Exceptions in message callbacks are logged but don't crash the client:

```python
async def potentially_failing_handler(msg: Message):
    if msg.payload == b"bad_data":
        raise ValueError("Invalid data")
    print(f"Processed: {msg.payload.decode()}")

# Client continues running even if handler raises exceptions
await client.subscribe("topic", potentially_failing_handler)
```

## Best Practices

### Connection Management
```python
# Use context managers for automatic cleanup
async with NatsPubSub("localhost", 4222) as client:
    # Use client
    pass
# Automatically disconnected

# Or manual management
client = NatsPubSub("localhost", 4222, auto_connect=False)
try:
    await client.connect()
    # Use client
finally:
    await client.disconnect()
```

### Threading Considerations
- Create separate NatsPubSub instances per thread
- Use sync methods in traditional threaded code
- Async methods require running event loop

```python
# Good - separate instance per thread
def worker_thread():
    client = NatsPubSub("localhost", 4222, auto_connect=False)
    client.connect_sync()
    # Use client
    client.disconnect_sync()

# Avoid - sharing instances between threads
global_client = NatsPubSub("localhost", 4222)  # Don't do this
```

### Performance Tips
1. **Reuse connections**: One NatsPubSub instance per logical component
2. **Batch operations**: Group related publishes when possible
3. **Appropriate timeouts**: Set realistic timeouts based on network conditions
4. **Queue groups**: Use for horizontal scaling of message processing

### Topic Design
```python
# Good topic hierarchy
"sensors.temperature.room1"
"sensors.humidity.room1"  
"events.user.login"
"events.user.logout"

# Use wildcards for flexible subscriptions
await client.subscribe("sensors.*.*", sensor_handler)      # All sensors
await client.subscribe("sensors.temperature.*", temp_handler)  # All temperature sensors
await client.subscribe("events.>", event_handler)          # All events
```

## Troubleshooting

### Connection Issues
- **"Connection refused"**: Check if NATS server is running
- **Timeout on connect**: Verify host/port and network connectivity
- **"No PONG response"**: NATS server may be overloaded

### Message Issues  
- **Messages not received**: Check topic names and namespace configuration
- **Duplicate messages**: Ensure proper unsubscribe handling
- **Memory leaks**: Always disconnect clients or use context managers

### Performance Issues
- **Slow message processing**: Check if callbacks are blocking
- **High memory usage**: Monitor subscription count and message queues
- **Connection drops**: Verify network stability and NATS server resources

### Debug Information
```python
# Check connection state
print(f"Connected: {client.is_connected()}")
print(f"Namespace: {client.get_namespace()}")

# Monitor subscriptions
print(f"Active subscriptions: {len(client.subscriptions)}")
print(f"Subject handlers: {len(client.subject_handlers)}")
```

## Examples Repository

See the `examples/` directory for more complete examples:
- Basic pub/sub patterns
- Request/response services  
- Worker queue implementations
- Multi-threaded applications
- Error handling strategies

## License

MIT License

## Contributing

Contributions welcome! Please ensure:
- Code follows existing style
- Tests pass for both sync and async APIs  
- Documentation is updated for new features