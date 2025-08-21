# NasPubSub

A lightweight Python publish-subscribe client for NATS messaging system with namespace support. This library provides a simple interface for publishing messages and subscribing to topics with automatic namespace isolation, wildcard patterns, and request-reply messaging.

## Features

- 📡 **Publish/Subscribe** - Core pub/sub messaging patterns
- 🏷️ **Namespace Isolation** - Automatic topic namespacing for multi-tenant support
- 🎯 **Wildcard Subscriptions** - Support for `*` and `>` wildcard patterns
- 🔄 **Request-Reply** - Built-in request-reply pattern with timeout
- 👥 **Queue Groups** - Load balancing across multiple subscribers
- ⚡ **Async/Await** - Full asynchronous operation with Python asyncio
- 📦 **Zero Dependencies** - Direct NATS protocol implementation
- 🔌 **Connection Management** - Automatic reconnection and health checks
- 📝 **Message Metadata** - Original topic tracking and timestamps

## Installation

### Prerequisites

- Python 3.7+
- NATS Server running (default: localhost:4222)

### Install NATS Server

```bash
# Using Docker
docker run -p 4222:4222 -ti nats:latest

# Or download from https://nats.io/download/
```

### Install Dependencies

```bash
pip install asyncio
```

## Quick Start

### Basic Publish and Subscribe

```python
import asyncio
from nas_pubsub import NasPubSub, Message

async def main():
    # Create client
    client = NasPubSub("localhost", 4222, namespace="myapp")
    await client.connect()
    
    # Define message handler
    async def on_message(msg: Message):
        print(f"Received: {msg.payload.decode()} on {msg.topic}")
    
    # Subscribe to a topic
    await client.subscribe("events.user.login", on_message)
    
    # Publish a message
    await client.publish("events.user.login", b"User 123 logged in")
    
    # Keep running for a bit
    await asyncio.sleep(1)
    
    # Cleanup
    await client.disconnect()

asyncio.run(main())
```

## API Reference

### Initialization

```python
client = NasPubSub(
    host="localhost",      # NATS server host
    port=4222,            # NATS server port
    namespace="default",  # Namespace for topic isolation
    auto_connect=True     # Auto-connect on initialization
)
```

### Connection Management

#### `connect()`
Manually connect to NATS server.

```python
client = NasPubSub("localhost", 4222, namespace="myapp", auto_connect=False)
success = await client.connect()
if success:
    print("Connected to NATS!")
```

#### `wait_connected(timeout=5.0)`
Wait for connection to be established.

```python
if await client.wait_connected(timeout=10.0):
    print("Connected within 10 seconds")
else:
    print("Connection timeout")
```

#### `disconnect()`
Close connection and cleanup resources.

```python
await client.disconnect()
```

#### `is_connected()`
Check if client is connected.

```python
if client.is_connected():
    await client.publish("test", b"message")
```

#### `get_namespace()`
Get the current namespace.

```python
namespace = client.get_namespace()
print(f"Current namespace: {namespace}")
```

### Publishing

#### `publish(subject, payload, reply_to=None)`
Publish a message to a subject.

```python
# Simple publish
await client.publish("events.created", b"New event")

# Publish string (auto-encoded)
await client.publish("notifications", "Hello, World!")

# Publish with reply-to subject
await client.publish("requests.info", b"Get info", reply_to="responses.info")
```

### Subscribing

#### `subscribe(subject, callback, queue=None, use_namespace=True)`
Subscribe to a subject with a callback function.

```python
# Simple subscription
async def handler(msg: Message):
    print(f"Got: {msg.payload.decode()}")

sid = await client.subscribe("events", handler)

# With queue group for load balancing
sid = await client.subscribe("tasks", handler, queue="workers")

# Without namespace (raw subject)
sid = await client.subscribe("_SYS.STATS", handler, use_namespace=False)
```

#### `subscribe_pattern(pattern, callback, queue=None)`
Subscribe using wildcard patterns.

```python
# Single token wildcard (*)
await client.subscribe_pattern("events.*", handler)
# Matches: events.created, events.deleted
# Not: events.user.login

# Multi-token wildcard (>)
await client.subscribe_pattern("events.>", handler)
# Matches: events.created, events.user.login, events.user.profile.updated
```

#### `unsubscribe(sid=None, subject=None, max_msgs=None)`
Unsubscribe from subscriptions.

```python
# Unsubscribe by subscription ID
sid = await client.subscribe("events", handler)
await client.unsubscribe(sid=sid)

# Unsubscribe by subject (all subscriptions)
await client.unsubscribe(subject="events")

# Auto-unsubscribe after N messages
await client.unsubscribe(sid=sid, max_msgs=10)
```

### Request-Reply Pattern

#### `request(subject, payload, timeout=1.0)`
Send a request and wait for a reply.

```python
# Send request and wait for response
response = await client.request("service.echo", b"Hello", timeout=2.0)
if response:
    print(f"Reply: {response.payload.decode()}")
else:
    print("Request timed out")
```

### Message Object

The `Message` dataclass contains:

```python
@dataclass
class Message:
    topic: str                    # Full topic with namespace
    payload: bytes               # Message payload
    timestamp: datetime          # When received
    reply_to: Optional[str]      # Reply subject if any
    sid: Optional[str]           # Subscription ID
    original_topic: Optional[str] # Topic without namespace
```

## Advanced Usage

### Wildcard Subscriptions

```python
client = NasPubSub("localhost", 4222, namespace="monitoring")
await client.connect()

# Handler that processes all events
async def event_handler(msg: Message):
    # original_topic shows topic without namespace
    print(f"Event on {msg.original_topic}: {msg.payload.decode()}")

# Subscribe to all sensor readings
await client.subscribe("sensors.*", event_handler)

# These will all be received
await client.publish("sensors.temperature", b"22.5")
await client.publish("sensors.humidity", b"65")
await client.publish("sensors.pressure", b"1013")

# Subscribe to all metrics (any depth)
await client.subscribe("metrics.>", event_handler)

# These will all be received
await client.publish("metrics.cpu", b"45%")
await client.publish("metrics.memory.used", b"8GB")
await client.publish("metrics.disk.sda.free", b"100GB")
```

### Queue Groups for Load Balancing

```python
# Worker 1
worker1 = NasPubSub("localhost", 4222, namespace="tasks")
await worker1.connect()

async def process_task(msg: Message):
    print(f"Worker1 processing: {msg.payload.decode()}")
    await asyncio.sleep(1)  # Simulate work

await worker1.subscribe("jobs", process_task, queue="worker_pool")

# Worker 2 (same queue group)
worker2 = NasPubSub("localhost", 4222, namespace="tasks")
await worker2.connect()

async def process_task2(msg: Message):
    print(f"Worker2 processing: {msg.payload.decode()}")
    await asyncio.sleep(1)

await worker2.subscribe("jobs", process_task2, queue="worker_pool")

# Publisher
publisher = NasPubSub("localhost", 4222, namespace="tasks")
await publisher.connect()

# Messages will be distributed between workers
for i in range(10):
    await publisher.publish("jobs", f"Task {i+1}")
```

### Request-Reply Service

```python
# Service provider
service = NasPubSub("localhost", 4222, namespace="services")
await service.connect()

async def echo_handler(msg: Message):
    if msg.reply_to:
        # Echo back the message
        response = f"Echo: {msg.payload.decode()}"
        await service.publish(msg.reply_to, response.encode())

await service.subscribe("echo", echo_handler)

# Service consumer
client = NasPubSub("localhost", 4222, namespace="services")
await client.connect()

# Make requests
response = await client.request("echo", b"Hello, World!", timeout=2.0)
if response:
    print(f"Got response: {response.payload.decode()}")
```

### Namespace Isolation

```python
# Production environment
prod_client = NasPubSub("localhost", 4222, namespace="production")
await prod_client.connect()

async def prod_handler(msg: Message):
    print(f"[PROD] {msg.payload.decode()}")

await prod_client.subscribe("alerts", prod_handler)

# Development environment (isolated)
dev_client = NasPubSub("localhost", 4222, namespace="development")
await dev_client.connect()

async def dev_handler(msg: Message):
    print(f"[DEV] {msg.payload.decode()}")

await dev_client.subscribe("alerts", dev_handler)

# Messages are isolated by namespace
await prod_client.publish("alerts", b"Production alert!")
await dev_client.publish("alerts", b"Development test")

# Each client only receives messages from its namespace
```

### Event-Driven Architecture

```python
class EventBus:
    def __init__(self, namespace="events"):
        self.client = NasPubSub("localhost", 4222, namespace=namespace)
        self.handlers = {}
    
    async def connect(self):
        await self.client.connect()
    
    def on(self, event_type: str):
        """Decorator for event handlers"""
        def decorator(func):
            if event_type not in self.handlers:
                self.handlers[event_type] = []
            self.handlers[event_type].append(func)
            return func
        return decorator
    
    async def emit(self, event_type: str, data: dict):
        """Emit an event"""
        import json
        payload = json.dumps(data).encode()
        await self.client.publish(event_type, payload)
    
    async def start(self):
        """Start listening for events"""
        for event_type, handlers in self.handlers.items():
            async def create_handler(handlers_list):
                async def handle(msg: Message):
                    import json
                    data = json.loads(msg.payload.decode())
                    for handler in handlers_list:
                        await handler(data)
                return handle
            
            handler = await create_handler(handlers)
            await self.client.subscribe(event_type, handler)

# Usage
bus = EventBus()

@bus.on("user.registered")
async def send_welcome_email(data):
    print(f"Sending welcome email to {data['email']}")

@bus.on("user.registered")
async def add_to_crm(data):
    print(f"Adding {data['name']} to CRM")

@bus.on("order.placed")
async def process_order(data):
    print(f"Processing order {data['order_id']}")

await bus.connect()
await bus.start()

# Emit events
await bus.emit("user.registered", {
    "name": "John Doe",
    "email": "john@example.com"
})

await bus.emit("order.placed", {
    "order_id": "ORD-123",
    "amount": 99.99
})
```

### Microservices Communication

```python
# User Service
user_service = NasPubSub("localhost", 4222, namespace="microservices")
await user_service.connect()

async def get_user_handler(msg: Message):
    import json
    request = json.loads(msg.payload.decode())
    user_id = request.get("user_id")
    
    # Fetch user from database
    user = {"id": user_id, "name": "John Doe", "email": "john@example.com"}
    
    if msg.reply_to:
        await user_service.publish(msg.reply_to, json.dumps(user).encode())

await user_service.subscribe("user.get", get_user_handler)

# Order Service (calling User Service)
order_service = NasPubSub("localhost", 4222, namespace="microservices")
await order_service.connect()

# Get user details
import json
response = await order_service.request(
    "user.get",
    json.dumps({"user_id": 123}).encode(),
    timeout=2.0
)

if response:
    user = json.loads(response.payload.decode())
    print(f"User: {user['name']} ({user['email']})")
```

### Context Manager Usage

```python
async with NasPubSub("localhost", 4222, namespace="temp") as client:
    # Subscribe to events
    async def handler(msg: Message):
        print(f"Received: {msg.payload.decode()}")
    
    await client.subscribe("test", handler)
    
    # Publish some messages
    await client.publish("test", b"Message 1")
    await client.publish("test", b"Message 2")
    
    await asyncio.sleep(1)
    
# Connection automatically closed
```

### Raw NATS Subjects (Without Namespace)

```python
client = NasPubSub("localhost", 4222, namespace="myapp")
await client.connect()

# Subscribe to NATS system subjects (no namespace)
async def sys_handler(msg: Message):
    print(f"System: {msg.topic} - {msg.payload.decode()}")

# Use use_namespace=False for raw subjects
await client.subscribe("$SYS.STATS", sys_handler, use_namespace=False)
await client.subscribe("_INBOX.>", sys_handler, use_namespace=False)
```

## Message Patterns

### Fan-Out Pattern
```python
# One publisher, multiple subscribers
publisher = NasPubSub(namespace="news")
await publisher.connect()

# Multiple subscribers
for i in range(3):
    subscriber = NasPubSub(namespace="news")
    await subscriber.connect()
    
    async def handler(msg: Message):
        print(f"Subscriber {i}: {msg.payload.decode()}")
    
    await subscriber.subscribe("updates", handler)

# All subscribers receive the message
await publisher.publish("updates", b"Breaking news!")
```

### Fan-In Pattern
```python
# Multiple publishers, one subscriber
aggregator = NasPubSub(namespace="metrics")
await aggregator.connect()

metrics = []
async def collect_metrics(msg: Message):
    metrics.append(msg.payload.decode())

await aggregator.subscribe("sensor.data", collect_metrics)

# Multiple sensors publishing
for sensor_id in range(5):
    sensor = NasPubSub(namespace="metrics")
    await sensor.connect()
    await sensor.publish("sensor.data", f"Sensor {sensor_id}: OK")
```

## Performance Considerations

### Connection Pooling
```python
# Reuse connections for better performance
class PubSubPool:
    def __init__(self, size=5, namespace="default"):
        self.clients = []
        self.namespace = namespace
        self.size = size
        self.current = 0
    
    async def initialize(self):
        for _ in range(self.size):
            client = NasPubSub("localhost", 4222, self.namespace)
            await client.connect()
            self.clients.append(client)
    
    def get_client(self):
        client = self.clients[self.current]
        self.current = (self.current + 1) % self.size
        return client
    
    async def publish(self, subject, payload):
        client = self.get_client()
        await client.publish(subject, payload)

# Usage
pool = PubSubPool(size=10, namespace="app")
await pool.initialize()

# Distribute publishes across connections
for i in range(1000):
    await pool.publish("events", f"Event {i}")
```

### Message Batching
```python
class BatchPublisher:
    def __init__(self, client, batch_size=100, flush_interval=1.0):
        self.client = client
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.flush_task = None
    
    async def publish(self, subject, payload):
        self.buffer.append((subject, payload))
        
        if len(self.buffer) >= self.batch_size:
            await self.flush()
        elif not self.flush_task:
            self.flush_task = asyncio.create_task(self._auto_flush())
    
    async def flush(self):
        if self.buffer:
            for subject, payload in self.buffer:
                await self.client.publish(subject, payload)
            self.buffer.clear()
    
    async def _auto_flush(self):
        await asyncio.sleep(self.flush_interval)
        await self.flush()
        self.flush_task = None
```

## Testing

```python
import asyncio
import pytest
from nas_pubsub import NasPubSub, Message

@pytest.mark.asyncio
async def test_publish_subscribe():
    client = NasPubSub(namespace="test")
    await client.connect()
    
    received = []
    
    async def handler(msg: Message):
        received.append(msg.payload.decode())
    
    await client.subscribe("test.topic", handler)
    await client.publish("test.topic", b"Hello")
    
    await asyncio.sleep(0.1)
    
    assert len(received) == 1
    assert received[0] == "Hello"
    
    await client.disconnect()

@pytest.mark.asyncio
async def test_request_reply():
    server = NasPubSub(namespace="test")
    await server.connect()
    
    async def echo(msg: Message):
        if msg.reply_to:
            await server.publish(msg.reply_to, msg.payload)
    
    await server.subscribe("echo", echo)
    
    client = NasPubSub(namespace="test")
    await client.connect()
    
    response = await client.request("echo", b"Test", timeout=1.0)
    assert response is not None
    assert response.payload == b"Test"
    
    await client.disconnect()
    await server.disconnect()
```

## Troubleshooting

### Connection Issues
```python
client = NasPubSub("localhost", 4222, namespace="myapp")

if not await client.connect():
    print("Failed to connect")
    print("Check:")
    print("1. NATS server is running: docker ps | grep nats")
    print("2. Port is accessible: telnet localhost 4222")
    print("3. No firewall blocking connection")
```

### Messages Not Received
- Verify publisher and subscriber are in same namespace
- Check subscription is active before publishing
- Use wildcards correctly (`*` for single token, `>` for multiple)
- Add small delay after subscribe for message propagation

### Debugging Subscriptions
```python
# Log all received messages
async def debug_handler(msg: Message):
    print(f"DEBUG: Topic={msg.topic}, "
          f"Original={msg.original_topic}, "
          f"Payload={msg.payload}, "
          f"Timestamp={msg.timestamp}")

# Subscribe to everything in namespace
await client.subscribe_pattern(">", debug_handler)
```

## Best Practices

1. **Always Check Connection**
```python
if not client.is_connected():
    await client.connect()
```

2. **Use Appropriate Namespaces**
```python
# Environment-based
namespace = os.getenv("ENVIRONMENT", "development")

# Service-based
namespace = f"{service_name}.{environment}"
```

3. **Handle Errors in Callbacks**
```python
async def safe_handler(msg: Message):
    try:
        # Process message
        await process(msg)
    except Exception as e:
        print(f"Error processing message: {e}")
```

4. **Use Queue Groups for Scaling**
```python
# Distribute load across workers
await client.subscribe("work", handler, queue="workers")
```

5. **Clean Shutdown**
```python
try:
    # Run application
    await run_app()
finally:
    await client.disconnect()
```

## Limitations

1. **No Message Persistence** - Messages are not stored
2. **At-Most-Once Delivery** - No guaranteed delivery
3. **No Message Ordering** - With multiple publishers
4. **Namespace Bound** - Cannot publish across namespaces
5. **No Built-in Retry** - Client must implement retry logic

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is provided as-is for educational and development purposes.

## Acknowledgments

Built on top of [NATS.io](https://nats.io/), a high-performance messaging system for cloud native applications.