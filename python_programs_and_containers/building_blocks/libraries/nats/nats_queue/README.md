# NASQueue

A lightweight Python queue implementation over NATS messaging system with namespace support. This library provides push/pop queue functionality with automatic message queuing, subscription-based message collection, and namespace isolation for multi-tenant environments.

## Features

- 📦 **Queue Operations** - Simple push/pop interface for message queuing
- 🏷️ **Namespace Isolation** - Separate queues by environment or application
- 📨 **Auto-Subscribe** - Automatically queue incoming messages from subscriptions
- 👥 **Queue Groups** - Load balancing support for distributed workers
- ⏱️ **Timeout Support** - Pop operations with configurable timeout
- 🔄 **Multiple Data Types** - Support for strings, bytes, JSON, and binary data
- 📊 **Queue Management** - Monitor queue size and clear operations
- 🔌 **Direct NATS Protocol** - No external dependencies, pure Python implementation
- ⚡ **Async/Await** - Full asynchronous operation with Python asyncio

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

### Basic Queue Operations

```python
import asyncio
from nas_queue import NASQueue

async def main():
    # Create queue client
    queue = NASQueue("localhost", 4222, namespace="myapp")
    await queue.connect()
    
    # Push messages to queue
    await queue.push("orders", "Order #1001")
    await queue.push("orders", {"id": 1002, "amount": 99.99})
    
    # Check queue size
    print(f"Queue size: {queue.queue_size()}")
    
    # Pop messages from queue
    while queue.queue_size() > 0:
        msg = await queue.pop()
        print(f"Popped: {msg['message']}")
    
    # Cleanup
    await queue.disconnect()

asyncio.run(main())
```

## API Reference

### Initialization

```python
queue = NASQueue(
    host="localhost",      # NATS server host
    port=4222,            # NATS server port
    namespace="default"   # Namespace for queue isolation
)
```

### Connection Management

#### `connect()`
Establish connection to NATS server.

```python
success = await queue.connect()
if success:
    print("Connected to NATS!")
```

#### `wait_connected(timeout=5.0)`
Wait for connection with timeout.

```python
if await queue.wait_connected(timeout=10.0):
    print("Connected within 10 seconds")
```

#### `disconnect()`
Close connection and cleanup resources.

```python
await queue.disconnect()
```

#### `is_connected()`
Check connection status.

```python
if queue.is_connected():
    await queue.push("test", "message")
```

### Queue Operations

#### `push(subject, message, headers=None)`
Push a message to the queue and publish to NATS.

```python
# Push string
await queue.push("events", "User logged in")

# Push JSON object
await queue.push("events", {"type": "login", "user_id": 123})

# Push binary data
await queue.push("files", b"Binary content here")

# Push with headers (stored but not used in basic NATS)
await queue.push("priority", "Important message", headers={"priority": "high"})
```

#### `pop(timeout=None)`
Pop a message from the queue.

```python
# Pop immediately (returns None if empty)
msg = await queue.pop()

# Pop with timeout (waits up to 5 seconds)
msg = await queue.pop(timeout=5.0)

if msg:
    print(f"Subject: {msg['subject']}")
    print(f"Message: {msg['message']}")
    print(f"Timestamp: {msg['timestamp']}")
```

#### `subscribe_and_queue(subject, queue_group="")`
Subscribe to a subject and automatically queue received messages.

```python
# Simple subscription
await queue.subscribe_and_queue("events")

# With queue group for load balancing
await queue.subscribe_and_queue("tasks", queue_group="workers")
```

#### `unsubscribe(sid=None)`
Unsubscribe from subscriptions.

```python
# Unsubscribe from specific subscription
sid = await queue.subscribe_and_queue("events")
await queue.unsubscribe(sid)

# Unsubscribe from all
await queue.unsubscribe()
```

### Queue Management

#### `queue_size()`
Get the current number of messages in the queue.

```python
size = queue.queue_size()
print(f"Messages in queue: {size}")
```

#### `clear_queue()`
Remove all messages from the queue.

```python
queue.clear_queue()
print("Queue cleared")
```

#### `get_namespace()`
Get the current namespace.

```python
namespace = queue.get_namespace()
print(f"Current namespace: {namespace}")
```

## Advanced Usage

### Producer-Consumer Pattern

```python
import asyncio
from nas_queue import NASQueue

# Producer
async def producer():
    queue = NASQueue("localhost", 4222, namespace="tasks")
    await queue.connect()
    
    for i in range(10):
        await queue.push("work", f"Task #{i+1}")
        print(f"Produced: Task #{i+1}")
        await asyncio.sleep(0.5)
    
    await queue.disconnect()

# Consumer
async def consumer(worker_id):
    queue = NASQueue("localhost", 4222, namespace="tasks")
    await queue.connect()
    
    # Subscribe to receive tasks
    await queue.subscribe_and_queue("work")
    
    while True:
        msg = await queue.pop(timeout=1.0)
        if msg:
            print(f"Worker {worker_id} processing: {msg['message'].decode()}")
            await asyncio.sleep(1)  # Simulate work
        else:
            break
    
    await queue.disconnect()

# Run producer and consumers
async def main():
    await asyncio.gather(
        producer(),
        consumer(1),
        consumer(2)
    )

asyncio.run(main())
```

### Load Balancing with Queue Groups

```python
# Multiple workers sharing the load
async def worker(worker_id):
    queue = NASQueue("localhost", 4222, namespace="workers")
    await queue.connect()
    
    # All workers subscribe with same queue group
    await queue.subscribe_and_queue("tasks", queue_group="worker_pool")
    
    while True:
        msg = await queue.pop(timeout=2.0)
        if msg:
            print(f"Worker {worker_id}: {msg['message'].decode()}")
            # Process task...
        else:
            break
    
    await queue.disconnect()

# Dispatcher
async def dispatcher():
    queue = NASQueue("localhost", 4222, namespace="workers")
    await queue.connect()
    
    # Send tasks that will be distributed among workers
    for i in range(20):
        await queue.push("tasks", f"Task {i+1}")
    
    await queue.disconnect()

async def main():
    # Start 3 workers and dispatcher
    await asyncio.gather(
        dispatcher(),
        worker(1),
        worker(2),
        worker(3)
    )

asyncio.run(main())
```

### Namespace Isolation

```python
# Production queue
queue_prod = NASQueue("localhost", 4222, namespace="production")
await queue_prod.connect()
await queue_prod.push("orders", "Production order")

# Development queue (isolated)
queue_dev = NASQueue("localhost", 4222, namespace="development")
await queue_dev.connect()
await queue_dev.push("orders", "Test order")

# Each namespace has its own queue
print(f"Prod queue: {queue_prod.queue_size()}")  # 1
print(f"Dev queue: {queue_dev.queue_size()}")    # 1

# Subscriptions are also isolated by namespace
await queue_prod.subscribe_and_queue("events")
await queue_dev.push("events", "Dev event")  # Won't be received by prod
```

### Message Types and Serialization

```python
queue = NASQueue("localhost", 4222, namespace="data")
await queue.connect()

# String messages (automatically encoded to bytes)
await queue.push("text", "Hello, World!")

# JSON objects (automatically serialized)
await queue.push("json", {
    "user_id": 123,
    "action": "login",
    "timestamp": "2024-01-01T10:00:00Z"
})

# Binary data
image_data = b"\x89PNG\r\n\x1a\n..."
await queue.push("images", image_data)

# Complex nested structures
await queue.push("complex", {
    "metadata": {
        "version": "1.0",
        "author": "system"
    },
    "data": [1, 2, 3, 4, 5],
    "nested": {
        "deep": {
            "value": 42
        }
    }
})

# Pop and handle different types
msg = await queue.pop()
if msg:
    try:
        # Try to decode as string
        text = msg['message'].decode()
        print(f"Text message: {text}")
    except:
        # Handle as binary
        print(f"Binary message of {len(msg['message'])} bytes")
```

### Context Manager Usage

```python
async with NASQueue("localhost", 4222, namespace="temp") as queue:
    await queue.push("test", "Message in context")
    
    msg = await queue.pop()
    if msg:
        print(f"Got: {msg['message'].decode()}")
    
# Connection automatically closed
```

### Event-Driven Processing

```python
class EventProcessor:
    def __init__(self, namespace="events"):
        self.queue = NASQueue("localhost", 4222, namespace=namespace)
    
    async def start(self):
        await self.queue.connect()
        
        # Subscribe to different event types
        await self.queue.subscribe_and_queue("user.login")
        await self.queue.subscribe_and_queue("user.logout")
        await self.queue.subscribe_and_queue("order.created")
        await self.queue.subscribe_and_queue("order.completed")
    
    async def process_events(self):
        while True:
            msg = await self.queue.pop(timeout=1.0)
            if msg:
                subject = msg['subject']
                data = msg['message'].decode()
                
                if subject == "user.login":
                    await self.handle_login(data)
                elif subject == "user.logout":
                    await self.handle_logout(data)
                elif subject == "order.created":
                    await self.handle_order_created(data)
                elif subject == "order.completed":
                    await self.handle_order_completed(data)
    
    async def handle_login(self, data):
        print(f"User logged in: {data}")
    
    async def handle_logout(self, data):
        print(f"User logged out: {data}")
    
    async def handle_order_created(self, data):
        print(f"Order created: {data}")
    
    async def handle_order_completed(self, data):
        print(f"Order completed: {data}")
    
    async def stop(self):
        await self.queue.disconnect()

# Usage
async def main():
    processor = EventProcessor()
    await processor.start()
    
    # Simulate events
    event_queue = NASQueue("localhost", 4222, namespace="events")
    await event_queue.connect()
    
    await event_queue.push("user.login", "user123")
    await event_queue.push("order.created", "order456")
    await event_queue.push("order.completed", "order456")
    await event_queue.push("user.logout", "user123")
    
    # Process events
    await processor.process_events()
    
    await event_queue.disconnect()
    await processor.stop()

asyncio.run(main())
```

## Message Format

Messages in the queue have the following structure:

```python
{
    'subject': 'original.subject',    # Subject without namespace
    'message': b'message bytes',      # Message content as bytes
    'headers': None,                  # Optional headers
    'reply_to': None,                 # Reply subject if any
    'timestamp': datetime.now()       # When message was received
}
```

## Performance Considerations

### Queue Size Management
```python
# Monitor queue size
if queue.queue_size() > 1000:
    print("Warning: Queue is getting large")
    
# Clear old messages if needed
if queue.queue_size() > 10000:
    queue.clear_queue()
```

### Batch Processing
```python
# Process messages in batches
async def process_batch(queue, batch_size=10):
    batch = []
    
    for _ in range(batch_size):
        msg = await queue.pop()
        if msg:
            batch.append(msg)
        else:
            break
    
    if batch:
        # Process entire batch at once
        await process_messages(batch)
```

### Connection Reuse
```python
# Reuse single connection for multiple operations
queue = NASQueue("localhost", 4222, namespace="app")
await queue.connect()

# Multiple operations on same connection
for i in range(100):
    await queue.push("events", f"Event {i}")

# Don't create new connections unnecessarily
# BAD: Creating connection for each message
for i in range(100):
    q = NASQueue()
    await q.connect()
    await q.push("events", f"Event {i}")
    await q.disconnect()
```

## Use Cases

### Task Queue
```python
# Task queue for background jobs
task_queue = NASQueue(namespace="tasks")
await task_queue.connect()

# Producer adds tasks
await task_queue.push("email", json.dumps({
    "to": "user@example.com",
    "subject": "Welcome!",
    "template": "welcome"
}))

# Worker processes tasks
await task_queue.subscribe_and_queue("email")
while True:
    task = await task_queue.pop(timeout=5.0)
    if task:
        email_data = json.loads(task['message'].decode())
        await send_email(email_data)
```

### Log Aggregation
```python
# Centralized log collection
log_queue = NASQueue(namespace="logs")
await log_queue.connect()

# Applications push logs
await log_queue.push("app.errors", json.dumps({
    "timestamp": datetime.now().isoformat(),
    "level": "ERROR",
    "message": "Database connection failed",
    "service": "user-service"
}))

# Log processor
await log_queue.subscribe_and_queue("app.errors")
while True:
    log_entry = await log_queue.pop()
    if log_entry:
        # Store in database or forward to monitoring
        await process_log(log_entry)
```

### Message Buffer
```python
# Buffer messages during high load
buffer = NASQueue(namespace="buffer")
await buffer.connect()

# Receive from external source and buffer
await buffer.subscribe_and_queue("incoming.data")

# Process at controlled rate
async def controlled_processor():
    while True:
        # Process one message per second
        msg = await buffer.pop()
        if msg:
            await slow_process(msg)
        await asyncio.sleep(1)
```

## Testing

```python
import asyncio
import pytest
from nas_queue import NASQueue

@pytest.mark.asyncio
async def test_push_pop():
    queue = NASQueue(namespace="test")
    await queue.connect()
    
    # Test push
    await queue.push("test", "Hello")
    assert queue.queue_size() == 1
    
    # Test pop
    msg = await queue.pop()
    assert msg is not None
    assert msg['message'] == b"Hello"
    assert msg['subject'] == "test"
    
    # Test empty pop
    msg = await queue.pop()
    assert msg is None
    
    await queue.disconnect()

@pytest.mark.asyncio
async def test_subscription():
    queue = NASQueue(namespace="test")
    await queue.connect()
    
    # Subscribe first
    await queue.subscribe_and_queue("events")
    
    # Publish message
    await queue.push("events", "Test event")
    
    # Wait for message to arrive
    await asyncio.sleep(0.1)
    
    # Should have one message
    assert queue.queue_size() == 1
    
    msg = await queue.pop()
    assert msg['message'] == b"Test event"
    
    await queue.disconnect()
```

## Troubleshooting

### Connection Issues
```python
queue = NASQueue("localhost", 4222, namespace="myapp")

if not await queue.connect():
    print("Failed to connect to NATS")
    print("Check if NATS server is running: docker ps | grep nats")
    print("Check if port 4222 is accessible: telnet localhost 4222")
```

### Messages Not Being Received
- Verify publisher and subscriber are in the same namespace
- Check subscription is active before publishing
- Add small delay after subscribe before publishing
- Ensure NATS server is running and accessible

### Queue Growing Too Large
```python
# Monitor and manage queue size
async def queue_monitor(queue):
    while True:
        size = queue.queue_size()
        if size > 10000:
            print(f"Warning: Queue size is {size}")
            # Consider clearing old messages
            # queue.clear_queue()
        await asyncio.sleep(60)
```

### Memory Usage
- Clear queue periodically if messages accumulate
- Process messages in batches
- Use queue groups to distribute load

## Limitations

1. **Local Queue Only** - Messages are queued locally, not persisted
2. **No Persistence** - Messages lost on disconnect
3. **Memory Bound** - Queue size limited by available memory
4. **No Priority** - FIFO order only
5. **No Deduplication** - Duplicate messages are queued

## Best Practices

1. **Always Check Connection**
```python
if await queue.connect():
    await queue.push("test", "message")
else:
    print("Connection failed")
```

2. **Handle Pop Timeouts**
```python
msg = await queue.pop(timeout=5.0)
if msg:
    process(msg)
else:
    print("No messages available")
```

3. **Use Queue Groups for Scaling**
```python
# Multiple workers with same queue group
await queue.subscribe_and_queue("tasks", queue_group="workers")
```

4. **Clear Queue When Needed**
```python
if queue.queue_size() > MAX_SIZE:
    queue.clear_queue()
```

5. **Use Appropriate Namespaces**
```python
# Environment-based
namespace = os.getenv("ENVIRONMENT", "development")

# Service-based
namespace = f"{service_name}.{environment}"
```

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is provided as-is for educational and development purposes.

## Acknowledgments

Built on top of [NATS.io](https://nats.io/), a high-performance messaging system for cloud native applications.