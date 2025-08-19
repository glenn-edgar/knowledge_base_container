# NASQueue - NATS-based Message Queue Implementation

A Python implementation of a message queue system built on top of NATS messaging system, providing simple push/pop operations with internal queueing capabilities.

## Features

- **Simple Queue Operations**: Push and pop messages with ease
- **Internal Message Queue**: Local deque-based queue for message storage
- **NATS Integration**: Full NATS pub/sub capabilities
- **Subscription Support**: Subscribe to subjects and automatically queue received messages
- **Connection Sharing**: Create instances from existing NATS connections
- **Async/Await Support**: Fully asynchronous operations
- **Message Type Flexibility**: Support for strings, bytes, and other data types
- **Headers Support**: Optional message headers for metadata

## Installation

### Prerequisites

```bash
pip install nats-py
```

### Quick Start

```python
import asyncio
from nas_queue import NASQueue

async def main():
    # Create and connect
    queue = NASQueue("nats://localhost:4222")
    await queue.connect()
    
    # Push messages
    await queue.push("my.subject", "Hello, World!")
    await queue.push("my.subject", {"type": "data", "value": 42})
    
    # Pop messages
    while queue.queue_size() > 0:
        msg = await queue.pop()
        print(f"Got: {msg['message']}")
    
    await queue.disconnect()

asyncio.run(main())
```

## Usage

### Creating a Queue Instance

#### Standard Connection
```python
queue = NASQueue("nats://localhost:4222")
await queue.connect()
```

#### From Existing Connection
```python
# Reuse an existing NATS connection
existing_connection = some_nats_client.nc
queue = NASQueue.from_connection(existing_connection)
```

#### With Connection Options
```python
queue = NASQueue(
    "nats://localhost:4222",
    name="my-queue-client",
    reconnect_time_wait=2,
    max_reconnect_attempts=10
)
await queue.connect()
```

### Push Operations

#### Basic Push
```python
# Push a string message
await queue.push("subject.name", "Hello, NATS!")

# Push bytes
await queue.push("subject.name", b"Binary data")

# Push other types (will be converted to string)
await queue.push("subject.name", {"key": "value"})
await queue.push("subject.name", 12345)
```

#### Push with Headers
```python
headers = {
    "correlation-id": "123456",
    "content-type": "application/json"
}
await queue.push("subject.name", "Message content", headers=headers)
```

### Pop Operations

```python
# Pop a message from the queue
message = await queue.pop()

if message:
    print(f"Subject: {message['subject']}")
    print(f"Message: {message['message'].decode()}")  # bytes to string
    print(f"Headers: {message['headers']}")
```

### Subscription and Auto-Queueing

```python
# Subscribe to a subject and automatically queue received messages
await queue.subscribe_and_queue("events.>")

# Messages received on matching subjects will be added to the queue
# Pop them as needed
while queue.queue_size() > 0:
    msg = await queue.pop()
    # Process message
```

#### Queue Groups
```python
# Subscribe with queue group for load balancing
await queue.subscribe_and_queue("worker.tasks", queue="workers")
```

### Queue Management

```python
# Check queue size
size = queue.queue_size()
print(f"Queue has {size} messages")

# Clear all messages
queue.clear_queue()

# Unsubscribe from current subscription
await queue.unsubscribe()
```

## Complete Example

```python
import asyncio
import json
from nas_queue import NASQueue

async def producer_example():
    """Example of a message producer."""
    producer = NASQueue("nats://localhost:4222")
    await producer.connect()
    
    # Send different types of messages
    for i in range(5):
        # Send JSON-like data
        data = {
            "id": i,
            "timestamp": f"2024-01-01T12:00:{i:02d}",
            "value": i * 10
        }
        await producer.push("data.stream", json.dumps(data))
        
        # Send with headers
        await producer.push(
            "events.important",
            f"Event {i}",
            headers={"priority": "high", "sequence": str(i)}
        )
    
    await producer.disconnect()
    print("Producer finished sending messages")

async def consumer_example():
    """Example of a message consumer."""
    consumer = NASQueue("nats://localhost:4222")
    await consumer.connect()
    
    # Subscribe to multiple subjects
    await consumer.subscribe_and_queue("data.>")
    
    # Wait a bit for messages to arrive
    await asyncio.sleep(1)
    
    # Process all queued messages
    print(f"Consumer has {consumer.queue_size()} messages in queue")
    
    while consumer.queue_size() > 0:
        msg = await consumer.pop()
        if msg:
            try:
                # Try to parse as JSON
                data = json.loads(msg['message'].decode())
                print(f"Received data: {data}")
            except json.JSONDecodeError:
                # Handle as plain text
                print(f"Received: {msg['message'].decode()}")
    
    await consumer.unsubscribe()
    await consumer.disconnect()
    print("Consumer finished processing")

async def shared_connection_example():
    """Example of sharing connections between instances."""
    # Create primary client
    primary = NASQueue("nats://localhost:4222")
    await primary.connect()
    
    # Create secondary clients using the same connection
    queue1 = NASQueue.from_connection(primary.nc)
    queue2 = NASQueue.from_connection(primary.nc)
    
    # Each queue maintains its own internal message queue
    await queue1.push("channel.1", "Message for queue 1")
    await queue2.push("channel.2", "Message for queue 2")
    
    # Each queue has independent message storage
    print(f"Queue 1 size: {queue1.queue_size()}")  # 1
    print(f"Queue 2 size: {queue2.queue_size()}")  # 1
    
    await primary.disconnect()

async def main():
    # Run producer and consumer
    await producer_example()
    await consumer_example()
    
    # Demonstrate shared connections
    await shared_connection_example()

if __name__ == "__main__":
    asyncio.run(main())
```

## API Reference

### Class: `NASQueue`

#### Constructor
```python
NASQueue(servers: str = "nats://localhost:4222", **connection_options)
```
- `servers`: NATS server URL(s)
- `**connection_options`: Additional NATS connection options

#### Class Methods
- `from_connection(existing_connection: NATS, servers: str = "nats://localhost:4222")` - Create instance from existing NATS connection

#### Connection Methods
- `async connect()` - Establish connection to NATS server
- `async disconnect()` - Close connection to NATS server

#### Queue Operations
- `async push(subject: str, message: Any, headers: Optional[Dict] = None)` - Push message to queue and publish to NATS
- `async pop(timeout: Optional[float] = None)` - Pop message from local queue
- `queue_size()` - Get current queue size
- `clear_queue()` - Clear all messages from queue

#### Subscription Methods
- `async subscribe_and_queue(subject: str, queue: str = "")` - Subscribe and auto-queue messages
- `async unsubscribe()` - Unsubscribe from current subscription

## Use Cases

### 1. Work Queue Pattern
```python
# Worker subscribes to task queue
worker = NASQueue()
await worker.connect()
await worker.subscribe_and_queue("tasks.pending", queue="workers")

# Process tasks as they arrive
while True:
    if worker.queue_size() > 0:
        task = await worker.pop()
        # Process task
    await asyncio.sleep(0.1)
```

### 2. Event Buffer
```python
# Buffer events for batch processing
buffer = NASQueue()
await buffer.connect()
await buffer.subscribe_and_queue("events.>")

# Process in batches
while True:
    await asyncio.sleep(5)  # Wait 5 seconds
    batch = []
    while buffer.queue_size() > 0 and len(batch) < 100:
        msg = await buffer.pop()
        batch.append(msg)
    
    if batch:
        # Process batch
        process_batch(batch)
```

### 3. Message Bridge
```python
# Bridge between different subjects
bridge = NASQueue()
await bridge.connect()
await bridge.subscribe_and_queue("source.>")

while True:
    if bridge.queue_size() > 0:
        msg = await bridge.pop()
        # Transform and republish
        transformed = transform_message(msg)
        await bridge.push("destination.topic", transformed)
```

## Best Practices

1. **Connection Management**: Always disconnect when done to free resources
2. **Error Handling**: Wrap operations in try-except blocks for production code
3. **Queue Size Monitoring**: Check queue size to prevent memory issues
4. **Message Encoding**: Be consistent with message encoding/decoding
5. **Subscription Patterns**: Use wildcards (`>`, `*`) for flexible subject matching
6. **Queue Groups**: Use queue groups for load balancing across multiple consumers

## Differences from Standard NATS

- **Internal Queue**: Messages are stored locally in addition to NATS pub/sub
- **Pop Operation**: Retrieves from local queue, not from NATS
- **Push Operation**: Adds to local queue AND publishes to NATS
- **Message Persistence**: Local queue provides temporary message storage

## Limitations

- Messages in local queue are lost if process crashes
- No built-in persistence mechanism
- Queue size limited by available memory
- No automatic queue overflow handling
- Single subscription at a time per instance

## Troubleshooting

### Connection Issues
```python
# Check connection status
if not queue._connected:
    await queue.connect()
```

### Empty Queue
```python
# Ensure subscription is active
await queue.subscribe_and_queue("subject")

# Wait for messages to arrive
await asyncio.sleep(0.5)

# Then check queue
if queue.queue_size() > 0:
    msg = await queue.pop()
```

### Memory Usage
```python
# Monitor and clear queue periodically
if queue.queue_size() > 10000:
    print("Queue too large, clearing old messages")
    # Process or clear messages
    queue.clear_queue()
```

## Requirements

- Python 3.7+
- nats-py library
- Running NATS server

## License

[Your License Here]

## See Also

- [NATS Documentation](https://docs.nats.io/)
- [nats-py Documentation](https://github.com/nats-io/nats.py)
- [NATS Pub/Sub Pattern](https://docs.nats.io/nats-concepts/core-nats/pubsub)
- [NATS Queue Groups](https://docs.nats.io/nats-concepts/core-nats/queue)

