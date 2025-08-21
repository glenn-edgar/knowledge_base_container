# NAS Utilities

A comprehensive collection of NATS-based utilities for Python applications, providing publish/subscribe messaging, queue operations, and RPC functionality.

## Overview

This repository contains three core NAS (NATS) utility modules that provide different messaging patterns for distributed systems:

- **Publish/Subscribe** - Event-driven messaging with topic-based routing
- **Queue Operations** - Message queueing with push/pop functionality
- **RPC (Remote Procedure Call)** - Request/response pattern for distributed function calls

## Directory Structure

```
nas_utilities/
├── nas_pub_sub/       # Publish/Subscribe implementation
│   └── ...
├── nas_queue/         # Queue operations implementation
│   └── ...
├── nas_rpc/           # RPC client/server implementation
│   └── ...
├── nas_pub_sub_main.sh   # Test script for pub/sub functions
├── nas_queue_main.sh     # Test script for queue functions
├── nas_rpc_main.sh       # Test script for RPC functions
└── README.md
```

## Installation

### Prerequisites

All utilities require the NATS Python client:

```bash
pip install nats-py
```

Ensure you have a NATS server running:
```bash
# Using Docker
docker run -p 4222:4222 nats:latest

# Or download and run NATS server directly
# See: https://docs.nats.io/running-a-nats-service/introduction/installation
```

## Modules

### 1. NAS Publish/Subscribe (`nas_pub_sub/`)

Provides a class for NATS publication and subscription connections, enabling event-driven communication between distributed components.

#### Key Features:
- Topic-based message routing
- Multiple subscriber support
- Wildcard subscriptions
- Asynchronous message handling

#### Basic Usage:
```python
from nas_pub_sub import NASPubSub

# Publisher
pub = NASPubSub()
await pub.connect()
await pub.publish("events.user.login", {"user_id": 123})

# Subscriber
sub = NASPubSub()
await sub.connect()
await sub.subscribe("events.>", callback=handle_event)
```

### 2. NAS Queue (`nas_queue/`)

Implements a queue system with push and pop operations, combining NATS messaging with local queue storage for reliable message processing.

#### Key Features:
- Push messages to queue and NATS simultaneously
- Pop messages from local queue
- Subscribe and auto-queue incoming messages
- Queue size management
- Support for queue groups

#### Basic Usage:
```python
from nas_queue import NASQueue

queue = NASQueue()
await queue.connect()

# Push messages
await queue.push("task.queue", {"task": "process_data"})

# Pop messages
message = await queue.pop()
```

### 3. NAS RPC (`nas_rpc/`)

Provides RPC (Remote Procedure Call) functionality with both client and server capabilities in a single class.

#### Key Features:
- Bidirectional RPC support
- Method registration via decorators
- Timeout handling
- Batch RPC calls
- Error propagation
- Async/await support

#### Basic Usage:
```python
from nas_rpc import NAS_RPC

rpc = NAS_RPC()
await rpc.connect()

# Server side - register method
@rpc.rpc_method("math.add")
async def add(a, b):
    return a + b

await rpc.start_server()

# Client side - call method
result = await rpc.call("math.add", {"a": 5, "b": 3})
```

## Testing

Test scripts are provided for each module to verify functionality:

### Test Publish/Subscribe
```bash
./nas_pub_sub_main.sh
```
Tests the publish and subscribe functions including:
- Basic pub/sub operations
- Wildcard subscriptions
- Multiple subscribers
- Message routing

### Test Queue Operations
```bash
./nas_queue_main.sh
```
Tests queue functionality including:
- Push/pop operations
- Queue size management
- Subscription with auto-queueing
- Queue groups

### Test RPC Functions
```bash
./nas_rpc_main.sh
```
Tests RPC capabilities including:
- Method registration
- Synchronous calls
- Asynchronous calls
- Batch operations
- Error handling

## Quick Start Example

Here's a complete example using all three utilities together:

```python
import asyncio
from nas_pub_sub import NASPubSub
from nas_queue import NASQueue
from nas_rpc import NAS_RPC

async def main():
    # Setup Pub/Sub for events
    events = NASPubSub()
    await events.connect()
    
    # Setup Queue for task processing
    task_queue = NASQueue()
    await task_queue.connect()
    
    # Setup RPC for service calls
    rpc = NAS_RPC()
    await rpc.connect()
    
    # Register an RPC method
    @rpc.rpc_method("process.task")
    async def process_task(task_id):
        # Process the task
        result = f"Processed task {task_id}"
        
        # Publish completion event
        await events.publish("task.completed", {"task_id": task_id})
        
        return result
    
    await rpc.start_server()
    
    # Subscribe to events and queue them
    await task_queue.subscribe_and_queue("new.tasks")
    
    # Process queued tasks via RPC
    while task_queue.queue_size() > 0:
        task = await task_queue.pop()
        result = await rpc.call("process.task", {"task_id": task['id']})
        print(f"Result: {result}")
    
    # Cleanup
    await events.disconnect()
    await task_queue.disconnect()
    await rpc.disconnect()

asyncio.run(main())
```

## Use Cases

### When to Use Each Utility

#### Use Publish/Subscribe when:
- Broadcasting events to multiple consumers
- Implementing event-driven architectures
- Decoupling components through events
- Building real-time notification systems

#### Use Queue when:
- Implementing work queues
- Buffering messages for batch processing
- Load balancing tasks across workers
- Ensuring message processing order

#### Use RPC when:
- Need request/response patterns
- Calling remote functions
- Building microservice APIs
- Requiring synchronous-style communication in async systems

## Common Patterns

### Event-Driven Task Processing
```python
# Use pub/sub to trigger, queue to buffer, RPC to process
await events.subscribe("order.created", 
    lambda msg: task_queue.push("orders.process", msg))

# Worker processes from queue using RPC
while True:
    if task_queue.queue_size() > 0:
        order = await task_queue.pop()
        result = await rpc.call("order.process", order)
        await events.publish("order.processed", result)
```

### Microservice Communication
```python
# Service A publishes events
await events.publish("user.registered", user_data)

# Service B queues events for processing
await task_queue.subscribe_and_queue("user.registered")

# Service C provides processing via RPC
@rpc.rpc_method("user.sendWelcomeEmail")
async def send_welcome(user_data):
    # Send email logic
    return {"status": "sent"}
```

## Configuration

All utilities support the same connection parameters:

```python
# Basic connection
client = NASQueue("nats://localhost:4222")

# With options
client = NASQueue(
    "nats://localhost:4222",
    name="my-service",
    reconnect_time_wait=2,
    max_reconnect_attempts=10,
    user="username",
    password="password"
)
```

## Performance Considerations

- **Pub/Sub**: Lowest latency, best for real-time events
- **Queue**: Adds local buffering overhead, best for reliability
- **RPC**: Includes round-trip time, best for request/response patterns

## Error Handling

All utilities support proper error handling:

```python
try:
    await client.connect()
    # Operations
except Exception as e:
    print(f"Error: {e}")
finally:
    await client.disconnect()
```

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## Requirements

- Python 3.7+
- nats-py library
- Running NATS server

## License

[Your License Here]

## Documentation

For detailed documentation on each module, see:
- [NAS Publish/Subscribe Documentation](./nas_pub_sub/README.md)
- [NAS Queue Documentation](./nas_queue/README.md)  
- [NAS RPC Documentation](./nas_rpc/README.md)

## Support

For issues, questions, or contributions, please:
1. Check the individual module documentation
2. Review the test scripts for usage examples
3. Open an issue on the repository
4. Consult the [NATS documentation](https://docs.nats.io/)

## See Also

- [NATS.io Official Site](https://nats.io/)
- [NATS Python Client](https://github.com/nats-io/nats.py)
- [NATS Concepts](https://docs.nats.io/nats-concepts/overview)
- [NATS Protocol Documentation](https://docs.nats.io/reference/reference-protocols/nats-protocol)

