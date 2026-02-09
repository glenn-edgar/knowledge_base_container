# NAS_RPC

A lightweight Remote Procedure Call (RPC) implementation over NATS messaging system with both synchronous and asynchronous support.

## Features

- **Dual API Support**: Both async/await and synchronous programming patterns
- **Instance-specific routing**: Target specific server instances when needed
- **Load balancing**: Automatic distribution across multiple server instances (via NATS)
- **Health monitoring**: Built-in health check endpoints
- **Batch operations**: Execute multiple RPC calls in parallel
- **Error handling**: Comprehensive timeout and error management
- **Thread-safe**: Safe to use from multiple threads with sync wrappers

## Requirements

- Python 3.7+
- NATS Server running locally or remotely
- No additional Python dependencies (uses only standard library)

## Quick Start

### Basic Server Setup

```python
import asyncio
from nas_rpc import NAS_RPC

# Async server
async def setup_async_server():
    server = NAS_RPC("localhost", 4222, namespace="production", instance_id="math_server_1")
    
    # Register RPC methods
    async def add(a: float, b: float) -> float:
        return a + b
    
    async def multiply(x: float, y: float) -> float:
        return x * y
    
    server.register_handler("math.add", add)
    server.register_handler("math.multiply", multiply)
    
    await server.connect()
    await server.start_server("api")
    
    # Server is now running and accepting RPC calls
    return server

# Synchronous server (same functionality)
def setup_sync_server():
    server = NAS_RPC("localhost", 4222, namespace="production", instance_id="math_server_2")
    
    def subtract(a: float, b: float) -> float:
        return a - b
    
    def divide(x: float, y: float) -> float:
        if y == 0:
            raise ValueError("Cannot divide by zero")
        return x / y
    
    server.register_handler("math.subtract", subtract)
    server.register_handler("math.divide", divide)
    
    server.connect_sync()
    server.start_server_sync("api")
    
    return server
```

### Basic Client Usage

```python
# Async client
async def async_client_example():
    client = NAS_RPC("localhost", 4222, namespace="production", instance_id="client_1")
    await client.connect()
    
    # Make RPC calls
    result = await client.call("api.math.add", {"a": 10, "b": 5})
    print(f"10 + 5 = {result}")  # Output: 10 + 5 = 15
    
    # Batch operations
    batch_calls = [
        ("api.math.add", {"a": 1, "b": 2}),
        ("api.math.multiply", {"x": 3, "y": 4}),
        ("api.math.subtract", {"a": 10, "b": 3})
    ]
    results = await client.call_batch(batch_calls)
    print(f"Batch results: {results}")  # [3, 12, 7]
    
    await client.disconnect()

# Synchronous client (same functionality)
def sync_client_example():
    client = NAS_RPC("localhost", 4222, namespace="production", instance_id="client_2")
    client.connect_sync()
    
    # Make RPC calls
    result = client.call_sync("api.math.add", {"a": 20, "b": 15})
    print(f"20 + 15 = {result}")  # Output: 20 + 15 = 35
    
    # Target specific server instance
    result = client.call_sync("api.math.subtract", {"a": 100, "b": 25}, target_instance="math_server_2")
    print(f"100 - 25 = {result}")  # Output: 100 - 25 = 75
    
    client.disconnect_sync()
```

## Advanced Features

### Using Decorators

```python
server = NAS_RPC("localhost", 4222, namespace="api")

@server.rpc_method("calculate.fibonacci")
async def fibonacci(n: int) -> int:
    if n <= 1:
        return n
    return await fibonacci(n-1) + await fibonacci(n-2)

@server.rpc_method("calculate.factorial", instance_specific=True)
def factorial(n: int) -> int:
    if n <= 1:
        return 1
    return n * factorial(n-1)

await server.start_server()
```

### Instance-Specific Routing

```python
# Register method only available on specific instance
server.register_handler("admin.restart", restart_handler, instance_specific=True)

# Client calls specific instance
result = await client.call("admin.restart", target_instance="server_1")
```

### Async Operations

```python
# Fire-and-forget call
request_id = await client.call_async("heavy.computation", {"data": large_dataset})

# Later, get the response
result = await client.get_response(request_id, timeout=30.0)
```

### Error Handling

```python
try:
    result = await client.call("api.divide", {"x": 10, "y": 0}, timeout=5.0)
except TimeoutError:
    print("RPC call timed out")
except Exception as e:
    print(f"RPC error: {e}")
```

## API Reference

### NAS_RPC Class

#### Constructor
```python
NAS_RPC(host="localhost", port=4222, namespace="default", 
        instance_id=None, enable_health_checks=True)
```

- `host`: NATS server hostname
- `port`: NATS server port
- `namespace`: Namespace prefix for all subjects
- `instance_id`: Unique instance identifier (auto-generated if None)
- `enable_health_checks`: Enable built-in `_health` endpoint

#### Connection Methods
```python
# Async
await rpc.connect()
await rpc.wait_connected(timeout=5.0)
await rpc.disconnect()

# Sync
rpc.connect_sync(timeout=5.0)
rpc.wait_connected_sync(timeout=5.0)
rpc.disconnect_sync(timeout=5.0)
```

#### Server Methods
```python
# Register RPC handler
rpc.register_handler(method, handler, instance_specific=False, 
                    load_balancing=True, metadata=None)

# Start server
await rpc.start_server(prefix="")  # Async
rpc.start_server_sync(prefix="", timeout=10.0)  # Sync

# Decorator
@rpc.rpc_method(name="optional_name", instance_specific=False)
def my_handler(param1, param2):
    return param1 + param2
```

#### Client Methods
```python
# Basic calls
result = await rpc.call(method, params, timeout=5.0, target_instance=None)
result = rpc.call_sync(method, params, timeout=5.0, target_instance=None)

# Async calls (fire-and-forget)
request_id = await rpc.call_async(method, params, target_instance=None)
result = await rpc.get_response(request_id, timeout=5.0)

# Batch operations
results = await rpc.call_batch(calls, timeout=5.0)
results = rpc.call_batch_sync(calls, timeout=5.0)
```

#### Utility Methods
```python
# Instance information
info = rpc.get_instance_info()
namespace = rpc.get_namespace()
connected = rpc.is_connected()
```

## Configuration

### NATS Server Setup

Install and run NATS server:
```bash
# Using Docker
docker run -p 4222:4222 -p 8222:8222 nats:latest

# Or install locally
# https://docs.nats.io/running-a-nats-service/introduction/installation
```

### Connection Parameters

```python
# Local development
rpc = NAS_RPC("localhost", 4222, namespace="dev")

# Production
rpc = NAS_RPC("nats.production.com", 4222, namespace="prod")

# Custom instance ID
rpc = NAS_RPC("localhost", 4222, instance_id="worker_1")
```

## Best Practices

### Error Handling
Always wrap RPC calls in try-except blocks to handle timeouts and errors:

```python
try:
    result = await client.call("api.method", params, timeout=10.0)
except TimeoutError:
    # Handle timeout
    pass
except Exception as e:
    # Handle other RPC errors
    pass
```

### Resource Management
Use context managers for automatic cleanup:

```python
# Async context manager
async with NAS_RPC("localhost", 4222) as rpc:
    await rpc.start_server()
    # Server automatically disconnected on exit

# Sync context manager
with NAS_RPC("localhost", 4222) as rpc:
    rpc.start_server_sync()
    # Server automatically disconnected on exit
```

### Performance Tips

1. **Reuse connections**: Create one NAS_RPC instance per process/thread
2. **Batch operations**: Use `call_batch()` for multiple related calls
3. **Appropriate timeouts**: Set realistic timeouts based on expected processing time
4. **Instance-specific routing**: Use when you need guaranteed execution on specific servers

### Threading Considerations

The synchronous wrappers are thread-safe, but avoid sharing the same NAS_RPC instance across multiple threads. Instead, create one instance per thread:

```python
import threading

def worker_thread():
    rpc = NAS_RPC("localhost", 4222, namespace="worker")
    rpc.connect_sync()
    
    # Do RPC work
    result = rpc.call_sync("api.process", {"data": thread_data})
    
    rpc.disconnect_sync()

# Create multiple worker threads
for i in range(5):
    thread = threading.Thread(target=worker_thread)
    thread.start()
```

## Troubleshooting

### Common Issues

**Connection refused:**
- Ensure NATS server is running on specified host/port
- Check firewall settings
- Verify network connectivity

**Timeout errors:**
- Increase timeout values for slow operations
- Check server instance availability
- Verify method names and parameters

**Method not found:**
- Ensure server has registered the method
- Check namespace and prefix configuration
- Verify server is running and connected

**Memory leaks:**
- Always call `disconnect()` or use context managers
- Don't create excessive NAS_RPC instances
- Monitor `pending_requests` in `get_instance_info()`

### Debug Information

Get detailed instance information:
```python
info = rpc.get_instance_info()
print(f"Instance ID: {info['instance_id']}")
print(f"Connected: {info['state']}")
print(f"Handlers: {info['handlers']}")
print(f"Request count: {info['request_count']}")
print(f"Error count: {info['error_count']}")
print(f"Pending requests: {info['pending_requests']}")
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please read CONTRIBUTING.md for guidelines.

## Changelog

### v2.0.0
- Added synchronous wrapper methods
- Removed service discovery (simplified for single NATS server)
- Improved error handling and timeout management
- Added context manager support
- Enhanced thread safety