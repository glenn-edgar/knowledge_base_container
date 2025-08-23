# NAS_RPC

A lightweight Python RPC (Remote Procedure Call) implementation over NATS messaging system with namespace support. This library enables both RPC server and client functionality with automatic request-response handling, timeout support, and namespace isolation.

## Features

- 🚀 **Full RPC Implementation** - Both server and client in a single class
- 📦 **Namespace Isolation** - Separate RPC methods by environment or application
- ⚡ **Async/Await Support** - Full asynchronous operation with Python asyncio
- 🔄 **Multiple Call Patterns** - Synchronous, asynchronous, and batch calls
- 🎯 **Method Registration** - Simple decorator-based method registration
- ⏱️ **Timeout Support** - Configurable timeouts for all RPC calls
- 🛡️ **Error Handling** - Proper RPC error codes and exception propagation
- 🔌 **Direct NATS Protocol** - No external dependencies, pure Python implementation
- 🏷️ **Type Hints** - Full type hint support for better IDE integration

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

### Basic RPC Server and Client

```python
import asyncio
from nas_rpc import NAS_RPC

async def main():
    # Create RPC server
    server = NAS_RPC("localhost", 4222, namespace="myapp")
    await server.connect()
    
    # Register a method
    @server.rpc_method("greet")
    async def greet(name: str) -> str:
        return f"Hello, {name}!"
    
    # Start the server
    await server.start_server()
    
    # Create RPC client (same namespace)
    client = NAS_RPC("localhost", 4222, namespace="myapp")
    await client.connect()
    
    # Make RPC call
    result = await client.call("greet", {"name": "World"})
    print(result)  # Output: Hello, World!
    
    # Cleanup
    await client.disconnect()
    await server.disconnect()

asyncio.run(main())
```

## API Reference

### Initialization

```python
rpc = NAS_RPC(
    host="localhost",      # NATS server host
    port=4222,            # NATS server port
    namespace="default"   # Namespace for RPC isolation
)
```

### Connection Management

#### `connect()`
Establish connection to NATS server.

```python
success = await rpc.connect()
if success:
    print("Connected!")
```

#### `wait_connected(timeout=5.0)`
Wait for connection with timeout.

```python
if await rpc.wait_connected(timeout=10.0):
    print("Connected within 10 seconds")
```

#### `disconnect()`
Close connection and cleanup resources.

```python
await rpc.disconnect()
```

### RPC Server Methods

#### `register_handler(method, handler)`
Register an RPC method handler.

```python
async def add(a: float, b: float) -> float:
    return a + b

server.register_handler("math.add", add)
```

#### `rpc_method(name=None)`
Decorator for registering RPC methods.

```python
@server.rpc_method("math.multiply")
async def multiply(x: float, y: float) -> float:
    return x * y

# Or use function name as method name
@server.rpc_method()
async def divide(a: float, b: float) -> float:
    return a / b  # Method name will be "divide"
```

#### `start_server(prefix="")`
Start the RPC server with optional prefix.

```python
# Without prefix - methods registered as-is
await server.start_server()

# With prefix - all methods get prefix
await server.start_server("api")
# "math.add" becomes "api.math.add"
```

### RPC Client Methods

#### `call(method, params=None, timeout=5.0)`
Make a synchronous RPC call and wait for response.

```python
# With dictionary parameters
result = await client.call("math.add", {"a": 5, "b": 3})

# With list parameters
result = await client.call("math.multiply", [4, 7])

# With timeout
result = await client.call("slow.method", {"data": "test"}, timeout=30.0)
```

#### `call_async(method, params=None)`
Make an asynchronous RPC call without waiting.

```python
# Start async call
request_id = await client.call_async("long.process", {"input": data})

# Do other work...
await do_something_else()

# Get the result later
result = await client.get_response(request_id, timeout=10.0)
```

#### `call_batch(calls, timeout=5.0)`
Make multiple RPC calls in parallel.

```python
batch_calls = [
    ("math.add", {"a": 1, "b": 2}),
    ("math.multiply", {"x": 3, "y": 4}),
    ("string.concat", {"str1": "Hello", "str2": "World"})
]

results = await client.call_batch(batch_calls)
for i, result in enumerate(results):
    if isinstance(result, Exception):
        print(f"Call {i} failed: {result}")
    else:
        print(f"Call {i} result: {result}")
```

## Advanced Usage

### Complete RPC Service Example

```python
import asyncio
from nas_rpc import NAS_RPC

class CalculatorService:
    def __init__(self):
        self.rpc = NAS_RPC("localhost", 4222, namespace="calculator")
        self.setup_methods()
    
    def setup_methods(self):
        @self.rpc.rpc_method("add")
        async def add(a: float, b: float) -> float:
            return a + b
        
        @self.rpc.rpc_method("subtract")
        async def subtract(a: float, b: float) -> float:
            return a - b
        
        @self.rpc.rpc_method("multiply")
        async def multiply(a: float, b: float) -> float:
            return a * b
        
        @self.rpc.rpc_method("divide")
        async def divide(a: float, b: float) -> float:
            if b == 0:
                raise ValueError("Division by zero")
            return a / b
        
        @self.rpc.rpc_method("power")
        async def power(base: float, exponent: float) -> float:
            return base ** exponent
    
    async def start(self):
        await self.rpc.connect()
        await self.rpc.start_server()
        print("Calculator service started")
    
    async def stop(self):
        await self.rpc.disconnect()

# Run the service
async def main():
    service = CalculatorService()
    await service.start()
    
    # Keep running
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await service.stop()

asyncio.run(main())
```

### Error Handling

```python
# Server-side error handling
@server.rpc_method("validate")
async def validate(data: str) -> bool:
    if not data:
        raise ValueError("Data cannot be empty")
    if len(data) < 5:
        raise ValueError("Data too short")
    return True

# Client-side error handling
try:
    result = await client.call("validate", {"data": ""})
except Exception as e:
    print(f"Validation failed: {e}")
    # Output: RPC Error (-32603): Internal error: Data cannot be empty
```

### Namespace Isolation

```python
# Production server
server_prod = NAS_RPC("localhost", 4222, namespace="production")
await server_prod.connect()

@server_prod.rpc_method("get_data")
async def get_prod_data():
    return {"env": "production", "data": "real data"}

await server_prod.start_server()

# Development server (different namespace)
server_dev = NAS_RPC("localhost", 4222, namespace="development")
await server_dev.connect()

@server_dev.rpc_method("get_data")
async def get_dev_data():
    return {"env": "development", "data": "test data"}

await server_dev.start_server()

# Clients can only call methods in their namespace
client_prod = NAS_RPC("localhost", 4222, namespace="production")
await client_prod.connect()
result = await client_prod.call("get_data")  # Gets production data

client_dev = NAS_RPC("localhost", 4222, namespace="development")
await client_dev.connect()
result = await client_dev.call("get_data")  # Gets development data
```

### Context Manager Usage

```python
async with NAS_RPC("localhost", 4222, namespace="temp") as rpc:
    @rpc.rpc_method("echo")
    async def echo(message: str) -> str:
        return f"Echo: {message}"
    
    await rpc.start_server()
    
    result = await rpc.call("echo", {"message": "Hello"})
    print(result)  # Output: Echo: Hello
    
# Connection automatically closed
```

### Method Prefixes

```python
# Register methods with different prefixes
@server.rpc_method("add")
async def add(a, b):
    return a + b

@server.rpc_method("concat")
async def concat(s1, s2):
    return s1 + s2

# Start with prefix
await server.start_server("v1")

# Client must use full path
result = await client.call("v1.add", {"a": 1, "b": 2})
result = await client.call("v1.concat", {"s1": "Hello", "s2": "World"})
```

## RPC Protocol

### Request Format
```json
{
    "id": "unique-request-id",
    "method": "method.name",
    "params": {
        "param1": "value1",
        "param2": "value2"
    }
}
```

### Response Format (Success)
```json
{
    "id": "unique-request-id",
    "result": "method result",
    "error": null
}
```

### Response Format (Error)
```json
{
    "id": "unique-request-id",
    "result": null,
    "error": {
        "code": -32603,
        "message": "Internal error: Division by zero"
    }
}
```

### Error Codes
- `-32700`: Parse error - Invalid JSON
- `-32602`: Invalid params - Wrong parameter types or count
- `-32603`: Internal error - Method execution failed

## Performance Considerations

### Connection Management
- Each NAS_RPC instance maintains its own connection
- Use a single server instance for all methods
- Reuse client instances for multiple calls

### Timeout Settings
```python
# Short timeout for fast methods
result = await client.call("fast.method", params, timeout=1.0)

# Longer timeout for slow operations
result = await client.call("slow.process", params, timeout=60.0)

# Batch calls with appropriate timeout
results = await client.call_batch(calls, timeout=30.0)
```

### Namespace Best Practices
```python
# Environment-based namespaces
namespace = os.getenv("ENVIRONMENT", "development")
rpc = NAS_RPC("localhost", 4222, namespace=namespace)

# Service-based namespaces
auth_service = NAS_RPC(namespace="auth")
user_service = NAS_RPC(namespace="users")
payment_service = NAS_RPC(namespace="payments")
```

## Example Applications

### Microservices Communication
```python
# User service
user_service = NAS_RPC(namespace="users")
await user_service.connect()

@user_service.rpc_method("get_user")
async def get_user(user_id: int):
    # Fetch from database
    return {"id": user_id, "name": "John Doe"}

await user_service.start_server()

# Order service calling user service
order_service = NAS_RPC(namespace="users")  # Same namespace to call users
await order_service.connect()

user = await order_service.call("get_user", {"user_id": 123})
```

### Task Distribution
```python
# Worker
worker = NAS_RPC(namespace="tasks")
await worker.connect()

@worker.rpc_method("process_image")
async def process_image(image_url: str):
    # Process image
    return {"status": "completed", "url": processed_url}

await worker.start_server()

# Dispatcher
dispatcher = NAS_RPC(namespace="tasks")
await dispatcher.connect()

# Distribute tasks
tasks = [
    ("process_image", {"image_url": url1}),
    ("process_image", {"image_url": url2}),
    ("process_image", {"image_url": url3})
]

results = await dispatcher.call_batch(tasks, timeout=30.0)
```

## Testing

```python
import asyncio
import pytest
from nas_rpc import NAS_RPC

@pytest.mark.asyncio
async def test_rpc_call():
    # Setup server
    server = NAS_RPC(namespace="test")
    await server.connect()
    
    @server.rpc_method("add")
    async def add(a, b):
        return a + b
    
    await server.start_server()
    
    # Test client
    client = NAS_RPC(namespace="test")
    await client.connect()
    
    result = await client.call("add", {"a": 5, "b": 3})
    assert result == 8
    
    # Cleanup
    await client.disconnect()
    await server.disconnect()
```

## Troubleshooting

### Connection Issues
```python
rpc = NAS_RPC("localhost", 4222, namespace="myapp")

if not await rpc.connect():
    print("Failed to connect to NATS")
    print("Check if NATS server is running: docker ps | grep nats")
```

### Timeout Errors
- Increase timeout for slow methods
- Check if server and client are in same namespace
- Verify NATS server is running and accessible

### Method Not Found
- Ensure server has started with `start_server()`
- Check namespace matches between client and server
- Verify method name and prefix are correct

## Limitations

1. **No Bidirectional Streaming** - Only request-response pattern
2. **JSON Serialization** - Parameters and results must be JSON serializable
3. **Single Response** - Each request gets exactly one response
4. **Namespace Isolation** - No cross-namespace calls

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is provided as-is for educational and development purposes.

## Acknowledgments

Built on top of [NATS.io](https://nats.io/), a high-performance messaging system for cloud native applications.