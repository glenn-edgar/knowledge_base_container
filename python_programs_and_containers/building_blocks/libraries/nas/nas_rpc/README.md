# NAS_RPC - NATS-based RPC Implementation

A robust Python implementation of Remote Procedure Calls (RPC) over NATS messaging system, providing both client and server functionality in a single, easy-to-use class.

## Features

- **Bidirectional RPC**: Single class acts as both RPC client and server
- **Async/Await Support**: Full asynchronous operation support
- **Multiple Registration Methods**: Use decorators or direct registration for RPC methods
- **Error Handling**: Automatic exception propagation from server to client
- **Timeout Support**: Configurable timeouts for all RPC calls
- **Batch Operations**: Execute multiple RPC calls in parallel
- **Connection Sharing**: Create instances from existing NATS connections
- **Request Tracking**: Unique request IDs with proper correlation
- **JSON-RPC Style**: Familiar request/response format

## Installation

### Prerequisites

```bash
pip install nats-py
```

### Quick Start

```python
import asyncio
from nas_rpc import NAS_RPC

async def main():
    # Create and connect RPC instance
    rpc = NAS_RPC("nats://localhost:4222")
    await rpc.connect()
    
    # Register an RPC method
    @rpc.rpc_method("math.add")
    async def add(a: float, b: float) -> float:
        return a + b
    
    # Start the server
    await rpc.start_server()
    
    # Make an RPC call
    result = await rpc.call("math.add", {"a": 5, "b": 3})
    print(f"Result: {result}")  # Output: Result: 8
    
    await rpc.disconnect()

asyncio.run(main())
```

## Usage

### Creating an RPC Instance

#### Standard Connection
```python
rpc = NAS_RPC("nats://localhost:4222")
await rpc.connect()
```

#### From Existing Connection
```python
# Use an existing NATS connection
existing_conn = some_nats_connection
rpc = await NAS_RPC.from_connection(existing_conn)
```

### Server-Side: Registering RPC Methods

#### Using Decorators
```python
@rpc.rpc_method("user.get")
async def get_user(user_id: int) -> dict:
    # Your implementation here
    return {"id": user_id, "name": "John Doe"}
```

#### Direct Registration
```python
async def calculate(expression: str) -> float:
    return eval(expression)  # Example only - don't use eval in production!

rpc.register_handler("math.calculate", calculate)
```

#### Starting the Server
```python
# Start listening on all registered methods
await rpc.start_server()

# Or with a prefix for all methods
await rpc.start_server("api.v1")  # Methods will be: api.v1.method_name
```

### Client-Side: Making RPC Calls

#### Synchronous Call (with await)
```python
# Call with dictionary parameters
result = await rpc.call("math.add", {"a": 10, "b": 20})

# Call with list parameters
result = await rpc.call("math.multiply", [5, 4])

# Call with timeout
result = await rpc.call("slow.operation", {"data": "value"}, timeout=10.0)
```

#### Asynchronous Call (fire and check later)
```python
# Send request without waiting
request_id = await rpc.call_async("long.operation", {"input": "data"})

# Do other work...

# Get the result later
result = await rpc.get_response(request_id, timeout=5.0)
```

#### Batch Calls
```python
# Execute multiple calls in parallel
batch_calls = [
    ("user.get", {"user_id": 1}),
    ("user.get", {"user_id": 2}),
    ("stats.calculate", {"metric": "daily"}),
]

results = await rpc.call_batch(batch_calls, timeout=5.0)
# Returns list of results in the same order
```

### Error Handling

The RPC system automatically propagates exceptions from server to client:

```python
# Server side
@rpc.rpc_method("divide")
async def divide(a: float, b: float) -> float:
    if b == 0:
        raise ValueError("Division by zero")
    return a / b

# Client side
try:
    result = await rpc.call("divide", {"a": 10, "b": 0})
except Exception as e:
    print(f"RPC Error: {e}")  # Will print the division by zero error
```

## Complete Example

```python
import asyncio
from nas_rpc import NAS_RPC

async def setup_server():
    """Setup and start RPC server."""
    server = NAS_RPC("nats://localhost:4222")
    await server.connect()
    
    # Register multiple methods
    @server.rpc_method("user.create")
    async def create_user(name: str, email: str) -> dict:
        # Simulate user creation
        return {
            "id": 123,
            "name": name,
            "email": email,
            "created": "2024-01-01T00:00:00Z"
        }
    
    @server.rpc_method("user.list")
    async def list_users(limit: int = 10) -> list:
        # Return sample users
        return [
            {"id": i, "name": f"User {i}"} 
            for i in range(1, min(limit + 1, 101))
        ]
    
    @server.rpc_method("math.factorial")
    async def factorial(n: int) -> int:
        if n < 0:
            raise ValueError("Factorial not defined for negative numbers")
        if n == 0:
            return 1
        result = 1
        for i in range(1, n + 1):
            result *= i
        return result
    
    await server.start_server("api")
    return server

async def run_client():
    """Run RPC client examples."""
    client = NAS_RPC("nats://localhost:4222")
    await client.connect()
    
    # Create a user
    user = await client.call("api.user.create", {
        "name": "Alice Smith",
        "email": "alice@example.com"
    })
    print(f"Created user: {user}")
    
    # List users
    users = await client.call("api.user.list", {"limit": 5})
    print(f"Users: {users}")
    
    # Calculate factorial
    result = await client.call("api.math.factorial", {"n": 5})
    print(f"5! = {result}")
    
    # Batch operations
    batch = [
        ("api.math.factorial", {"n": 3}),
        ("api.math.factorial", {"n": 4}),
        ("api.math.factorial", {"n": 5}),
    ]
    results = await client.call_batch(batch)
    print(f"Batch factorial results: {results}")
    
    return client

async def main():
    # Start server
    server = await setup_server()
    print("RPC Server started")
    
    # Run client operations
    client = await run_client()
    
    # Cleanup
    await client.disconnect()
    await server.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

## API Reference

### Class: `NAS_RPC`

#### Constructor
```python
NAS_RPC(servers: str = "nats://localhost:4222", **connection_options)
```

#### Class Methods
- `async from_connection(existing_connection: NATS, servers: str = "nats://localhost:4222")` - Create instance from existing connection

#### Connection Methods
- `async connect()` - Establish connection to NATS server
- `async disconnect()` - Close connection and cleanup resources

#### Server Methods
- `register_handler(method: str, handler: Callable)` - Register an RPC method handler
- `rpc_method(name: str = None)` - Decorator for registering RPC methods
- `async start_server(prefix: str = "")` - Start the RPC server

#### Client Methods
- `async call(method: str, params: Union[Dict, list] = None, timeout: float = 5.0)` - Make RPC call and wait for response
- `async call_async(method: str, params: Union[Dict, list] = None)` - Make async RPC call without waiting
- `async get_response(request_id: str, timeout: float = 5.0)` - Get response for async call
- `async call_batch(calls: list, timeout: float = 5.0)` - Execute multiple RPC calls in parallel

## Advanced Usage

### Custom Error Handling

```python
@rpc.rpc_method("validate.email")
async def validate_email(email: str) -> bool:
    if "@" not in email:
        raise ValueError("Invalid email format")
    # More validation logic
    return True

# Client side with specific error handling
try:
    is_valid = await rpc.call("validate.email", {"email": "invalid"})
except Exception as e:
    if "Invalid email format" in str(e):
        print("Email validation failed")
    else:
        raise
```

### Performance Optimization

```python
# For high-throughput scenarios, use async calls
request_ids = []
for i in range(100):
    req_id = await rpc.call_async("process.item", {"id": i})
    request_ids.append(req_id)

# Collect results
results = []
for req_id in request_ids:
    try:
        result = await rpc.get_response(req_id, timeout=10.0)
        results.append(result)
    except TimeoutError:
        results.append(None)  # Handle timeout as needed
```

## Best Practices

1. **Always use async/await**: The library is built for asynchronous operations
2. **Handle timeouts appropriately**: Set reasonable timeouts based on expected operation duration
3. **Use batch calls for multiple operations**: More efficient than sequential calls
4. **Implement proper error handling**: Both on server and client side
5. **Clean up connections**: Always call `disconnect()` when done
6. **Use descriptive method names**: Follow a naming convention like `service.action`
7. **Validate inputs on server side**: Don't trust client input blindly

## Limitations

- Requires NATS server to be running
- All parameters and return values must be JSON-serializable
- Maximum message size limited by NATS configuration (default 1MB)
- No built-in authentication (use NATS security features)

## Troubleshooting

### Connection Issues
```python
# Check if connected
if not rpc._connected:
    await rpc.connect()
```

### Timeout Errors
- Increase timeout value for long-running operations
- Check NATS server is running and accessible
- Verify method name is correct and server is listening

### Serialization Errors
- Ensure all parameters and return values are JSON-serializable
- Use basic types: dict, list, str, int, float, bool, None

## Contributing

Feel free to submit issues and enhancement requests!

## License

[Your License Here]

## See Also

- [NATS Documentation](https://docs.nats.io/)
- [nats-py Documentation](https://github.com/nats-io/nats.py)


