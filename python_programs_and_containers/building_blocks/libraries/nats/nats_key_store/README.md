# NASKeyStore

A lightweight Python client for key-value storage using NATS (NATS.io) messaging system. This implementation provides simple store and retrieve operations with local caching, without the complexity of subscriptions or persistent connections.

## Features

- 🚀 **Simple API** - Easy-to-use store and retrieve operations
- 📦 **Multiple Storage Formats** - Support for JSON, Pickle, Text, and Binary data
- 💾 **Local Caching** - Built-in cache with configurable size limits
- ⏱️ **TTL Support** - Automatic key expiration
- 🏷️ **Metadata & Tags** - Rich metadata including versioning and custom tags
- 🔄 **Bulk Operations** - Store and retrieve multiple keys at once
- 🔌 **Connection-per-Operation** - No persistent connections or subscriptions
- ✅ **Data Integrity** - Automatic checksum generation and validation

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

```python
import asyncio
from nas_keystore import NASKeyStore, KeyFormat

async def main():
    # Create client
    client = NASKeyStore("localhost", 4222, "my_namespace")
    
    # Check connection
    if await client.ping():
        print("Connected to NATS!")
    
    # Store data
    await client.store("user:123", {"name": "Alice", "age": 30})
    
    # Retrieve data
    user = await client.retrieve("user:123")
    print(f"User: {user}")
    
    # Delete data
    await client.delete("user:123")

asyncio.run(main())
```

## API Reference

### Initialization

```python
client = NASKeyStore(
    host="localhost",      # NATS server host
    port=4222,            # NATS server port
    namespace="default",  # Namespace for key organization
    cache_size=100       # Maximum number of cached items
)
```

### Core Methods

#### `store(key, value, format=KeyFormat.JSON, ttl=None, tags=None)`
Store a key-value pair with optional TTL and tags.

```python
# Store JSON data
await client.store("config", {"debug": True})

# Store with TTL (expires in 60 seconds)
await client.store("session", token, ttl=60)

# Store with tags
await client.store("doc:1", content, tags=["important", "v2"])

# Store binary data
await client.store("image", image_bytes, format=KeyFormat.BINARY)
```

#### `retrieve(key, use_cache=True)`
Retrieve a value by key from the local cache.

```python
value = await client.retrieve("config")
if value is not None:
    print(f"Config: {value}")
```

#### `delete(key)`
Delete a key and publish delete notification.

```python
deleted = await client.delete("old_key")
print(f"Deleted: {deleted}")
```

#### `exists(key)`
Check if a key exists in the cache.

```python
if await client.exists("user:123"):
    print("User exists")
```

#### `update(key, value, format=None)`
Update an existing key's value, incrementing its version.

```python
await client.update("config", {"debug": False})
```

### Bulk Operations

```python
# Bulk store
items = {
    "key1": "value1",
    "key2": "value2",
    "key3": "value3"
}
results = await client.bulk_store(items)

# Bulk retrieve
keys = ["key1", "key2", "key3"]
values = await client.bulk_retrieve(keys)
```

### Metadata Operations

```python
# Get metadata for a key
meta = await client.get_metadata("user:123")
if meta:
    print(f"Version: {meta.version}")
    print(f"Size: {meta.size} bytes")
    print(f"Tags: {meta.tags}")
    print(f"Created: {meta.created_at}")
```

### Cache Management

```python
# Get cache statistics
stats = client.get_cache_stats()
print(f"Cache size: {stats['size']}/{stats['max_size']}")
print(f"Memory usage: {stats['memory_usage']} bytes")

# Clear cache
client.clear_cache()

# List cached keys
keys = await client.list_keys("user:*")  # Supports wildcards
```

### Connection Testing

```python
# Test connection to NATS
is_alive = await client.ping()
if not is_alive:
    print("Cannot connect to NATS server")
```

## Storage Formats

The NASKeyStore supports multiple data formats:

| Format | Description | Use Case |
|--------|-------------|----------|
| `KeyFormat.JSON` | JSON serialization (default) | Structured data, APIs |
| `KeyFormat.PICKLE` | Python pickle serialization | Complex Python objects |
| `KeyFormat.TEXT` | Plain text | Configuration files, logs |
| `KeyFormat.BINARY` | Binary data with base64 encoding | Images, files, raw bytes |

### Format Examples

```python
# JSON (default)
await client.store("api_response", {"status": "ok", "data": [1, 2, 3]})

# Pickle - for complex Python objects
class CustomObject:
    def __init__(self, name):
        self.name = name
        self.created = datetime.now()

obj = CustomObject("example")
await client.store("complex_obj", obj, format=KeyFormat.PICKLE)

# Text
await client.store("config.ini", "[database]\nhost=localhost", format=KeyFormat.TEXT)

# Binary
with open("image.png", "rb") as f:
    await client.store("image", f.read(), format=KeyFormat.BINARY)
```

## TTL (Time-To-Live)

Keys can have an optional TTL in seconds. Expired keys are automatically removed from cache on access.

```python
# Store with 5 minute TTL
await client.store("session:abc", session_data, ttl=300)

# Check if key exists (returns False if expired)
if await client.exists("session:abc"):
    data = await client.retrieve("session:abc")
```

## Pattern Matching

List keys using wildcard patterns:

```python
# Get all user keys
user_keys = await client.list_keys("user:*")

# Get all cache keys
cache_keys = await client.list_keys("cache:*")

# Get all keys
all_keys = await client.list_keys("*")
```

## Architecture

### Design Principles

1. **No Persistent Connections**: Each operation creates its own connection to NATS
2. **No Subscriptions**: Simplified architecture without subscription management
3. **Local Cache Only**: Retrieve operations work only with local cache
4. **Fire-and-Forget Publishing**: Store operations publish to NATS without waiting for acknowledgment
5. **Automatic Cache Management**: LRU-style cache with configurable size limits

### Data Flow

```
Store Operation:
Client -> Create Connection -> Publish to NATS -> Close Connection -> Update Cache

Retrieve Operation:
Client -> Check Cache -> Return Value (or None)

Delete Operation:
Client -> Remove from Cache -> Publish Delete Message -> Close Connection
```

## Performance Considerations

- **Connection Overhead**: Each operation creates a new connection. For high-frequency operations, consider batching or using the original implementation with persistent connections.
- **Cache-Only Retrieval**: Retrieve operations don't fetch from NATS, only from local cache. This provides fast reads but no distributed synchronization.
- **Local Storage**: All retrieved data must fit in memory based on `cache_size` setting.

## Example Use Cases

### Session Storage
```python
async def store_session(session_id, user_data):
    client = NASKeyStore("localhost", 4222, "sessions")
    await client.store(
        f"session:{session_id}", 
        user_data, 
        ttl=3600,  # 1 hour expiry
        tags=["web", "authenticated"]
    )
```

### Configuration Management
```python
async def load_config():
    client = NASKeyStore("localhost", 4222, "config")
    
    # Store configuration
    config = {
        "database": {"host": "localhost", "port": 5432},
        "cache": {"ttl": 300, "size": 1000}
    }
    await client.store("app:config", config, tags=["production"])
    
    # Retrieve configuration
    return await client.retrieve("app:config")
```

### Caching Layer
```python
async def cached_api_call(endpoint):
    client = NASKeyStore("localhost", 4222, "api_cache")
    cache_key = f"api:{endpoint}"
    
    # Check cache
    result = await client.retrieve(cache_key)
    if result:
        return result
    
    # Make API call
    result = await make_api_request(endpoint)
    
    # Cache result
    await client.store(cache_key, result, ttl=300)
    return result
```

## Error Handling

```python
async def safe_store(client, key, value):
    try:
        success = await client.store(key, value)
        if success:
            print(f"Stored {key}")
        else:
            print(f"Failed to store {key}")
    except ConnectionError as e:
        print(f"Connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
```

## Testing

Run the built-in tests:

```python
python nas_keystore.py
```

This will run tests for:
- Basic store/retrieve operations
- TTL functionality
- Bulk operations
- Different storage formats
- Cache management

## Limitations

1. **No Distributed Retrieval**: The retrieve operation only checks local cache, not other NATS nodes
2. **No Subscription Support**: Cannot listen for key changes or updates from other clients
3. **Connection Per Operation**: May have performance impact for high-frequency operations
4. **Cache-Only Queries**: List and search operations only work on locally cached data

## Migration from Original Implementation

If migrating from the original `NASKeyStoreRetrieve` class:

| Original | NASKeyStore | Notes |
|----------|-------------|-------|
| `connect()` | Not needed | Connections are automatic |
| `disconnect()` | Not needed | No persistent connections |
| `_request()` | Removed | No request/response pattern |
| Distributed retrieve | Local only | Only retrieves from cache |
| Subscriptions | Removed | No subscription support |

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is provided as-is for educational and development purposes.

## Acknowledgments

Built on top of [NATS.io](https://nats.io/), a high-performance messaging system for cloud native applications.