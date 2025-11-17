# NATS KeyStore and JobQueue

A Python implementation of a distributed key-value store and job queue system built on NATS JetStream.

## Features

### KeyStore
- **Async/Sync API**: Dual interface for both asynchronous and synchronous operations
- **Memory Storage**: Utilizes NATS JetStream's RAM-only storage for fast operations
- **Atomic Operations**: Support for atomic increment/decrement operations
- **Pattern Matching**: Key listing with glob pattern support
- **Session Management**: Context managers for connection lifecycle
- **Auto-reconnection**: Configurable retry logic for connection failures
- **JSON Support**: Automatic serialization/deserialization of complex data types

### JobQueue
- **Priority Processing**: Jobs processed in priority order (higher priority first)
- **Retry Mechanism**: Automatic retries with exponential backoff
- **Job Timeouts**: Configurable timeout per job with automatic failure handling
- **Multiple Queues**: Support for named queues with isolated processing
- **Worker Management**: Worker registration, heartbeats, and monitoring
- **Result Storage**: Persistent storage of job results and error messages
- **Stale Job Recovery**: Automatic recovery of jobs from failed workers

## Installation

### Prerequisites
```bash
# Install Python dependencies
pip install nats-py

# Start NATS server with JetStream
docker run -p 4222:4222 nats:latest -js
```

## Quick Start

### KeyStore Basic Usage

```python
from nats_key_store import KeyStore

# Create a KeyStore instance
store = KeyStore.create_keystore(
    server="nats://127.0.0.1:4222",
    bucket="myapp"
)

# Synchronous usage with session
with store.sync_session() as session:
    # Store and retrieve values
    session.put_sync("user:123", {"name": "Alice", "age": 30})
    user = session.get_sync("user:123")
    
    # Atomic operations
    count = session.increment_sync("counter")
    
    # Pattern matching
    user_keys = session.keys_sync("user:*")
```

### Asynchronous Usage

```python
import asyncio

async def async_example():
    async with store.session() as session:
        await session.put("config", {"timeout": 30})
        config = await session.get("config")
        
        # Concurrent increments are thread-safe
        await session.increment("visits")

asyncio.run(async_example())
```

### JobQueue Usage

```python
from nats_key_store import KeyStore, JobQueue

# Initialize KeyStore and JobQueue
keystore = KeyStore.create_keystore()
job_queue = JobQueue(keystore, worker_id="worker-1")

async def process_jobs():
    async with keystore.session():
        # Submit jobs
        job_id = await job_queue.submit(
            payload={"action": "send_email", "to": "user@example.com"},
            queue="emails",
            priority=10,
            max_retries=3
        )
        
        # Define job handler
        async def job_handler(job):
            print(f"Processing: {job.payload}")
            # Do work here
            return {"status": "sent"}
        
        # Process jobs (runs until stopped)
        await job_queue.process_jobs(
            handler=job_handler,
            queues=["emails"],
            batch_size=5
        )

asyncio.run(process_jobs())
```

## API Reference

### KeyStore Configuration

```python
@dataclass
class KeyStoreConfig:
    server: str = "nats://127.0.0.1:4222"
    bucket: str = "keystore"
    create_bucket: bool = True
    history: int = 1
    ttl_seconds: Optional[float] = None
    description: str = "NATS JetStream KeyStore"
    client_name: str = "keystore-client"
    max_reconnect_attempts: int = 3
    reconnect_delay: float = 1.0
```

### KeyStore Methods

| Method | Description | Returns |
|--------|-------------|---------|
| `put(key, value)` | Store a value | Revision number |
| `get(key, as_bytes=False)` | Retrieve a value | Value or None |
| `delete(key)` | Delete a key | None |
| `exists(key)` | Check if key exists | Boolean |
| `keys(pattern=None)` | List keys with optional pattern | List of keys |
| `increment(key, delta=1)` | Atomic increment | New value |
| `decrement(key, delta=1)` | Atomic decrement | New value |

### Job Structure

```python
@dataclass
class Job:
    id: str                    # Unique job identifier
    queue: str                 # Queue name
    payload: Dict[str, Any]    # Job data
    status: JobStatus          # PENDING, RUNNING, COMPLETED, FAILED, etc.
    priority: int              # Higher = processed first
    max_retries: int          # Maximum retry attempts
    retry_count: int          # Current retry count
    created_at: str           # ISO timestamp
    started_at: Optional[str] # When job started
    completed_at: Optional[str] # When job completed
    error: Optional[str]      # Error message if failed
    result: Optional[Any]     # Job result if successful
    worker_id: Optional[str]  # Worker processing the job
    timeout_seconds: int      # Job timeout in seconds
```

### JobQueue Methods

| Method | Description | Returns |
|--------|-------------|---------|
| `submit(payload, ...)` | Submit a new job | Job ID |
| `get_job(job_id)` | Get job details | Job object or None |
| `cancel_job(job_id)` | Cancel a pending job | Success boolean |
| `claim_job(queues)` | Claim next available job | Job object or None |
| `complete_job(job_id, result)` | Mark job as completed | Success boolean |
| `fail_job(job_id, error)` | Mark job as failed | Success boolean |
| `get_queue_stats(queue)` | Get queue statistics | Dict of counts |
| `get_active_workers()` | List active workers | List of worker info |
| `cleanup_stale_jobs(timeout)` | Recover stuck jobs | Number cleaned |

## Advanced Examples

### Bulk Operations with Session

```python
# Synchronous bulk operations
with store.sync_session() as session:
    # Perform multiple operations in one session
    for i in range(100):
        session.put_sync(f"item:{i}", {"value": i})
    
    # Efficient - reuses single connection
    all_items = session.keys_sync("item:*")
```

### Job Priority and Retries

```python
# Submit high-priority job with custom retry logic
job_id = await job_queue.submit(
    payload={"critical": True, "data": "important"},
    queue="critical",
    priority=100,      # Highest priority
    max_retries=5,     # More retries for critical jobs
    timeout_seconds=60 # Shorter timeout
)

# Monitor job status
job = await job_queue.get_job(job_id)
print(f"Status: {job.status}, Retries: {job.retry_count}")
```

### Worker Pool Management

```python
# Create multiple workers for parallel processing
async def create_worker_pool(num_workers=5):
    workers = []
    for i in range(num_workers):
        worker = JobQueue(keystore, worker_id=f"worker-{i}")
        task = asyncio.create_task(
            worker.process_jobs(
                handler=job_handler,
                queues=["default", "priority"],
                batch_size=3
            )
        )
        workers.append((worker, task))
    
    # Monitor active workers
    active = await workers[0][0].get_active_workers()
    print(f"Active workers: {len(active)}")
```

### Pattern-Based Key Management

```python
async with store.session() as session:
    # Store related data with patterns
    await session.put("cache:user:123", {"name": "Alice"})
    await session.put("cache:post:456", {"title": "Hello"})
    await session.put("cache:user:789", {"name": "Bob"})
    
    # Clean up by pattern
    cache_keys = await session.keys("cache:*")
    for key in cache_keys:
        await session.delete(key)
    
    # Or just user cache
    user_cache = await session.keys("cache:user:*")
```

## Performance Considerations

- **Connection Pooling**: Reuse sessions for multiple operations
- **Batch Processing**: Use `batch_size` parameter for concurrent job processing
- **Memory Storage**: Uses RAM for maximum speed (data not persistent across NATS restarts)
- **Atomic Operations**: Increment/decrement operations use optimistic locking with retries

## Testing

Run the included test suite:

```python
# Run all tests
python nats_key_store.py
# Choose option 3 for tests

# Or run specific demos
# Option 1: KeyStore demo
# Option 2: JobQueue demo
# Option 4: Run all
```

## Error Handling

The library includes robust error handling for:
- Connection failures with automatic retry
- Concurrent modifications with optimistic locking
- Job failures with configurable retry logic
- Worker failures with automatic job recovery

## License

[Your License Here]

## Contributing

[Contributing Guidelines]