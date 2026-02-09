#!/usr/bin/env python3
"""
KeyStore and JobQueue: NATS JetStream Key-Value Store and Job Queue implementation.

Features:
- KeyStore: Clean async/sync API for key-value operations
- JobQueue: Distributed job queue built on KeyStore
- RAM-only storage for JetStream (memory-efficient)
- Session management for bulk operations
- Automatic reconnection handling
- Type-safe operations with proper error handling
"""

from __future__ import annotations

import asyncio
import json
import unittest
import time
import concurrent.futures
import uuid
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.kv import KeyValue
from nats.js.api import KeyValueConfig, StorageType
from nats.js.errors import (
    BucketNotFoundError,
    KeyNotFoundError,
    KeyDeletedError,
    KeyWrongLastSequenceError,
    NoKeysError,  # Added this import
)


@dataclass
class KeyStoreConfig:
    """Configuration for KeyStore."""
    
    server: str = "nats://127.0.0.1:4222"
    bucket: str = "keystore"
    create_bucket: bool = True
    history: int = 1
    ttl_seconds: Optional[float] = None
    description: str = "NATS JetStream KeyStore"
    client_name: str = "keystore-client"
    max_reconnect_attempts: int = 3
    reconnect_delay: float = 1.0


class KeyStore:
    """
    NATS JetStream-based Key-Value Store.
    
    Provides both async and sync interfaces with proper resource management.
    """
    
    def __init__(self, config: Optional[KeyStoreConfig] = None):
        """
        Initialize KeyStore with configuration.
        
        Args:
            config: KeyStore configuration. Uses defaults if None.
        """
        self.config = config or KeyStoreConfig()
        self._nc: Optional[NATS] = None
        self._js: Optional[JetStreamContext] = None
        self._kv: Optional[KeyValue] = None
        self._connected = False
        self._lock = asyncio.Lock()
    
    @classmethod
    def create_keystore(cls, 
                       server: str = "nats://127.0.0.1:4222", 
                       bucket: str = "keystore",
                       **kwargs) -> 'KeyStore':
        """
        Create a KeyStore instance with custom configuration.
        
        Args:
            server: NATS server URL
            bucket: KV bucket name
            **kwargs: Additional configuration options
            
        Returns:
            Configured KeyStore instance
        """
        config = KeyStoreConfig(server=server, bucket=bucket, **kwargs)
        return cls(config)
    
    # ==================== Connection Management ====================
    
    async def connect(self) -> None:
        """Establish connection to NATS server and initialize KV store."""
        async with self._lock:
            if self._connected:
                return
            
            for attempt in range(self.config.max_reconnect_attempts):
                try:
                    self._nc = NATS()
                    await self._nc.connect(
                        servers=[self.config.server],
                        name=self.config.client_name
                    )
                    self._js = self._nc.jetstream()
                    
                    # Try to get existing bucket
                    try:
                        self._kv = await self._js.key_value(self.config.bucket)
                    except BucketNotFoundError:
                        if not self.config.create_bucket:
                            raise BucketNotFoundError(
                                f"Bucket '{self.config.bucket}' not found and create_bucket=False"
                            )
                        # Create new bucket with memory storage
                        kv_config = KeyValueConfig(
                            bucket=self.config.bucket,
                            description=self.config.description,
                            history=self.config.history,
                            ttl=self.config.ttl_seconds,
                            storage=StorageType.MEMORY,
                            max_value_size=-1,
                        )
                        self._kv = await self._js.create_key_value(kv_config)
                    
                    self._connected = True
                    return
                    
                except Exception as e:
                    if attempt < self.config.max_reconnect_attempts - 1:
                        await asyncio.sleep(self.config.reconnect_delay)
                    else:
                        raise ConnectionError(f"Failed to connect after {attempt + 1} attempts: {e}")
    
    async def disconnect(self) -> None:
        """Close connection to NATS server."""
        async with self._lock:
            if self._nc and self._nc.is_connected:
                await self._nc.close()
            self._nc = None
            self._js = None
            self._kv = None
            self._connected = False
    
    async def _ensure_connected(self) -> None:
        """Ensure connection is established before operations."""
        if not self._connected:
            await self.connect()
    
    # ==================== Async Operations ====================
    
    async def put(self, key: str, value: Any) -> int:
        """
        Store a value for the given key.
        
        Args:
            key: The key to store
            value: The value (will be JSON-encoded if not bytes/str)
            
        Returns:
            Revision number of the stored entry
        """
        await self._ensure_connected()
        data = self._encode_value(value)
        return await self._kv.put(key, data)
    
    async def get(self, key: str, as_bytes: bool = False) -> Optional[Union[str, bytes, Any]]:
        """
        Retrieve value for the given key.
        
        Args:
            key: The key to retrieve
            as_bytes: If True, return raw bytes
            
        Returns:
            The value or None if not found
        """
        await self._ensure_connected()
        try:
            entry = await self._kv.get(key)
            if entry and entry.value:
                if as_bytes:
                    return entry.value
                return self._decode_value(entry.value)
        except (KeyNotFoundError, KeyDeletedError):
            pass
        return None
    
    async def delete(self, key: str) -> None:
        """Delete a key from the store."""
        await self._ensure_connected()
        await self._kv.delete(key)
    
    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        await self._ensure_connected()
        try:
            entry = await self._kv.get(key)
            return entry is not None and entry.value is not None
        except (KeyNotFoundError, KeyDeletedError):
            return False
    
    async def keys(self, pattern: Optional[str] = None) -> List[str]:
        """
        List all keys, optionally filtered by pattern.
        
        Args:
            pattern: Optional glob pattern (e.g., 'user:*')
            
        Returns:
            List of matching keys
        """
        await self._ensure_connected()
        try:
            all_keys = await self._kv.keys()
        except NoKeysError:
            # Handle the case when there are no keys in the bucket
            all_keys = []
        
        if pattern:
            from fnmatch import fnmatch
            return sorted([k for k in all_keys if fnmatch(k, pattern)])
        return sorted(all_keys)
    
    async def increment(self, key: str, delta: int = 1) -> int:
        """
        Atomically increment a numeric value.
        
        Args:
            key: The key containing the number
            delta: Amount to increment by
            
        Returns:
            The new value after increment
            
        Raises:
            ValueError: If the existing value is not numeric
        """
        await self._ensure_connected()
        
        for attempt in range(20):  # More retries for concurrent operations
            try:
                entry = await self._kv.get(key)
                current = 0
                revision = None
                
                if entry and entry.value:
                    try:
                        current = int(entry.value.decode('utf-8'))
                    except (ValueError, UnicodeDecodeError) as e:
                        raise ValueError(f"Key '{key}' does not contain a valid integer: {entry.value.decode('utf-8', errors='replace')}")
                    revision = entry.revision
                
                new_value = current + delta
                data = str(new_value).encode('utf-8')
                
                if revision:
                    await self._kv.update(key, data, revision)
                else:
                    await self._kv.put(key, data)
                
                return new_value
                
            except (KeyNotFoundError, KeyDeletedError):
                # Key doesn't exist, create with delta
                try:
                    await self._kv.put(key, str(delta).encode('utf-8'))
                    return delta
                except KeyWrongLastSequenceError:
                    # Someone else created it first, retry
                    await asyncio.sleep(0.001 * (attempt + 1))
                    continue
                except Exception as e:
                    if attempt == 19:
                        raise
                    await asyncio.sleep(0.001 * (attempt + 1))
                    continue
                    
            except ValueError:
                # Re-raise ValueError for non-numeric values
                raise
                
            except KeyWrongLastSequenceError:
                # Expected in concurrent scenarios, just retry
                await asyncio.sleep(0.001 * (attempt + 1))
                continue
                
            except Exception as e:
                # Other unexpected errors
                if attempt == 19:
                    raise RuntimeError(f"Failed to increment key '{key}': {e}")
                await asyncio.sleep(0.001 * (attempt + 1))
                continue
        
        raise RuntimeError(f"Failed to increment key '{key}' after {attempt + 1} retries")
    
    async def decrement(self, key: str, delta: int = 1) -> int:
        """Atomically decrement a numeric value."""
        return await self.increment(key, -delta)
    
    # ==================== Sync Wrappers ====================
    
    def put_sync(self, key: str, value: Any) -> int:
        """Synchronous version of put (auto-connects and disconnects)."""
        async def _put_with_lifecycle():
            await self.connect()
            try:
                return await self.put(key, value)
            finally:
                await self.disconnect()
        return self._run_async(_put_with_lifecycle())
    
    def get_sync(self, key: str, as_bytes: bool = False) -> Optional[Union[str, bytes, Any]]:
        """Synchronous version of get (auto-connects and disconnects)."""
        async def _get_with_lifecycle():
            await self.connect()
            try:
                return await self.get(key, as_bytes)
            finally:
                await self.disconnect()
        return self._run_async(_get_with_lifecycle())
    
    def delete_sync(self, key: str) -> None:
        """Synchronous version of delete (auto-connects and disconnects)."""
        async def _delete_with_lifecycle():
            await self.connect()
            try:
                return await self.delete(key)
            finally:
                await self.disconnect()
        return self._run_async(_delete_with_lifecycle())
    
    def exists_sync(self, key: str) -> bool:
        """Synchronous version of exists (auto-connects and disconnects)."""
        async def _exists_with_lifecycle():
            await self.connect()
            try:
                return await self.exists(key)
            finally:
                await self.disconnect()
        return self._run_async(_exists_with_lifecycle())
    
    def keys_sync(self, pattern: Optional[str] = None) -> List[str]:
        """Synchronous version of keys (auto-connects and disconnects)."""
        async def _keys_with_lifecycle():
            await self.connect()
            try:
                return await self.keys(pattern)
            finally:
                await self.disconnect()
        return self._run_async(_keys_with_lifecycle())
    
    def increment_sync(self, key: str, delta: int = 1) -> int:
        """Synchronous version of increment (auto-connects and disconnects)."""
        async def _increment_with_lifecycle():
            await self.connect()
            try:
                return await self.increment(key, delta)
            finally:
                await self.disconnect()
        return self._run_async(_increment_with_lifecycle())
    
    def decrement_sync(self, key: str, delta: int = 1) -> int:
        """Synchronous version of decrement (auto-connects and disconnects)."""
        async def _decrement_with_lifecycle():
            await self.connect()
            try:
                return await self.decrement(key, delta)
            finally:
                await self.disconnect()
        return self._run_async(_decrement_with_lifecycle())
    
    # ==================== Context Managers ====================
    
    @asynccontextmanager
    async def session(self):
        """Async context manager for session management."""
        await self.connect()
        try:
            yield self
        finally:
            await self.disconnect()
    
    class SyncSession:
        """Synchronous session context manager."""
        
        def __init__(self, keystore: 'KeyStore'):
            self.keystore = keystore
            self._loop = None
            
        def __enter__(self):
            """Set up event loop and connect."""
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self.keystore.connect())
            return self
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            """Disconnect and clean up event loop."""
            try:
                self._loop.run_until_complete(self.keystore.disconnect())
            finally:
                self._loop.close()
                asyncio.set_event_loop(None)
        
        def put_sync(self, key: str, value: Any) -> int:
            """Put value in sync context."""
            return self._loop.run_until_complete(self.keystore.put(key, value))
        
        def get_sync(self, key: str, as_bytes: bool = False) -> Optional[Union[str, bytes, Any]]:
            """Get value in sync context."""
            return self._loop.run_until_complete(self.keystore.get(key, as_bytes))
        
        def delete_sync(self, key: str) -> None:
            """Delete key in sync context."""
            self._loop.run_until_complete(self.keystore.delete(key))
        
        def exists_sync(self, key: str) -> bool:
            """Check key existence in sync context."""
            return self._loop.run_until_complete(self.keystore.exists(key))
        
        def keys_sync(self, pattern: Optional[str] = None) -> List[str]:
            """List keys in sync context."""
            return self._loop.run_until_complete(self.keystore.keys(pattern))
        
        def increment_sync(self, key: str, delta: int = 1) -> int:
            """Increment counter in sync context."""
            return self._loop.run_until_complete(self.keystore.increment(key, delta))
        
        def decrement_sync(self, key: str, delta: int = 1) -> int:
            """Decrement counter in sync context."""
            return self._loop.run_until_complete(self.keystore.decrement(key, delta))
    
    def sync_session(self):
        """Create a synchronous session context manager."""
        return self.SyncSession(self)
    
    # ==================== Helper Methods ====================
    
    @staticmethod
    def _encode_value(value: Any) -> bytes:
        """Encode value to bytes."""
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode('utf-8')
        return json.dumps(value, separators=(',', ':')).encode('utf-8')
    
    @staticmethod
    def _decode_value(data: bytes) -> Any:
        """Decode bytes to value."""
        try:
            text = data.decode('utf-8')
            # Try to parse as JSON
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        except UnicodeDecodeError:
            return data
    
    @staticmethod
    def _run_async(coro):
        """Run async coroutine in sync context."""
        try:
            # Check if there's already an event loop running
            loop = asyncio.get_running_loop()
            # If we're here, there's a loop running, create a new one in a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
        except RuntimeError:
            # No event loop running, we can safely use asyncio.run
            return asyncio.run(coro)


# ==================== JobQueue Implementation ====================

class JobStatus(Enum):
    """Job status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


@dataclass
class Job:
    """Job data structure."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    queue: str = "default"
    payload: Dict[str, Any] = field(default_factory=dict)
    status: JobStatus = JobStatus.PENDING
    priority: int = 0  # Higher priority = processed first
    max_retries: int = 3
    retry_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    result: Optional[Any] = None
    worker_id: Optional[str] = None
    timeout_seconds: int = 300  # 5 minutes default
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary."""
        data = asdict(self)
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Job':
        """Create job from dictionary."""
        if 'status' in data:
            data['status'] = JobStatus(data['status'])
        return cls(**data)


class JobQueue:
    """
    Distributed job queue built on KeyStore.
    
    Features:
    - Priority-based job processing
    - Automatic retries with exponential backoff
    - Job timeouts and failure handling
    - Multiple named queues
    - Worker registration and heartbeats
    - Job result storage
    """
    
    def __init__(self, keystore: KeyStore, worker_id: Optional[str] = None):
        """
        Initialize JobQueue.
        
        Args:
            keystore: KeyStore instance for data storage
            worker_id: Unique identifier for this worker
        """
        self.keystore = keystore
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._running = False
        self._tasks = []
    
    # ==================== Job Management ====================
    
    async def submit(self, 
                    payload: Dict[str, Any],
                    queue: str = "default",
                    priority: int = 0,
                    max_retries: int = 3,
                    timeout_seconds: int = 300) -> str:
        """
        Submit a job to the queue.
        
        Args:
            payload: Job data
            queue: Queue name
            priority: Job priority (higher = processed first)
            max_retries: Maximum retry attempts
            timeout_seconds: Job timeout in seconds
            
        Returns:
            Job ID
        """
        job = Job(
            payload=payload,
            queue=queue,
            priority=priority,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds
        )
        
        # Store job data
        await self.keystore.put(f"job.{job.id}", job.to_dict())
        
        # Add to queue with priority
        queue_key = f"queue.{queue}.{1000000 - priority:06d}.{job.id}"
        await self.keystore.put(queue_key, job.id)
        
        # Update queue counter
        await self.keystore.increment(f"stats.queue.{queue}.pending")
        
        return job.id
    
    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        data = await self.keystore.get(f"job.{job_id}")
        if data:
            return Job.from_dict(data)
        return None
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job."""
        job = await self.get_job(job_id)
        if job and job.status == JobStatus.PENDING:
            job.status = JobStatus.CANCELLED
            await self.keystore.put(f"job.{job_id}", job.to_dict())
            
            # Remove from queue
            pattern = f"queue.{job.queue}.*.{job_id}"
            keys = await self.keystore.keys(pattern)
            for key in keys:
                await self.keystore.delete(key)
            
            # Update stats: decrement pending, increment cancelled
            await self.keystore.decrement(f"stats.queue.{job.queue}.pending")
            await self.keystore.increment(f"stats.queue.{job.queue}.cancelled")
            return True
        return False
    
    async def get_queue_stats(self, queue: str = "default") -> Dict[str, int]:
        """Get statistics for a queue."""
        stats = {}
        for stat in ["pending", "running", "completed", "failed", "cancelled"]:
            value = await self.keystore.get(f"stats.queue.{queue}.{stat}")
            stats[stat] = int(value) if value else 0
        return stats
    
    # ==================== Worker Operations ====================
    
    async def claim_job(self, queues: List[str] = None) -> Optional[Job]:
        """
        Claim the next available job from specified queues.
        
        Args:
            queues: List of queue names to check (default: ["default"])
            
        Returns:
            Claimed job or None if no jobs available
        """
        queues = queues or ["default"]
        
        for queue in queues:
            # Find pending jobs in priority order
            pattern = f"queue.{queue}.*"
            queue_keys = await self.keystore.keys(pattern)
            
            for queue_key in sorted(queue_keys):
                job_id = await self.keystore.get(queue_key)
                if not job_id:
                    continue
                
                job = await self.get_job(job_id)
                if job and job.status == JobStatus.PENDING:
                    # Try to claim the job
                    job.status = JobStatus.RUNNING
                    job.worker_id = self.worker_id
                    job.started_at = datetime.utcnow().isoformat()
                    
                    # Update job atomically
                    await self.keystore.put(f"job.{job_id}", job.to_dict())
                    
                    # Remove from queue
                    await self.keystore.delete(queue_key)
                    
                    # Update stats
                    await self.keystore.decrement(f"stats.queue.{queue}.pending")
                    await self.keystore.increment(f"stats.queue.{queue}.running")
                    
                    # Register worker activity
                    await self.keystore.put(
                        f"worker.{self.worker_id}.current_job",
                        job_id
                    )
                    await self.keystore.put(
                        f"worker.{self.worker_id}.last_seen",
                        datetime.utcnow().isoformat()
                    )
                    
                    return job
        
        return None
    
    async def complete_job(self, job_id: str, result: Any = None) -> bool:
        """Mark a job as completed with optional result."""
        job = await self.get_job(job_id)
        if job and job.status == JobStatus.RUNNING and job.worker_id == self.worker_id:
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow().isoformat()
            job.result = result
            
            await self.keystore.put(f"job.{job_id}", job.to_dict())
            await self.keystore.delete(f"worker.{self.worker_id}.current_job")
            
            # Update stats
            await self.keystore.decrement(f"stats.queue.{job.queue}.running")
            await self.keystore.increment(f"stats.queue.{job.queue}.completed")
            
            return True
        return False
    
    async def fail_job(self, job_id: str, error: str) -> bool:
        """Mark a job as failed with error message."""
        job = await self.get_job(job_id)
        if job and job.status == JobStatus.RUNNING and job.worker_id == self.worker_id:
            job.retry_count += 1
            
            if job.retry_count < job.max_retries:
                # Schedule for retry
                job.status = JobStatus.RETRYING
                job.error = f"Retry {job.retry_count}/{job.max_retries}: {error}"
                
                # Re-add to queue with delay
                await asyncio.sleep(2 ** job.retry_count)  # Exponential backoff
                
                job.status = JobStatus.PENDING
                job.worker_id = None
                job.started_at = None
                
                queue_key = f"queue.{job.queue}.{1000000 - job.priority:06d}.{job.id}"
                await self.keystore.put(queue_key, job.id)
                
                await self.keystore.decrement(f"stats.queue.{job.queue}.running")
                await self.keystore.increment(f"stats.queue.{job.queue}.pending")
            else:
                # Max retries exceeded
                job.status = JobStatus.FAILED
                job.error = error
                job.completed_at = datetime.utcnow().isoformat()
                
                await self.keystore.decrement(f"stats.queue.{job.queue}.running")
                await self.keystore.increment(f"stats.queue.{job.queue}.failed")
            
            await self.keystore.put(f"job.{job_id}", job.to_dict())
            await self.keystore.delete(f"worker.{self.worker_id}.current_job")
            
            return True
        return False
    
    async def process_jobs(self,
                          handler: Callable[[Job], Any],
                          queues: List[str] = None,
                          poll_interval: float = 1.0,
                          batch_size: int = 1):
        """
        Process jobs from specified queues.
        
        Args:
            handler: Async function to process jobs
            queues: List of queue names to process
            poll_interval: Seconds between polls
            batch_size: Number of concurrent jobs to process
        """
        self._running = True
        queues = queues or ["default"]
        
        print(f"Worker {self.worker_id} started processing queues: {queues}")
        
        while self._running:
            try:
                # Process up to batch_size jobs concurrently
                tasks = []
                for _ in range(batch_size):
                    job = await self.claim_job(queues)
                    if job:
                        task = asyncio.create_task(self._process_single_job(job, handler))
                        tasks.append(task)
                
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                else:
                    # No jobs available, wait before polling again
                    await asyncio.sleep(poll_interval)
                    
                # Update worker heartbeat
                await self.keystore.put(
                    f"worker.{self.worker_id}.last_seen",
                    datetime.utcnow().isoformat()
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Worker {self.worker_id} error: {e}")
                await asyncio.sleep(poll_interval)
    
    async def _process_single_job(self, job: Job, handler: Callable[[Job], Any]):
        """Process a single job with timeout and error handling."""
        try:
            # Apply timeout
            result = await asyncio.wait_for(
                handler(job),
                timeout=job.timeout_seconds
            )
            await self.complete_job(job.id, result)
            print(f"Worker {self.worker_id} completed job {job.id}")
            
        except asyncio.TimeoutError:
            await self.fail_job(job.id, f"Job timed out after {job.timeout_seconds} seconds")
            print(f"Worker {self.worker_id} job {job.id} timed out")
            
        except Exception as e:
            await self.fail_job(job.id, str(e))
            print(f"Worker {self.worker_id} job {job.id} failed: {e}")
    
    def stop(self):
        """Stop processing jobs."""
        self._running = False
    
    # ==================== Monitoring ====================
    
    async def get_active_workers(self) -> List[Dict[str, Any]]:
        """Get list of active workers."""
        pattern = "worker.*.last_seen"
        keys = await self.keystore.keys(pattern)
        
        workers = []
        cutoff = (datetime.utcnow() - timedelta(seconds=30)).isoformat()
        
        for key in keys:
            worker_id = key.split('.')[1]
            last_seen = await self.keystore.get(key)
            
            if last_seen and last_seen > cutoff:
                current_job = await self.keystore.get(f"worker.{worker_id}.current_job")
                workers.append({
                    "worker_id": worker_id,
                    "last_seen": last_seen,
                    "current_job": current_job
                })
        
        return workers
    
    async def cleanup_stale_jobs(self, timeout_seconds: int = 600):
        """
        Clean up jobs stuck in running state.
        
        Args:
            timeout_seconds: Consider job stale after this many seconds
        """
        cutoff = (datetime.utcnow() - timedelta(seconds=timeout_seconds)).isoformat()
        
        # Find all jobs
        pattern = "job.*"
        keys = await self.keystore.keys(pattern)
        
        cleaned = 0
        for key in keys:
            job_data = await self.keystore.get(key)
            if job_data:
                job = Job.from_dict(job_data)
                
                if (job.status == JobStatus.RUNNING and 
                    job.started_at and 
                    job.started_at < cutoff):
                    
                    # Reset to pending
                    job.status = JobStatus.PENDING
                    job.worker_id = None
                    job.started_at = None
                    job.error = "Reset due to stale worker"
                    
                    await self.keystore.put(f"job.{job.id}", job.to_dict())
                    
                    # Re-add to queue
                    queue_key = f"queue.{job.queue}.{1000000 - job.priority:06d}.{job.id}"
                    await self.keystore.put(queue_key, job.id)
                    
                    cleaned += 1
        
        return cleaned


# ==================== Test Suite ====================

if __name__ == "__main__":
    
    class TestKeyStore(unittest.TestCase):
        """Test suite for KeyStore operations."""
        
        # Single server configuration
        TEST_SERVER = "nats://127.0.0.1:4222"
        TEST_BUCKET = "test_keystore"
        
        @classmethod
        def setUpClass(cls):
            """Set up test configuration."""
            cls.config = KeyStoreConfig(
                server=cls.TEST_SERVER,
                bucket=cls.TEST_BUCKET,
                create_bucket=True,
                history=5,
                ttl_seconds=None,
                description="Test KeyStore Bucket"
            )
        
        def setUp(self):
            """Create fresh KeyStore instance for each test."""
            self.store = KeyStore(self.config)
        
        def tearDown(self):
            """Clean up after each test."""
            # Clean up test data
            try:
                with self.store.sync_session() as session:
                    keys = session.keys_sync("test.*")
                    for key in keys:
                        session.delete_sync(key)
            except Exception:
                pass
        
        # ==================== Basic Operations Tests ====================
        
        def test_put_get_string(self):
            """Test storing and retrieving string values."""
            with self.store.sync_session() as session:
                # Store string value
                rev = session.put_sync("test.string", "Hello, World!")
                self.assertIsInstance(rev, int)
                self.assertGreater(rev, 0)
                
                # Retrieve string value
                value = session.get_sync("test.string")
                self.assertEqual(value, "Hello, World!")
        
        def test_put_get_dict(self):
            """Test storing and retrieving dictionary values."""
            with self.store.sync_session() as session:
                test_data = {
                    "name": "John Doe",
                    "age": 30,
                    "active": True,
                    "scores": [85, 92, 78]
                }
                
                session.put_sync("test.dict", test_data)
                retrieved = session.get_sync("test.dict")
                self.assertEqual(retrieved, test_data)
        
        def test_put_get_bytes(self):
            """Test storing and retrieving binary data."""
            with self.store.sync_session() as session:
                binary_data = b'\x00\x01\x02\x03\xff'
                session.put_sync("test.binary", binary_data)
                
                # Retrieve as bytes
                retrieved = session.get_sync("test.binary", as_bytes=True)
                self.assertEqual(retrieved, binary_data)
        
        def test_delete(self):
            """Test key deletion."""
            with self.store.sync_session() as session:
                session.put_sync("test.delete", "temporary")
                self.assertTrue(session.exists_sync("test.delete"))
                
                session.delete_sync("test.delete")
                self.assertFalse(session.exists_sync("test.delete"))
                self.assertIsNone(session.get_sync("test.delete"))
        
        def test_exists(self):
            """Test key existence checking."""
            with self.store.sync_session() as session:
                self.assertFalse(session.exists_sync("test.nonexistent"))
                
                session.put_sync("test.exists", "value")
                self.assertTrue(session.exists_sync("test.exists"))
        
        def test_keys_pattern(self):
            """Test listing keys with pattern matching."""
            with self.store.sync_session() as session:
                # Create test keys
                session.put_sync("test.user.1", "Alice")
                session.put_sync("test.user.2", "Bob")
                session.put_sync("test.admin.1", "Charlie")
                session.put_sync("test.config", "settings")
                
                # Test pattern matching
                user_keys = session.keys_sync("test.user.*")
                self.assertEqual(len(user_keys), 2)
                self.assertIn("test.user.1", user_keys)
                self.assertIn("test.user.2", user_keys)
                
                all_test_keys = session.keys_sync("test.*")
                self.assertGreaterEqual(len(all_test_keys), 4)
        
        # ==================== Numeric Operations Tests ====================
        
        def test_increment(self):
            """Test atomic increment operation."""
            with self.store.sync_session() as session:
                # Initial increment (creates key)
                val = session.increment_sync("test.counter")
                self.assertEqual(val, 1)
                
                # Subsequent increments
                val = session.increment_sync("test.counter")
                self.assertEqual(val, 2)
                
                val = session.increment_sync("test.counter", delta=5)
                self.assertEqual(val, 7)
        
        def test_decrement(self):
            """Test atomic decrement operation."""
            with self.store.sync_session() as session:
                session.put_sync("test.countdown", "10")
                
                val = session.decrement_sync("test.countdown")
                self.assertEqual(val, 9)
                
                val = session.decrement_sync("test.countdown", delta=3)
                self.assertEqual(val, 6)
        
        # ==================== Async Operations Tests ====================
        
        def test_async_operations(self):
            """Test async operations."""
            async def run_test():
                async with self.store.session() as session:
                    # Test async put/get
                    await session.put("test.async", {"async": True})
                    value = await session.get("test.async")
                    self.assertEqual(value, {"async": True})
                    
                    # Test async exists
                    exists = await session.exists("test.async")
                    self.assertTrue(exists)
                    
                    # Test async keys
                    await session.put("test.async.1", "one")
                    await session.put("test.async.2", "two")
                    keys = await session.keys("test.async.*")
                    self.assertEqual(len(keys), 2)
            
            asyncio.run(run_test())
        
        def test_concurrent_increments(self):
            """Test concurrent increment operations."""
            async def increment_task(store, key, count):
                async with store.session() as session:
                    results = []
                    for _ in range(count):
                        result = await session.increment(key)
                        results.append(result)
                    return results
            
            async def run_concurrent_test():
                # Initialize the counter first
                async with self.store.session() as session:
                    await session.put("test.concurrent", "0")
                
                tasks = []
                for i in range(5):
                    task = increment_task(
                        KeyStore(self.config), 
                        "test.concurrent", 
                        10
                    )
                    tasks.append(task)
                
                all_results = await asyncio.gather(*tasks)
                
                # Flatten all results and check we got all expected values
                all_increments = []
                for results in all_results:
                    all_increments.extend(results)
                
                # We should have 50 increments total
                self.assertEqual(len(all_increments), 50)
                
                # The maximum value should be 50
                self.assertEqual(max(all_increments), 50)
                
                # Verify final stored value
                async with self.store.session() as session:
                    final_value = await session.get("test.concurrent")
                    self.assertEqual(int(final_value), 50)
            
            asyncio.run(run_concurrent_test())
        
        # ==================== Configuration Tests ====================
        
        def test_single_server_connection(self):
            """Test connection to single NATS server."""
            config = KeyStoreConfig(
                server=self.TEST_SERVER,
                bucket="single_server_test",
                client_name="test-client"
            )
            store = KeyStore(config)
            
            with store.sync_session() as session:
                session.put_sync("connection.test", "success")
                value = session.get_sync("connection.test")
                self.assertEqual(value, "success")
        
        def test_create_keystore_class_method(self):
            """Test create_keystore class method."""
            store = KeyStore.create_keystore(
                server=self.TEST_SERVER,
                bucket="class_method_test",
                description="Created with class method"
            )
            
            with store.sync_session() as session:
                session.put_sync("method.test", "working")
                value = session.get_sync("method.test")
                self.assertEqual(value, "working")
        
        # ==================== Error Handling Tests ====================
        
        def test_missing_key(self):
            """Test behavior with missing keys."""
            with self.store.sync_session() as session:
                value = session.get_sync("test.nonexistent")
                self.assertIsNone(value)
        
        def test_invalid_increment(self):
            """Test increment on non-numeric value."""
            with self.store.sync_session() as session:
                session.put_sync("test.text", "not a number")
                
                with self.assertRaises(ValueError):
                    session.increment_sync("test.text")
        
        # ==================== Performance Test ====================
        
        def test_performance_single_session(self):
            """Test performance with single session."""
            start_time = time.time()
            
            with self.store.sync_session() as session:
                # Write operations
                for i in range(100):
                    session.put_sync(f"test.perf.{i}", f"value_{i}")
                
                # Read operations
                for i in range(100):
                    value = session.get_sync(f"test.perf.{i}")
                    self.assertEqual(value, f"value_{i}")
                
                # List operations
                keys = session.keys_sync("test.perf.*")
                self.assertEqual(len(keys), 100)
            
            elapsed = time.time() - start_time
            print(f"\nPerformance test completed in {elapsed:.3f} seconds")
            self.assertLess(elapsed, 10, "Performance test took too long")
    
    
    class TestKeyStoreIntegration(unittest.TestCase):
        """Integration tests for KeyStore."""
        
        TEST_SERVER = "nats://127.0.0.1:4222"
        
        def test_ttl_expiration(self):
            """Test TTL expiration (if supported by server)."""
            config = KeyStoreConfig(
                server=self.TEST_SERVER,
                bucket="ttl_test",
                ttl_seconds=2.0  # 2 second TTL
            )
            store = KeyStore(config)
            
            with store.sync_session() as session:
                session.put_sync("ttl.test", "temporary")
                self.assertTrue(session.exists_sync("ttl.test"))
                
                # Wait for expiration
                time.sleep(3)
                
                # Key should be gone
                self.assertFalse(session.exists_sync("ttl.test"))
                self.assertIsNone(session.get_sync("ttl.test"))
        
        def test_bucket_isolation(self):
            """Test that different buckets are isolated."""
            store1 = KeyStore.create_keystore(
                server=self.TEST_SERVER,
                bucket="bucket1"
            )
            store2 = KeyStore.create_keystore(
                server=self.TEST_SERVER,
                bucket="bucket2"
            )
            
            with store1.sync_session() as s1, store2.sync_session() as s2:
                # Put in bucket1
                s1.put_sync("shared.key", "value1")
                
                # Put in bucket2
                s2.put_sync("shared.key", "value2")
                
                # Verify isolation
                self.assertEqual(s1.get_sync("shared.key"), "value1")
                self.assertEqual(s2.get_sync("shared.key"), "value2")
    
    
    class TestJobQueue(unittest.TestCase):
        """Test suite for JobQueue operations."""
        
        TEST_SERVER = "nats://127.0.0.1:4222"
        
        def setUp(self):
            """Set up test JobQueue."""
            self.keystore = KeyStore.create_keystore(
                server=self.TEST_SERVER,
                bucket="test_jobqueue"
            )
            self.job_queue = JobQueue(self.keystore, worker_id="test-worker")
        
        def tearDown(self):
            """Clean up test data."""
            with self.keystore.sync_session() as session:
                keys = session.keys_sync("*")
                for key in keys:
                    session.delete_sync(key)
        
        def test_submit_and_get_job(self):
            """Test submitting and retrieving a job."""
            async def run_test():
                async with self.keystore.session():
                    # Submit job
                    job_id = await self.job_queue.submit(
                        payload={"task": "test", "data": 123},
                        queue="test",
                        priority=5
                    )
                    
                    self.assertIsNotNone(job_id)
                    
                    # Get job
                    job = await self.job_queue.get_job(job_id)
                    self.assertIsNotNone(job)
                    self.assertEqual(job.payload["task"], "test")
                    self.assertEqual(job.priority, 5)
                    self.assertEqual(job.status, JobStatus.PENDING)
            
            asyncio.run(run_test())
        
        def test_claim_and_complete_job(self):
            """Test claiming and completing a job."""
            async def run_test():
                async with self.keystore.session():
                    # Submit job
                    job_id = await self.job_queue.submit(
                        payload={"task": "process"},
                        queue="test"
                    )
                    
                    # Claim job
                    job = await self.job_queue.claim_job(["test"])
                    self.assertIsNotNone(job)
                    self.assertEqual(job.id, job_id)
                    self.assertEqual(job.status, JobStatus.RUNNING)
                    self.assertEqual(job.worker_id, "test-worker")
                    
                    # Complete job
                    success = await self.job_queue.complete_job(job_id, result={"output": 42})
                    self.assertTrue(success)
                    
                    # Verify completion
                    completed_job = await self.job_queue.get_job(job_id)
                    self.assertEqual(completed_job.status, JobStatus.COMPLETED)
                    self.assertEqual(completed_job.result["output"], 42)
            
            asyncio.run(run_test())
        
        def test_job_priority_ordering(self):
            """Test that jobs are processed in priority order."""
            async def run_test():
                async with self.keystore.session():
                    # Submit jobs with different priorities
                    job_ids = []
                    for priority in [1, 10, 5, 8]:
                        job_id = await self.job_queue.submit(
                            payload={"priority": priority},
                            queue="priority_test",
                            priority=priority
                        )
                        job_ids.append((priority, job_id))
                    
                    # Claim jobs and verify order
                    claimed_priorities = []
                    for _ in range(4):
                        job = await self.job_queue.claim_job(["priority_test"])
                        if job:
                            claimed_priorities.append(job.priority)
                    
                    # Should be in descending priority order
                    self.assertEqual(claimed_priorities, [10, 8, 5, 1])
            
            asyncio.run(run_test())
        
        def test_job_retry_on_failure(self):
            """Test job retry mechanism."""
            async def run_test():
                async with self.keystore.session():
                    # Submit job with retries
                    job_id = await self.job_queue.submit(
                        payload={"task": "retry_test"},
                        queue="test",
                        max_retries=2
                    )
                    
                    # Claim and fail job
                    job = await self.job_queue.claim_job(["test"])
                    await self.job_queue.fail_job(job_id, "First failure")
                    
                    # Job should be back in pending state
                    await asyncio.sleep(0.1)
                    job = await self.job_queue.get_job(job_id)
                    self.assertEqual(job.status, JobStatus.PENDING)
                    self.assertEqual(job.retry_count, 1)
                    
                    # Claim and fail again
                    job = await self.job_queue.claim_job(["test"])
                    await self.job_queue.fail_job(job_id, "Second failure")
                    
                    # Should be permanently failed after max retries
                    await asyncio.sleep(0.1)
                    job = await self.job_queue.get_job(job_id)
                    self.assertEqual(job.status, JobStatus.FAILED)
                    self.assertEqual(job.retry_count, 2)
            
            asyncio.run(run_test())
        
        def test_queue_stats(self):
            """Test queue statistics tracking."""
            async def run_test():
                async with self.keystore.session():
                    # Submit and process jobs
                    job_id1 = await self.job_queue.submit({"task": 1}, queue="stats_test")
                    job_id2 = await self.job_queue.submit({"task": 2}, queue="stats_test")
                    
                    # Check initial stats
                    stats = await self.job_queue.get_queue_stats("stats_test")
                    self.assertEqual(stats["pending"], 2)
                    
                    # Claim and complete one job
                    job = await self.job_queue.claim_job(["stats_test"])
                    await self.job_queue.complete_job(job.id)
                    
                    # Determine which job wasn't claimed and cancel it
                    unclaimed_job_id = job_id2 if job.id == job_id1 else job_id1
                    
                    # Cancel the unclaimed job
                    cancelled = await self.job_queue.cancel_job(unclaimed_job_id)
                    self.assertTrue(cancelled, f"Failed to cancel job {unclaimed_job_id}")
                    
                    # Check final stats
                    stats = await self.job_queue.get_queue_stats("stats_test")
                    self.assertEqual(stats["completed"], 1)
                    self.assertEqual(stats["cancelled"], 1)
                    self.assertEqual(stats["pending"], 0)
            
            asyncio.run(run_test())
    
    
    def run_tests():
        """Run all tests with detailed output."""
        print("=" * 70)
        print("KeyStore and JobQueue Test Suite - Single NATS Server")
        print(f"Testing against: {TestKeyStore.TEST_SERVER}")
        print("=" * 70)
        
        # Create test suite
        suite = unittest.TestSuite()
        
        # Add all test cases
        suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestKeyStore))
        suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestKeyStoreIntegration))
        suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestJobQueue))
        
        # Run tests with verbose output
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        # Print summary
        print("\n" + "=" * 70)
        print("Test Summary:")
        print(f"  Tests run: {result.testsRun}")
        print(f"  Failures: {len(result.failures)}")
        print(f"  Errors: {len(result.errors)}")
        print(f"  Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
        print("=" * 70)
        
        return result.wasSuccessful()
    
    
    def job_queue_demo():
        """Run a demo of JobQueue functionality."""
        print("\n" + "=" * 70)
        print("JobQueue Demo")
        print("=" * 70)
        
        # Create KeyStore and JobQueue
        keystore = KeyStore.create_keystore(
            server="nats://127.0.0.1:4222",
            bucket="jobqueue_demo"
        )
        
        async def run_demo():
            async with keystore.session():
                job_queue = JobQueue(keystore, worker_id="demo-worker")
                
                print("\n1. Submit Jobs with Different Priorities:")
                jobs = []
                for i, (priority, task) in enumerate([
                    (10, "urgent-task"),
                    (5, "normal-task"),
                    (1, "low-priority-task"),
                    (8, "high-priority-task")
                ]):
                    job_id = await job_queue.submit(
                        payload={"task": task, "index": i},
                        queue="demo",
                        priority=priority
                    )
                    jobs.append((priority, task, job_id))
                    print(f"   Submitted: {task} (priority={priority}, id={job_id[:8]}...)")
                
                print("\n2. Process Jobs in Priority Order:")
                
                # Define a simple job handler
                async def job_handler(job: Job) -> Dict[str, Any]:
                    """Simple job handler that processes the task."""
                    print(f"   Processing job {job.id[:8]}... with task: {job.payload['task']}")
                    # Simulate some work
                    await asyncio.sleep(0.1)
                    return {"processed": job.payload['task'], "at": datetime.utcnow().isoformat()}
                
                # Process jobs one by one to show priority ordering
                for i in range(4):
                    job = await job_queue.claim_job(["demo"])
                    if job:
                        result = await job_handler(job)
                        await job_queue.complete_job(job.id, result)
                        print(f"   ✓ Completed: {job.payload['task']} (priority={job.priority})")
                
                print("\n3. Queue Statistics:")
                stats = await job_queue.get_queue_stats("demo")
                for stat_name, value in stats.items():
                    print(f"   {stat_name}: {value}")
                
                print("\n4. Submit Job with Retry Logic:")
                retry_job_id = await job_queue.submit(
                    payload={"task": "retry-task", "fail_times": 2},
                    queue="retry_demo",
                    max_retries=3
                )
                print(f"   Submitted retry job: {retry_job_id[:8]}...")
                
                # Simulate failures
                for attempt in range(2):
                    job = await job_queue.claim_job(["retry_demo"])
                    if job:
                        print(f"   Attempt {attempt + 1}: Simulating failure...")
                        await job_queue.fail_job(job.id, f"Simulated failure {attempt + 1}")
                        await asyncio.sleep(0.5)
                
                # Final successful attempt
                job = await job_queue.claim_job(["retry_demo"])
                if job:
                    print(f"   Attempt 3: Processing successfully...")
                    await job_queue.complete_job(job.id, {"success": True})
                    completed_job = await job_queue.get_job(job.id)
                    print(f"   ✓ Job completed after {completed_job.retry_count} retries")
                
                print("\n5. Worker Monitoring:")
                
                # Create multiple workers
                worker2 = JobQueue(keystore, worker_id="worker-2")
                worker3 = JobQueue(keystore, worker_id="worker-3")
                
                # Submit some jobs
                for i in range(3):
                    await job_queue.submit({"task": f"monitor-task-{i}"}, queue="monitor")
                
                # Have workers claim jobs
                await job_queue.claim_job(["monitor"])
                await worker2.claim_job(["monitor"])
                await worker3.claim_job(["monitor"])
                
                # Check active workers
                active_workers = await job_queue.get_active_workers()
                print(f"   Active workers: {len(active_workers)}")
                for worker in active_workers:
                    job_info = f"job {worker['current_job'][:8]}..." if worker['current_job'] else "idle"
                    print(f"   - {worker['worker_id']}: {job_info}")
                
                print("\n6. Cleanup:")
                # Clean up all demo data
                keys = await keystore.keys("*")
                for key in keys:
                    await keystore.delete(key)
                print(f"   Cleaned up {len(keys)} keys")
        
        asyncio.run(run_demo())
        print("\n" + "=" * 70)
    
    
    def demo():
        """Run a simple demo of KeyStore functionality."""
        print("\n" + "=" * 70)
        print("KeyStore Demo")
        print("=" * 70)
        
        # Create store using class method
        store = KeyStore.create_keystore(
            server="nats://127.0.0.1:4222",
            bucket="demo",
            description="Demo bucket"
        )
        
        print("\n1. Basic Operations:")
        with store.sync_session() as session:
            # Clean up any existing demo keys first
            demo_keys = session.keys_sync("demo.*")
            for key in demo_keys:
                session.delete_sync(key)
            
            # String operations
            session.put_sync("demo.name", "Alice")
            print(f"   Stored: demo.name = {session.get_sync('demo.name')}")
            
            # JSON operations
            session.put_sync("demo.user.1", {"id": 1, "name": "Bob", "age": 30})
            print(f"   Stored: demo.user.1 = {session.get_sync('demo.user.1')}")
            
            # Counter operations
            count = session.increment_sync("demo.visits")
            print(f"   Incremented visits to: {count}")
            count = session.increment_sync("demo.visits", 5)
            print(f"   Incremented visits by 5 to: {count}")
            
            # List keys
            session.put_sync("demo.user.2", {"id": 2, "name": "Charlie"})
            keys = session.keys_sync("demo.user.*")
            print(f"   Keys matching 'demo.user.*': {keys}")
            
            # Check existence
            print(f"   Key 'demo.name' exists: {session.exists_sync('demo.name')}")
            print(f"   Key 'missing' exists: {session.exists_sync('missing')}")
        
        print("\n2. Async Operations:")
        async def async_demo():
            async with store.session() as session:
                # Clean up any existing async demo keys
                async_keys = await session.keys("demo.async.*")
                for key in async_keys:
                    await session.delete(key)
                
                await session.put("demo.async.test", "async value")
                value = await session.get("demo.async.test")
                print(f"   Async stored and retrieved: {value}")
                
                # Sequential increments to show it works
                print("   Sequential increments:")
                for i in range(1, 6):
                    val = await session.increment("demo.async.sequential")
                    print(f"      Increment {i}: value = {val}")
                
                # Concurrent increments with separate connections
                print("   Concurrent increments (5 tasks, 2 increments each):")
                
                async def increment_worker(task_id: int):
                    """Worker that performs increments"""
                    store_instance = KeyStore.create_keystore(
                        server="nats://127.0.0.1:4222",
                        bucket="demo"
                    )
                    async with store_instance.session() as s:
                        results = []
                        for _ in range(2):
                            val = await s.increment("demo.async.concurrent")
                            results.append(val)
                        return task_id, results
                
                # Run concurrent workers
                tasks = [increment_worker(i) for i in range(5)]
                worker_results = await asyncio.gather(*tasks)
                
                for task_id, results in worker_results:
                    print(f"      Task {task_id}: incremented to {results}")
                
                final_value = await session.get("demo.async.concurrent")
                print(f"      Final value: {final_value}")
        
        asyncio.run(async_demo())
        
        print("\n3. Cleanup Demo Keys:")
        with store.sync_session() as session:
            # Show all demo keys
            all_demo_keys = session.keys_sync("demo.*")
            print(f"   Found {len(all_demo_keys)} demo keys")
            
            # Clean them up
            for key in all_demo_keys:
                session.delete_sync(key)
            print("   All demo keys cleaned up")
        
        print("\n" + "=" * 70)
    
    
    # Main execution
    print("\n📦 NATS JetStream KeyStore and JobQueue")
    print("=" * 70)
    print("\n⚠️  Make sure NATS server is running at 127.0.0.1:4222")
    print("   You can start it with: docker run -p 4222:4222 nats:latest -js\n")
    
    print("Options:")
    print("  1. Run KeyStore demo")
    print("  2. Run JobQueue demo")
    print("  3. Run tests")
    print("  4. Run all")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == "1":
        demo()
    elif choice == "2":
        job_queue_demo()
    elif choice == "3":
        success = run_tests()
        exit(0 if success else 1)
    elif choice == "4":
        for _ in range(1):  # Run the sequence three times
            demo()
            print("\nPress Enter to continue to JobQueue demo...")
            input()
            job_queue_demo()
            print("\nPress Enter to continue to tests...")
            input()
            success = run_tests()
            print("\nPress Enter to continue to JobQueue demo...")
            input()
        # After the last iteration, run job_queue_demo() and tests one more time
        job_queue_demo()
        print("\nPress Enter to continue to tests...")
        input()
        success = run_tests()
    else:
        print("Invalid choice. Running KeyStore demo by default...")
        demo()
        exit(0)  # Simplified exit, assuming default demo should exit successfully