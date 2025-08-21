import asyncio
import json
import pickle
import base64
import hashlib
import uuid
from typing import Optional, Any, Dict, List
from datetime import datetime
from dataclasses import dataclass
from enum import Enum


class KeyFormat(Enum):
    """Supported storage formats for keys"""
    JSON = "json"
    PICKLE = "pickle"
    TEXT = "text"
    BINARY = "binary"


@dataclass
class KeyMetadata:
    """Metadata for stored keys"""
    key: str
    format: KeyFormat
    size: int
    created_at: datetime
    updated_at: datetime
    version: int = 1
    ttl: Optional[int] = None
    tags: List[str] = None
    checksum: Optional[str] = None


class NASKeyStore:
    """
    Simplified NAS client for storing and retrieving keys.
    Uses direct request/response pattern without subscriptions.
    Each operation creates its own connection.
    """
    
    def __init__(self, host: str = "localhost", port: int = 4222,
                 namespace: str = "default", cache_size: int = 100):
        """
        Initialize NAS Key Store client.
        
        Args:
            host: NATS server hostname or IP
            port: NATS server port (default: 4222)
            namespace: Namespace for key organization
            cache_size: Local cache size limit
        """
        self.host = host
        self.port = port
        self.namespace = namespace
        self.cache_size = cache_size
        
        # Subjects for key-value operations
        self.kv_prefix = f"_kv.{namespace}"
        
        # Local cache
        self._cache: Dict[str, Any] = {}
        self._cache_metadata: Dict[str, KeyMetadata] = {}
    
    async def _create_connection(self):
        """Create a new connection to NATS server"""
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            
            # Read INFO from server
            info_line = await reader.readline()
            if not info_line.startswith(b'INFO '):
                raise Exception("Invalid server response")
            
            # Send CONNECT command
            connect_options = {
                "verbose": False,
                "pedantic": False,
                "name": f"simple_nas_{uuid.uuid4().hex[:8]}",
                "lang": "python",
                "version": "1.0.0",
                "protocol": 1
            }
            
            connect_cmd = f"CONNECT {json.dumps(connect_options)}\r\n"
            writer.write(connect_cmd.encode())
            await writer.drain()
            
            # Send PING to verify
            writer.write(b"PING\r\n")
            await writer.drain()
            
            # Wait for PONG
            response = await reader.readline()
            if response.strip() != b'PONG':
                raise Exception(f"Unexpected response: {response}")
            
            return reader, writer
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to NATS at {self.host}:{self.port}: {e}")
    
    async def _close_connection(self, writer):
        """Close a connection"""
        if writer:
            writer.close()
            await writer.wait_closed()
    
    async def _direct_publish(self, subject: str, data: bytes):
        """Publish a message and close connection (fire-and-forget)"""
        reader, writer = await self._create_connection()
        try:
            pub_cmd = f"PUB {subject} {len(data)}\r\n"
            writer.write(pub_cmd.encode())
            writer.write(data)
            writer.write(b"\r\n")
            await writer.drain()
            
            # Wait for server acknowledgment
            await asyncio.sleep(0.1)
        finally:
            await self._close_connection(writer)
    
    async def store(self, key: str, value: Any, format: KeyFormat = KeyFormat.JSON,
                   ttl: Optional[int] = None, tags: Optional[List[str]] = None) -> bool:
        """
        Store a key-value pair in the NAS.
        
        Args:
            key: The key identifier
            value: The value to store
            format: Storage format
            ttl: Optional time-to-live in seconds
            tags: Optional tags for categorization
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Serialize value
            serialized = self._serialize_value(value, format)
            checksum = hashlib.md5(serialized.encode()).hexdigest()
            
            # Create metadata
            now = datetime.now()
            metadata = KeyMetadata(
                key=key,
                format=format,
                size=len(serialized),
                created_at=now,
                updated_at=now,
                version=1 if key not in self._cache_metadata else 
                         self._cache_metadata[key].version + 1,
                ttl=ttl,
                tags=tags or [],
                checksum=checksum
            )
            
            # Store in cache
            self._cache[key] = value
            self._cache_metadata[key] = metadata
            self._manage_cache_size()
            
            # Create store message
            store_msg = {
                "operation": "store",
                "key": key,
                "value": serialized,
                "metadata": {
                    "format": format.value,
                    "ttl": ttl,
                    "tags": tags or [],
                    "checksum": checksum,
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                    "version": metadata.version
                }
            }
            
            # Publish to NATS
            key_subject = f"{self.kv_prefix}.key.{key.replace('.', '_')}"
            await self._direct_publish(key_subject, json.dumps(store_msg).encode())
            
            return True
            
        except Exception as e:
            print(f"Error storing key '{key}': {e}")
            return False
    
    async def retrieve(self, key: str, use_cache: bool = True) -> Optional[Any]:
        """
        Retrieve a value by key from the cache.
        Note: In this simplified version, only retrieves from local cache.
        
        Args:
            key: The key to retrieve
            use_cache: Whether to use local cache
            
        Returns:
            The value if found in cache, None otherwise
        """
        # Check cache
        if use_cache and key in self._cache:
            metadata = self._cache_metadata[key]
            
            # Check TTL
            if metadata.ttl:
                age = (datetime.now() - metadata.updated_at).total_seconds()
                if age > metadata.ttl:
                    # Expired - remove from cache
                    del self._cache[key]
                    del self._cache_metadata[key]
                    return None
                else:
                    return self._cache[key]
            else:
                return self._cache[key]
        
        return None
    
    async def delete(self, key: str) -> bool:
        """
        Delete a key from the cache and publish delete message.
        
        Args:
            key: The key to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Remove from cache
            deleted = key in self._cache
            self._cache.pop(key, None)
            self._cache_metadata.pop(key, None)
            
            # Publish delete message
            delete_msg = {
                "operation": "delete",
                "key": key,
                "timestamp": datetime.now().isoformat()
            }
            
            key_subject = f"{self.kv_prefix}.key.{key.replace('.', '_')}"
            await self._direct_publish(key_subject, json.dumps(delete_msg).encode())
            
            return deleted
            
        except Exception as e:
            print(f"Error deleting key '{key}': {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in cache.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists in cache, False otherwise
        """
        if key in self._cache:
            metadata = self._cache_metadata[key]
            if metadata.ttl:
                age = (datetime.now() - metadata.updated_at).total_seconds()
                return age <= metadata.ttl
            return True
        return False
    
    async def list_keys(self, pattern: str = "*") -> List[str]:
        """
        List all keys in cache matching a pattern.
        
        Args:
            pattern: Pattern to match keys (supports * wildcard)
            
        Returns:
            List of matching keys
        """
        import fnmatch
        
        keys = []
        for key in self._cache.keys():
            # Check TTL
            metadata = self._cache_metadata[key]
            if metadata.ttl:
                age = (datetime.now() - metadata.updated_at).total_seconds()
                if age > metadata.ttl:
                    continue
            
            if fnmatch.fnmatch(key, pattern):
                keys.append(key)
        
        return sorted(keys)
    
    async def get_metadata(self, key: str) -> Optional[KeyMetadata]:
        """
        Get metadata for a key from cache.
        
        Args:
            key: The key to get metadata for
            
        Returns:
            KeyMetadata if found in cache, None otherwise
        """
        if key in self._cache_metadata:
            metadata = self._cache_metadata[key]
            # Check TTL
            if metadata.ttl:
                age = (datetime.now() - metadata.updated_at).total_seconds()
                if age > metadata.ttl:
                    return None
            return metadata
        return None
    
    async def update(self, key: str, value: Any, format: Optional[KeyFormat] = None) -> bool:
        """
        Update an existing key's value.
        
        Args:
            key: The key to update
            value: New value
            format: Optional new format
            
        Returns:
            True if successful, False otherwise
        """
        # Get existing metadata
        metadata = await self.get_metadata(key)
        if not metadata:
            # Key doesn't exist, create new
            return await self.store(key, value, format or KeyFormat.JSON)
        
        # Use existing format if not specified
        if format is None:
            format = metadata.format
        
        # Store with incremented version
        return await self.store(key, value, format, metadata.ttl, metadata.tags)
    
    async def bulk_store(self, items: Dict[str, Any], format: KeyFormat = KeyFormat.JSON) -> Dict[str, bool]:
        """
        Store multiple key-value pairs.
        
        Args:
            items: Dictionary of key-value pairs
            format: Storage format for all items
            
        Returns:
            Dictionary of key -> success status
        """
        results = {}
        for key, value in items.items():
            results[key] = await self.store(key, value, format)
        return results
    
    async def bulk_retrieve(self, keys: List[str]) -> Dict[str, Any]:
        """
        Retrieve multiple keys from cache.
        
        Args:
            keys: List of keys to retrieve
            
        Returns:
            Dictionary of key -> value for found keys
        """
        results = {}
        for key in keys:
            value = await self.retrieve(key)
            if value is not None:
                results[key] = value
        return results
    
    def _serialize_value(self, value: Any, format: KeyFormat) -> str:
        """Serialize value based on format"""
        if format == KeyFormat.JSON:
            return json.dumps(value)
        elif format == KeyFormat.PICKLE:
            return base64.b64encode(pickle.dumps(value)).decode()
        elif format == KeyFormat.TEXT:
            return str(value)
        elif format == KeyFormat.BINARY:
            if isinstance(value, bytes):
                return base64.b64encode(value).decode()
            else:
                return base64.b64encode(str(value).encode()).decode()
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _deserialize_value(self, data: str, format: KeyFormat) -> Any:
        """Deserialize value based on format"""
        if format == KeyFormat.JSON:
            return json.loads(data)
        elif format == KeyFormat.PICKLE:
            return pickle.loads(base64.b64decode(data))
        elif format == KeyFormat.TEXT:
            return data
        elif format == KeyFormat.BINARY:
            return base64.b64decode(data)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _manage_cache_size(self):
        """Manage cache size by removing oldest entries"""
        if len(self._cache) > self.cache_size:
            sorted_keys = sorted(
                self._cache_metadata.keys(),
                key=lambda k: self._cache_metadata[k].updated_at
            )
            
            # Remove oldest entries
            for key in sorted_keys[:len(self._cache) - self.cache_size]:
                del self._cache[key]
                del self._cache_metadata[key]
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "size": len(self._cache),
            "max_size": self.cache_size,
            "keys": list(self._cache.keys()),
            "memory_usage": sum(self._cache_metadata[k].size for k in self._cache)
        }
    
    def clear_cache(self):
        """Clear all cached data"""
        self._cache.clear()
        self._cache_metadata.clear()
    
    async def ping(self) -> bool:
        """Test connection to NATS server"""
        try:
            reader, writer = await self._create_connection()
            await self._close_connection(writer)
            return True
        except:
            return False


# Test functions
if __name__ == "__main__":
    async def test_basic_operations():
        """Test basic store and retrieve operations"""
        print("=" * 50)
        print("Testing simplified NAS operations...")
        
        # Create client
        client = NASKeyStore("localhost", 4222, "test_namespace")
        
        # Test connection
        connected = await client.ping()
        print(f"NATS server reachable: {connected}")
        
        if not connected:
            print("Cannot reach NATS server on localhost:4222")
            return
        
        # Store different types of data
        success = await client.store("user:1", {"name": "Alice", "age": 30}, KeyFormat.JSON)
        print(f"Stored user:1: {success}")
        
        success = await client.store("config:app", "debug=true\nport=8080", KeyFormat.TEXT)
        print(f"Stored config:app: {success}")
        
        success = await client.store("data:binary", b"Hello World", KeyFormat.BINARY)
        print(f"Stored data:binary: {success}")
        
        # Retrieve from cache
        user = await client.retrieve("user:1")
        print(f"Retrieved user: {user}")
        
        config = await client.retrieve("config:app")
        print(f"Retrieved config: {config}")
        
        binary_data = await client.retrieve("data:binary")
        print(f"Retrieved binary: {binary_data}")
        
        # Check existence
        exists = await client.exists("user:1")
        print(f"user:1 exists: {exists}")
        
        exists = await client.exists("nonexistent")
        print(f"nonexistent exists: {exists}")
        
        # List keys
        keys = await client.list_keys()
        print(f"All keys: {keys}")
        
        # Get metadata
        meta = await client.get_metadata("user:1")
        if meta:
            print(f"Metadata for user:1: version={meta.version}, size={meta.size}")
        
        # Delete key
        success = await client.delete("data:binary")
        print(f"Deleted data:binary: {success}")
        
        # Cache stats
        stats = client.get_cache_stats()
        print(f"Cache stats: {stats}")
    
    async def test_ttl():
        """Test TTL functionality"""
        print("\n" + "=" * 50)
        print("Testing TTL...")
        
        client = NASKeyStore("localhost", 4222)
        
        # Store with TTL
        await client.store("temp:data", "temporary", KeyFormat.TEXT, ttl=2)
        
        # Retrieve immediately
        value = await client.retrieve("temp:data")
        print(f"Value before TTL expires: {value}")
        
        # Wait for TTL
        print("Waiting for TTL to expire...")
        await asyncio.sleep(3)
        
        # Try to retrieve after TTL
        value = await client.retrieve("temp:data")
        print(f"Value after TTL expires: {value}")  # Should be None
    
    async def test_bulk_operations():
        """Test bulk operations"""
        print("\n" + "=" * 50)
        print("Testing bulk operations...")
        
        client = NASKeyStore("localhost", 4222)
        
        # Bulk store
        items = {
            "bulk:1": {"data": "first"},
            "bulk:2": {"data": "second"},
            "bulk:3": {"data": "third"}
        }
        
        results = await client.bulk_store(items, KeyFormat.JSON)
        print(f"Bulk store results: {results}")
        
        # Bulk retrieve
        keys = ["bulk:1", "bulk:2", "bulk:3", "bulk:nonexistent"]
        retrieved = await client.bulk_retrieve(keys)
        print(f"Bulk retrieve results: {retrieved}")
    
    async def main():
        """Run all tests"""
        print("Starting NASKeyStore tests...")
        print("=" * 50)
        
        try:
            await test_basic_operations()
            await test_ttl()
            await test_bulk_operations()
            
            print("\n" + "=" * 50)
            print("All tests completed!")
            
        except Exception as e:
            print(f"\nTest failed with error: {e}")
            import traceback
            traceback.print_exc()
    
    # Run tests
    asyncio.run(main())