import asyncio
import json
import pickle
import base64
import hashlib
import uuid
from typing import Optional, Any, Dict, List
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum
from functools import wraps
import time


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
    
    def to_dict(self):
        """Convert metadata to dictionary for serialization"""
        data = asdict(self)
        data['format'] = self.format.value
        data['created_at'] = self.created_at.isoformat()
        data['updated_at'] = self.updated_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create metadata from dictionary"""
        data['format'] = KeyFormat(data['format'])
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        data['updated_at'] = datetime.fromisoformat(data['updated_at'])
        return cls(**data)


def run_async(coro):
    """Helper to run async function in sync context"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop, create new one
        return asyncio.run(coro)
    else:
        # Loop already running, create task
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, coro)
            return future.result()


class NATSKeyStore:
    """
    NATS-based distributed key-value store client.
    Uses request/response pattern for true distributed storage.
    No local caching - all operations go directly to NATS.
    """
    
    def __init__(self, host: str = "localhost", port: int = 4222,
                 namespace: str = "default", request_timeout: float = 5.0):
        """
        Initialize NATS Key Store client.
        
        Args:
            host: NATS server hostname or IP
            port: NATS server port (default: 4222)
            namespace: Namespace for key organization
            request_timeout: Timeout for request/response operations in seconds
        """
        self.host = host
        self.port = port
        self.namespace = namespace
        self.request_timeout = request_timeout
        
        # Subjects for key-value operations
        self.kv_prefix = f"_kv.{namespace}"
        
        # Store metadata locally for tracking versions
        # This is minimal and only used for version tracking
        self._version_tracker: Dict[str, int] = {}
    
    def get_namespace(self) -> str:
        """
        Get the current namespace.
        
        Returns:
            Current namespace string
        """
        return self.namespace
    
    def set_namespace(self, namespace: str) -> None:
        """
        Set a new namespace. This will update the key-value prefix for NATS operations.
        
        Args:
            namespace: New namespace to use
        
        Note:
            Changing namespace will affect all future operations.
            Version tracker is cleared when changing namespaces.
        """
        old_namespace = self.namespace
        self.namespace = namespace
        self.kv_prefix = f"_kv.{namespace}"
        
        if old_namespace != namespace:
            self._version_tracker.clear()
            print(f"Namespace changed from '{old_namespace}' to '{namespace}'.")
    
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
                "name": f"nats_kv_{uuid.uuid4().hex[:8]}",
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
    
    async def _request_response(self, subject: str, data: bytes, timeout: Optional[float] = None) -> Optional[bytes]:
        """
        Send a request and wait for response.
        
        Args:
            subject: Subject to send request to
            data: Request data
            timeout: Optional timeout override
            
        Returns:
            Response data or None if timeout
        """
        reader, writer = await self._create_connection()
        timeout = timeout or self.request_timeout
        
        try:
            # Generate unique inbox for response
            inbox = f"_INBOX.{uuid.uuid4().hex}"
            
            # Subscribe to inbox
            sub_cmd = f"SUB {inbox} 1\r\n"
            writer.write(sub_cmd.encode())
            await writer.drain()
            
            # Publish request with reply-to
            pub_cmd = f"PUB {subject} {inbox} {len(data)}\r\n"
            writer.write(pub_cmd.encode())
            writer.write(data)
            writer.write(b"\r\n")
            await writer.drain()
            
            # Wait for response with timeout
            try:
                response = await asyncio.wait_for(
                    self._read_message(reader),
                    timeout=timeout
                )
                return response
            except asyncio.TimeoutError:
                return None
                
        finally:
            await self._close_connection(writer)
    
    async def _read_message(self, reader) -> Optional[bytes]:
        """Read a message from NATS"""
        while True:
            line = await reader.readline()
            if not line:
                return None
            
            line = line.decode().strip()
            
            if line.startswith('MSG'):
                # Parse MSG header: MSG <subject> <sid> <size>
                parts = line.split()
                if len(parts) >= 4:
                    size = int(parts[-1])
                    # Read payload
                    payload = await reader.read(size)
                    # Read trailing CRLF
                    await reader.read(2)
                    return payload
            elif line == 'PING':
                # Respond to PING
                continue
    
    async def _publish(self, subject: str, data: bytes):
        """Publish a message (fire-and-forget)"""
        reader, writer = await self._create_connection()
        try:
            pub_cmd = f"PUB {subject} {len(data)}\r\n"
            writer.write(pub_cmd.encode())
            writer.write(data)
            writer.write(b"\r\n")
            await writer.drain()
            
            # Wait briefly for server acknowledgment
            await asyncio.sleep(0.05)
        finally:
            await self._close_connection(writer)
    
    async def store(self, key: str, value: Any, format: KeyFormat = KeyFormat.JSON,
                   ttl: Optional[int] = None, tags: Optional[List[str]] = None) -> bool:
        """
        Store a key-value pair in NATS. Always creates or overwrites the key.
        
        This method will:
        - Create a new key if it doesn't exist
        - Overwrite an existing key completely
        - Reset version to 1 for new keys or increment for existing
        
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
            
            # Track version
            version = self._version_tracker.get(key, 0) + 1
            self._version_tracker[key] = version
            
            # Create metadata
            now = datetime.now()
            metadata = KeyMetadata(
                key=key,
                format=format,
                size=len(serialized),
                created_at=now,
                updated_at=now,
                version=version,
                ttl=ttl,
                tags=tags or [],
                checksum=checksum
            )
            
            # Create store message
            store_msg = {
                "operation": "store",
                "key": key,
                "value": serialized,
                "metadata": metadata.to_dict()
            }
            
            # Store in NATS
            store_subject = f"{self.kv_prefix}.store.{key.replace('.', '_')}"
            await self._publish(store_subject, json.dumps(store_msg).encode())
            
            # Also publish to a general subject for persistence
            await self._publish(f"{self.kv_prefix}.data", json.dumps(store_msg).encode())
            
            return True
            
        except Exception as e:
            print(f"Error storing key '{key}': {e}")
            return False
    
    async def retrieve(self, key: str) -> Optional[Any]:
        """
        Retrieve a value by key from NATS.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found, None otherwise
        """
        try:
            # Request data from NATS
            request_msg = {
                "operation": "get",
                "key": key
            }
            
            # Use request/response pattern
            get_subject = f"{self.kv_prefix}.get"
            response = await self._request_response(
                get_subject,
                json.dumps(request_msg).encode()
            )
            
            if response:
                data = json.loads(response.decode())
                
                # Check if key exists
                if data.get("status") == "found":
                    # Deserialize value
                    format = KeyFormat(data["metadata"]["format"])
                    value = self._deserialize_value(data["value"], format)
                    
                    # Update version tracker
                    self._version_tracker[key] = data["metadata"]["version"]
                    
                    # Check TTL if present
                    if data["metadata"].get("ttl"):
                        created_at = datetime.fromisoformat(data["metadata"]["created_at"])
                        age = (datetime.now() - created_at).total_seconds()
                        if age > data["metadata"]["ttl"]:
                            # Expired - delete it
                            await self.delete(key)
                            return None
                    
                    return value
            
            return None
            
        except Exception as e:
            print(f"Error retrieving key '{key}': {e}")
            return None
    
    async def delete(self, key: str) -> bool:
        """
        Delete a key from NATS.
        
        Args:
            key: The key to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Remove from version tracker
            self._version_tracker.pop(key, None)
            
            # Publish delete message
            delete_msg = {
                "operation": "delete",
                "key": key,
                "timestamp": datetime.now().isoformat()
            }
            
            delete_subject = f"{self.kv_prefix}.delete.{key.replace('.', '_')}"
            await self._publish(delete_subject, json.dumps(delete_msg).encode())
            
            # Also publish to general subject
            await self._publish(f"{self.kv_prefix}.data", json.dumps(delete_msg).encode())
            
            return True
            
        except Exception as e:
            print(f"Error deleting key '{key}': {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in NATS.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists, False otherwise
        """
        value = await self.retrieve(key)
        return value is not None
    
    async def list_keys(self, pattern: str = "*") -> List[str]:
        """
        List all keys matching a pattern.
        
        Args:
            pattern: Pattern to match keys (supports * wildcard)
            
        Returns:
            List of matching keys
        """
        try:
            import fnmatch
            
            # Request all keys from NATS
            request_msg = {
                "operation": "list",
                "pattern": pattern
            }
            
            list_subject = f"{self.kv_prefix}.list"
            response = await self._request_response(
                list_subject,
                json.dumps(request_msg).encode(),
                timeout=10.0  # Longer timeout for list operations
            )
            
            if response:
                data = json.loads(response.decode())
                keys = data.get("keys", [])
                
                # Filter by pattern
                matched_keys = []
                for key in keys:
                    if fnmatch.fnmatch(key, pattern):
                        matched_keys.append(key)
                
                return sorted(matched_keys)
            
            return []
            
        except Exception as e:
            print(f"Error listing keys: {e}")
            return []
    
    async def get_metadata(self, key: str) -> Optional[KeyMetadata]:
        """
        Get metadata for a key.
        
        Args:
            key: The key to get metadata for
            
        Returns:
            KeyMetadata if found, None otherwise
        """
        try:
            # Request metadata from NATS
            request_msg = {
                "operation": "metadata",
                "key": key
            }
            
            meta_subject = f"{self.kv_prefix}.metadata"
            response = await self._request_response(
                meta_subject,
                json.dumps(request_msg).encode()
            )
            
            if response:
                data = json.loads(response.decode())
                if data.get("status") == "found":
                    return KeyMetadata.from_dict(data["metadata"])
            
            return None
            
        except Exception as e:
            print(f"Error getting metadata for key '{key}': {e}")
            return None
    
    async def update(self, key: str, value: Any, format: Optional[KeyFormat] = None) -> bool:
        """
        Update an existing key's value while preserving metadata.
        
        This method will:
        - Update an existing key's value while preserving its metadata (TTL, tags)
        - Increment the version number
        - Preserve the original creation time
        - If the key doesn't exist, it creates a new one (falls back to store)
        
        Key differences from store():
        - store(): Always overwrites completely, can reset version
        - update(): Preserves metadata, always increments version
        
        Args:
            key: The key to update
            value: New value
            format: Optional new format (if None, uses existing format)
            
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
        
        # Preserve metadata but update value
        try:
            # Serialize new value
            serialized = self._serialize_value(value, format)
            checksum = hashlib.md5(serialized.encode()).hexdigest()
            
            # Update metadata
            metadata.format = format
            metadata.size = len(serialized)
            metadata.updated_at = datetime.now()
            metadata.version += 1
            metadata.checksum = checksum
            
            # Update version tracker
            self._version_tracker[key] = metadata.version
            
            # Create update message
            update_msg = {
                "operation": "update",
                "key": key,
                "value": serialized,
                "metadata": metadata.to_dict()
            }
            
            # Update in NATS
            update_subject = f"{self.kv_prefix}.update.{key.replace('.', '_')}"
            await self._publish(update_subject, json.dumps(update_msg).encode())
            
            # Also publish to general subject
            await self._publish(f"{self.kv_prefix}.data", json.dumps(update_msg).encode())
            
            return True
            
        except Exception as e:
            print(f"Error updating key '{key}': {e}")
            return False
    
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
        Retrieve multiple keys.
        
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
    
    async def ping(self) -> bool:
        """Test connection to NATS server"""
        try:
            reader, writer = await self._create_connection()
            await self._close_connection(writer)
            return True
        except:
            return False
    
    # ============ SYNCHRONOUS HELPER FUNCTIONS ============
    
    def store_sync(self, key: str, value: Any, format: KeyFormat = KeyFormat.JSON,
                   ttl: Optional[int] = None, tags: Optional[List[str]] = None) -> bool:
        """Synchronous version of store()"""
        return run_async(self.store(key, value, format, ttl, tags))
    
    def retrieve_sync(self, key: str) -> Optional[Any]:
        """Synchronous version of retrieve()"""
        return run_async(self.retrieve(key))
    
    def delete_sync(self, key: str) -> bool:
        """Synchronous version of delete()"""
        return run_async(self.delete(key))
    
    def exists_sync(self, key: str) -> bool:
        """Synchronous version of exists()"""
        return run_async(self.exists(key))
    
    def list_keys_sync(self, pattern: str = "*") -> List[str]:
        """Synchronous version of list_keys()"""
        return run_async(self.list_keys(pattern))
    
    def get_metadata_sync(self, key: str) -> Optional[KeyMetadata]:
        """Synchronous version of get_metadata()"""
        return run_async(self.get_metadata(key))
    
    def update_sync(self, key: str, value: Any, format: Optional[KeyFormat] = None) -> bool:
        """Synchronous version of update()"""
        return run_async(self.update(key, value, format))
    
    def bulk_store_sync(self, items: Dict[str, Any], format: KeyFormat = KeyFormat.JSON) -> Dict[str, bool]:
        """Synchronous version of bulk_store()"""
        return run_async(self.bulk_store(items, format))
    
    def bulk_retrieve_sync(self, keys: List[str]) -> Dict[str, Any]:
        """Synchronous version of bulk_retrieve()"""
        return run_async(self.bulk_retrieve(keys))
    
    def ping_sync(self) -> bool:
        """Synchronous version of ping()"""
        return run_async(self.ping())
    
    # Aliases for convenience
    get = retrieve_sync
    set = store_sync
    put = store_sync
    remove = delete_sync
    has = exists_sync
    keys = list_keys_sync
    
    # ============ INTERNAL HELPER METHODS ============
    
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


# ============ SERVER COMPONENT (Run separately) ============

class NATSKeyValueServer:
    """
    Simple NATS KV server that handles storage requests.
    This should run as a separate process/service.
    """
    
    def __init__(self, host: str = "localhost", port: int = 4222):
        self.host = host
        self.port = port
        self.storage: Dict[str, dict] = {}  # In-memory storage
        
    async def start(self):
        """Start the KV server"""
        print(f"Starting NATS KV Server on {self.host}:{self.port}")
        
        reader, writer = await asyncio.open_connection(self.host, self.port)
        
        # Read INFO
        info_line = await reader.readline()
        
        # Send CONNECT
        connect_options = {
            "verbose": False,
            "pedantic": False,
            "name": "kv_server",
            "lang": "python",
            "version": "1.0.0",
            "protocol": 1
        }
        
        connect_cmd = f"CONNECT {json.dumps(connect_options)}\r\n"
        writer.write(connect_cmd.encode())
        await writer.drain()
        
        # Subscribe to all KV subjects
        writer.write(b"SUB _kv.*.get 1\r\n")
        writer.write(b"SUB _kv.*.list 2\r\n")
        writer.write(b"SUB _kv.*.metadata 3\r\n")
        writer.write(b"SUB _kv.*.data 4\r\n")
        await writer.drain()
        
        print("KV Server ready and listening...")
        
        # Process messages
        while True:
            try:
                line = await reader.readline()
                if not line:
                    break
                
                line = line.decode().strip()
                
                if line.startswith('MSG'):
                    # Parse MSG header
                    parts = line.split()
                    subject = parts[1]
                    sid = parts[2]
                    reply_to = parts[3] if len(parts) > 4 else None
                    size = int(parts[-1])
                    
                    # Read payload
                    payload = await reader.read(size)
                    await reader.read(2)  # CRLF
                    
                    # Process request
                    response = await self.handle_request(subject, payload)
                    
                    # Send response if there's a reply-to
                    if reply_to and response:
                        resp_data = response.encode()
                        pub_cmd = f"PUB {reply_to} {len(resp_data)}\r\n"
                        writer.write(pub_cmd.encode())
                        writer.write(resp_data)
                        writer.write(b"\r\n")
                        await writer.drain()
                        
                elif line == 'PING':
                    writer.write(b"PONG\r\n")
                    await writer.drain()
                    
            except Exception as e:
                print(f"Server error: {e}")
                continue
    
    async def handle_request(self, subject: str, payload: bytes) -> Optional[str]:
        """Handle incoming requests"""
        try:
            data = json.loads(payload.decode())
            operation = data.get("operation")
            
            if subject.endswith(".data"):
                # Store/update/delete operations
                if operation in ["store", "update"]:
                    key = data["key"]
                    self.storage[key] = data
                    return None
                elif operation == "delete":
                    key = data["key"]
                    self.storage.pop(key, None)
                    return None
                    
            elif subject.endswith(".get"):
                # Get operation
                key = data["key"]
                if key in self.storage:
                    stored = self.storage[key]
                    return json.dumps({
                        "status": "found",
                        "value": stored["value"],
                        "metadata": stored["metadata"]
                    })
                else:
                    return json.dumps({"status": "not_found"})
                    
            elif subject.endswith(".list"):
                # List operation
                keys = list(self.storage.keys())
                return json.dumps({"keys": keys})
                
            elif subject.endswith(".metadata"):
                # Metadata operation
                key = data["key"]
                if key in self.storage:
                    return json.dumps({
                        "status": "found",
                        "metadata": self.storage[key]["metadata"]
                    })
                else:
                    return json.dumps({"status": "not_found"})
                    
        except Exception as e:
            print(f"Error handling request: {e}")
            return json.dumps({"error": str(e)})
        
        return None


# ============ TEST FUNCTIONS ============

def test_sync_operations():
    """Test synchronous operations without local cache"""
    print("=" * 50)
    print("Testing synchronous operations (no cache)...")
    
    # Create client
    client = NATSKeyStore("localhost", 4222, "test_sync")
    
    # Test namespace operations
    print(f"Current namespace: {client.get_namespace()}")
    
    # Test connection
    connected = client.ping_sync()
    print(f"NATS server reachable: {connected}")
    
    if not connected:
        print("Cannot reach NATS server on localhost:4222")
        print("Make sure the NATSKeyValueServer is running!")
        return
    
    # Store data - goes directly to NATS
    print("\n--- Testing direct NATS storage ---")
    success = client.store_sync("user:1", {"name": "Alice", "age": 30})
    print(f"Stored user:1: {success}")
    
    # Retrieve - fetches from NATS
    user = client.retrieve_sync("user:1")
    print(f"Retrieved user:1 from NATS: {user}")
    
    # Test store vs update
    print("\n--- Testing store vs update ---")
    
    # Initial store with metadata
    client.store_sync("doc:1", {"content": "v1"}, tags=["important"], ttl=3600)
    meta1 = client.get_metadata_sync("doc:1")
    if meta1:
        print(f"After store - Version: {meta1.version}, Tags: {meta1.tags}, TTL: {meta1.ttl}")
    
    # Update preserves metadata
    client.update_sync("doc:1", {"content": "v2"})
    meta2 = client.get_metadata_sync("doc:1")
    if meta2:
        print(f"After update - Version: {meta2.version}, Tags: {meta2.tags}, TTL: {meta2.ttl}")
    
    # Store overwrites
    client.store_sync("doc:1", {"content": "v3"})
    meta3 = client.get_metadata_sync("doc:1")
    if meta3:
        print(f"After store - Version: {meta3.version}, Tags: {meta3.tags}, TTL: {meta3.ttl}")
    
    # Change namespace
    print("\n--- Testing namespace change ---")
    client.set_namespace("production")
    print(f"Changed to namespace: {client.get_namespace()}")
    
    # Data from previous namespace not accessible
    user = client.retrieve_sync("user:1")
    print(f"user:1 in production namespace: {user}")  # Should be None
    
    # Store in new namespace
    client.store_sync("prod:data", {"env": "production"})
    prod_data = client.retrieve_sync("prod:data")
    print(f"Retrieved prod:data: {prod_data}")
    
    # List keys
    keys = client.list_keys_sync("*")
    print(f"All keys in production: {keys}")
    
    # Switch back
    client.set_namespace("test_sync")
    user = client.retrieve_sync("user:1")
    print(f"user:1 back in test_sync namespace: {user}")


async def test_multiple_clients():
    """Test that multiple clients see the same data"""
    print("\n" + "=" * 50)
    print("Testing multiple clients (distributed behavior)...")
    
    # Create two clients
    client1 = NATSKeyStore("localhost", 4222, "shared")
    client2 = NATSKeyStore("localhost", 4222, "shared")
    
    # Client 1 stores data
    await client1.store("shared:data", {"from": "client1", "value": 42})
    print("Client1 stored data")
    
    # Client 2 retrieves it immediately
    data = await client2.retrieve("shared:data")
    print(f"Client2 retrieved: {data}")
    
    # Client 2 updates it
    await client2.update("shared:data", {"from": "client2", "value": 100})
    print("Client2 updated data")
    
    # Client 1 sees the update
    data = await client1.retrieve("shared:data")
    print(f"Client1 sees update: {data}")
    
    # Get metadata to check version
    meta = await client1.get_metadata("shared:data")
    if meta:
        print(f"Version after update: {meta.version}")


async def test_ttl():
    """Test TTL functionality"""
    print("\n" + "=" * 50)
    print("Testing TTL...")
    
    client = NATSKeyStore("localhost", 4222, "ttl_test")
    
    # Store with short TTL
    await client.store("temp:data", "temporary", KeyFormat.TEXT, ttl=2)
    
    # Retrieve immediately
    value = await client.retrieve("temp:data")
    print(f"Value before TTL expires: {value}")
    
    # Wait for TTL
    print("Waiting 3 seconds for TTL to expire...")
    await asyncio.sleep(3)
    
    # Try to retrieve after TTL
    value = await client.retrieve("temp:data")
    print(f"Value after TTL expires: {value}")  # Should be None


async def run_server():
    """Run the KV server"""
    server = NATSKeyValueServer("localhost", 4222)
    await server.start()


async def main():
    """Run all tests"""
    print("NATS Distributed Key-Value Store Tests")
    print("=" * 50)
    print("NOTE: Make sure NATS server is running on localhost:4222")
    print("Also run the KV server in a separate terminal:")
    print("  python this_script.py --server")
    print("=" * 50)
    
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--server":
        print("Starting KV Server...")
        await run_server()
    else:
        try:
            # Test sync operations
            test_sync_operations()
            
            # Test distributed behavior
            await test_multiple_clients()
            
            # Test TTL
            await test_ttl()
            
            print("\n" + "=" * 50)
            print("All tests completed!")
            
        except Exception as e:
            print(f"\nTest failed with error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())