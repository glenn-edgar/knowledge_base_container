import asyncio
from typing import Optional, Any, Dict
from collections import deque
import json
from datetime import datetime
import uuid
from enum import Enum


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


class NASQueue:
    """
    NAS Queue with push/pop functionality and namespace support.
    
    This client manages a connection to a NATS server and provides
    methods to push and pop messages from a queue with namespace isolation.
    """
    
    def __init__(self, host: str = "localhost", port: int = 4222, 
                 namespace: str = "default"):
        """
        Initialize NAS Queue client.
        
        Args:
            host: NATS server hostname or IP (default: localhost)
            port: NATS server port (default: 4222)
            namespace: Namespace prefix for all subjects (default: "default")
        """
        self.host = host
        self.port = port
        self.namespace = namespace
        self.client_id = f"queue_{uuid.uuid4().hex[:8]}"
        
        # Connection state
        self.state = ConnectionState.DISCONNECTED
        self.reader = None
        self.writer = None
        
        # NATS protocol state
        self.server_info = {}
        self.sid_counter = 0
        self.pending_pongs = 0
        
        # Queue management
        self.message_queue = deque()
        self.subscriptions: Dict[str, Dict] = {}  # sid -> {subject, callback, queue}
        self.running = False
        
        # Task references
        self._msg_handler_task = None
        self._ping_handler_task = None
        
        # Don't auto-connect - let user call connect() or wait_connected()
        self._connect_task = None
    
    def _add_namespace(self, subject: str) -> str:
        """
        Add namespace prefix to a subject.
        
        Args:
            subject: Original subject
            
        Returns:
            Subject with namespace prefix
        """
        # Don't add namespace to internal NATS subjects (starting with _)
        if subject.startswith('_'):
            return subject
        return f"{self.namespace}.{subject}"
    
    def _remove_namespace(self, subject: str) -> str:
        """
        Remove namespace prefix from a subject.
        
        Args:
            subject: Subject with namespace prefix
            
        Returns:
            Original subject without namespace
        """
        if subject.startswith(f"{self.namespace}."):
            return subject[len(self.namespace) + 1:]
        return subject
    
    async def _connect(self):
        """Establish connection to NATS server."""
        try:
            self.state = ConnectionState.CONNECTING
            
            # Establish TCP connection
            self.reader, self.writer = await asyncio.open_connection(
                self.host, self.port
            )
            
            # Read INFO from server
            info_line = await self.reader.readline()
            if info_line.startswith(b'INFO '):
                info_json = info_line[5:].strip()
                self.server_info = json.loads(info_json)
            
            # Send CONNECT command
            connect_options = {
                "verbose": False,
                "pedantic": False,
                "name": self.client_id,
                "lang": "python",
                "version": "1.0.0",
                "protocol": 1
            }
            
            connect_cmd = f"CONNECT {json.dumps(connect_options)}\r\n"
            self.writer.write(connect_cmd.encode())
            await self.writer.drain()
            
            # Send PING to verify
            self.writer.write(b"PING\r\n")
            await self.writer.drain()
            
            # Wait for PONG
            response = await self.reader.readline()
            if response.strip() == b'PONG':
                self.state = ConnectionState.CONNECTED
                self.running = True
                print(f"Connected to NATS at {self.host}:{self.port} (namespace: {self.namespace})")
                
                # Start handlers
                self._msg_handler_task = asyncio.create_task(self._message_handler())
                self._ping_handler_task = asyncio.create_task(self._ping_handler())
                return True
            else:
                raise Exception(f"Unexpected response: {response}")
                
        except Exception as e:
            self.state = ConnectionState.ERROR
            print(f"Failed to connect: {e}")
            return False
    
    async def _message_handler(self):
        """Handle incoming messages from NATS server."""
        while self.running:
            try:
                line = await self.reader.readline()
                if not line:
                    break
                
                line = line.strip()
                
                if line.startswith(b'MSG '):
                    # Parse MSG command
                    parts = line[4:].split(b' ')
                    if len(parts) == 3:
                        subject, sid, size = parts
                        reply_to = None
                    elif len(parts) == 4:
                        subject, sid, reply_to, size = parts
                        reply_to = reply_to.decode()
                    else:
                        continue
                    
                    subject = subject.decode()
                    sid = sid.decode()
                    size = int(size)
                    
                    # Read payload
                    payload = await self.reader.readexactly(size)
                    await self.reader.readline()  # Read trailing \r\n
                    
                    # Process message for this subscription
                    if sid in self.subscriptions:
                        # Add to queue with original subject (without namespace)
                        original_subject = self._remove_namespace(subject)
                        self.message_queue.append({
                            'subject': original_subject,
                            'message': payload,
                            'headers': None,
                            'reply_to': reply_to,
                            'timestamp': datetime.now()
                        })
                        print(f"Queued message on '{original_subject}': {payload.decode()[:50]}...")
                    
                elif line == b'PING':
                    self.writer.write(b"PONG\r\n")
                    await self.writer.drain()
                    
                elif line == b'PONG':
                    self.pending_pongs = max(0, self.pending_pongs - 1)
                    
                elif line.startswith(b'+OK'):
                    pass
                    
                elif line.startswith(b'-ERR'):
                    error_msg = line[5:].decode() if len(line) > 5 else "Unknown error"
                    print(f"NATS Error: {error_msg}")
                    
            except Exception as e:
                if self.running:
                    await asyncio.sleep(1)
    
    async def _ping_handler(self):
        """Send periodic PING to keep connection alive."""
        while self.running:
            await asyncio.sleep(30)
            if self.state == ConnectionState.CONNECTED:
                self.pending_pongs += 1
                self.writer.write(b"PING\r\n")
                await self.writer.drain()
                
                if self.pending_pongs > 3:
                    self.state = ConnectionState.ERROR
                    self.running = False
    
    async def connect(self):
        """
        Manually connect to NATS server.
        
        Returns:
            True if connected successfully, False otherwise
        """
        if self.state == ConnectionState.CONNECTED:
            return True
        
        success = await self._connect()
        if not success:
            # Try one more time
            await asyncio.sleep(0.5)
            success = await self._connect()
        
        return success
    
    async def wait_connected(self, timeout: float = 5.0) -> bool:
        """
        Wait for connection to be established.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if connected, False if timeout
        """
        # First try to connect if not already connected
        if self.state == ConnectionState.DISCONNECTED:
            self._connect_task = asyncio.create_task(self._connect())
        
        start_time = asyncio.get_event_loop().time()
        while self.state != ConnectionState.CONNECTED:
            if asyncio.get_event_loop().time() - start_time > timeout:
                return False
            if self.state == ConnectionState.ERROR:
                return False
            await asyncio.sleep(0.1)
        return True
    
    async def push(self, subject: str, message: Any, headers: Optional[Dict] = None):
        """
        Push a message to the queue and publish it to the specified subject.
        
        Args:
            subject: NATS subject to publish to (namespace will be added)
            message: Message to send (will be encoded as bytes if string)
            headers: Optional headers (not used in basic NATS protocol)
        """
        if self.state != ConnectionState.CONNECTED:
            await self.wait_connected()
            if self.state != ConnectionState.CONNECTED:
                raise ConnectionError("Not connected to NATS")
        
        # Convert message to bytes
        if isinstance(message, str):
            message_bytes = message.encode()
        elif isinstance(message, bytes):
            message_bytes = message
        else:
            # For other types, convert to JSON
            message_bytes = json.dumps(message).encode()
        
        # Add to internal queue with original subject
        self.message_queue.append({
            'subject': subject,  # Store original subject without namespace
            'message': message_bytes,
            'headers': headers,
            'reply_to': None,
            'timestamp': datetime.now()
        })
        
        # Publish to NATS with namespace
        full_subject = self._add_namespace(subject)
        pub_cmd = f"PUB {full_subject} {len(message_bytes)}\r\n"
        
        self.writer.write(pub_cmd.encode())
        self.writer.write(message_bytes)
        self.writer.write(b"\r\n")
        await self.writer.drain()
        
        print(f"Pushed message to '{subject}': {message_bytes.decode()[:50]}...")
    
    async def pop(self, timeout: Optional[float] = None) -> Optional[Dict]:
        """
        Pop a message from the local queue.
        
        Args:
            timeout: Optional timeout in seconds to wait for a message
            
        Returns:
            Dictionary containing subject, message, headers, and timestamp, or None if queue is empty
        """
        if timeout:
            # Wait for a message with timeout
            start_time = asyncio.get_event_loop().time()
            while not self.message_queue:
                if asyncio.get_event_loop().time() - start_time > timeout:
                    return None
                await asyncio.sleep(0.1)
        
        if self.message_queue:
            message_data = self.message_queue.popleft()
            print(f"Popped message from '{message_data['subject']}'")
            return message_data
        else:
            print("Queue is empty")
            return None
    
    async def subscribe_and_queue(self, subject: str, queue_group: str = ""):
        """
        Subscribe to a subject and automatically add received messages to the queue.
        
        Args:
            subject: NATS subject to subscribe to (namespace will be added)
            queue_group: Optional queue group name for load balancing
        """
        if self.state != ConnectionState.CONNECTED:
            await self.wait_connected()
            if self.state != ConnectionState.CONNECTED:
                raise ConnectionError("Not connected to NATS")
        
        # Add namespace to subject
        full_subject = self._add_namespace(subject)
        
        # Generate subscription ID
        self.sid_counter += 1
        sid = str(self.sid_counter)
        
        # Store subscription
        self.subscriptions[sid] = {
            'subject': full_subject,
            'original_subject': subject,
            'queue_group': queue_group
        }
        
        # Send SUB command
        if queue_group:
            sub_cmd = f"SUB {full_subject} {queue_group} {sid}\r\n"
        else:
            sub_cmd = f"SUB {full_subject} {sid}\r\n"
        
        self.writer.write(sub_cmd.encode())
        await self.writer.drain()
        
        print(f"Subscribed to '{subject}'" + (f" with queue group '{queue_group}'" if queue_group else ""))
        return sid
    
    async def unsubscribe(self, sid: str = None):
        """
        Unsubscribe from a subscription.
        
        Args:
            sid: Subscription ID to unsubscribe. If None, unsubscribe all.
        """
        if sid:
            if sid in self.subscriptions:
                unsub_cmd = f"UNSUB {sid}\r\n"
                self.writer.write(unsub_cmd.encode())
                await self.writer.drain()
                
                subject = self.subscriptions[sid]['original_subject']
                del self.subscriptions[sid]
                print(f"Unsubscribed from '{subject}'")
        else:
            # Unsubscribe all
            for sid in list(self.subscriptions.keys()):
                unsub_cmd = f"UNSUB {sid}\r\n"
                self.writer.write(unsub_cmd.encode())
                await self.writer.drain()
            
            self.subscriptions.clear()
            print("Unsubscribed from all subjects")
    
    def queue_size(self) -> int:
        """
        Get the current size of the message queue.
        
        Returns:
            Number of messages in the queue
        """
        return len(self.message_queue)
    
    def clear_queue(self):
        """
        Clear all messages from the queue.
        """
        self.message_queue.clear()
        print("Queue cleared")
    
    def get_namespace(self) -> str:
        """
        Get the current namespace.
        
        Returns:
            The namespace string
        """
        return self.namespace
    
    def is_connected(self) -> bool:
        """
        Check if client is connected.
        
        Returns:
            True if connected, False otherwise
        """
        return self.state == ConnectionState.CONNECTED
    
    async def disconnect(self):
        """
        Close the connection to the NATS server.
        """
        self.running = False
        
        # Cancel handler tasks
        if self._msg_handler_task:
            self._msg_handler_task.cancel()
        if self._ping_handler_task:
            self._ping_handler_task.cancel()
        
        if self.writer:
            # Unsubscribe all
            for sid in list(self.subscriptions.keys()):
                try:
                    unsub_cmd = f"UNSUB {sid}\r\n"
                    self.writer.write(unsub_cmd.encode())
                    await self.writer.drain()
                except:
                    pass
            
            # Close connection
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except:
                pass
        
        self.state = ConnectionState.DISCONNECTED
        print(f"Disconnected from NATS (namespace: {self.namespace})")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.wait_connected()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


# Example usage
async def test_connection():
    """Test basic connection."""
    print("Testing NASQueue connection...")
    print("-" * 50)
    
    queue = NASQueue("localhost", 4222, namespace="test")
    
    # Manually connect
    if await queue.connect():
        print("✓ Connection successful!")
        print(f"  Namespace: {queue.get_namespace()}")
        
        # Test basic push/pop
        await queue.push("test", "Hello NATS!")
        print(f"  Queue size: {queue.queue_size()}")
        
        msg = await queue.pop()
        if msg:
            print(f"  Popped: {msg['message'].decode()}")
        
        await queue.disconnect()
    else:
        print("✗ Connection failed!")
        print("  Check if NATS is running: docker ps | grep nats")
    
    print("-" * 50)


async def main():
    """
    Example demonstrating the NASQueue usage with namespaces.
    """
    # First test connection
    await test_connection()
    
    print("\n" + "=" * 50)
    print("Testing NASQueue with Namespace Support")
    print("=" * 50)
    
    # Create clients with different namespaces
    queue_prod = NASQueue("localhost", 4222, namespace="production")
    queue_dev = NASQueue("localhost", 4222, namespace="development")
    
    # Manually connect
    prod_connected = await queue_prod.connect()
    dev_connected = await queue_dev.connect()
    
    if not prod_connected:
        print("Production queue failed to connect")
        print("Make sure NATS is running: docker ps | grep nats")
        return
    if not dev_connected:
        print("Development queue failed to connect")
        return
    
    print(f"\nProduction namespace: {queue_prod.get_namespace()}")
    print(f"Development namespace: {queue_dev.get_namespace()}")
    
    # Push messages to different namespaces
    print("\n--- Pushing messages ---")
    await queue_prod.push("orders", "Order #1001 from production")
    await queue_prod.push("orders", "Order #1002 from production")
    await queue_dev.push("orders", "Test order from development")
    
    # Push different data types
    await queue_prod.push("data", {"type": "json", "value": 123})
    await queue_prod.push("data", b"Binary data")
    
    # Pop messages from local queue
    print("\n--- Popping from production queue ---")
    print(f"Queue size: {queue_prod.queue_size()}")
    while queue_prod.queue_size() > 0:
        msg = await queue_prod.pop()
        if msg:
            try:
                decoded = msg['message'].decode() if isinstance(msg['message'], bytes) else str(msg['message'])
                print(f"  Subject: {msg['subject']}, Message: {decoded[:50]}")
            except:
                print(f"  Subject: {msg['subject']}, Message: [binary data]")
    
    print("\n--- Popping from development queue ---")
    print(f"Queue size: {queue_dev.queue_size()}")
    while queue_dev.queue_size() > 0:
        msg = await queue_dev.pop()
        if msg:
            print(f"  Subject: {msg['subject']}, Message: {msg['message'].decode()[:50]}")
    
    # Test subscriptions with namespace isolation
    print("\n--- Testing subscriptions ---")
    
    # Subscribe to receive messages
    await queue_prod.subscribe_and_queue("events")
    await queue_dev.subscribe_and_queue("events")
    
    # Publish to specific namespaces
    await queue_prod.push("events", "Production event 1")
    await queue_dev.push("events", "Development event 1")
    
    # Give time for messages to be received
    await asyncio.sleep(0.5)
    
    # Check queues - each should only have their namespace's message
    print(f"\nProduction queue size: {queue_prod.queue_size()}")
    msg = await queue_prod.pop()
    if msg:
        print(f"  Prod received: {msg['message'].decode()}")
    
    print(f"\nDevelopment queue size: {queue_dev.queue_size()}")
    msg = await queue_dev.pop()
    if msg:
        print(f"  Dev received: {msg['message'].decode()}")
    
    # Test queue groups for load balancing
    print("\n--- Testing queue groups ---")
    
    # Create two workers in same namespace with same queue group
    worker1 = NASQueue("localhost", 4222, namespace="workers")
    worker2 = NASQueue("localhost", 4222, namespace="workers")
    
    if not await worker1.connect():
        print("Worker1 failed to connect")
        return
    if not await worker2.connect():
        print("Worker2 failed to connect")
        return
    
    # Subscribe both to same subject with queue group
    await worker1.subscribe_and_queue("tasks", queue_group="worker_pool")
    await worker2.subscribe_and_queue("tasks", queue_group="worker_pool")
    
    # Push multiple tasks
    for i in range(4):
        await worker1.push("tasks", f"Task {i+1}")
    
    await asyncio.sleep(0.5)
    
    # Check distribution (messages should be load balanced)
    print(f"Worker1 queue size: {worker1.queue_size()}")
    print(f"Worker2 queue size: {worker2.queue_size()}")
    
    # Pop all messages
    while worker1.queue_size() > 0:
        msg = await worker1.pop()
        if msg:
            print(f"  Worker1: {msg['message'].decode()}")
    
    while worker2.queue_size() > 0:
        msg = await worker2.pop()
        if msg:
            print(f"  Worker2: {msg['message'].decode()}")
    
    # Test context manager
    print("\n--- Testing context manager ---")
    async with NASQueue("localhost", 4222, namespace="context_test") as queue_test:
        if queue_test.is_connected():
            await queue_test.push("test_message", "Hello from context manager")
            print(f"Test queue size: {queue_test.queue_size()}")
            msg = await queue_test.pop()
            if msg:
                print(f"  Context manager message: {msg['message'].decode()}")
    
    # Cleanup
    print("\n--- Cleanup ---")
    await worker1.disconnect()
    await worker2.disconnect()
    await queue_prod.disconnect()
    await queue_dev.disconnect()
    
    print("\nTest completed!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()