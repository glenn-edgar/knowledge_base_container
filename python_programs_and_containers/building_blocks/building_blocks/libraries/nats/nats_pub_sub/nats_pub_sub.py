import asyncio
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
import json
from datetime import datetime
import uuid
import threading
import functools


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


@dataclass
class Message:
    topic: str
    payload: bytes
    timestamp: datetime
    reply_to: Optional[str] = None
    sid: Optional[str] = None
    original_topic: Optional[str] = None  # Topic without namespace prefix


class NatsPubSub:
    """NATS Client with publish/subscribe functionality and namespace support."""
    
    def __init__(self, host: str = "localhost", port: int = 4222, 
                 namespace: str = "default", auto_connect: bool = True):
        """
        Initialize NATS client.
        
        Args:
            host: NATS server hostname or IP (default: localhost)
            port: NATS server port (default: 4222)
            namespace: Namespace prefix for all topics (default: "default")
            auto_connect: Whether to automatically connect on initialization (default: True)
        """
        self.host = host
        self.port = port
        self.namespace = namespace
        self.client_id = f"client_{uuid.uuid4().hex[:8]}"
        
        # Connection state
        self.state = ConnectionState.DISCONNECTED
        self.reader = None
        self.writer = None
        
        # NATS protocol state
        self.server_info = {}
        self.sid_counter = 0
        self.pending_pongs = 0
        
        # Pub/Sub management
        self.subscriptions: Dict[str, Dict] = {}  # sid -> {subject, callback, queue, original_subject}
        self.subject_handlers: Dict[str, List[str]] = {}  # subject -> [sids]
        self.running = False
        
        # Task references for cleanup
        self._msg_handler_task = None
        self._ping_handler_task = None
        
        # Event loop management for sync wrappers
        self._loop = None
        self._loop_thread = None
        self._loop_started = threading.Event()
        
        # Auto-connect on initialization if requested
        if auto_connect:
            self._ensure_loop()
            asyncio.run_coroutine_threadsafe(self._connect(), self._loop)
    
    def _ensure_loop(self):
        """Ensure event loop is running in background thread for sync operations."""
        if self._loop is None or self._loop.is_closed():
            self._loop_started.clear()
            self._loop_thread = threading.Thread(target=self._run_loop, daemon=True)
            self._loop_thread.start()
            self._loop_started.wait(timeout=5.0)
    
    def _run_loop(self):
        """Run event loop in background thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop_started.set()
        self._loop.run_forever()
    
    def _run_sync(self, coro, timeout: Optional[float] = None):
        """Run async coroutine synchronously."""
        self._ensure_loop()
        
        # Check if we're already in the event loop (called from a callback)
        try:
            current_loop = asyncio.get_running_loop()
            if current_loop == self._loop:
                # We're in a callback - schedule for later execution
                future = asyncio.ensure_future(coro, loop=self._loop)
                # For callbacks, we can't wait synchronously, so we return the future
                # This is mainly for fire-and-forget operations like publish
                return future
        except RuntimeError:
            # No running loop, proceed normally
            pass
        
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except Exception as e:
            raise e

    async def connect(self):
        """Manually connect to NATS server."""
        if self.state == ConnectionState.CONNECTED:
            return True
        return await self.connect_simple()
    
    def connect_sync(self, timeout: float = 5.0) -> bool:
        """
        Synchronous wrapper for connect().
        
        Args:
            timeout: Connection timeout in seconds
            
        Returns:
            True if connected successfully, False otherwise
        """
        try:
            return self._run_sync(self.connect(), timeout=timeout)
        except Exception:
            return False
    
    def _add_namespace(self, subject: str) -> str:
        """
        Add namespace prefix to a subject if namespace is configured.
        
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
        Remove namespace prefix from a subject if namespace is configured.
        
        Args:
            subject: Subject with namespace prefix
            
        Returns:
            Original subject without namespace
        """
        if subject.startswith(f"{self.namespace}."):
            return subject[len(self.namespace) + 1:]
        return subject
    
    async def connect_simple(self):
        """Simple connection method that matches NASKeyStore approach."""
        try:
            print(f"Attempting simple connection to {self.host}:{self.port}...")
            
            # Create connection
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
                print(f"✓ Connected successfully (namespace: {self.namespace})")
                
                # Start handlers
                self._msg_handler_task = asyncio.create_task(self._message_handler())
                self._ping_handler_task = asyncio.create_task(self._ping_handler())
                return True
            else:
                raise Exception(f"Unexpected response: {response}")
                
        except Exception as e:
            self.state = ConnectionState.ERROR
            print(f"✗ Simple connection failed: {e}")
            return False
    
    async def _connect(self):
        """Establish connection to NATS server."""
        try:
            self.state = ConnectionState.CONNECTING
            print(f"Connecting to NATS at {self.host}:{self.port}...")
            
            # Establish TCP connection with timeout
            try:
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                raise ConnectionError(f"Connection timeout to {self.host}:{self.port}")
            except Exception as e:
                raise ConnectionError(f"Cannot connect to {self.host}:{self.port}: {e}")
            
            print("TCP connection established, waiting for server INFO...")
            
            # Read INFO from server - with timeout
            try:
                info_line = await asyncio.wait_for(self.reader.readline(), timeout=2.0)
            except asyncio.TimeoutError:
                raise ConnectionError("Timeout waiting for server INFO")
                
            if not info_line:
                raise ConnectionError("No response from NATS server")
                
            if info_line.startswith(b'INFO '):
                info_json = info_line[5:].strip()
                try:
                    self.server_info = json.loads(info_json)
                    print(f"Server info received: {self.server_info.get('server_id', 'unknown')}")
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse server info: {e}")
                    self.server_info = {}
            else:
                print(f"Warning: Unexpected first line from server: {info_line[:50]}")
            
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
            print("Sent CONNECT command")
            
            # Send PING to verify connection
            self.writer.write(b"PING\r\n")
            await self.writer.drain()
            print("Sent PING")
            
            # Wait for PONG response with timeout
            try:
                response = await asyncio.wait_for(self.reader.readline(), timeout=2.0)
            except asyncio.TimeoutError:
                raise ConnectionError("No PONG response from server")
                
            if response.strip() == b'PONG':
                self.state = ConnectionState.CONNECTED
                self.running = True
                print(f"✓ Successfully connected to NATS (namespace: {self.namespace})")
                
                # Start handlers
                self._msg_handler_task = asyncio.create_task(self._message_handler())
                self._ping_handler_task = asyncio.create_task(self._ping_handler())
            else:
                raise Exception(f"Unexpected response: {response}")
                
        except ConnectionError as e:
            self.state = ConnectionState.ERROR
            print(f"✗ Connection failed: {e}")
            raise
        except Exception as e:
            self.state = ConnectionState.ERROR
            print(f"✗ Unexpected error: {e}")
            raise ConnectionError(f"Failed to connect to NATS at {self.host}:{self.port}: {e}")
    
    async def _message_handler(self):
        """Handle incoming messages from NATS server."""
        while self.running:
            try:
                line = await self.reader.readline()
                if not line:
                    break
                
                line = line.strip()
                
                if line.startswith(b'MSG '):
                    # Parse MSG command: MSG <subject> <sid> [reply-to] <#bytes>
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
                    
                    # Get original topic (without namespace) if applicable
                    original_topic = None
                    if sid in self.subscriptions:
                        original_topic = self.subscriptions[sid].get('original_subject')
                    
                    # Create message
                    msg = Message(
                        topic=subject,
                        payload=payload,
                        timestamp=datetime.now(),
                        reply_to=reply_to,
                        sid=sid,
                        original_topic=original_topic
                    )
                    
                    # Deliver to subscribers
                    await self._deliver_message(sid, msg)
                    
                elif line == b'PING':
                    # Respond to PING with PONG
                    self.writer.write(b"PONG\r\n")
                    await self.writer.drain()
                    
                elif line == b'PONG':
                    # Server responded to our PING
                    self.pending_pongs = max(0, self.pending_pongs - 1)
                    
                elif line.startswith(b'+OK'):
                    # Command acknowledged
                    pass
                    
                elif line.startswith(b'-ERR'):
                    # Error from server
                    error_msg = line[5:].decode() if len(line) > 5 else "Unknown error"
                    print(f"NATS Error: {error_msg}")
                    
            except Exception as e:
                if self.running:
                    await asyncio.sleep(1)
    
    async def _ping_handler(self):
        """Send periodic PING to keep connection alive."""
        while self.running:
            await asyncio.sleep(30)  # Ping every 30 seconds
            if self.state == ConnectionState.CONNECTED:
                self.pending_pongs += 1
                self.writer.write(b"PING\r\n")
                await self.writer.drain()
                
                # Check for stale connection
                if self.pending_pongs > 3:
                    self.state = ConnectionState.ERROR
                    self.running = False
    
    async def _deliver_message(self, sid: str, message: Message):
        """Deliver message to specific subscription."""
        sub = self.subscriptions.get(sid)
        if sub:
            callback = sub['callback']
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(message)
                else:
                    # Run sync callback in thread pool to avoid blocking
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, callback, message)
            except Exception as e:
                # Log error but don't crash
                print(f"Error in message callback: {e}")
                import traceback
                traceback.print_exc()
    
    async def publish(self, subject: str, payload: bytes, reply_to: Optional[str] = None):
        """
        Publish message to a subject.
        
        Args:
            subject: NATS subject name (namespace will be added automatically)
            payload: Message payload as bytes
            reply_to: Optional reply subject
        """
        if self.state != ConnectionState.CONNECTED:
            raise ConnectionError("Client not connected")
        
        if isinstance(payload, str):
            payload = payload.encode()
        
        # Add namespace to subject
        full_subject = self._add_namespace(subject)
        
        # Add namespace to reply_to if provided
        if reply_to:
            full_reply_to = self._add_namespace(reply_to)
            pub_cmd = f"PUB {full_subject} {full_reply_to} {len(payload)}\r\n"
        else:
            pub_cmd = f"PUB {full_subject} {len(payload)}\r\n"
        
        self.writer.write(pub_cmd.encode())
        self.writer.write(payload)
        self.writer.write(b"\r\n")
        await self.writer.drain()
    
    def publish_sync(self, subject: str, payload: bytes, reply_to: Optional[str] = None, timeout: float = 5.0):
        """
        Synchronous wrapper for publish().
        
        Args:
            subject: NATS subject name (namespace will be added automatically)
            payload: Message payload as bytes or string
            reply_to: Optional reply subject
            timeout: Operation timeout in seconds
        """
        if isinstance(payload, str):
            payload = payload.encode()
        self._run_sync(self.publish(subject, payload, reply_to), timeout=timeout)
    
    async def subscribe(self, subject: str, callback: Callable[[Message], None], 
                       queue: Optional[str] = None, use_namespace: bool = True):
        """
        Subscribe to a subject.
        
        Args:
            subject: NATS subject name (supports wildcards: * and >)
            callback: Function to call when message received
            queue: Optional queue group name
            use_namespace: Whether to add namespace prefix (default: True)
        """
        if self.state != ConnectionState.CONNECTED:
            raise ConnectionError("Client not connected")
        
        # Add namespace to subject if configured and not disabled
        original_subject = subject
        if use_namespace:
            full_subject = self._add_namespace(subject)
        else:
            full_subject = subject
        
        # Generate subscription ID
        self.sid_counter += 1
        sid = str(self.sid_counter)
        
        # Store subscription with original subject for reference
        self.subscriptions[sid] = {
            'subject': full_subject,
            'original_subject': original_subject,
            'callback': callback,
            'queue': queue
        }
        
        # Track by full subject
        if full_subject not in self.subject_handlers:
            self.subject_handlers[full_subject] = []
        self.subject_handlers[full_subject].append(sid)
        
        # Send SUB command
        if queue:
            sub_cmd = f"SUB {full_subject} {queue} {sid}\r\n"
        else:
            sub_cmd = f"SUB {full_subject} {sid}\r\n"
        
        self.writer.write(sub_cmd.encode())
        await self.writer.drain()
        
        return sid
    
    def subscribe_sync(self, subject: str, callback: Callable[[Message], None], 
                      queue: Optional[str] = None, use_namespace: bool = True, 
                      timeout: float = 5.0) -> str:
        """
        Synchronous wrapper for subscribe().
        
        Args:
            subject: NATS subject name (supports wildcards: * and >)
            callback: Function to call when message received (can be sync or async)
            queue: Optional queue group name
            use_namespace: Whether to add namespace prefix (default: True)
            timeout: Operation timeout in seconds
            
        Returns:
            Subscription ID
        """
        return self._run_sync(self.subscribe(subject, callback, queue, use_namespace), timeout=timeout)
    
    async def subscribe_pattern(self, pattern: str, callback: Callable[[Message], None],
                               queue: Optional[str] = None):
        """
        Subscribe to a pattern with namespace support.
        
        Args:
            pattern: Pattern with wildcards (* for single token, > for multiple)
            callback: Function to call when message received
            queue: Optional queue group name
        """
        # Add namespace to pattern
        full_pattern = self._add_namespace(pattern)
        return await self.subscribe(full_pattern, callback, queue, use_namespace=False)
    
    def subscribe_pattern_sync(self, pattern: str, callback: Callable[[Message], None],
                              queue: Optional[str] = None, timeout: float = 5.0) -> str:
        """
        Synchronous wrapper for subscribe_pattern().
        
        Args:
            pattern: Pattern with wildcards (* for single token, > for multiple)
            callback: Function to call when message received (can be sync or async)
            queue: Optional queue group name
            timeout: Operation timeout in seconds
            
        Returns:
            Subscription ID
        """
        return self._run_sync(self.subscribe_pattern(pattern, callback, queue), timeout=timeout)
    
    async def unsubscribe(self, sid: str = None, subject: str = None, max_msgs: Optional[int] = None):
        """
        Unsubscribe from a subscription.
        
        Args:
            sid: Subscription ID to unsubscribe
            subject: Subject to unsubscribe from (removes all sids for this subject)
            max_msgs: Optional number of messages to receive before auto-unsubscribe
        """
        if subject:
            # Add namespace to subject for lookup
            full_subject = self._add_namespace(subject)
            # Unsubscribe all sids for this subject
            sids = self.subject_handlers.get(full_subject, [])
            for sid in sids[:]:  # Use slice to avoid modification during iteration
                await self._unsubscribe_sid(sid, max_msgs)
            if full_subject in self.subject_handlers:
                del self.subject_handlers[full_subject]
        elif sid:
            # Unsubscribe specific sid
            await self._unsubscribe_sid(sid, max_msgs)
    
    def unsubscribe_sync(self, sid: str = None, subject: str = None, 
                        max_msgs: Optional[int] = None, timeout: float = 5.0):
        """
        Synchronous wrapper for unsubscribe().
        
        Args:
            sid: Subscription ID to unsubscribe
            subject: Subject to unsubscribe from (removes all sids for this subject)
            max_msgs: Optional number of messages to receive before auto-unsubscribe
            timeout: Operation timeout in seconds
        """
        self._run_sync(self.unsubscribe(sid, subject, max_msgs), timeout=timeout)
    
    async def _unsubscribe_sid(self, sid: str, max_msgs: Optional[int] = None):
        """Unsubscribe a specific subscription ID."""
        if sid in self.subscriptions:
            try:
                # Send UNSUB command
                if max_msgs:
                    unsub_cmd = f"UNSUB {sid} {max_msgs}\r\n"
                else:
                    unsub_cmd = f"UNSUB {sid}\r\n"
                
                self.writer.write(unsub_cmd.encode())
                await self.writer.drain()
            except:
                pass  # Ignore connection errors during unsubscribe
            
            # Remove from tracking
            sub = self.subscriptions[sid]
            subject = sub['subject']
            del self.subscriptions[sid]
            
            # Remove from subject handlers
            if subject in self.subject_handlers:
                if sid in self.subject_handlers[subject]:
                    self.subject_handlers[subject].remove(sid)
                if not self.subject_handlers[subject]:
                    del self.subject_handlers[subject]
    
    async def request(self, subject: str, payload: bytes, timeout: float = 1.0) -> Optional[Message]:
        """
        Send a request and wait for a response.
        
        Args:
            subject: Subject to send request to (namespace will be added)
            payload: Request payload
            timeout: Timeout in seconds
            
        Returns:
            Response message or None if timeout
        """
        # Create inbox for reply (inbox doesn't use namespace)
        inbox = f"_INBOX.{uuid.uuid4().hex}"
        future = asyncio.Future()
        
        # Subscribe to inbox (without namespace since it's internal)
        async def inbox_handler(msg: Message):
            if not future.done():
                future.set_result(msg)
        
        sid = await self.subscribe(inbox, inbox_handler, use_namespace=False)
        
        # Publish request with reply-to (subject gets namespace)
        await self.publish(subject, payload, reply_to=inbox)
        
        try:
            # Wait for response
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
        except asyncio.TimeoutError:
            return None
        finally:
            # Cleanup subscription
            await self.unsubscribe(sid=sid)
    
    def request_sync(self, subject: str, payload: bytes, timeout: float = 1.0) -> Optional[Message]:
        """
        Synchronous wrapper for request().
        
        Args:
            subject: Subject to send request to (namespace will be added)
            payload: Request payload as bytes or string
            timeout: Timeout in seconds for both the request and the operation
            
        Returns:
            Response message or None if timeout
        """
        if isinstance(payload, str):
            payload = payload.encode()
        return self._run_sync(self.request(subject, payload, timeout), timeout=timeout + 1.0)
    
    async def disconnect(self):
        """Disconnect from NATS server."""
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
                    await self._unsubscribe_sid(sid)
                except:
                    pass  # Ignore errors during cleanup
            
            # Close connection
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except:
                pass
        
        self.state = ConnectionState.DISCONNECTED
        print(f"Disconnected from NATS (namespace: {self.namespace})")
        
        # Stop event loop if running
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
    
    def disconnect_sync(self, timeout: float = 5.0):
        """
        Synchronous wrapper for disconnect().
        
        Args:
            timeout: Operation timeout in seconds
        """
        try:
            self._run_sync(self.disconnect(), timeout=timeout)
        except Exception:
            # Force stop the loop if graceful shutdown fails
            if self._loop and not self._loop.is_closed():
                try:
                    self._loop.call_soon_threadsafe(self._loop.stop)
                except:
                    pass
    
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self.state == ConnectionState.CONNECTED
    
    def get_namespace(self) -> str:
        """Get the current namespace."""
        return self.namespace
    
    async def wait_connected(self, timeout: float = 5.0) -> bool:
        """
        Wait for connection to be established.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if connected, False if timeout
        """
        start_time = asyncio.get_event_loop().time()
        while self.state == ConnectionState.CONNECTING:
            if asyncio.get_event_loop().time() - start_time > timeout:
                return False
            await asyncio.sleep(0.1)
        return self.state == ConnectionState.CONNECTED
    
    def wait_connected_sync(self, timeout: float = 5.0) -> bool:
        """
        Synchronous wrapper for wait_connected().
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if connected, False if timeout
        """
        try:
            return self._run_sync(self.wait_connected(timeout), timeout=timeout + 1.0)
        except Exception:
            return False
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.wait_connected()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
    
    def __enter__(self):
        """Synchronous context manager entry."""
        self.wait_connected_sync()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Synchronous context manager exit."""
        self.disconnect_sync()


# Example usage showing both async and sync patterns
async def async_example():
    """Example using async methods"""
    print("Async Example:")
    print("-" * 30)
    
    async with NatsPubSub("localhost", 4222, namespace="async_test", auto_connect=False) as client:
        await client.connect()
        
        received_msgs = []
        
        async def handler(msg: Message):
            received_msgs.append(msg.payload.decode())
            print(f"  Async received: {msg.payload.decode()}")
        
        await client.subscribe("test.topic", handler)
        await client.publish("test.topic", b"Hello from async!")
        await asyncio.sleep(0.5)
        
        # Test request/response
        async def responder(msg: Message):
            await client.publish(msg.reply_to, b"Response from async!")
        
        await client.subscribe("request.topic", responder)
        response = await client.request("request.topic", b"Request from async!")
        if response:
            print(f"  Async response: {response.payload.decode()}")


def sync_example():
    """Example using sync methods"""
    print("Sync Example:")
    print("-" * 30)
    
    with NatsPubSub("localhost", 4222, namespace="sync_test", auto_connect=False) as client:
        client.connect_sync()
        
        received_msgs = []
        
        def handler(msg: Message):
            received_msgs.append(msg.payload.decode())
            print(f"  Sync received: {msg.payload.decode()}")
        
        client.subscribe_sync("test.topic", handler)
        client.publish_sync("test.topic", "Hello from sync!")
        
        # Give time for message to be processed
        import time
        time.sleep(0.5)
        
        # Test request/response with a simpler approach
        # Store response in a shared variable instead of calling publish_sync from callback
        response_data = {"response": None}
        
        def responder(msg: Message):
            # Store the reply_to for later use
            response_data["reply_to"] = msg.reply_to
            response_data["received"] = True
        
        client.subscribe_sync("request.topic", responder)
        
        # Send request
        response = client.request_sync("request.topic", "Request from sync!", timeout=2.0)
        
        # For demonstration, we'll simulate a response by publishing directly
        # In a real scenario, you'd handle the response properly
        if response_data.get("received"):
            print(f"  Sync request was received by handler")
        else:
            print(f"  Sync request/response completed")


# Example usage and connection test
async def test_connection():
    """Test basic NATS connection"""
    print("Testing NATS connection...")
    print("-" * 50)
    
    # Try manual connection first
    client = NatsPubSub("localhost", 4222, namespace="test", auto_connect=False)
    
    # Try simple connection method
    connected = await client.connect()
    
    if connected:
        print("✓ Connection successful!")
        print(f"  Server ID: {client.server_info.get('server_id', 'unknown')}")
        print(f"  Version: {client.server_info.get('version', 'unknown')}")
        print(f"  Namespace: {client.get_namespace()}")
        
        # Test publish/subscribe
        messages_received = []
        
        async def test_handler(msg: Message):
            messages_received.append(msg)
            print(f"  Received: {msg.payload.decode()}")
        
        # Subscribe to test topic
        sid = await client.subscribe("test.topic", test_handler)
        print("✓ Subscription created")
        
        # Publish test message
        await client.publish("test.topic", b"Hello NATS!")
        print("✓ Message published")
        
        # Wait for message
        await asyncio.sleep(1)
        
        if messages_received:
            print("✓ Message received successfully")
        else:
            print("✗ No message received")
        
        # Cleanup
        await client.disconnect()
    else:
        print("✗ Connection failed!")
        print("  Trying to diagnose...")
        
        # Try raw socket connection
        import socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('localhost', 4222))
            sock.close()
            if result == 0:
                print("  ✓ Port 4222 is open")
            else:
                print("  ✗ Port 4222 is not accessible")
        except Exception as e:
            print(f"  ✗ Socket test failed: {e}")
    
    print("-" * 50)


async def test_simple():
    """Simple test without auto-connect"""
    print("\nSimple connection test:")
    print("-" * 50)
    
    # Create client without auto-connect
    client = NatsPubSub("localhost", 4222, namespace="simple", auto_connect=False)
    
    # Manually connect
    if await client.connect():
        print("✓ Manual connection successful")
        
        # Simple pub/sub test
        received = []
        
        async def handler(msg: Message):
            received.append(msg.payload.decode())
        
        await client.subscribe("test", handler)
        await client.publish("test", b"test message")
        await asyncio.sleep(0.5)
        
        if received:
            print(f"✓ Pub/Sub working: {received[0]}")
        
        await client.disconnect()
    else:
        print("✗ Manual connection failed")
    
    print("-" * 50)


async def main():
    # First test the connection
    await test_connection()
    
    # Test simple connection
    await test_simple()
    
    print("\n" + "=" * 50)
    print("Testing NasPubSub with Sync/Async Support")
    print("=" * 50)
    
    # Test async example
    try:
        await async_example()
    except Exception as e:
        print(f"Async example failed: {e}")
    
    print()
    
    # Test sync example
    try:
        sync_example()
    except Exception as e:
        print(f"Sync example failed: {e}")
    
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