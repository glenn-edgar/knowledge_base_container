import asyncio
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
import json
from datetime import datetime
import uuid


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


class NasPubSub:
    """NATS Client with publish/subscribe functionality."""
    
    def __init__(self, host: str = "localhost", port: int = 4222, client_id: Optional[str] = None):
        """
        Initialize NATS client and connect to server without password.
        
        Args:
            host: NATS server hostname or IP (default: localhost)
            port: NATS server port (default: 4222)
            client_id: Optional client identifier
        """
        self.host = host
        self.port = port
        self.client_id = client_id or f"client_{uuid.uuid4().hex[:8]}"
        
        # Connection state
        self.state = ConnectionState.DISCONNECTED
        self.reader = None
        self.writer = None
        self.is_shared_connection = False
        self.parent_client = None
        self.child_clients = []
        
        # NATS protocol state
        self.server_info = {}
        self.sid_counter = 0
        self.pending_pongs = 0
        
        # Pub/Sub management
        self.subscriptions: Dict[str, Dict] = {}  # sid -> {subject, callback, queue}
        self.subject_handlers: Dict[str, List[str]] = {}  # subject -> [sids]
        self.running = False
        
        # Auto-connect on initialization (no password required)
        asyncio.create_task(self._connect())
    
    @classmethod
    def from_connection(cls, existing_client: 'NasPubSub') -> 'NasPubSub':
        """
        Create a new NATS client instance using an existing client's connection.
        
        Args:
            existing_client: An already connected NasPubSub instance
            
        Returns:
            New NasPubSub instance sharing the connection
        """
        if existing_client.state != ConnectionState.CONNECTED:
            raise ValueError("Existing client must be connected")
        
        # Create new instance without auto-connecting
        new_client = object.__new__(cls)
        new_client.host = existing_client.host
        new_client.port = existing_client.port
        new_client.client_id = f"client_{uuid.uuid4().hex[:8]}"
        
        # Share connection resources
        new_client.state = ConnectionState.CONNECTED
        new_client.reader = existing_client.reader
        new_client.writer = existing_client.writer
        new_client.server_info = existing_client.server_info
        new_client.is_shared_connection = True  # Mark as shared
        
        # Mark original as having shared connection
        if not hasattr(existing_client, 'is_shared_connection'):
            existing_client.is_shared_connection = True
        
        # Share the subscription tracking with the parent client
        # This ensures all clients see all messages
        new_client.subscriptions = {}  # Own subscriptions
        new_client.subject_handlers = {}  # Own subject handlers
        new_client.sid_counter = existing_client.sid_counter  # Share counter
        
        # Store reference to parent for shared subscription management
        new_client.parent_client = existing_client
        existing_client.child_clients = getattr(existing_client, 'child_clients', [])
        existing_client.child_clients.append(new_client)
        
        # Share pending pongs counter
        new_client.pending_pongs = existing_client.pending_pongs
        
        new_client.running = True
        
        # Don't start a new message handler - use parent's
        
        return new_client
    
    async def _connect(self):
        """Establish connection to NATS server (no password required)."""
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
            
            # Send PING to verify connection
            self.writer.write(b"PING\r\n")
            await self.writer.drain()
            
            # Wait for PONG response
            response = await self.reader.readline()
            if response.strip() == b'PONG':
                self.state = ConnectionState.CONNECTED
                self.running = True
                
                # Start handlers
                asyncio.create_task(self._message_handler())
                asyncio.create_task(self._ping_handler())
            else:
                raise Exception(f"Unexpected response: {response}")
                
        except Exception as e:
            self.state = ConnectionState.ERROR
            raise
    
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
                    
                    # Create message
                    msg = Message(
                        topic=subject,
                        payload=payload,
                        timestamp=datetime.now(),
                        reply_to=reply_to,
                        sid=sid
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
        # Check this client's subscriptions
        sub = self.subscriptions.get(sid)
        if sub:
            callback = sub['callback']
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(message)
                else:
                    callback(message)
            except Exception as e:
                pass  # Silently ignore callback errors
        
        # Also check child clients if this is a parent
        for child in getattr(self, 'child_clients', []):
            sub = child.subscriptions.get(sid)
            if sub:
                callback = sub['callback']
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(message)
                    else:
                        callback(message)
                except Exception as e:
                    pass
    
    async def publish(self, subject: str, payload: bytes, reply_to: Optional[str] = None):
        """
        Publish message to a subject.
        
        Args:
            subject: NATS subject name
            payload: Message payload as bytes
            reply_to: Optional reply subject
        """
        if self.state != ConnectionState.CONNECTED:
            raise ConnectionError("Client not connected")
        
        if isinstance(payload, str):
            payload = payload.encode()
        
        if reply_to:
            pub_cmd = f"PUB {subject} {reply_to} {len(payload)}\r\n"
        else:
            pub_cmd = f"PUB {subject} {len(payload)}\r\n"
        
        self.writer.write(pub_cmd.encode())
        self.writer.write(payload)
        self.writer.write(b"\r\n")
        await self.writer.drain()
    
    async def subscribe(self, subject: str, callback: Callable[[Message], None], queue: Optional[str] = None):
        """
        Subscribe to a subject.
        
        Args:
            subject: NATS subject name (supports wildcards: * and >)
            callback: Function to call when message received
            queue: Optional queue group name
        """
        if self.state != ConnectionState.CONNECTED:
            raise ConnectionError("Client not connected")
        
        # For shared connections, use parent's sid_counter
        if self.parent_client:
            self.parent_client.sid_counter += 1
            sid = str(self.parent_client.sid_counter)
        else:
            self.sid_counter += 1
            sid = str(self.sid_counter)
        
        # Store subscription
        self.subscriptions[sid] = {
            'subject': subject,
            'callback': callback,
            'queue': queue
        }
        
        # Track by subject
        if subject not in self.subject_handlers:
            self.subject_handlers[subject] = []
        self.subject_handlers[subject].append(sid)
        
        # Send SUB command
        if queue:
            sub_cmd = f"SUB {subject} {queue} {sid}\r\n"
        else:
            sub_cmd = f"SUB {subject} {sid}\r\n"
        
        self.writer.write(sub_cmd.encode())
        await self.writer.drain()
        
        return sid
    
    async def unsubscribe(self, sid: str = None, subject: str = None, max_msgs: Optional[int] = None):
        """
        Unsubscribe from a subscription.
        
        Args:
            sid: Subscription ID to unsubscribe
            subject: Subject to unsubscribe from (removes all sids for this subject)
            max_msgs: Optional number of messages to receive before auto-unsubscribe
        """
        if subject:
            # Unsubscribe all sids for this subject
            sids = self.subject_handlers.get(subject, [])
            for sid in sids:
                await self._unsubscribe_sid(sid, max_msgs)
            if subject in self.subject_handlers:
                del self.subject_handlers[subject]
        elif sid:
            # Unsubscribe specific sid
            await self._unsubscribe_sid(sid, max_msgs)
    
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
                self.subject_handlers[subject].remove(sid)
                if not self.subject_handlers[subject]:
                    del self.subject_handlers[subject]
    
    async def request(self, subject: str, payload: bytes, timeout: float = 1.0) -> Optional[Message]:
        """
        Send a request and wait for a response.
        
        Args:
            subject: Subject to send request to
            payload: Request payload
            timeout: Timeout in seconds
            
        Returns:
            Response message or None if timeout
        """
        # Create inbox for reply
        inbox = f"_INBOX.{uuid.uuid4().hex}"
        future = asyncio.Future()
        
        # Subscribe to inbox
        async def inbox_handler(msg: Message):
            if not future.done():
                future.set_result(msg)
        
        sid = await self.subscribe(inbox, inbox_handler)
        
        # Publish request with reply-to
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
    
    async def disconnect(self):
        """Disconnect from NATS server."""
        self.running = False
        
        if self.writer:
            # Unsubscribe all
            for sid in list(self.subscriptions.keys()):
                try:
                    await self._unsubscribe_sid(sid)
                except:
                    pass  # Ignore errors during cleanup
            
            # Only close connection if not shared or if we're the owner
            if not self.is_shared_connection:
                self.writer.close()
                await self.writer.wait_closed()
        
        self.state = ConnectionState.DISCONNECTED
    
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self.state == ConnectionState.CONNECTED
    
    async def __aenter__(self):
        """Async context manager entry."""
        while self.state == ConnectionState.CONNECTING:
            await asyncio.sleep(0.1)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


# Example usage
async def main():
    # Create primary client (auto-connects without password)
    client1 = NasPubSub("localhost", port=4222)
    
    # Wait for connection to establish
    await asyncio.sleep(2)
    
    # Check if connected before creating secondary client
    if not client1.is_connected():
        print("Client1 failed to connect. Check if NATS server is running on localhost:4222")
        return
    
    # Create secondary client using existing connection
    client2 = NasPubSub.from_connection(client1)
    
    # Define callback for messages
    async def on_message(msg: Message):
        print(f"Received: {msg.topic} -> {msg.payload.decode()}")
    
    # Subscribe to topics
    await client1.subscribe("sensors.temperature", on_message)
    await client2.subscribe("sensors.humidity", on_message)
    
    # Publish messages
    await client1.publish("sensors.temperature", b'{"value": 22.5, "unit": "C"}')
    await client2.publish("sensors.humidity", b'{"value": 65, "unit": "%"}')
    
    # Test request-reply pattern
    async def echo_handler(msg: Message):
        if msg.reply_to:
            await client1.publish(msg.reply_to, msg.payload)
    
    await client1.subscribe("echo", echo_handler)
    
    response = await client2.request("echo", b"Hello NATS!", timeout=2.0)
    if response:
        print(f"Echo response: {response.payload.decode()}")
    
    # Keep running for a bit
    await asyncio.sleep(5)
    
    # Cleanup - only disconnect clients without shared connections
    # For shared connections, just clean up subscriptions
    await client2.disconnect()  # This will just unsubscribe, not close connection
    await client1.disconnect()  # This will close the actual connection


if __name__ == "__main__":
    asyncio.run(main())