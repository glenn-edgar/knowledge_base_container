import asyncio
from typing import Optional, Any, Dict
from collections import deque
import nats
from nats.aio.client import Client as NATS


class NASQueue:
    """
    NAS Queue with push/pop functionality.
    
    This client manages a connection to a NATS server and provides
    methods to push and pop messages from a queue.
    """
    
    def __init__(self, servers: str = "nats://localhost:4222", **connection_options):
        """
        Initialize the NAS client with connection parameters.
        
        Args:
            servers: NATS server URL(s)
            **connection_options: Additional connection options for NATS
        """
        self.servers = servers
        self.connection_options = connection_options
        self.nc: Optional[NATS] = None
        self.message_queue = deque()
        self.subscription = None
        self._connected = False
        
    @classmethod
    def from_connection(cls, existing_connection: NATS, servers: str = "nats://localhost:4222"):
        """
        Create a NASQueue instance from an existing NATS connection.
        
        Args:
            existing_connection: An already established NATS connection
            servers: Server URL for reference
            
        Returns:
            NASQueue instance using the existing connection
        """
        instance = cls.__new__(cls)
        instance.servers = servers
        instance.connection_options = {}
        instance.nc = existing_connection
        instance.message_queue = deque()
        instance.subscription = None
        instance._connected = existing_connection.is_connected
        return instance
    
    async def connect(self):
        """
        Establish connection to the NATS server if not already connected.
        """
        if self.nc is None:
            self.nc = NATS()
            
        if not self._connected:
            await self.nc.connect(self.servers, **self.connection_options)
            self._connected = True
            print(f"Connected to NATS server at {self.servers}")
    
    async def disconnect(self):
        """
        Close the connection to the NATS server.
        """
        if self.nc and self._connected:
            await self.nc.close()
            self._connected = False
            print("Disconnected from NATS server")
    
    async def push(self, subject: str, message: Any, headers: Optional[Dict] = None):
        """
        Push a message to the queue and publish it to the specified subject.
        
        Args:
            subject: NATS subject to publish to
            message: Message to send (will be encoded as bytes if string)
            headers: Optional headers to include with the message
        """
        if not self._connected:
            await self.connect()
        
        # Convert message to bytes if it's a string
        if isinstance(message, str):
            message_bytes = message.encode()
        elif isinstance(message, bytes):
            message_bytes = message
        else:
            # For other types, convert to string first
            message_bytes = str(message).encode()
        
        # Add to internal queue
        self.message_queue.append({
            'subject': subject,
            'message': message_bytes,
            'headers': headers
        })
        
        # Publish to NATS
        await self.nc.publish(subject, message_bytes, headers=headers)
        print(f"Pushed message to subject '{subject}': {message}")
    
    async def pop(self, timeout: Optional[float] = None) -> Optional[Dict]:
        """
        Pop a message from the local queue.
        
        Args:
            timeout: Optional timeout in seconds (not used for local queue)
            
        Returns:
            Dictionary containing subject, message, and headers, or None if queue is empty
        """
        if self.message_queue:
            message_data = self.message_queue.popleft()
            print(f"Popped message from queue: {message_data['message'].decode()}")
            return message_data
        else:
            print("Queue is empty")
            return None
    
    async def subscribe_and_queue(self, subject: str, queue: str = ""):
        """
        Subscribe to a subject and automatically add received messages to the queue.
        
        Args:
            subject: NATS subject to subscribe to
            queue: Optional queue group name
        """
        if not self._connected:
            await self.connect()
        
        async def message_handler(msg):
            """Handler that adds received messages to the internal queue."""
            self.message_queue.append({
                'subject': msg.subject,
                'message': msg.data,
                'headers': msg.headers
            })
            print(f"Received and queued message on '{msg.subject}': {msg.data.decode()}")
        
        self.subscription = await self.nc.subscribe(subject, queue=queue, cb=message_handler)
        print(f"Subscribed to subject '{subject}'" + (f" with queue '{queue}'" if queue else ""))
    
    async def unsubscribe(self):
        """
        Unsubscribe from the current subscription.
        """
        if self.subscription:
            await self.subscription.unsubscribe()
            self.subscription = None
            print("Unsubscribed from subject")
    
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


# Example usage
async def main():
    """
    Example demonstrating the NASQueue usage.
    """
    # Method 1: Create client with new connection
    client1 = NASQueue("nats://localhost:4222")
    await client1.connect()
    
    # Push some messages
    await client1.push("test.subject", "Hello, World!")
    await client1.push("test.subject", "Second message")
    await client1.push("test.subject", {"type": "json", "value": 123})
    
    # Pop messages from queue
    print(f"\nQueue size: {client1.queue_size()}")
    while client1.queue_size() > 0:
        msg = await client1.pop()
        if msg:
            print(f"  Subject: {msg['subject']}, Message: {msg['message']}")
    
    # Method 2: Create client from existing connection
    existing_nc = client1.nc  # Get the existing connection
    client2 = NASQueue.from_connection(existing_nc)
    
    # Use client2 with the shared connection
    await client2.push("another.subject", "Message from client2")
    
    # Subscribe to receive messages
    await client1.subscribe_and_queue("test.subject")
    
    # Simulate receiving messages (in real scenario, these would come from other publishers)
    await client1.push("test.subject", "Subscribed message 1")
    await client1.push("test.subject", "Subscribed message 2")
    
    # Give some time for messages to be received
    await asyncio.sleep(0.1)
    
    # Pop the subscribed messages
    print(f"\nQueue size after subscription: {client1.queue_size()}")
    while client1.queue_size() > 0:
        msg = await client1.pop()
    
    # Cleanup
    await client1.unsubscribe()
    await client1.disconnect()


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
