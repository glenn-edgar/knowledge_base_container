import asyncio
import json
import uuid
from typing import Optional, Any, Dict, Callable, Union
from datetime import datetime
from enum import Enum


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


class NAS_RPC:
    """
    NAS RPC implementation providing both client and server functionality.
    
    This class enables Remote Procedure Calls over NATS messaging system,
    supporting request-response patterns with timeout and error handling.
    """
    
    def __init__(self, host: str = "localhost", port: int = 4222, 
                 namespace: str = "default"):
        """
        Initialize NAS_RPC client.
        
        Args:
            host: NATS server hostname or IP (default: localhost)
            port: NATS server port (default: 4222)
            namespace: Namespace prefix for all subjects (default: "default")
        """
        self.host = host
        self.port = port
        self.namespace = namespace
        self.client_id = f"rpc_{uuid.uuid4().hex[:8]}"
        
        # Connection state
        self.state = ConnectionState.DISCONNECTED
        self.reader = None
        self.writer = None
        
        # NATS protocol state
        self.server_info = {}
        self.sid_counter = 0
        self.pending_pongs = 0
        
        # RPC server handlers
        self.handlers: Dict[str, Callable] = {}
        self.subscriptions: Dict[str, Dict] = {}  # sid -> {subject, handler}
        
        # RPC client tracking
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.inbox_subject = None
        self.inbox_sid = None
        
        # Running state
        self.running = False
        
        # Task references
        self._msg_handler_task = None
        self._ping_handler_task = None
    
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
                
                # Setup inbox for RPC responses
                await self._setup_inbox()
                
                return True
            else:
                raise Exception(f"Unexpected response: {response}")
                
        except Exception as e:
            self.state = ConnectionState.ERROR
            print(f"Failed to connect: {e}")
            return False
    
    async def connect(self):
        """
        Manually connect to NATS server.
        
        Returns:
            True if connected successfully, False otherwise
        """
        if self.state == ConnectionState.CONNECTED:
            return True
        return await self._connect()
    
    async def wait_connected(self, timeout: float = 5.0) -> bool:
        """
        Wait for connection to be established.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if connected, False if timeout
        """
        if self.state == ConnectionState.DISCONNECTED:
            asyncio.create_task(self._connect())
        
        start_time = asyncio.get_event_loop().time()
        while self.state != ConnectionState.CONNECTED:
            if asyncio.get_event_loop().time() - start_time > timeout:
                return False
            if self.state == ConnectionState.ERROR:
                return False
            await asyncio.sleep(0.1)
        return True
    
    async def _setup_inbox(self):
        """Setup inbox for RPC responses."""
        if not self.inbox_subject:
            self.inbox_subject = f"_INBOX.{uuid.uuid4().hex}"
            
            # Subscribe to inbox
            self.sid_counter += 1
            self.inbox_sid = str(self.sid_counter)
            
            self.subscriptions[self.inbox_sid] = {
                'subject': f"{self.inbox_subject}.*",
                'handler': self._handle_response,
                'is_inbox': True
            }
            
            # Send SUB command for inbox
            sub_cmd = f"SUB {self.inbox_subject}.* {self.inbox_sid}\r\n"
            self.writer.write(sub_cmd.encode())
            await self.writer.drain()
            
            print(f"RPC inbox created: {self.inbox_subject}")
    
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
                    
                    # Process message
                    if sid in self.subscriptions:
                        sub_info = self.subscriptions[sid]
                        
                        if sub_info.get('is_inbox'):
                            # Handle RPC response
                            await self._handle_response(payload, subject)
                        else:
                            # Handle RPC request
                            handler = sub_info['handler']
                            await self._handle_request(payload, reply_to, handler)
                    
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
    
    # ============== RPC Server Functions ==============
    
    def register_handler(self, method: str, handler: Callable):
        """
        Register an RPC method handler.
        
        Args:
            method: The RPC method name/subject
            handler: Async function to handle the RPC call
        """
        self.handlers[method] = handler
        return handler
    
    async def start_server(self, prefix: str = ""):
        """
        Start the RPC server and subscribe to registered methods.
        
        Args:
            prefix: Optional prefix for all method subjects
        """
        if self.state != ConnectionState.CONNECTED:
            if not await self.connect():
                raise ConnectionError("Failed to connect to NATS")
        
        for method, handler in self.handlers.items():
            # Build subject with prefix and namespace
            if prefix:
                subject = f"{prefix}.{method}"
            else:
                subject = method
            
            # Add namespace
            full_subject = self._add_namespace(subject)
            
            # Generate subscription ID
            self.sid_counter += 1
            sid = str(self.sid_counter)
            
            # Store subscription
            self.subscriptions[sid] = {
                'subject': full_subject,
                'original_subject': subject,
                'handler': handler,
                'is_inbox': False
            }
            
            # Send SUB command
            sub_cmd = f"SUB {full_subject} {sid}\r\n"
            self.writer.write(sub_cmd.encode())
            await self.writer.drain()
            
            print(f"RPC server listening on: {subject} (full: {full_subject})")
    
    async def _handle_request(self, payload: bytes, reply_to: str, handler: Callable):
        """
        Handle incoming RPC request and send response.
        
        Args:
            payload: The request payload
            reply_to: Reply subject
            handler: The registered handler function
        """
        response = {
            "id": None,
            "result": None,
            "error": None
        }
        
        try:
            # Parse request
            request = json.loads(payload.decode())
            response["id"] = request.get("id")
            
            # Extract parameters
            params = request.get("params", {})
            
            # Call handler
            if asyncio.iscoroutinefunction(handler):
                if isinstance(params, dict):
                    result = await handler(**params)
                else:
                    result = await handler(*params)
            else:
                if isinstance(params, dict):
                    result = handler(**params)
                else:
                    result = handler(*params)
            
            response["result"] = result
            
        except json.JSONDecodeError as e:
            response["error"] = {"code": -32700, "message": f"Parse error: {str(e)}"}
        except TypeError as e:
            response["error"] = {"code": -32602, "message": f"Invalid params: {str(e)}"}
        except Exception as e:
            response["error"] = {"code": -32603, "message": f"Internal error: {str(e)}"}
        
        # Send response if reply_to is provided
        if reply_to:
            response_data = json.dumps(response).encode()
            pub_cmd = f"PUB {reply_to} {len(response_data)}\r\n"
            self.writer.write(pub_cmd.encode())
            self.writer.write(response_data)
            self.writer.write(b"\r\n")
            await self.writer.drain()
    
    # ============== RPC Client Functions ==============
    
    async def call(self, method: str, params: Union[Dict, list] = None, 
                   timeout: float = 5.0) -> Any:
        """
        Make an RPC call and wait for response.
        
        Args:
            method: The RPC method name/subject to call
            params: Parameters to pass to the method (dict or list)
            timeout: Timeout in seconds for the response
            
        Returns:
            The result from the RPC call
        """
        if self.state != ConnectionState.CONNECTED:
            if not await self.wait_connected():
                raise ConnectionError("Not connected to NATS")
        
        # Generate unique request ID
        request_id = str(uuid.uuid4())
        
        # Prepare request
        request = {
            "id": request_id,
            "method": method,
            "params": params or {}
        }
        
        # Create response inbox
        reply_to = f"{self.inbox_subject}.{request_id}"
        
        # Create future for response
        future = asyncio.Future()
        self.pending_requests[request_id] = future
        
        # Add namespace to method
        full_method = self._add_namespace(method)
        
        # Send request
        request_data = json.dumps(request).encode()
        pub_cmd = f"PUB {full_method} {reply_to} {len(request_data)}\r\n"
        self.writer.write(pub_cmd.encode())
        self.writer.write(request_data)
        self.writer.write(b"\r\n")
        await self.writer.drain()
        
        try:
            # Wait for response with timeout
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self.pending_requests.pop(request_id, None)
            raise TimeoutError(f"RPC call to '{method}' timed out after {timeout} seconds")
        finally:
            self.pending_requests.pop(request_id, None)
    
    async def call_async(self, method: str, params: Union[Dict, list] = None) -> str:
        """
        Make an async RPC call without waiting for response.
        
        Args:
            method: The RPC method name/subject to call
            params: Parameters to pass to the method
            
        Returns:
            Request ID that can be used to check for response later
        """
        if self.state != ConnectionState.CONNECTED:
            if not await self.wait_connected():
                raise ConnectionError("Not connected to NATS")
        
        # Generate unique request ID
        request_id = str(uuid.uuid4())
        
        # Prepare request
        request = {
            "id": request_id,
            "method": method,
            "params": params or {}
        }
        
        # Create response inbox
        reply_to = f"{self.inbox_subject}.{request_id}"
        
        # Create future for response
        future = asyncio.Future()
        self.pending_requests[request_id] = future
        
        # Add namespace to method
        full_method = self._add_namespace(method)
        
        # Send request
        request_data = json.dumps(request).encode()
        pub_cmd = f"PUB {full_method} {reply_to} {len(request_data)}\r\n"
        self.writer.write(pub_cmd.encode())
        self.writer.write(request_data)
        self.writer.write(b"\r\n")
        await self.writer.drain()
        
        return request_id
    
    async def get_response(self, request_id: str, timeout: float = 5.0) -> Any:
        """
        Get response for a previously made async call.
        
        Args:
            request_id: The request ID returned from call_async
            timeout: Timeout in seconds to wait for response
            
        Returns:
            The result from the RPC call
        """
        future = self.pending_requests.get(request_id)
        if not future:
            raise ValueError(f"No pending request with ID: {request_id}")
        
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(f"Response for request '{request_id}' timed out")
        finally:
            self.pending_requests.pop(request_id, None)
    
    async def _handle_response(self, payload: bytes, subject: str):
        """
        Handle RPC response messages.
        
        Args:
            payload: The response payload
            subject: The inbox subject
        """
        try:
            # Extract request ID from subject
            # Format: _INBOX.xxx.request_id
            parts = subject.split('.')
            if len(parts) >= 3:
                request_id = parts[-1]
            else:
                return
            
            # Parse response
            response = json.loads(payload.decode())
            
            if request_id in self.pending_requests:
                future = self.pending_requests[request_id]
                
                if "error" in response and response["error"]:
                    error = response["error"]
                    future.set_exception(
                        Exception(f"RPC Error ({error.get('code', 'Unknown')}): {error.get('message', 'Unknown error')}")
                    )
                else:
                    future.set_result(response.get("result"))
        except Exception as e:
            print(f"Error handling RPC response: {e}")
    
    # ============== Utility Functions ==============
    
    async def call_batch(self, calls: list, timeout: float = 5.0) -> list:
        """
        Make multiple RPC calls in parallel.
        
        Args:
            calls: List of tuples (method, params)
            timeout: Timeout for all calls
            
        Returns:
            List of results in the same order as calls
        """
        tasks = []
        for method, params in calls:
            task = self.call(method, params, timeout)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    def rpc_method(self, name: str = None):
        """
        Decorator for registering RPC methods.
        
        Args:
            name: Optional method name (uses function name if not provided)
        """
        def decorator(func):
            method_name = name or func.__name__
            self.register_handler(method_name, func)
            return func
        return decorator
    
    def get_namespace(self) -> str:
        """Get the current namespace."""
        return self.namespace
    
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self.state == ConnectionState.CONNECTED
    
    async def disconnect(self):
        """Close the connection to the NATS server and cleanup resources."""
        self.running = False
        
        # Cancel handler tasks
        if self._msg_handler_task:
            self._msg_handler_task.cancel()
        if self._ping_handler_task:
            self._ping_handler_task.cancel()
        
        # Cancel pending requests
        for future in self.pending_requests.values():
            if not future.done():
                future.cancel()
        self.pending_requests.clear()
        
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
async def main():
    """
    Example demonstrating NAS_RPC usage for both client and server.
    """
    print("=" * 50)
    print("Testing NAS_RPC with Namespace Support")
    print("=" * 50)
    
    # Create RPC server
    server = NAS_RPC("localhost", 4222, namespace="production")
    if not await server.connect():
        print("Server failed to connect")
        return
    
    # Register RPC methods using decorator
    @server.rpc_method("math.add")
    async def add(a: float, b: float) -> float:
        """Add two numbers."""
        return a + b
    
    @server.rpc_method("math.multiply")
    async def multiply(x: float, y: float) -> float:
        """Multiply two numbers."""
        await asyncio.sleep(0.1)  # Simulate some work
        return x * y
    
    @server.rpc_method("string.concat")
    async def concat(str1: str, str2: str) -> str:
        """Concatenate two strings."""
        return f"{str1}{str2}"
    
    # Direct registration
    async def divide(a: float, b: float) -> float:
        if b == 0:
            raise ValueError("Division by zero")
        return a / b
    
    server.register_handler("math.divide", divide)
    
    # Start the RPC server
    await server.start_server("rpc")
    
    # Create RPC client in same namespace
    client = NAS_RPC("localhost", 4222, namespace="production")
    if not await client.connect():
        print("Client failed to connect")
        return
    
    # Make RPC calls
    print("\n=== RPC Calls (same namespace) ===")
    
    # Simple call
    result = await client.call("rpc.math.add", {"a": 5, "b": 3})
    print(f"5 + 3 = {result}")
    
    # Call with positional params
    result = await client.call("rpc.math.multiply", [4, 7])
    print(f"4 * 7 = {result}")
    
    # String operation
    result = await client.call("rpc.string.concat", {"str1": "Hello, ", "str2": "World!"})
    print(f"Concatenation: {result}")
    
    # Error handling
    try:
        result = await client.call("rpc.math.divide", {"a": 10, "b": 0})
    except Exception as e:
        print(f"Division error: {e}")
    
    # Valid division
    result = await client.call("rpc.math.divide", {"a": 10, "b": 2})
    print(f"10 / 2 = {result}")
    
    # Test with different namespace (should fail)
    print("\n=== Cross-namespace test ===")
    client_dev = NAS_RPC("localhost", 4222, namespace="development")
    if not await client_dev.connect():
        print("Dev client failed to connect")
        return
    
    try:
        # This should timeout as server is in different namespace
        result = await client_dev.call("rpc.math.add", {"a": 1, "b": 1}, timeout=1.0)
        print(f"Cross-namespace call succeeded: {result}")
    except TimeoutError as e:
        print(f"Cross-namespace call failed as expected: {e}")
    
    # Batch calls
    print("\n=== Batch RPC Calls ===")
    batch_calls = [
        ("rpc.math.add", {"a": 1, "b": 2}),
        ("rpc.math.multiply", {"x": 3, "y": 4}),
        ("rpc.math.divide", {"a": 10, "b": 2}),
    ]
    results = await client.call_batch(batch_calls, timeout=2.0)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Batch result {i+1}: Error - {result}")
        else:
            print(f"Batch result {i+1}: {result}")
    
    # Async call without waiting
    print("\n=== Async RPC Call ===")
    request_id = await client.call_async("rpc.math.multiply", {"x": 10, "y": 5})
    print(f"Async request sent with ID: {request_id}")
    
    # Do other work...
    await asyncio.sleep(0.2)
    
    # Get the response
    result = await client.get_response(request_id)
    print(f"Async result: {result}")
    
    # Test context manager
    print("\n=== Context Manager Test ===")
    async with NAS_RPC("localhost", 4222, namespace="test") as rpc_test:
        # Register and start a simple method
        @rpc_test.rpc_method("echo")
        async def echo(msg: str) -> str:
            return f"Echo: {msg}"
        
        await rpc_test.start_server()
        
        # Call it
        result = await rpc_test.call("echo", {"msg": "Hello from context!"})
        print(f"Context manager result: {result}")
    
    # Cleanup
    print("\n--- Cleanup ---")
    await client_dev.disconnect()
    await client.disconnect()
    await server.disconnect()
    
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