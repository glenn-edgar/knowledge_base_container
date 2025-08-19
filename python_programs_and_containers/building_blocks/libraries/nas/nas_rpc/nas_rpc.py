import asyncio
import json
import uuid
from typing import Optional, Any, Dict, Callable, Union
from datetime import datetime, timedelta
import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg


class NAS_RPC:
    """
    NAS RPC implementation providing both client and server functionality.
    
    This class enables Remote Procedure Calls over NATS messaging system,
    supporting request-response patterns with timeout and error handling.
    """
    
    def __init__(self, servers: str = "nats://localhost:4222", **connection_options):
        """
        Initialize the NAS_RPC with connection parameters.
        
        Args:
            servers: NATS server URL(s)
            **connection_options: Additional connection options for NATS
        """
        self.servers = servers
        self.connection_options = connection_options
        self.nc: Optional[NATS] = None
        self._connected = False
        
        # RPC server handlers
        self.handlers: Dict[str, Callable] = {}
        self.subscriptions: Dict[str, Any] = {}
        
        # RPC client tracking
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.inbox_subscription = None
        self.inbox_subject = None
        
    @classmethod
    async def from_connection(cls, existing_connection: NATS, servers: str = "nats://localhost:4222"):
        """
        Create an NAS_RPC instance from an existing NATS connection.
        
        Args:
            existing_connection: An already established NATS connection
            servers: Server URL for reference
            
        Returns:
            NAS_RPC instance using the existing connection
        """
        instance = cls.__new__(cls)
        instance.servers = servers
        instance.connection_options = {}
        instance.nc = existing_connection
        instance._connected = existing_connection.is_connected
        
        # Initialize RPC components
        instance.handlers = {}
        instance.subscriptions = {}
        instance.pending_requests = {}
        instance.inbox_subscription = None
        instance.inbox_subject = None
        
        # Setup inbox for RPC responses if connected
        if instance._connected:
            instance.inbox_subject = f"_INBOX.{uuid.uuid4().hex}"
            instance.inbox_subscription = await instance.nc.subscribe(
                f"{instance.inbox_subject}.*", 
                cb=instance._handle_response
            )
            print(f"RPC inbox created for existing connection: {instance.inbox_subject}")
        
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
        
        # Setup inbox for RPC responses if not already setup
        if not self.inbox_subject:
            self.inbox_subject = f"_INBOX.{uuid.uuid4().hex}"
            self.inbox_subscription = await self.nc.subscribe(
                f"{self.inbox_subject}.*", 
                cb=self._handle_response
            )
            print(f"RPC inbox created: {self.inbox_subject}")
    
    async def disconnect(self):
        """
        Close the connection to the NATS server and cleanup resources.
        """
        # Unsubscribe from all registered handlers
        for sub in self.subscriptions.values():
            await sub.unsubscribe()
        self.subscriptions.clear()
        
        # Cleanup inbox subscription
        if self.inbox_subscription:
            await self.inbox_subscription.unsubscribe()
            self.inbox_subscription = None
        
        # Cancel pending requests
        for future in self.pending_requests.values():
            if not future.done():
                future.cancel()
        self.pending_requests.clear()
        
        # Close connection
        if self.nc and self._connected:
            await self.nc.close()
            self._connected = False
            print("Disconnected from NATS server")
    
    # ============== RPC Server Functions ==============
    
    def register_handler(self, method: str, handler: Callable):
        """
        Register an RPC method handler (decorator or direct registration).
        
        Args:
            method: The RPC method name/subject
            handler: Async function to handle the RPC call
            
        Example:
            @rpc.register_handler("math.add")
            async def add(a, b):
                return a + b
        """
        self.handlers[method] = handler
        return handler
    
    async def start_server(self, prefix: str = ""):
        """
        Start the RPC server and subscribe to registered methods.
        
        Args:
            prefix: Optional prefix for all method subjects
        """
        if not self._connected:
            await self.connect()
        
        for method, handler in self.handlers.items():
            subject = f"{prefix}.{method}" if prefix else method
            
            async def create_handler(h):
                async def msg_handler(msg: Msg):
                    await self._handle_request(msg, h)
                return msg_handler
            
            handler_func = await create_handler(handler)
            subscription = await self.nc.subscribe(subject, cb=handler_func)
            self.subscriptions[subject] = subscription
            print(f"RPC server listening on: {subject}")
    
    async def _handle_request(self, msg: Msg, handler: Callable):
        """
        Handle incoming RPC request and send response.
        
        Args:
            msg: The NATS message containing the request
            handler: The registered handler function
        """
        response = {
            "id": None,
            "result": None,
            "error": None
        }
        
        try:
            # Parse request
            request = json.loads(msg.data.decode())
            response["id"] = request.get("id")
            
            # Extract parameters
            params = request.get("params", {})
            
            # Call handler
            if asyncio.iscoroutinefunction(handler):
                result = await handler(**params) if isinstance(params, dict) else await handler(*params)
            else:
                result = handler(**params) if isinstance(params, dict) else handler(*params)
            
            response["result"] = result
            
        except json.JSONDecodeError as e:
            response["error"] = {"code": -32700, "message": f"Parse error: {str(e)}"}
        except TypeError as e:
            response["error"] = {"code": -32602, "message": f"Invalid params: {str(e)}"}
        except Exception as e:
            response["error"] = {"code": -32603, "message": f"Internal error: {str(e)}"}
        
        # Send response
        if msg.reply:
            response_data = json.dumps(response).encode()
            await self.nc.publish(msg.reply, response_data)
    
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
            
        Raises:
            TimeoutError: If response not received within timeout
            Exception: If RPC call returns an error
        """
        if not self._connected:
            await self.connect()
        
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
        
        # Send request
        request_data = json.dumps(request).encode()
        await self.nc.publish(method, request_data, reply=reply_to)
        
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
        if not self._connected:
            await self.connect()
        
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
        
        # Send request
        request_data = json.dumps(request).encode()
        await self.nc.publish(method, request_data, reply=reply_to)
        
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
    
    async def _handle_response(self, msg: Msg):
        """
        Handle RPC response messages.
        
        Args:
            msg: The NATS message containing the response
        """
        try:
            # Parse response
            response = json.loads(msg.data.decode())
            request_id = response.get("id")
            
            if request_id and request_id in self.pending_requests:
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
            
        Example:
            @rpc.rpc_method("math.multiply")
            async def multiply(x, y):
                return x * y
        """
        def decorator(func):
            method_name = name or func.__name__
            self.register_handler(method_name, func)
            return func
        return decorator


# Example usage
async def main():
    """
    Example demonstrating NAS_RPC usage for both client and server.
    """
    
    # Create RPC server
    server = NAS_RPC("nats://localhost:4222")
    await server.connect()
    
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
    
    # Create RPC client (could be separate process)
    client = NAS_RPC("nats://localhost:4222")
    await client.connect()
    
    # Make RPC calls
    print("\n=== RPC Calls ===")
    
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
    
    # Batch calls
    print("\n=== Batch RPC Calls ===")
    batch_calls = [
        ("rpc.math.add", {"a": 1, "b": 2}),
        ("rpc.math.multiply", {"x": 3, "y": 4}),
        ("rpc.math.divide", {"a": 10, "b": 2}),
    ]
    results = await client.call_batch(batch_calls, timeout=2.0)
    for i, result in enumerate(results):
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
    
    # Create client from existing connection
    existing_connection = client.nc
    client2 = await NAS_RPC.from_connection(existing_connection)
    result = await client2.call("rpc.math.add", {"a": 100, "b": 200})
    print(f"\nClient2 result: {result}")
    
    # Cleanup
    await client.disconnect()
    await server.disconnect()


if __name__ == "__main__":
    asyncio.run(main())