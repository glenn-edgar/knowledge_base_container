import asyncio
import json
import uuid
import time
import socket
import os
import threading
from typing import Optional, Any, Dict, Callable, Union, List
from datetime import datetime, timedelta
from enum import Enum


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


class LoadBalancingStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    FIRST_AVAILABLE = "first_available"


class NAS_RPC:
    """
    Enhanced NAS RPC implementation with multi-instance support and synchronous wrappers.
    
    This class enables Remote Procedure Calls over NATS messaging system,
    supporting request-response patterns with timeout, error handling,
    and multi-CPU/multi-machine deployment capabilities.
    """
    
    def __init__(self, host: str = "localhost", port: int = 4222, 
                 namespace: str = "default", instance_id: str = None,
                 enable_health_checks: bool = True):
        """
        Initialize NAS_RPC client with enhanced multi-instance support.
        
        Args:
            host: NATS server hostname or IP (default: localhost)
            port: NATS server port (default: 4222)
            namespace: Namespace prefix for all subjects (default: "default")
            instance_id: Unique instance identifier (auto-generated if None)
            enable_health_checks: Enable built-in health check endpoint
        """
        self.host = host
        self.port = port
        self.namespace = namespace
        self.enable_health_checks = enable_health_checks
        
        # Generate unique instance ID
        if instance_id:
            self.instance_id = instance_id
        else:
            hostname = socket.gethostname()
            pid = os.getpid()
            unique_id = uuid.uuid4().hex[:8]
            self.instance_id = f"{hostname}_{pid}_{unique_id}"
        
        self.client_id = f"rpc_{self.instance_id}"
        
        # Connection state
        self.state = ConnectionState.DISCONNECTED
        self.reader = None
        self.writer = None
        
        # NATS protocol state
        self.server_info = {}
        self.sid_counter = 0
        self.pending_pongs = 0
        
        # RPC server handlers with metadata
        self.handlers: Dict[str, Dict] = {}  # method -> {handler, metadata}
        self.subscriptions: Dict[str, Dict] = {}  # sid -> {subject, handler}
        
        # RPC client tracking
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.inbox_subject = None
        self.inbox_sid = None
        
        # Instance tracking
        self.start_time = datetime.utcnow()
        self.request_count = 0
        self.error_count = 0
        
        # Running state
        self.running = False
        
        # Task references
        self._msg_handler_task = None
        self._ping_handler_task = None
        
        # Event loop management for sync wrappers
        self._loop = None
        self._loop_thread = None
        self._loop_started = threading.Event()
    
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
                return future
        except RuntimeError:
            # No running loop, proceed normally
            pass
        
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except Exception as e:
            raise e
    
    def _generate_request_id(self) -> str:
        """
        Generate a collision-resistant request ID.
        
        Returns:
            Unique request ID incorporating instance info and timestamp
        """
        timestamp = int(time.time() * 1000000)  # microseconds
        counter = self.request_count
        self.request_count += 1
        return f"{self.instance_id}_{timestamp}_{counter}_{uuid.uuid4().hex[:4]}"
    
    def _generate_subscription_id(self) -> str:
        """
        Generate a unique subscription ID for this instance.
        
        Returns:
            Unique subscription ID
        """
        self.sid_counter += 1
        return f"{self.instance_id}_{self.sid_counter}"
    
    def _add_namespace(self, subject: str) -> str:
        """
        Add namespace prefix to a subject.
        
        Args:
            subject: Original subject
            
        Returns:
            Subject with namespace prefix
        """
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
            
            # Send CONNECT command with enhanced client info
            connect_options = {
                "verbose": False,
                "pedantic": False,
                "name": self.client_id,
                "lang": "python",
                "version": "2.0.0",
                "protocol": 1,
                # Enhanced metadata
                "instance_id": self.instance_id,
                "namespace": self.namespace,
                "start_time": self.start_time.isoformat()
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
                print(f"Connected to NATS at {self.host}:{self.port}")
                print(f"Instance: {self.instance_id}, Namespace: {self.namespace}")
                
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
    
    async def _setup_inbox(self):
        """Setup inbox for RPC responses."""
        if not self.inbox_subject:
            self.inbox_subject = f"_INBOX.{self.instance_id}.{uuid.uuid4().hex[:8]}"
            
            # Subscribe to inbox with unique SID
            self.inbox_sid = self._generate_subscription_id()
            
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
    
    async def _service_discovery_handler(self):
        """Handle service discovery and health monitoring."""
        # Removed - service discovery not needed for single NATS server
        pass
    
    # Removed service discovery methods - not needed for single NATS server
    # _publish_instance_status(), _cleanup_stale_instances(), _handle_discovery_message()
    # _subscribe_to_discovery() - methods removed for simplicity
    
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
                    
                    # Process RPC message
                    if sid in self.subscriptions:
                        sub_info = self.subscriptions[sid]
                        
                        if sub_info.get('is_inbox'):
                            await self._handle_response(payload, subject)
                        else:
                            handler_info = sub_info['handler_info']
                            # Run handler in thread pool for sync handlers
                            if asyncio.iscoroutinefunction(handler_info['handler']):
                                await self._handle_request(payload, reply_to, handler_info)
                            else:
                                loop = asyncio.get_event_loop()
                                await loop.run_in_executor(None, 
                                    lambda: asyncio.run_coroutine_threadsafe(
                                        self._handle_request(payload, reply_to, handler_info), 
                                        loop).result())
                    
                elif line == b'PING':
                    self.writer.write(b"PONG\r\n")
                    await self.writer.drain()
                    
                elif line == b'PONG':
                    self.pending_pongs = max(0, self.pending_pongs - 1)
                    
                elif line.startswith(b'+OK'):
                    pass
                    
                elif line.startswith(b'-ERR'):
                    self.error_count += 1
                    error_msg = line[5:].decode() if len(line) > 5 else "Unknown error"
                    print(f"NATS Error: {error_msg}")
                    
            except Exception as e:
                if self.running:
                    self.error_count += 1
                    await asyncio.sleep(1)
    
    # Removed discovery message handler - not needed for single server setup
    
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
    
    # ============== Enhanced RPC Server Functions ==============
    
    def register_handler(self, method: str, handler: Callable, 
                        instance_specific: bool = False,
                        load_balancing: bool = True,
                        metadata: Dict = None):
        """
        Register an RPC method handler with enhanced options.
        
        Args:
            method: The RPC method name/subject
            handler: Async function to handle the RPC call
            instance_specific: If True, method is only available on this instance
            load_balancing: If True, allows load balancing across instances
            metadata: Additional metadata for the handler
        """
        handler_info = {
            'handler': handler,
            'instance_specific': instance_specific,
            'load_balancing': load_balancing,
            'metadata': metadata or {},
            'registered_at': datetime.utcnow().isoformat(),
            'call_count': 0,
            'error_count': 0
        }
        
        self.handlers[method] = handler_info
        return handler
    
    async def start_server(self, prefix: str = ""):
        """
        Start the RPC server.
        
        Args:
            prefix: Optional prefix for all method subjects
        """
        if self.state != ConnectionState.CONNECTED:
            if not await self.connect():
                raise ConnectionError("Failed to connect to NATS")
        
        # Register built-in health check if enabled
        if self.enable_health_checks:
            await self._register_health_check()
        
        # Register all handlers
        for method, handler_info in self.handlers.items():
            await self._subscribe_to_method(method, handler_info, prefix)
        
        print(f"RPC server started with {len(self.handlers)} methods")
        print(f"Instance ID: {self.instance_id}")
    
    def start_server_sync(self, prefix: str = "", timeout: float = 10.0):
        """
        Synchronous wrapper for start_server().
        
        Args:
            prefix: Optional prefix for all method subjects
            timeout: Operation timeout in seconds
        """
        return self._run_sync(self.start_server(prefix), timeout=timeout)
    
    async def _register_health_check(self):
        """Register built-in health check endpoint."""
        async def health_check() -> Dict[str, Any]:
            """Health check endpoint returning instance status."""
            uptime = datetime.utcnow() - self.start_time
            
            return {
                "status": "healthy",
                "instance_id": self.instance_id,
                "namespace": self.namespace,
                "uptime_seconds": int(uptime.total_seconds()),
                "handlers": list(self.handlers.keys()),
                "request_count": self.request_count,
                "error_count": self.error_count,
                "timestamp": datetime.utcnow().isoformat(),
                "pending_requests": len(self.pending_requests)
            }
        
        self.register_handler("_health", health_check, instance_specific=True)
    
    # Removed _subscribe_to_discovery() - not needed for single server setup
    
    async def _subscribe_to_method(self, method: str, handler_info: Dict, prefix: str):
        """Subscribe to a specific RPC method."""
        # Build subject with prefix and namespace
        if prefix:
            base_subject = f"{prefix}.{method}"
        else:
            base_subject = method
        
        # Add instance-specific routing if requested
        if handler_info['instance_specific']:
            subject = f"{base_subject}.{self.instance_id}"
        else:
            subject = base_subject
        
        # Add namespace
        full_subject = self._add_namespace(subject)
        
        # Generate unique subscription ID
        sid = self._generate_subscription_id()
        
        # Store subscription
        self.subscriptions[sid] = {
            'subject': full_subject,
            'original_subject': subject,
            'handler_info': handler_info,
            'method': method,
            'is_inbox': False
        }
        
        # Send SUB command
        sub_cmd = f"SUB {full_subject} {sid}\r\n"
        self.writer.write(sub_cmd.encode())
        await self.writer.drain()
        
        routing_info = f" (instance-specific)" if handler_info['instance_specific'] else ""
        print(f"RPC method registered: {subject}{routing_info}")
    
    async def _handle_request(self, payload: bytes, reply_to: str, handler_info: Dict):
        """Handle incoming RPC request with enhanced error tracking."""
        response = {
            "id": None,
            "result": None,
            "error": None,
            "instance_id": self.instance_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        start_time = time.time()
        
        try:
            # Parse request
            request = json.loads(payload.decode())
            response["id"] = request.get("id")
            
            # Extract parameters
            params = request.get("params", {})
            
            # Get handler
            handler = handler_info['handler']
            
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
            handler_info['call_count'] += 1
            
        except json.JSONDecodeError as e:
            response["error"] = {"code": -32700, "message": f"Parse error: {str(e)}"}
            handler_info['error_count'] += 1
            self.error_count += 1
        except TypeError as e:
            response["error"] = {"code": -32602, "message": f"Invalid params: {str(e)}"}
            handler_info['error_count'] += 1
            self.error_count += 1
        except Exception as e:
            response["error"] = {"code": -32603, "message": f"Internal error: {str(e)}"}
            handler_info['error_count'] += 1
            self.error_count += 1
        
        # Add performance metadata
        response["processing_time_ms"] = round((time.time() - start_time) * 1000, 2)
        
        # Send response if reply_to is provided
        if reply_to:
            response_data = json.dumps(response).encode()
            pub_cmd = f"PUB {reply_to} {len(response_data)}\r\n"
            self.writer.write(pub_cmd.encode())
            self.writer.write(response_data)
            self.writer.write(b"\r\n")
            await self.writer.drain()
    
    # ============== Enhanced RPC Client Functions ==============
    
    async def call(self, method: str, params: Union[Dict, list] = None, 
                   timeout: float = 5.0, target_instance: str = None) -> Any:
        """
        Make an RPC call with enhanced targeting options.
        
        Args:
            method: The RPC method name/subject to call
            params: Parameters to pass to the method
            timeout: Timeout in seconds for the response
            target_instance: Optional specific instance to target
            
        Returns:
            The result from the RPC call
        """
        if self.state != ConnectionState.CONNECTED:
            if not await self.wait_connected():
                raise ConnectionError("Not connected to NATS")
        
        # Generate unique request ID
        request_id = self._generate_request_id()
        
        # Prepare request with enhanced metadata
        request = {
            "id": request_id,
            "method": method,
            "params": params or {},
            "caller_instance": self.instance_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Create response inbox
        reply_to = f"{self.inbox_subject}.{request_id}"
        
        # Create future for response
        future = asyncio.Future()
        self.pending_requests[request_id] = future
        
        # Determine target subject
        if target_instance:
            full_method = self._add_namespace(f"{method}.{target_instance}")
        else:
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
            self.error_count += 1
            raise TimeoutError(f"RPC call to '{method}' timed out after {timeout} seconds")
        finally:
            self.pending_requests.pop(request_id, None)
    
    def call_sync(self, method: str, params: Union[Dict, list] = None, 
                  timeout: float = 5.0, target_instance: str = None) -> Any:
        """
        Synchronous wrapper for call().
        
        Args:
            method: The RPC method name/subject to call
            params: Parameters to pass to the method
            timeout: Timeout in seconds for the response
            target_instance: Optional specific instance to target
            
        Returns:
            The result from the RPC call
        """
        return self._run_sync(self.call(method, params, timeout, target_instance), 
                             timeout=timeout + 1.0)
    
    # Removed call_with_discovery methods - not needed without service discovery
    
    async def call_async(self, method: str, params: Union[Dict, list] = None,
                        target_instance: str = None) -> str:
        """
        Make an async RPC call without waiting for response.
        
        Args:
            method: The RPC method name/subject to call
            params: Parameters to pass to the method
            target_instance: Optional specific instance to target
            
        Returns:
            Request ID that can be used to check for response later
        """
        if self.state != ConnectionState.CONNECTED:
            if not await self.wait_connected():
                raise ConnectionError("Not connected to NATS")
        
        # Generate unique request ID
        request_id = self._generate_request_id()
        
        # Prepare request
        request = {
            "id": request_id,
            "method": method,
            "params": params or {},
            "caller_instance": self.instance_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Create response inbox
        reply_to = f"{self.inbox_subject}.{request_id}"
        
        # Create future for response
        future = asyncio.Future()
        self.pending_requests[request_id] = future
        
        # Determine target subject
        if target_instance:
            full_method = self._add_namespace(f"{method}.{target_instance}")
        else:
            full_method = self._add_namespace(method)
        
        # Send request
        request_data = json.dumps(request).encode()
        pub_cmd = f"PUB {full_method} {reply_to} {len(request_data)}\r\n"
        self.writer.write(pub_cmd.encode())
        self.writer.write(request_data)
        self.writer.write(b"\r\n")
        await self.writer.drain()
        
        return request_id
    
    def call_async_sync(self, method: str, params: Union[Dict, list] = None,
                       target_instance: str = None, timeout: float = 5.0) -> str:
        """
        Synchronous wrapper for call_async().
        
        Args:
            method: The RPC method name/subject to call
            params: Parameters to pass to the method
            target_instance: Optional specific instance to target
            timeout: Operation timeout in seconds
            
        Returns:
            Request ID that can be used to check for response later
        """
        return self._run_sync(self.call_async(method, params, target_instance), timeout=timeout)
    
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
            self.error_count += 1
            raise TimeoutError(f"Response for request '{request_id}' timed out")
        finally:
            self.pending_requests.pop(request_id, None)
    
    def get_response_sync(self, request_id: str, timeout: float = 5.0) -> Any:
        """
        Synchronous wrapper for get_response().
        
        Args:
            request_id: The request ID returned from call_async
            timeout: Timeout in seconds to wait for response
            
        Returns:
            The result from the RPC call
        """
        return self._run_sync(self.get_response(request_id, timeout), timeout=timeout + 1.0)
    
    async def _handle_response(self, payload: bytes, subject: str):
        """Handle RPC response messages with enhanced metadata processing."""
        try:
            # Extract request ID from subject
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
            self.error_count += 1
    
    # ============== Enhanced Utility Functions ==============
    
    # Removed discover_services methods - not needed without service discovery
    
    async def call_batch(self, calls: list, timeout: float = 5.0) -> list:
        """
        Make multiple RPC calls in parallel.
        
        Args:
            calls: List of tuples (method, params) or (method, params, target_instance)
            timeout: Timeout for all calls
            
        Returns:
            List of results in the same order as calls
        """
        tasks = []
        for call_info in calls:
            if len(call_info) == 2:
                method, params = call_info
                task = self.call(method, params, timeout)
            elif len(call_info) == 3:
                method, params, target_instance = call_info
                task = self.call(method, params, timeout, target_instance)
            else:
                continue
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    def call_batch_sync(self, calls: list, timeout: float = 5.0) -> list:
        """
        Synchronous wrapper for call_batch().
        
        Args:
            calls: List of tuples (method, params) or (method, params, target_instance)
            timeout: Timeout for all calls
            
        Returns:
            List of results in the same order as calls
        """
        return self._run_sync(self.call_batch(calls, timeout), timeout=timeout + 1.0)
    
    def rpc_method(self, name: str = None, instance_specific: bool = False,
                  load_balancing: bool = True, metadata: Dict = None):
        """
        Enhanced decorator for registering RPC methods.
        
        Args:
            name: Optional method name (uses function name if not provided)
            instance_specific: If True, method is only available on this instance
            load_balancing: If True, allows load balancing across instances
            metadata: Additional metadata for the method
        """
        def decorator(func):
            method_name = name or func.__name__
            self.register_handler(method_name, func, instance_specific, 
                                load_balancing, metadata)
            return func
        return decorator
    
    def get_instance_info(self) -> Dict[str, Any]:
        """Get detailed information about this instance."""
        uptime = datetime.utcnow() - self.start_time
        
        return {
            "instance_id": self.instance_id,
            "namespace": self.namespace,
            "host": self.host,
            "port": self.port,
            "state": self.state.value,
            "start_time": self.start_time.isoformat(),
            "uptime_seconds": int(uptime.total_seconds()),
            "handlers": {
                method: {
                    "call_count": info['call_count'],
                    "error_count": info['error_count'],
                    "instance_specific": info['instance_specific'],
                    "registered_at": info['registered_at']
                }
                for method, info in self.handlers.items()
            },
            "request_count": self.request_count,
            "error_count": self.error_count,
            "pending_requests": len(self.pending_requests)
        }
    
    def get_namespace(self) -> str:
        """Get the current namespace."""
        return self.namespace
    
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self.state == ConnectionState.CONNECTED
    
    async def disconnect(self):
        """Close the connection and cleanup resources."""
        self.running = False
        
        # Cancel handler tasks
        for task in [self._msg_handler_task, self._ping_handler_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
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
        print(f"Disconnected from NATS (instance: {self.instance_id})")
        
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


# Simplified example usage without service discovery
def sync_example():
    """Example using synchronous methods"""
    print("Sync RPC Example:")
    print("-" * 30)
    
    # Create server and client
    server = NAS_RPC("localhost", 4222, namespace="test", instance_id="server1")
    client = NAS_RPC("localhost", 4222, namespace="test", instance_id="client1")
    
    # Connect both
    server.connect_sync()
    client.connect_sync()
    
    # Register handlers on server
    def add_handler(a: float, b: float) -> float:
        print(f"Server: Computing {a} + {b}")
        return a + b
    
    def multiply_handler(x: float, y: float) -> float:
        print(f"Server: Computing {x} * {y}")
        return x * y
    
    server.register_handler("math.add", add_handler)
    server.register_handler("math.multiply", multiply_handler)
    
    # Start server
    server.start_server_sync("rpc")
    
    # Give server time to start
    import time
    time.sleep(1)
    
    # Make RPC calls
    result1 = client.call_sync("rpc.math.add", {"a": 5, "b": 3})
    print(f"Add result: {result1}")
    
    result2 = client.call_sync("rpc.math.multiply", {"x": 4, "y": 6})
    print(f"Multiply result: {result2}")
    
    # Batch calls
    batch_ops = [
        ("rpc.math.add", {"a": 1, "b": 2}),
        ("rpc.math.multiply", {"x": 3, "y": 4}),
        ("rpc.math.add", {"a": 10, "b": 20})
    ]
    
    batch_results = client.call_batch_sync(batch_ops)
    for i, result in enumerate(batch_results):
        if isinstance(result, Exception):
            print(f"Batch result {i+1}: Error - {result}")
        else:
            print(f"Batch result {i+1}: {result}")
    
    # Health check
    health = client.call_sync("rpc._health", target_instance="server1")
    print(f"Server health: {health['status']}, uptime: {health['uptime_seconds']}s")
    
    # Cleanup
    client.disconnect_sync()
    server.disconnect_sync()


async def async_example():
    """Example using async methods"""
    print("Async RPC Example:")
    print("-" * 30)
    
    # Create server and client
    server = NAS_RPC("localhost", 4222, namespace="test", instance_id="server2")
    client = NAS_RPC("localhost", 4222, namespace="test", instance_id="client2")
    
    await server.connect()
    await client.connect()
    
    # Register handlers
    async def subtract_handler(a: float, b: float) -> float:
        print(f"Server: Computing {a} - {b}")
        await asyncio.sleep(0.1)  # Simulate work
        return a - b
    
    server.register_handler("math.subtract", subtract_handler)
    await server.start_server("rpc")
    
    # Make RPC calls
    result = await client.call("rpc.math.subtract", {"a": 15, "b": 5})
    print(f"Subtract result: {result}")
    
    # Async call without waiting
    request_id = await client.call_async("rpc.math.subtract", {"a": 20, "b": 8})
    response = await client.get_response(request_id)
    print(f"Async call result: {response}")
    
    await client.disconnect()
    await server.disconnect()


# Enhanced example usage
async def main():
    """
    Simplified RPC example without service discovery.
    """
    print("=" * 50)
    print("Testing Simplified NAS_RPC")
    print("=" * 50)
    
    # Test sync example
    try:
        sync_example()
    except Exception as e:
        print(f"Sync example failed: {e}")
    
    print()
    
    # Test async example
    try:
        await async_example()
    except Exception as e:
        print(f"Async example failed: {e}")
    
    print("\nSimplified RPC test completed!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()