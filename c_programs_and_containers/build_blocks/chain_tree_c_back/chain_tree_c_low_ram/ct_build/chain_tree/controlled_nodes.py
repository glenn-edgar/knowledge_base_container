from .column_flow import ColumnFlow


class ControlledNodes(ColumnFlow):
    """
    DSL for defining controlled (dead) nodes and their clients.
    
    Controlled nodes are dormant nodes that exist structurally within a container
    but do not receive ChainTree events until activated by a client node. This
    implements a client-server model where:
    
    - Server (controlled_node): Holds behavior, receives events when enabled
    - Client (client_controlled_node): Initiates activation, receives completion/exceptions
    - Container (controlled_node_container): Structural owner, memory scope
    
    Lifecycle:
        1. Client calls initialization method on server
        2. Client calls aux function with init event
        3. Client calls main function with configuration event (request_port data)
        4. Client sets enabled/initialized flags
        5. Server receives events, executes behavior
        6. Server sends completion event with response_port data
        7. Server clears flags, returns to dead state
    
    Exception routing:
        Exceptions in controlled nodes route to the client, not the structural
        parent. If unhandled, they bubble up the client's tree.
    
    Lifecycle coupling:
        If a client terminates, its controlled node is also terminated.
        One client controls exactly one server (enforced at runtime).
    
    Usage:
        cn = ControlledNodes(ctb)
        
        with cn.controlled_node_container("pool"):
            cn.controlled_node(
                api_name="sensor_handler",
                column_name="sensor_v1",
                aux_function_name="SENSOR_AUX",
                aux_data={"threshold": 100},
                request_port=cn.make_port("sensor_buffers.h", 1, "SENSOR_REQUEST"),
                response_port=cn.make_port("sensor_buffers.h", 2, "SENSOR_RESPONSE")
            )
        
        # Elsewhere in tree
        cn.client_controlled_node(
            api_name="sensor_handler",
            column_name="sensor_client",
            aux_function_name="CLIENT_AUX",
            aux_data={},
            request_port=cn.make_port("sensor_buffers.h", 1, "SENSOR_REQUEST"),
            response_port=cn.make_port("sensor_buffers.h", 2, "SENSOR_RESPONSE")
        )
    """
    
    def __init__(self, ctb):
        """
        Initialize ControlledNodes DSL.
        
        Args:
            ctb: ChainTree builder instance providing tree construction context
        """
        self.ctb = ctb
        ColumnFlow.__init__(self, ctb)
        
    def make_port(self, file_name: str, handler_id: int, event: str):
        """
        Create a port definition for typed buffer communication.
        
        Ports define the data contract between client and server nodes.
        Buffer types are defined in .h files using Avro-style schemas,
        identified by file name and handler ID.
        
        Args:
            file_name: Name of .h file containing buffer definition
            handler_id: Identifier for specific buffer type within file
            event: Event name that carries this buffer data
        
        Returns:
            dict: Port definition with file_name, handler_id, and event_id
        
        Raises:
            TypeError: If arguments are not of expected types
        """
        if not isinstance(file_name, str):
            raise TypeError("File name must be a string")
        if not isinstance(handler_id, int):
            raise TypeError("Handler id must be an integer")
        if not isinstance(event, str):
            raise TypeError("Event must be a string")
        event_id = self.ctb.register_event(event)
        port_data = {"file_name": file_name, "handler_id": handler_id, "event_id": event_id}
        return port_data
    
    def _validate_port_defined(self, port: dict, port_name: str, node_name: str):
        """
        Validate that a port is fully defined.
        
        Args:
            port: Port dictionary to validate
            port_name: Name of port for error messages (e.g., "request_port")
            node_name: Name of node for error messages
        
        Raises:
            ValueError: If port is empty or missing required fields
        """
        if not port:
            raise ValueError(f"{port_name} must be defined for {node_name}")
        required_fields = ["file_name", "handler_id", "event_id"]
        for field in required_fields:
            if field not in port:
                raise ValueError(f"{port_name} missing required field '{field}' for {node_name}")
    
    def _validate_port_match(self, port_name: str, client_port: dict, server_port: dict, api_name: str):
        """
        Validate that client and server port definitions match.
        
        Ensures type safety by verifying both ends of the client-server
        relationship agree on buffer types.
        
        Args:
            port_name: Name of port for error messages
            client_port: Client's port definition
            server_port: Server's port definition
            api_name: API name for error messages
        
        Raises:
            ValueError: If file_name or handler_id do not match
        """
        for field in ["file_name", "handler_id"]:
            if client_port.get(field) != server_port.get(field):
                raise ValueError(
                    f"{port_name} {field} mismatch for api '{api_name}': "
                    f"client={client_port.get(field)}, server={server_port.get(field)}"
                )
    
    def controlled_node_container(self, column_name: str):
        """
        Define a container for controlled nodes.
        
        Containers provide structural ownership and memory scope for controlled
        nodes. They do not control activation - that is the client's responsibility.
        Containers auto-start with the tree.
        
        Args:
            column_name: Identifier for this container in the tree
        
        Returns:
            Node identifier for the created container
        """
        return self.define_column(
            column_name,
            main_function="CFL_CONTROLLED_NODE_CONTAINER_MAIN",
            initialization_function="CFL_CONTROLLED_NODE_CONTAINER_INIT",
            termination_function="CFL_CONTROLLED_NODE_CONTAINER_TERM",
            aux_function="CFL_NULL",
            column_data={},
            auto_start=True
        )
        
    def controlled_node(self, api_name: str, column_name: str, aux_function_name: str, aux_data: dict, request_port: dict, response_port: dict):
        """
        Define a controlled (dead) node within a container.
        
        Controlled nodes are dormant until activated by a client. They exist
        structurally in the tree with memory allocated, but do not receive
        ChainTree events until enabled.
        
        Must be defined as a child of a controlled_node_container.
        Must be defined before its client_controlled_node.
        
        Args:
            api_name: Registry key for client binding. Must be unique.
            column_name: Identifier for this node in the tree
            aux_function_name: C function name for aux event handling
            aux_data: Static configuration passed to aux function
            request_port: Typed buffer definition for activation data (from client)
            response_port: Typed buffer definition for completion data (to client)
        
        Returns:
            Node identifier for the created controlled node
        
        Raises:
            ValueError: If parent is not a controlled_node_container
            ValueError: If ports are not fully defined
            ValueError: If api_name is already registered
        """
        # Validate ports are defined
        self._validate_port_defined(request_port, "request_port", column_name)
        self._validate_port_defined(response_port, "response_port", column_name)
        
        # Validate parent is a container
        parent_node = self.ctb.ltree_stack[-1]
        parent_data = self.ctb.yaml_data[parent_node]
    
        parent_main_function = parent_data["label_dict"]["main_function_name"]
        if parent_main_function != "CFL_CONTROLLED_NODE_CONTAINER_MAIN":
            raise ValueError(f"Parent node {parent_node} is not a controlled node container")
        
        column_data = {
            "request_port": request_port,
            "response_port": response_port,
            "aux_data": aux_data
        }
        
        return_value = self.define_column(
            column_name,
            main_function="CFL_CONTROLLED_NODE_MAIN",
            initialization_function="CFL_CONTROLLED_NODE_INIT",
            termination_function="CFL_CONTROLLED_NODE_TERM",
            aux_function=aux_function_name,
            column_data=column_data,
            auto_start=False
        )
        
        self.ctb.register_node_alias(api_name, return_value)
        return return_value
    
    def client_controlled_node(self, api_name: str, aux_function_name: str, aux_data: dict, request_port: dict, response_port: dict):
            """
            Define a client node that controls a dead node.
            """
            # Validate client ports are defined
            self._validate_port_defined(request_port, "request_port", api_name)
            self._validate_port_defined(response_port, "response_port", api_name)
            
            # Get server node - use ltree for yaml lookup, index for C code
            server_ltree = self.ctb.get_ltree_by_alias(api_name)
            server_node_index = self.ctb.get_node_by_alias(api_name)
            server_data = self.ctb.yaml_data[server_ltree]
            
            # column_data is in node_dict, not label_dict
            server_column_data = server_data["node_dict"]["column_data"]
            
            # Get server ports
            server_request_port = server_column_data.get("request_port")
            server_response_port = server_column_data.get("response_port")
            
            # Validate ports match server
            self._validate_port_match("request_port", request_port, server_request_port, api_name)
            self._validate_port_match("response_port", response_port, server_response_port, api_name)
            
            node_data = {
                "request_port": request_port,
                "response_port": response_port,
                "aux_data": aux_data,
                "api_name": api_name,
                "server_node_index": server_node_index
            }
            
            return self.define_column_link(
                "CFL_CLIENT_CONTROLLED_NODE_MAIN",
                "CFL_CLIENT_CONTROLLED_NODE_INIT",
                aux_function_name,
                "CFL_CLIENT_CONTROLLED_NODE_TERM",
                node_data,
                label="CLIENT"
            )