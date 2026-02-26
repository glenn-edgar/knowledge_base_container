from pathlib import Path
import yaml
from typing import Optional, Dict, List


class ChainTreeYaml:
    """
    Unified ChainTree YAML generator with ltree-based hierarchical structure.
    Combines YAML generation, node management, and function mapping.
    """
    
    def __init__(self, yaml_file: Path):
        """
        Initialize the ChainTree YAML generator.
        
        Args:
            yaml_file: Path to the YAML file to generate
        """
        self.yaml_file = yaml_file
        
        # Check yaml file path is valid (parent directory exists)
        if not self.yaml_file.parent.exists():
            raise FileNotFoundError(
                f"Parent directory for yaml file does not exist: {self.yaml_file.parent}"
            )
        
        # Core ltree structure
        self.separator = "."
        self.path_list: List[str] = []
        self.ltree_stack: List[str] = []
        self.yaml_data: Dict = {}  # Flat structure with ltree keys
        self.node_count = 0
        self.ltree_to_index: Dict[str, int] = {}  # Map ltree name to array index
        self.index_to_ltree: Dict[int, str] = {}  # Map array index to ltree name
        # Event string table for embedded systems
        self.event_string_table: Dict[str, int] = {}  # Map event_id to index
        self.event_index_counter = 0
        
        # Bitmask table for embedded systems (max 32 bits)
        
        self.bitmask_bit_counter = 0
        self.bitmask_table = {}           # str → int (event name to bit)
        self.used_bits = set()            # set of all occupied bit numbers
        self.next_auto_bit = 0     
        # Knowledge base management
        self.kb_dict: Dict[str, List[str]] = {}
        self.kb_log_dict: Dict[str, List[str]] = {}
        self.kb_metadata: Dict[str, Dict] = {}
        self.current_kb_name: Optional[str] = None
        
        # Node alias tables per KB (for named node references in C)
        self.node_alias_tables: Dict[str, Dict[str, int]] = {}
        
        # Function mappings per knowledge base
        self.main_functions: Dict[str, Dict[str, bool]] = {}
        self.one_shot_functions: Dict[str, Dict[str, bool]] = {}
        self.boolean_functions: Dict[str, Dict[str, bool]] = {}
        self.s_main_functions: Dict[str, Dict[str, bool]] = {}
        self.s_one_shot_functions: Dict[str, Dict[str, bool]] = {}
        self.s_boolean_functions: Dict[str, Dict[str, bool]] = {}
        
    
    # =========================================================================
    # Knowledge Base Management
    # =========================================================================
    
    def add_kb(self, kb_name: str) -> None:
        """Add a new knowledge base."""
        if not isinstance(kb_name, str):
            raise TypeError("kb_name must be a string")
        
        if kb_name in self.kb_dict:
            raise ValueError(f"Knowledge base {kb_name} already exists")
        
        path_list = ["kb", kb_name]
        self.kb_dict[kb_name] = path_list
        self.kb_log_dict[kb_name] = path_list.copy()
        
        # Initialize function mappings for this kb
        self._init_kb_function_mappings(kb_name)
    
    def select_kb(self, kb_name: str) -> None:
        """Select a knowledge base to work with."""
        if not isinstance(kb_name, str):
            raise TypeError("kb_name must be a string")
        
        if kb_name == self.current_kb_name:
            return
        
        # Auto-create kb if it doesn't exist
        if kb_name not in self.kb_dict:
            self.add_kb(kb_name)
        
        self.path_list = self.kb_dict[kb_name].copy()
        self.current_kb_name = kb_name
    
    def leave_kb(self) -> None:
        """Leave the current knowledge base."""
        if self.current_kb_name is None:
            raise ValueError("No knowledge base is currently selected")
        
        if len(self.path_list) != 2:
            raise ValueError(
                f"Path list is not at the root level: {self.path_list}"
            )
        
        # Ensure kb_metadata exists for this KB (defensive)
        if self.current_kb_name not in self.kb_metadata:
            self.kb_metadata[self.current_kb_name] = {
                "start_index": 0,
                "node_count": 0,
                "node_aliases": {}
            }
        
        kb_meta = self.kb_metadata[self.current_kb_name]
        
        # Calculate node count for this KB (if start_index exists)
        start_idx = kb_meta.get("start_index", 0)
        kb_meta["node_count"] = self.node_count - start_idx
        
        # Attach alias table to kb_metadata (if alias table exists)
        kb_meta["node_aliases"] = self.node_alias_tables.get(
            self.current_kb_name, {}
        ).copy()
        
        self.pop_path(self.path_list[0], self.path_list[1])
        del self.kb_dict[self.current_kb_name]
        self.current_kb_name = None
    
    def get_current_kb(self) -> Optional[str]:
        """Get the currently selected knowledge base name."""
        return self.current_kb_name
    
    def _init_kb_function_mappings(self, kb_name: str) -> None:
        """Initialize function mapping dictionaries for a knowledge base."""
        self.main_functions[kb_name] = {}
        self.one_shot_functions[kb_name] = {}
        self.boolean_functions[kb_name] = {}
        self.s_main_functions[kb_name] = {}
        self.s_one_shot_functions[kb_name] = {}
        self.s_boolean_functions[kb_name] = {}
        
        # Initialize alias table for this KB (only if not already set)
        if kb_name not in self.node_alias_tables:
            self.node_alias_tables[kb_name] = {}
        
        # Initialize KB metadata with start index (only if not already set)
        if kb_name not in self.kb_metadata:
            self.kb_metadata[kb_name] = {
                "start_index": self.node_count,
                "node_count": 0,
                "node_aliases": {}
            }
    
    # =========================================================================
    # Function Registration
    # =========================================================================
    
    def _check_kb_selected(self) -> None:
        """Ensure a knowledge base is selected."""
        if self.current_kb_name is None:
            raise ValueError("No knowledge base is currently selected")
    
    def add_main_function(self, function_name: str) -> None:
        """Register a main function in the current knowledge base."""
        self._check_kb_selected()
        self.main_functions[self.current_kb_name][function_name] = True
    
    def add_one_shot_function(self, function_name: str) -> None:
        """Register a one-shot function in the current knowledge base."""
        self._check_kb_selected()
        self.one_shot_functions[self.current_kb_name][function_name] = True
    
    def add_boolean_function(self, function_name: str) -> None:
        """Register a boolean function in the current knowledge base."""
        self._check_kb_selected()
        self.boolean_functions[self.current_kb_name][function_name] = True
    
    def add_s_main_function(self, function_name: str) -> None:
        """Register a secure main function in the current knowledge base."""
        self._check_kb_selected()
        self.s_main_functions[self.current_kb_name][function_name] = True
    
    def add_s_one_shot_function(self, function_name: str) -> None:
        """Register a secure one-shot function in the current knowledge base."""
        self._check_kb_selected()
        self.s_one_shot_functions[self.current_kb_name][function_name] = True
    
    def add_s_boolean_function(self, function_name: str) -> None:
        """Register a secure boolean function in the current knowledge base."""
        self._check_kb_selected()
        self.s_boolean_functions[self.current_kb_name][function_name] = True
    
    # =========================================================================
    # Node Alias Table Management
    # =========================================================================
    
    def register_node_alias(self, alias_name: str, ltree_name: Optional[str] = None) -> int:
            """
            Register a named alias for a node index.
            
            Returns:
                The node index
            """
            self._check_kb_selected()
            
            if not isinstance(alias_name, str):
                raise TypeError("alias_name must be a string")
            
            if ltree_name is None:
                if not self.ltree_stack:
                    raise ValueError("No current node and no ltree_name provided")
                ltree_name = self.ltree_stack[-1]
            
            if ltree_name not in self.ltree_to_index:
                raise KeyError(f"Node not found: {ltree_name}")
            
            node_index = self.ltree_to_index[ltree_name]
            
            # Ensure the KB has an alias table
            if self.current_kb_name not in self.node_alias_tables:
                self.node_alias_tables[self.current_kb_name] = {}
            
            # Check if alias already exists in THIS KB's table
            if alias_name in self.node_alias_tables[self.current_kb_name]:
                raise ValueError(f"Alias {alias_name} already exists in KB {self.current_kb_name}")
            
            # Store the node index
            self.node_alias_tables[self.current_kb_name][alias_name] = node_index
            
            return node_index
    
    def get_node_by_alias(self, alias_name: str, kb_name: Optional[str] = None) -> int:
        """
        Get node index by alias name.
        
        Returns:
            Integer node index
        """
        kb = kb_name or self.current_kb_name
        if kb is None:
            raise ValueError("No knowledge base specified")
        
        if alias_name not in self.node_alias_tables.get(kb, {}):
            raise KeyError(f"Alias not found: {alias_name}")
        
        return self.node_alias_tables[kb][alias_name]
    
    def get_ltree_by_alias(self, alias_name: str, kb_name: Optional[str] = None) -> str:
        """
        Get ltree path by alias name.
        
        Returns:
            ltree path string
        """
        node_index = self.get_node_by_alias(alias_name, kb_name)
        return self.index_to_ltree[node_index]
    def get_ltree_by_alias(self, alias_name: str, kb_name: Optional[str] = None) -> str:
        """
        Get ltree path by alias name.
        
        Args:
            alias_name: The alias to look up
            kb_name: Knowledge base name (uses current if None)
            
        Returns:
            ltree path string
            
        Raises:
            ValueError: If no KB specified
            KeyError: If alias not found
        """
        node_index = self.get_node_by_alias(alias_name, kb_name)
        return self.index_to_ltree[node_index]
    def get_node_alias_table(self, kb_name: Optional[str] = None) -> Dict[str, int]:
        """
        Get all aliases for a KB.
        
        Args:
            kb_name: Knowledge base name (uses current if None)
            
        Returns:
            Dictionary mapping alias names to node indices
        """
        kb = kb_name or self.current_kb_name
        if kb is None:
            raise ValueError("No knowledge base specified")
        
        return self.node_alias_tables.get(kb, {}).copy()
    
    def get_kb_metadata(self, kb_name: str) -> Dict:
        """
        Get complete KB metadata including start_index, node_count, and aliases.
        
        Args:
            kb_name: Knowledge base name
            
        Returns:
            Dictionary with start_index, node_count, and node_aliases
        """
        if kb_name not in self.kb_metadata:
            raise KeyError(f"KB not found: {kb_name}")
        return self.kb_metadata[kb_name].copy()
    
    # =========================================================================
    # Path Management
    # =========================================================================
    
    def get_current_path(self) -> List[str]:
        """Get the current path as a list."""
        return self.path_list.copy()
    
    def set_path_list(self, path_list: List[str]) -> None:
        """Set the current path list."""
        if not isinstance(path_list, list):
            raise TypeError("Path list must be a list")
        self.path_list = path_list.copy()
    
    def get_current_ltree_prefix(self) -> str:
        """Get the current path as an ltree prefix."""
        return self.separator.join(self.path_list) if self.path_list else ""
    
    def pop_path(self, label_name: str, node_name: str) -> None:
        """Pop the path to go back up the hierarchy."""
        if len(self.path_list) < 2:
            raise ValueError("Path list is too short to pop")
        
        local_node = self.path_list.pop()
        local_label = self.path_list.pop()
        
        if local_node != node_name or local_label != label_name:
            raise ValueError(
                f"Path mismatch: expected ({label_name}, {node_name}), "
                f"got ({local_label}, {local_node})"
            )
    
    def _create_ltree_name(self, label_name: str, node_name: str) -> tuple[str, str]:
        """
        Create an ltree name from current path plus label and node.
        
        Returns:
            Tuple of (ltree_name, parent_ltree_name)
        """
        all_parts = self.path_list + [label_name, node_name]
        ltree_name = self.separator.join(all_parts)
        parent_ltree_name = self.separator.join(self.path_list)
        return ltree_name, parent_ltree_name
    
    # =========================================================================
    # Node Creation - Core Methods
    # =========================================================================
    
    def define_composite_node(
            self,
            label_name: str,
            node_name: str,
            label_dict: Optional[Dict] = None,
            node_dict: Optional[Dict] = None
        ) -> int:
            """
            Define a composite node that can contain child nodes.
            Updates the path for nested nodes.
            
            Returns:
                Node count (node ID)
            """
            label_dict = label_dict or {}
            node_dict = node_dict or {}
            
            ltree_name, parent_ltree_name = self._create_ltree_name(label_name, node_name)
            label_dict["parent_ltree_name"] = parent_ltree_name
            label_dict["ltree_name"] = ltree_name
            label_dict["array_index"] = self.node_count  # Store array index for embedded C
            
            self.yaml_data[ltree_name] = {
                "label": label_name,
                "node_name": node_name,
                "label_dict": label_dict,
                "node_dict": node_dict
            }
            
            # Update path list to include this composite node
            self.path_list.append(label_name)
            self.path_list.append(node_name)
            
            # Store mapping from ltree_name to array index (and reverse)
            self.ltree_to_index[ltree_name] = self.node_count
            self.index_to_ltree[self.node_count] = ltree_name
            self.node_count += 1
            
            return self.node_count

    def define_simple_node(
        self,
        label_name: str,
        node_name: str,
        label_dict: Optional[Dict] = None,
        node_dict: Optional[Dict] = None
    ) -> int:
        """
        Define a simple node (leaf node) in the ltree structure.
        Does not update the path list.
        
        Returns:
            Node count (node ID)
        """
        label_dict = label_dict or {}
        node_dict = node_dict or {}
        
        ltree_name, parent_ltree_name = self._create_ltree_name(label_name, node_name)
        label_dict["parent_ltree_name"] = parent_ltree_name
        label_dict["ltree_name"] = ltree_name
        label_dict["array_index"] = self.node_count  # Store array index for embedded C
        
        self.yaml_data[ltree_name] = {
            "label": label_name,
            "node_name": node_name,
            "label_dict": label_dict,
            "node_dict": node_dict
        }
        
        # Store mapping from ltree_name to array index (and reverse)
        self.ltree_to_index[ltree_name] = self.node_count
        self.index_to_ltree[self.node_count] = ltree_name
        self.node_count += 1
        
        return self.node_count
    
    # =========================================================================
    # Node Creation - ChainTree-Specific Methods
    # =========================================================================
    
    def _add_node_link(self, ltree_name: str) -> None:
        """Add a link from the current parent node to a child node."""
        if len(self.ltree_stack) == 0:
            return
        
        if not isinstance(ltree_name, str):
            raise TypeError("ltree_name must be a string")
        
        parent_ltree = self.ltree_stack[-1]
        parent_data = self.yaml_data[parent_ltree]
        parent_data["label_dict"]["links"].append(ltree_name)
    
    def add_node_element(
        self,
        label_name: str,
        node_name: str,
        main_function_name: str,
        initialization_function_name: str,
        aux_function_name: str,
        termination_function_name: str,
        node_data: Dict,
        links_flag: bool = True
    ) -> str:
        """
        Add a composite node element with associated functions.
        
        Args:
            label_name: Label for the node
            node_name: Name of the node
            main_function_name: Main execution function
            initialization_function_name: Initialization function
            aux_function_name: Auxiliary boolean function
            termination_function_name: Termination function
            node_data: Node-specific data dictionary
            links_flag: Whether to add link from parent
            
        Returns:
            The ltree name of the created node
        """
        # Type validation
        if not isinstance(label_name, str):
            raise TypeError("label_name must be a string")
        if not isinstance(node_name, str):
            raise TypeError("node_name must be a string")
        if not isinstance(main_function_name, str):
            raise TypeError("main_function_name must be a string")
        if not isinstance(initialization_function_name, str):
            raise TypeError("initialization_function_name must be a string")
        if not isinstance(aux_function_name, str):
            raise TypeError("aux_function_name must be a string")
        if not isinstance(termination_function_name, str):
            raise TypeError("termination_function_name must be a string")
        if not isinstance(node_data, dict):
            raise TypeError("node_data must be a dictionary")
        
        # Build label data
        label_data = {
            "main_function_name": main_function_name,
            "initialization_function_name": initialization_function_name,
            "aux_function_name": aux_function_name,
            "termination_function_name": termination_function_name,
            "links": []
        }
        
        # Create the composite node
        self.define_composite_node(label_name, node_name, label_data, node_data)
        ltree_name = self.get_current_ltree_prefix()
        
        # Register functions
        self.add_main_function(main_function_name)
        self.add_boolean_function(aux_function_name)
        self.add_one_shot_function(termination_function_name)
        self.add_one_shot_function(initialization_function_name)
        
        # Add link from parent if requested
        if links_flag:
            self._add_node_link(ltree_name)
        
        # Push onto stack for children
        self.ltree_stack.append(ltree_name)
        
        return ltree_name
    
    def pop_node_element(self, ref_ltree_name: str) -> None:
        """
        Pop a node element from the stack and restore path.
        
        Args:
            ref_ltree_name: Expected ltree name to pop (for validation)
        """
        if not self.ltree_stack:
            raise ValueError("Ltree stack is empty")
        
        ltree_name = self.ltree_stack.pop()
        
        if ltree_name != ref_ltree_name:
            raise ValueError(
                f"Ltree name mismatch: expected {ref_ltree_name}, got {ltree_name}"
            )
        
        node_data = self.yaml_data[ltree_name]
        self.pop_path(node_data["label"], node_data["node_name"])
    
    def add_leaf_element(
        self,
        label_name: str,
        node_name: str,
        main_function_name: str,
        initialization_function_name: str,
        aux_function_name: str,
        termination_function_name: str,
        node_data: Dict
    ) -> str:
        """
        Add a leaf node element with associated functions.
        
        Args:
            label_name: Label for the node
            node_name: Name of the node
            main_function_name: Main execution function
            initialization_function_name: Initialization function
            aux_function_name: Auxiliary boolean function
            termination_function_name: Termination function
            node_data: Node-specific data dictionary
            
        Returns:
            The ltree name of the created node
        """
        # Type validation
        if not isinstance(label_name, str):
            raise TypeError("label_name must be a string")
        if not isinstance(node_name, str):
            raise TypeError("node_name must be a string")
        if not isinstance(main_function_name, str):
            raise TypeError("main_function_name must be a string")
        if not isinstance(initialization_function_name, str):
            raise TypeError("initialization_function_name must be a string")
        if not isinstance(aux_function_name, str):
            raise TypeError("aux_function_name must be a string")
        if not isinstance(termination_function_name, str):
            raise TypeError("termination_function_name must be a string")
        if not isinstance(node_data, dict):
            raise TypeError("node_data must be a dictionary")
        
        # Build label data
        label_data = {
            "main_function_name": main_function_name,
            "initialization_function_name": initialization_function_name,
            "aux_function_name": aux_function_name,
            "termination_function_name": termination_function_name,
            "links": []
        }
        
        # Create the simple node
        self.define_simple_node(label_name, node_name, label_data, node_data)
        ltree_name, _ = self._create_ltree_name(label_name, node_name)
        
        # Register functions
        self.add_main_function(main_function_name)
        self.add_boolean_function(aux_function_name)
        self.add_one_shot_function(termination_function_name)
        self.add_one_shot_function(initialization_function_name)
        
        # Add link from parent
        self._add_node_link(ltree_name)
        
        return ltree_name
    
    # =========================================================================
    # Event String Table Management (for embedded C)
    # =========================================================================
    
    def register_event(self, event_id: str) -> int:
        """
        Register an event ID in the string table and return its position.
        If the event_id was previously registered, return existing position.
        
        This is used for embedded C systems to avoid storing duplicate strings.
        Event IDs are stored once in a string table and referenced by integer index.
        
        Args:
            event_id: String identifier for the event
            
        Returns:
            Integer index/position in the string table (0-based)
            
        Example:
            >>> idx1 = ct.register_event("BUTTON_PRESSED")  # Returns: 0
            >>> idx2 = ct.register_event("TIMER_EXPIRED")   # Returns: 1
            >>> idx3 = ct.register_event("BUTTON_PRESSED")  # Returns: 0 (already exists)
        """
        if not isinstance(event_id, str):
            raise TypeError("event_id must be a string")
        
        # Check if already registered
        if event_id in self.event_string_table:
            return self.event_string_table[event_id]
        
        # Register new event
        index = self.event_index_counter
        self.event_string_table[event_id] = index
        self.event_index_counter += 1
        
        return index
    
    def get_event_index(self, event_id: str) -> int:
        """
        Get the index for a previously registered event.
        
        Args:
            event_id: String identifier for the event
            
        Returns:
            Integer index in the string table
            
        Raises:
            KeyError: If event_id not registered
        """
        if event_id not in self.event_string_table:
            raise KeyError(f"Event ID not registered: {event_id}")
        return self.event_string_table[event_id]
    
    def get_all_events(self) -> Dict[str, int]:
        """
        Get all registered events and their indices.
        
        Returns:
            Dictionary mapping event_id to index
        """
        return self.event_string_table.copy()
    
    def get_event_string_table_size(self) -> int:
        """
        Get the total number of unique events registered.
        
        Returns:
            Number of unique event IDs
        """
        return self.event_index_counter
    


    def register_bitmask(self, event: str | int) -> int:
        """
        Register an event and return its bit position.
        
        - If `event` is a **string**:
          - If already registered → return existing bit
          - Else → auto-assign the next free bit (skipping any explicitly reserved bits)
        - If `event` is an **integer**:
          - Try to reserve exactly that bit position
          - Raise ValueError if already in use
        
        Once a bit is allocated (auto or explicit), it cannot be overwritten.
        
        Raises:
            TypeError: If event is neither str nor int
            ValueError: If bit position out of 0–31
            ValueError: If requested bit (explicit) is already used
            ValueError: If more than 32 bits needed
        """
        if isinstance(event, str):
            name = event
            if name in self.bitmask_table:
                return self.bitmask_table[name]

            # Find next free bit (skip used ones)
            bit = self.next_auto_bit
            while bit in self.used_bits:
                bit += 1
                if bit > 31:
                    raise ValueError(
                        "No free bits left (0-31). "
                        f"Used bits: {sorted(self.used_bits)}"
                    )

            # Register
            self.bitmask_table[name] = bit
            self.used_bits.add(bit)
            self.next_auto_bit = bit + 1  # next auto starts after this one

            return bit

        elif isinstance(event, int):
            bit = event
            if not (0 <= bit <= 31):
                raise ValueError(f"Bit position must be 0–31, got {bit}")

            if bit in self.used_bits:
                # Find who owns it
                owner = next(
                    (n for n, b in self.bitmask_table.items() if b == bit),
                    "explicit reservation"
                )
                raise ValueError(f"Bit {bit} already in use by '{owner}'")

            # Reserve it
            self.bitmask_table[f"EXPLICIT_{bit}"] = bit  # optional pseudo-name
            self.used_bits.add(bit)

            # Update auto-next if needed
            if bit >= self.next_auto_bit:
                self.next_auto_bit = bit + 1

            return bit

        else:
            raise TypeError("event must be str (event name) or int (bit position)")
        def get_bitmask_bit(self, event: str | int) -> int:
            """
            Get the bit position for a previously registered event.
            
            - If a string is passed: Returns the bit position for the registered event name.
            - If an integer is passed: Returns the integer itself, but only if it has been 
            previously registered (explicitly or auto-allocated); otherwise raises ValueError.
            
            Args:
                event: Either a string (event name) or an integer (bit position)
                
            Returns:
                Integer bit position (0-31)
                
            Raises:
                TypeError: If event is neither str nor int
                KeyError: If string event_name is not registered
                ValueError: If integer bit position has not been allocated/registered
            """
            if isinstance(event, str):
                if event not in self.bitmask_table:
                    raise KeyError(f"Bitmask event not registered: {event}")
                return self.bitmask_table[event]

            elif isinstance(event, int):
                bit = event
                
                # Check if this bit has been allocated to any event
                if bit not in self.bitmask_table.values():
                    raise ValueError(
                        f"Bit position {bit} has not been allocated "
                        f"(no event registered with this bit)"
                    )
                
                # Optionally: you can return the bit directly, or even return the event name too
                # But since the function is expected to return the bit position, just return it
                return bit

            else:
                raise TypeError("event must be a string (event name) or an integer (bit position)")
            
            
    def get_all_bitmasks(self) -> Dict[str, int]:
        """
        Get all registered bitmask events and their bit positions.
        
        Returns:
            Dictionary mapping event_name to bit_number
        """
        return self.bitmask_table.copy()
    
    def get_bitmask_count(self) -> int:
        """
        Get the total number of bitmask events registered.
        
        Returns:
            Number of registered bitmask events (0-32)
        """
        return self.bitmask_bit_counter
    
    def get_bitmask_value(self, event_name: str) -> int:
        """
        Get the bitmask value (1 << bit_number) for an event.
        
        Args:
            event_name: String identifier for the event
            
        Returns:
            Integer bitmask value (e.g., 1, 2, 4, 8, ...)
            
        Example:
            >>> ct.register_bitmask("BUTTON_PRESSED")  # bit 0
            >>> ct.get_bitmask_value("BUTTON_PRESSED")  # Returns: 1 (0x01)
        """
        bit_number = self.get_bitmask_bit(event_name)
        return 1 << bit_number
    
    # =========================================================================
    # Array Index Mapping (for embedded C)
    # =========================================================================
    
    def get_node_index(self, ltree_name: str) -> int:
        """
        Get the integer array index for a given ltree name.
        For use in embedded C systems where nodes are stored in arrays.
        
        Args:
            ltree_name: The full ltree path name
            
        Returns:
            Integer index (0-based) for the node in the array
            
        Raises:
            KeyError: If ltree_name not found
        """
        if ltree_name not in self.ltree_to_index:
            raise KeyError(f"Ltree name not found: {ltree_name}")
        return self.ltree_to_index[ltree_name]
    
    def get_all_node_indices(self) -> Dict[str, int]:
        """
        Get all ltree name to index mappings.
        
        Returns:
            Dictionary mapping ltree names to integer indices
        """
        return self.ltree_to_index.copy()
    
    def get_total_node_count(self) -> int:
        """
        Get the total number of nodes created.
        This is the size of the array needed in embedded C.
        
        Returns:
            Total number of nodes
        """
        return self.node_count
    
    # =========================================================================
    # Assembly and Validation
    # =========================================================================
    
    def start_assembly(self) -> None:
        """Start a new assembly session by clearing the ltree stack."""
        self.ltree_stack = []
    
    def check_for_balance_ltree(self) -> None:
        """Verify that all composite nodes have been properly closed."""
        if self.ltree_stack:
            raise ValueError(
                f"Ltrees have not been closed: {self.ltree_stack}"
            )
    
    # =========================================================================
    # YAML Generation
    # =========================================================================
    
    def generate_yaml(self) -> Dict:
        """
        Generate the YAML file with all collected data.
        
        Returns:
            The complete yaml_data dictionary
        """
        # Add event string table node if any events were registered
        if self.event_string_table:
            self._create_event_string_table_node()
        
        # Add bitmask table node if any bitmasks were registered
        if self.bitmask_table:
            self._create_bitmask_table_node()
        
        self.yaml_data["kb_log_dict"] = self.kb_log_dict
        self.yaml_data["ltree_to_index"] = self.ltree_to_index
        self.yaml_data["total_nodes"] = self.node_count
        self.yaml_data["kb_metadata"] = self.kb_metadata
        if self.kb_dict:
            raise ValueError(f"Knowledge bases still open: {list(self.kb_dict.keys())}")
        
        with open(self.yaml_file, 'w') as f:
            yaml.dump(self.yaml_data, f, default_flow_style=False, sort_keys=False)
        
        return self.yaml_data
    
    def _create_event_string_table_node(self) -> None:
        """
        Internal method to create a node containing the event string table.
        Called automatically by generate_yaml() if events were registered.
        """
        # Temporarily create a KB for the event string table
        temp_kb_name = "event_string_table_kb"
        self.add_kb(temp_kb_name)
        self.select_kb(temp_kb_name)
        
        # Create the node with event string table
        # Store as dictionary mapping event_id -> index
        self.add_leaf_element(
            "event_strings",
            "event_string_table",
            "CFL_NULL",
            "CFL_NULL",
            "CFL_NULL",
            "CFL_NULL",
            self.event_string_table.copy()
        )
        
        # Leave and clean up the temporary KB
        self.leave_kb()
        
        # Remove from kb_log_dict since this is just for the event table
        if temp_kb_name in self.kb_log_dict:
            del self.kb_log_dict[temp_kb_name]
    
    def _create_bitmask_table_node(self) -> None:
        """
        Internal method to create a node containing the bitmask table.
        Called automatically by generate_yaml() if bitmasks were registered.
        """
        # Temporarily create a KB for the bitmask table
        temp_kb_name = "bitmask_table_kb"
        self.add_kb(temp_kb_name)
        self.select_kb(temp_kb_name)
        
        # Create the node with bitmask table
        # Store as dictionary mapping event_name -> bit_number
        self.add_leaf_element(
            "bitmask",
            "bitmask_table",
            "CFL_NULL",
            "CFL_NULL",
            "CFL_NULL",
            "CFL_NULL",
            self.bitmask_table.copy()
        )
        
        # Leave and clean up the temporary KB
        self.leave_kb()
        
        # Remove from kb_log_dict since this is just for the bitmask table
        if temp_kb_name in self.kb_log_dict:
            del self.kb_log_dict[temp_kb_name]
    
    # =========================================================================
    # Function Mapping Access
    # =========================================================================
    
    def get_function_mappings(self, kb_name: Optional[str] = None) -> Dict:
        """
        Get all function mappings for a knowledge base.
        
        Args:
            kb_name: Knowledge base name (uses current if None)
            
        Returns:
            Dictionary with all function mapping types
        """
        kb = kb_name or self.current_kb_name
        if kb is None:
            raise ValueError("No knowledge base specified")
        
        return {
            "main_functions": self.main_functions.get(kb, {}),
            "one_shot_functions": self.one_shot_functions.get(kb, {}),
            "boolean_functions": self.boolean_functions.get(kb, {}),
            "s_main_functions": self.s_main_functions.get(kb, {}),
            "s_one_shot_functions": self.s_one_shot_functions.get(kb, {}),
            "s_boolean_functions": self.s_boolean_functions.get(kb, {})
        }


if __name__ == "__main__":
    print("ChainTree YAML Generator Test")
    print("=" * 70)
    
    yaml_file = Path("chaintree_config.yaml")
    print(f"Creating YAML file: {yaml_file.absolute()}")
    print("-" * 70)
    
    # Initialize the generator
    ct = ChainTreeYaml(yaml_file)
    
    # Create a knowledge base
    ct.add_kb("robot_control")
    ct.select_kb("robot_control")
    
    print("Building robot control tree...")
    ct.start_assembly()
    
    # Root node
    root = ct.add_node_element(
        "behavior", "robot_main",
        main_function_name="robot_main_loop",
        initialization_function_name="init_robot",
        aux_function_name="check_robot_status",
        termination_function_name="shutdown_robot",
        node_data={"priority": 1, "enabled": True},
        links_flag=False
    )
    
    # Register alias for root node
    ct.register_node_alias("root")
    
    # Navigation subsystem
    nav = ct.add_node_element(
        "subsystem", "navigation",
        main_function_name="nav_update",
        initialization_function_name="init_navigation",
        aux_function_name="check_nav_ready",
        termination_function_name="stop_navigation",
        node_data={"update_rate_hz": 50}
    )
    
    # Register alias for navigation
    ct.register_node_alias("nav_root")
    
    # Navigation leaves
    lidar_ltree = ct.add_leaf_element(
        "sensor", "lidar",
        main_function_name="read_lidar",
        initialization_function_name="init_lidar",
        aux_function_name="lidar_ready",
        termination_function_name="close_lidar",
        node_data={"port": "/dev/ttyUSB0", "range_m": 10.0}
    )
    
    # Register alias for lidar using explicit ltree name
    ct.register_node_alias("main_lidar", lidar_ltree)
    
    ct.add_leaf_element(
        "control", "path_planner",
        main_function_name="plan_path",
        initialization_function_name="init_planner",
        aux_function_name="planner_ready",
        termination_function_name="stop_planner",
        node_data={"algorithm": "A*", "grid_size": 0.1}
    )
    
    ct.pop_node_element(nav)
    
    # Manipulation subsystem
    manip = ct.add_node_element(
        "subsystem", "manipulation",
        main_function_name="manip_update",
        initialization_function_name="init_manipulation",
        aux_function_name="check_manip_ready",
        termination_function_name="stop_manipulation",
        node_data={"update_rate_hz": 100}
    )
    
    # Register alias for manipulation
    ct.register_node_alias("manip_root")
    
    gripper_ltree = ct.add_leaf_element(
        "actuator", "gripper",
        main_function_name="control_gripper",
        initialization_function_name="init_gripper",
        aux_function_name="gripper_ready",
        termination_function_name="close_gripper",
        node_data={"max_force_n": 50, "speed_mm_s": 100}
    )
    
    # Register alias for gripper
    ct.register_node_alias("gripper", gripper_ltree)
    
    ct.pop_node_element(manip)
    ct.pop_node_element(root)
    
    # Demonstrate event registration
    print("\nRegistering events...")
    event1 = ct.register_event("BUTTON_PRESSED")
    event2 = ct.register_event("TIMER_EXPIRED")
    event3 = ct.register_event("SENSOR_READY")
    event4 = ct.register_event("BUTTON_PRESSED")  # Duplicate
    event5 = ct.register_event("EMERGENCY_STOP")
    
    print(f"  'BUTTON_PRESSED' -> index {event1}")
    print(f"  'TIMER_EXPIRED' -> index {event2}")
    print(f"  'SENSOR_READY' -> index {event3}")
    print(f"  'BUTTON_PRESSED' (again) -> index {event4}")
    print(f"  'EMERGENCY_STOP' -> index {event5}")
    print(f"  Total unique events: {ct.get_event_string_table_size()}")
    
    # Demonstrate bitmask registration
    print("\nRegistering bitmasks...")
    bit1 = ct.register_bitmask("MOTOR_ENABLED")
    bit2 = ct.register_bitmask("SENSOR_ACTIVE")
    bit3 = ct.register_bitmask("ERROR_FLAG")
    bit4 = ct.register_bitmask("MOTOR_ENABLED")  # Duplicate
    bit5 = ct.register_bitmask("CALIBRATED")
    
    print(f"  'MOTOR_ENABLED' -> bit {bit1} (mask: 0x{ct.get_bitmask_value('MOTOR_ENABLED'):08X})")
    print(f"  'SENSOR_ACTIVE' -> bit {bit2} (mask: 0x{ct.get_bitmask_value('SENSOR_ACTIVE'):08X})")
    print(f"  'ERROR_FLAG' -> bit {bit3} (mask: 0x{ct.get_bitmask_value('ERROR_FLAG'):08X})")
    print(f"  'MOTOR_ENABLED' (again) -> bit {bit4}")
    print(f"  'CALIBRATED' -> bit {bit5} (mask: 0x{ct.get_bitmask_value('CALIBRATED'):08X})")
    print(f"  Total bitmask events: {ct.get_bitmask_count()}")
    
    # Verify balance
    ct.check_for_balance_ltree()
    
    # Leave the kb
    ct.leave_kb()
    
    # Generate YAML
    print("\nGenerating YAML...")
    data = ct.generate_yaml()
    
    print("\nGenerated YAML content:")
    print("-" * 70)
    with open(yaml_file, 'r') as f:
        print(f.read())
    
    # Show function mappings
    print("\nFunction Mappings:")
    print("-" * 70)
    mappings = ct.get_function_mappings("robot_control")
    for func_type, functions in mappings.items():
        if functions:
            print(f"\n{func_type}:")
            for func_name in functions:
                print(f"  - {func_name}")
    
    print("\n" + "=" * 70)
    print(f"Total nodes created: {ct.node_count}")
    print(f"YAML file: {yaml_file.absolute()}")
    
    # Demonstrate KB metadata with aliases
    print("\n" + "=" * 70)
    print("KB Metadata (for embedded C kb_descriptor_t):")
    print("-" * 70)
    
    kb_meta = ct.get_kb_metadata("robot_control")
    print(f"\nKB: robot_control")
    print(f"  start_index: {kb_meta['start_index']}")
    print(f"  node_count: {kb_meta['node_count']}")
    print(f"  node_aliases:")
    for alias, index in kb_meta['node_aliases'].items():
        print(f"    {alias}: {index}")
    
    print("\nGenerated C structures:")
    print("-" * 70)
    print("""
// Generic KB descriptor structure
typedef struct {
    const char *name;
    uint16_t start_index;
    uint16_t node_count;
    const node_alias_t *aliases;
    uint16_t alias_count;
} kb_descriptor_t;

typedef struct {
    const char *alias;
    uint16_t node_index;
} node_alias_t;
""")
    
    print("// Per-KB alias tables")
    print("static const node_alias_t kb_0_aliases[] = {")
    for alias, index in kb_meta['node_aliases'].items():
        print(f'    {{"{alias}", {index}}},')
    print("};")
    
    print("\n// KB descriptor table")
    print("static const kb_descriptor_t kb_table[] = {")
    print("    {")
    print(f'        .name = "robot_control",')
    print(f'        .start_index = {kb_meta["start_index"]},')
    print(f'        .node_count = {kb_meta["node_count"]},')
    print(f'        .aliases = kb_0_aliases,')
    print(f'        .alias_count = {len(kb_meta["node_aliases"])}')
    print("    },")
    print("};")
    print(f"#define KB_COUNT 1")
    
    # Demonstrate array indexing features
    print("\n" + "=" * 70)
    print("Array Index Features (for embedded C):")
    print("-" * 70)
    
    print(f"\nTotal array size needed: {ct.get_total_node_count()} nodes")
    
    print("\nNode array indices:")
    test_ltree_names = [
        "kb.robot_control.behavior.robot_main",
        "kb.robot_control.behavior.robot_main.subsystem.navigation",
        "kb.robot_control.behavior.robot_main.subsystem.navigation.sensor.lidar"
    ]
    
    for ltree_name in test_ltree_names:
        try:
            index = ct.get_node_index(ltree_name)
            print(f"  [{index}] {ltree_name}")
        except KeyError as e:
            print(f"  Error: {e}")
    
    print("\nAll node indices:")
    all_indices = ct.get_all_node_indices()
    for ltree_name, index in sorted(all_indices.items(), key=lambda x: x[1]):
        print(f"  node_array[{index}] = {ltree_name}")
    
    print("\n" + "=" * 70)
    print("Event String Table (for embedded C):")
    print("-" * 70)
    
    print(f"\nTotal unique events: {ct.get_event_string_table_size()}")
    print("\nEvent string array:")
    
    all_events = ct.get_all_events()
    for event_id, index in sorted(all_events.items(), key=lambda x: x[1]):
        print(f"  event_strings[{index}] = \"{event_id}\"")
    
    print("\nIn C code:")
    print("  const char *event_strings[] = {")
    for event_id, index in sorted(all_events.items(), key=lambda x: x[1]):
        print(f"    /* [{index}] */ \"{event_id}\",")
    print("  };")
    
    print("\n" + "=" * 70)
    print("Bitmask Table (for embedded C):")
    print("-" * 70)
    
    print(f"\nTotal bitmask events: {ct.get_bitmask_count()}")
    print("\nBitmask definitions:")
    
    all_bitmasks = ct.get_all_bitmasks()
    for event_name, bit_number in sorted(all_bitmasks.items(), key=lambda x: x[1]):
        mask_value = 1 << bit_number
        print(f"  Bit {bit_number:2d}: {event_name:20s} = 0x{mask_value:08X}")
    
    print("\nIn C code:")
    print("  // Bitmask defines")
    for event_name, bit_number in sorted(all_bitmasks.items(), key=lambda x: x[1]):
        mask_value = 1 << bit_number
        define_name = event_name.upper().replace(" ", "_").replace("-", "_")
        print(f"  #define BIT_{define_name:25s} {bit_number:2d}  // 0x{mask_value:08X}")
    
    print("\n  // Usage:")
    print("  uint32_t status = 0;")
    for event_name, bit_number in sorted(all_bitmasks.items(), key=lambda x: x[1]):
        define_name = event_name.upper().replace(" ", "_").replace("-", "_")
        print(f"  status |= (1 << BIT_{define_name});  // Set bit")
        break  # Just show one example
    print("  if (status & (1 << BIT_MOTOR_ENABLED)) { /* check bit */ }")
    
    print("\n" + "=" * 70)
    print("✓ Test completed successfully!")