"""
ChainTree JSON Generator - DSL for building ChainTree configurations.

Generates JSON output for consumption by:
- C code generator (Python)
- Zig runtime (std.json parsing)
- Any other JSON-capable runtime

JSON is preferred over YAML because:
- Zig's std.json works at comptime and runtime
- Faster parsing, stricter format
- No ambiguity in edge cases
"""

from pathlib import Path
import json
from typing import Optional, Dict, List, Any


class ChainTreeJson:
    """
    Unified ChainTree JSON generator with ltree-based hierarchical structure.
    Combines JSON generation, node management, and function mapping.
    """
    
    def __init__(self, json_file: Path):
        """
        Initialize the ChainTree JSON generator.
        
        Args:
            json_file: Path to the JSON file to generate
        """
        self.json_file = json_file
        
        # Check json file path is valid (parent directory exists)
        if not self.json_file.parent.exists():
            raise FileNotFoundError(
                f"Parent directory for json file does not exist: {self.json_file.parent}"
            )
        
        # Core ltree structure
        self.separator = "."
        self.path_list: List[str] = []
        self.ltree_stack: List[str] = []
        self.config_data: Dict[str, Any] = {}  # Flat structure with ltree keys
        self.node_count = 0
        self.ltree_to_index: Dict[str, int] = {}  # Map ltree name to array index
        
        # Event string table for embedded systems
        self.event_string_table: Dict[str, int] = {}  # Map event_id to index
        self.event_index_counter = 0
        
        # Bitmask table for embedded systems (max 32 bits)
        self.bitmask_table: Dict[str, int] = {}  # Map event_name to bit_number
        self.bitmask_bit_counter = 0
        
        # Knowledge base management
        self.kb_dict: Dict[str, List[str]] = {}
        self.kb_log_dict: Dict[str, List[str]] = {}
        self.kb_metadata: Dict[str, Dict] = {}
        self.current_kb_name: Optional[str] = None
        
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
        
        self.pop_path(self.path_list[0], self.path_list[1])
        del self.kb_dict[self.current_kb_name]
        self.current_kb_name = None
    
    def get_current_kb(self) -> Optional[str]:
        """Get the currently selected knowledge base name."""
        return self.current_kb_name
    
    def set_kb_metadata(self, kb_name: str, key: str, value: Any) -> None:
        """Set metadata for a knowledge base."""
        if kb_name not in self.kb_metadata:
            self.kb_metadata[kb_name] = {}
        self.kb_metadata[kb_name][key] = value
    
    def _init_kb_function_mappings(self, kb_name: str) -> None:
        """Initialize function mapping dictionaries for a knowledge base."""
        self.main_functions[kb_name] = {}
        self.one_shot_functions[kb_name] = {}
        self.boolean_functions[kb_name] = {}
        self.s_main_functions[kb_name] = {}
        self.s_one_shot_functions[kb_name] = {}
        self.s_boolean_functions[kb_name] = {}
    
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
    
    def _create_ltree_name(self, label_name: str, node_name: str) -> tuple:
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
        label_dict["array_index"] = self.node_count
        
        self.config_data[ltree_name] = {
            "label": label_name,
            "node_name": node_name,
            "label_dict": label_dict,
            "node_dict": node_dict
        }
        
        # Update path list to include this composite node
        self.path_list.append(label_name)
        self.path_list.append(node_name)
        
        # Store mapping from ltree_name to array index
        self.ltree_to_index[ltree_name] = self.node_count
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
        label_dict["array_index"] = self.node_count
        
        self.config_data[ltree_name] = {
            "label": label_name,
            "node_name": node_name,
            "label_dict": label_dict,
            "node_dict": node_dict
        }
        
        # Store mapping from ltree_name to array index
        self.ltree_to_index[ltree_name] = self.node_count
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
        parent_data = self.config_data[parent_ltree]
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
        
        node_data = self.config_data[ltree_name]
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
    # Event String Table Management
    # =========================================================================
    
    def register_event(self, event_id: str) -> int:
        """
        Register an event ID in the string table and return its position.
        If the event_id was previously registered, return existing position.
        
        Args:
            event_id: String identifier for the event
            
        Returns:
            Integer index/position in the string table (0-based)
        """
        if not isinstance(event_id, str):
            raise TypeError("event_id must be a string")
        
        if event_id in self.event_string_table:
            return self.event_string_table[event_id]
        
        index = self.event_index_counter
        self.event_string_table[event_id] = index
        self.event_index_counter += 1
        
        return index
    
    def get_event_index(self, event_id: str) -> int:
        """Get the index for a previously registered event."""
        if event_id not in self.event_string_table:
            raise KeyError(f"Event ID not registered: {event_id}")
        return self.event_string_table[event_id]
    
    def get_all_events(self) -> Dict[str, int]:
        """Get all registered events and their indices."""
        return self.event_string_table.copy()
    
    def get_event_string_table_size(self) -> int:
        """Get the total number of unique events registered."""
        return self.event_index_counter
    
    # =========================================================================
    # Bitmask Table Management
    # =========================================================================
    
    def register_bitmask(self, event_name: str) -> int:
        """
        Register an event in the bitmask table and return its bit position.
        
        Args:
            event_name: String identifier for the event
            
        Returns:
            Integer bit position (0-31)
            
        Raises:
            ValueError: If more than 32 events are registered
        """
        if not isinstance(event_name, str):
            raise TypeError("event_name must be a string")
        
        if event_name in self.bitmask_table:
            return self.bitmask_table[event_name]
        
        if self.bitmask_bit_counter >= 32:
            raise ValueError(
                f"Bitmask table full: cannot register more than 32 events. "
                f"Already registered: {list(self.bitmask_table.keys())}"
            )
        
        bit_number = self.bitmask_bit_counter
        self.bitmask_table[event_name] = bit_number
        self.bitmask_bit_counter += 1
        
        return bit_number
    
    def get_bitmask_bit(self, event_name: str) -> int:
        """Get the bit position for a previously registered bitmask event."""
        if event_name not in self.bitmask_table:
            raise KeyError(f"Bitmask event not registered: {event_name}")
        return self.bitmask_table[event_name]
    
    def get_all_bitmasks(self) -> Dict[str, int]:
        """Get all registered bitmask events and their bit positions."""
        return self.bitmask_table.copy()
    
    def get_bitmask_count(self) -> int:
        """Get the total number of bitmask events registered."""
        return self.bitmask_bit_counter
    
    def get_bitmask_value(self, event_name: str) -> int:
        """Get the bitmask value (1 << bit_number) for an event."""
        bit_number = self.get_bitmask_bit(event_name)
        return 1 << bit_number
    
    # =========================================================================
    # Array Index Mapping
    # =========================================================================
    
    def get_node_index(self, ltree_name: str) -> int:
        """Get the integer array index for a given ltree name."""
        if ltree_name not in self.ltree_to_index:
            raise KeyError(f"Ltree name not found: {ltree_name}")
        return self.ltree_to_index[ltree_name]
    
    def get_all_node_indices(self) -> Dict[str, int]:
        """Get all ltree name to index mappings."""
        return self.ltree_to_index.copy()
    
    def get_total_node_count(self) -> int:
        """Get the total number of nodes created."""
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
    # JSON Generation
    # =========================================================================
    
    def generate_json(self, indent: int = 2, compact: bool = False) -> Dict:
        """
        Generate the JSON file with all collected data.
        
        Args:
            indent: Indentation level for pretty-printing (None for compact)
            compact: If True, generate compact JSON without indentation
            
        Returns:
            The complete config_data dictionary
        """
        # Add event string table node if any events were registered
        if self.event_string_table:
            self._create_event_string_table_node()
        
        # Add bitmask table node if any bitmasks were registered
        if self.bitmask_table:
            self._create_bitmask_table_node()
        
        # Add metadata to config
        self.config_data["kb_log_dict"] = self.kb_log_dict
        self.config_data["ltree_to_index"] = self.ltree_to_index
        self.config_data["total_nodes"] = self.node_count
        self.config_data["kb_metadata"] = self.kb_metadata
        
        if self.kb_dict:
            raise ValueError(f"Knowledge bases still open: {list(self.kb_dict.keys())}")
        
        # Write JSON file
        with open(self.json_file, 'w') as f:
            if compact:
                json.dump(self.config_data, f, separators=(',', ':'))
            else:
                json.dump(self.config_data, f, indent=indent)
        
        return self.config_data
    
    def _create_event_string_table_node(self) -> None:
        """Create a node containing the event string table."""
        temp_kb_name = "event_string_table_kb"
        self.add_kb(temp_kb_name)
        self.select_kb(temp_kb_name)
        
        self.add_leaf_element(
            "event_strings",
            "event_string_table",
            "CFL_NULL",
            "CFL_NULL",
            "CFL_NULL",
            "CFL_NULL",
            self.event_string_table.copy()
        )
        
        self.leave_kb()
        
        if temp_kb_name in self.kb_log_dict:
            del self.kb_log_dict[temp_kb_name]
    
    def _create_bitmask_table_node(self) -> None:
        """Create a node containing the bitmask table."""
        temp_kb_name = "bitmask_table_kb"
        self.add_kb(temp_kb_name)
        self.select_kb(temp_kb_name)
        
        self.add_leaf_element(
            "bitmask",
            "bitmask_table",
            "CFL_NULL",
            "CFL_NULL",
            "CFL_NULL",
            "CFL_NULL",
            self.bitmask_table.copy()
        )
        
        self.leave_kb()
        
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


# ============================================================================
# Backward Compatibility Alias
# ============================================================================

# Allow existing code to use ChainTreeYaml name
ChainTreeYaml = ChainTreeJson


if __name__ == "__main__":
    print("ChainTree JSON Generator Test")
    print("=" * 70)
    
    json_file = Path("chaintree_config.json")
    print(f"Creating JSON file: {json_file.absolute()}")
    print("-" * 70)
    
    # Initialize the generator
    ct = ChainTreeJson(json_file)
    
    # Create a knowledge base
    ct.add_kb("robot_control")
    ct.select_kb("robot_control")
    
    # Set some KB metadata
    ct.set_kb_metadata("robot_control", "node_memory_factor", 10)
    ct.set_kb_metadata("robot_control", "version", "1.0.0")
    
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
    
    # Navigation subsystem
    nav = ct.add_node_element(
        "subsystem", "navigation",
        main_function_name="nav_update",
        initialization_function_name="init_navigation",
        aux_function_name="check_nav_ready",
        termination_function_name="stop_navigation",
        node_data={"update_rate_hz": 50}
    )
    
    # Navigation leaves
    ct.add_leaf_element(
        "sensor", "lidar",
        main_function_name="read_lidar",
        initialization_function_name="init_lidar",
        aux_function_name="lidar_ready",
        termination_function_name="close_lidar",
        node_data={"port": "/dev/ttyUSB0", "range_m": 10.0}
    )
    
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
    
    ct.add_leaf_element(
        "actuator", "gripper",
        main_function_name="control_gripper",
        initialization_function_name="init_gripper",
        aux_function_name="gripper_ready",
        termination_function_name="close_gripper",
        node_data={"max_force_n": 50, "speed_mm_s": 100}
    )
    
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
    
    # Generate JSON
    print("\nGenerating JSON...")
    data = ct.generate_json(indent=2)
    
    print("\nGenerated JSON content:")
    print("-" * 70)
    with open(json_file, 'r') as f:
        print(f.read())
    
    # Show file size comparison hint
    file_size = json_file.stat().st_size
    print(f"\nFile size: {file_size} bytes")
    
    # Generate compact version for comparison
    compact_file = Path("chaintree_config_compact.json")
    ct_compact = ChainTreeJson(compact_file)
    # (would need to rebuild tree - just showing the concept)
    
    print("\n" + "=" * 70)
    print(f"Total nodes created: {ct.node_count}")
    print(f"JSON file: {json_file.absolute()}")
    print("\n✓ Test completed successfully!")
    print("\nThis JSON can be consumed by:")
    print("  - Python C code generator (existing pipeline)")
    print("  - Zig std.json at runtime")
    print("  - Zig std.json at comptime (@embedFile)")
    print("  - Any other JSON parser")