"""
ChainTree JSON Resolver

Takes raw ChainTreeJson output and produces fully-indexed JSON that can be
directly consumed by Zig runtime without any post-processing.

All string references are resolved to integer indices:
- Parent ltree names -> parent index
- Link ltree names -> link indices  
- Function names -> function indices

Function usage counts are computed for main functions.
"""

import json
from pathlib import Path
from typing import Dict, List, Set, Any, Optional, Tuple


class ChainTreeJsonResolver:
    """
    Resolves a ChainTree configuration to fully-indexed JSON.
    
    Input: Raw JSON from ChainTreeJson (string-based references)
    Output: Resolved JSON with integer indices (direct array access)
    """
    
    # Node labels that are metadata, not operational
    METADATA_NODE_LABELS = {
        'virtual_functions',
        'complete_functions', 
        'main_functions',
        'one_shot_functions',
        'boolean_functions',
    }
    
    # Function fields in node_dict that should be resolved to indices
    # Format: field_name -> function_type ('main', 'one_shot', 'boolean')
    FUNCTION_FIELDS = {
        'error_function': 'one_shot',
        'boolean_function': 'boolean',
        'finalize_function': 'one_shot',
        'initialize_function': 'one_shot',
        'wd_fn': 'one_shot',
        'logging_function': 'one_shot',
    }
    
    def __init__(self, input_file: Path):
        """
        Initialize resolver with input JSON file.
        
        Args:
            input_file: Path to raw ChainTreeJson output
        """
        self.input_file = input_file
        
        with open(input_file, 'r') as f:
            self.raw_data = json.load(f)
        
        # Extract metadata
        self.ltree_to_index: Dict[str, int] = self.raw_data.get('ltree_to_index', {})
        self.total_nodes: int = self.raw_data.get('total_nodes', 0)
        self.kb_log_dict: Dict[str, List[str]] = self.raw_data.get('kb_log_dict', {})
        self.kb_metadata: Dict[str, Dict] = self.raw_data.get('kb_metadata', {})
        
        # Function tables (index 0 = CFL_NULL for each)
        self.main_functions: List[str] = ['CFL_NULL']
        self.one_shot_functions: List[str] = ['CFL_NULL']
        self.boolean_functions: List[str] = ['CFL_NULL']
        
        # Function name to index mappings
        self.main_fn_to_idx: Dict[str, int] = {'CFL_NULL': 0}
        self.one_shot_fn_to_idx: Dict[str, int] = {'CFL_NULL': 0}
        self.boolean_fn_to_idx: Dict[str, int] = {'CFL_NULL': 0}
        
        # Main function usage counts
        self.main_fn_usage: Dict[int, int] = {0: 0}  # index -> count
        
        # Resolved nodes (sparse array - may have gaps from filtered nodes)
        self.resolved_nodes: Dict[int, Dict] = {}
        
        # Filtered metadata nodes
        self.filtered_nodes: Set[str] = set()
        
        # Event and bitmask tables
        self.events: List[str] = []
        self.bitmasks: List[str] = []
        
        # KB info
        self.kb_info: List[Dict] = []
        
        # Max index for array sizing
        self.max_index: int = 0
    
    def resolve(self) -> Dict:
        """
        Resolve all references and produce final JSON structure.
        
        Returns:
            Fully resolved configuration dictionary
        """
        print("=" * 70)
        print("ChainTree JSON Resolver")
        print("=" * 70)
        
        # Stage 1: Identify and filter metadata nodes
        print("\nStage 1: Filtering metadata nodes...")
        self._filter_metadata_nodes()
        
        # Stage 2: Build function tables from all nodes
        print("\nStage 2: Building function tables...")
        self._build_function_tables()
        
        # Stage 3: Resolve node references to indices
        print("\nStage 3: Resolving node references...")
        self._resolve_nodes()
        
        # Stage 4: Extract event/bitmask tables
        print("\nStage 4: Extracting event and bitmask tables...")
        self._extract_tables()
        
        # Stage 5: Build KB info
        print("\nStage 5: Building KB info...")
        self._build_kb_info()
        
        # Stage 6: Build final structure
        print("\nStage 6: Building final JSON structure...")
        result = self._build_final_json()
        
        self._print_summary()
        
        return result
    
    def _is_metadata_node(self, ltree_name: str, node_data: Dict) -> bool:
        """Check if node is metadata (should be filtered)."""
        label = node_data.get('label', '')
        if label in self.METADATA_NODE_LABELS:
            return True
        
        # Check if parent was filtered
        parent_ltree = node_data.get('label_dict', {}).get('parent_ltree_name', '')
        if parent_ltree in self.filtered_nodes:
            return True
        
        return False
    
    def _filter_metadata_nodes(self) -> None:
        """Identify metadata nodes that should be excluded."""
        # Special table KBs to filter
        SPECIAL_TABLE_KBS = {'event_string_table_kb', 'bitmask_table_kb'}
        
        # Multiple passes to catch nested metadata
        changed = True
        while changed:
            changed = False
            for ltree_name, node_data in self.raw_data.items():
                if not isinstance(node_data, dict):
                    continue
                if 'label_dict' not in node_data:
                    continue
                if ltree_name in self.filtered_nodes:
                    continue
                
                # Filter special table KBs
                parts = ltree_name.split('.')
                if len(parts) >= 2 and parts[1] in SPECIAL_TABLE_KBS:
                    self.filtered_nodes.add(ltree_name)
                    changed = True
                    continue
                
                if self._is_metadata_node(ltree_name, node_data):
                    self.filtered_nodes.add(ltree_name)
                    changed = True
        
        print(f"  Filtered {len(self.filtered_nodes)} metadata nodes")
    
    def _add_main_function(self, name: str) -> int:
        """Add main function to table, return index."""
        if name in self.main_fn_to_idx:
            return self.main_fn_to_idx[name]
        
        idx = len(self.main_functions)
        self.main_functions.append(name)
        self.main_fn_to_idx[name] = idx
        self.main_fn_usage[idx] = 0
        return idx
    
    def _add_one_shot_function(self, name: str) -> int:
        """Add one-shot function to table, return index."""
        if name in self.one_shot_fn_to_idx:
            return self.one_shot_fn_to_idx[name]
        
        idx = len(self.one_shot_functions)
        self.one_shot_functions.append(name)
        self.one_shot_fn_to_idx[name] = idx
        return idx
    
    def _add_boolean_function(self, name: str) -> int:
        """Add boolean function to table, return index."""
        if name in self.boolean_fn_to_idx:
            return self.boolean_fn_to_idx[name]
        
        idx = len(self.boolean_functions)
        self.boolean_functions.append(name)
        self.boolean_fn_to_idx[name] = idx
        return idx
    
    def _resolve_function_fields(self, data: Any) -> Any:
        """
        Recursively resolve function name fields in node_dict data.
        
        Looks for keys in FUNCTION_FIELDS and replaces string values
        with integer function indices (appending '_id' suffix to key).
        
        Args:
            data: Dictionary, list, or primitive value
            
        Returns:
            Data with function names resolved to indices
        """
        if data is None:
            return None
        
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in self.FUNCTION_FIELDS and isinstance(value, str):
                    # Resolve function name to index
                    func_type = self.FUNCTION_FIELDS[key]
                    func_name = value
                    
                    if func_type == 'main':
                        func_idx = self._add_main_function(func_name)
                    elif func_type == 'one_shot':
                        func_idx = self._add_one_shot_function(func_name)
                    elif func_type == 'boolean':
                        func_idx = self._add_boolean_function(func_name)
                    else:
                        # Unknown type, keep original
                        result[key] = value
                        continue
                    
                    # Store as key_id with index value
                    result[f"{key}_id"] = func_idx
                else:
                    # Recurse into nested structures
                    result[key] = self._resolve_function_fields(value)
            return result
        
        elif isinstance(data, list):
            return [self._resolve_function_fields(item) for item in data]
        
        else:
            # Primitive value, return as-is
            return data
    
    def _build_function_tables(self) -> None:
        """Build function index tables from all nodes."""
        for ltree_name, node_data in self.raw_data.items():
            if not isinstance(node_data, dict):
                continue
            if 'label_dict' not in node_data:
                continue
            if ltree_name in self.filtered_nodes:
                continue
            
            label_dict = node_data['label_dict']
            
            # Add functions from label_dict to tables
            main_fn = label_dict.get('main_function_name', 'CFL_NULL')
            init_fn = label_dict.get('initialization_function_name', 'CFL_NULL')
            aux_fn = label_dict.get('aux_function_name', 'CFL_NULL')
            term_fn = label_dict.get('termination_function_name', 'CFL_NULL')
            
            self._add_main_function(main_fn)
            self._add_one_shot_function(init_fn)
            self._add_boolean_function(aux_fn)
            self._add_one_shot_function(term_fn)
            
            # Scan node_dict for function fields
            node_dict = node_data.get('node_dict', {})
            self._scan_for_function_fields(node_dict)
        
        print(f"  Main functions: {len(self.main_functions)}")
        print(f"  One-shot functions: {len(self.one_shot_functions)}")
        print(f"  Boolean functions: {len(self.boolean_functions)}")
    
    def _scan_for_function_fields(self, data: Any) -> None:
        """
        Recursively scan data for function fields and add them to tables.
        
        Args:
            data: Dictionary, list, or primitive value
        """
        if data is None:
            return
        
        if isinstance(data, dict):
            for key, value in data.items():
                if key in self.FUNCTION_FIELDS and isinstance(value, str):
                    func_type = self.FUNCTION_FIELDS[key]
                    func_name = value
                    
                    if func_type == 'main':
                        self._add_main_function(func_name)
                    elif func_type == 'one_shot':
                        self._add_one_shot_function(func_name)
                    elif func_type == 'boolean':
                        self._add_boolean_function(func_name)
                else:
                    # Recurse into nested structures
                    self._scan_for_function_fields(value)
        
        elif isinstance(data, list):
            for item in data:
                self._scan_for_function_fields(item)
    
    def _get_node_depth(self, ltree_name: str) -> int:
        """Calculate tree depth from ltree path."""
        parts = ltree_name.split('.')
        if len(parts) < 2:
            return 0
        
        # Depth is (parts - 2) / 2 for kb.name.label.node.label.node...
        return max(0, (len(parts) - 2) // 2)
    
    def _resolve_nodes(self) -> None:
        """Resolve all node references to integer indices."""
        for ltree_name, node_data in self.raw_data.items():
            if not isinstance(node_data, dict):
                continue
            if 'label_dict' not in node_data:
                continue
            if ltree_name in self.filtered_nodes:
                continue
            
            # Get array index
            label_dict = node_data['label_dict']
            array_index = label_dict.get('array_index')
            if array_index is None:
                array_index = self.ltree_to_index.get(ltree_name)
            if array_index is None:
                print(f"  Warning: No index for {ltree_name}")
                continue
            
            self.max_index = max(self.max_index, array_index)
            
            # Resolve parent
            parent_ltree = label_dict.get('parent_ltree_name', '')
            if parent_ltree and parent_ltree in self.ltree_to_index:
                parent_index = self.ltree_to_index[parent_ltree]
            else:
                parent_index = 0xFFFF
            
            # Resolve links (filter out metadata children)
            links = label_dict.get('links', [])
            resolved_links = []
            for link_ltree in links:
                if link_ltree in self.filtered_nodes:
                    continue
                if link_ltree in self.ltree_to_index:
                    resolved_links.append(self.ltree_to_index[link_ltree])
            
            # Resolve function indices
            main_fn = label_dict.get('main_function_name', 'CFL_NULL')
            init_fn = label_dict.get('initialization_function_name', 'CFL_NULL')
            aux_fn = label_dict.get('aux_function_name', 'CFL_NULL')
            term_fn = label_dict.get('termination_function_name', 'CFL_NULL')
            
            mf_idx = self.main_fn_to_idx.get(main_fn, 0)
            if_idx = self.one_shot_fn_to_idx.get(init_fn, 0)
            af_idx = self.boolean_fn_to_idx.get(aux_fn, 0)
            tf_idx = self.one_shot_fn_to_idx.get(term_fn, 0)
            
            # Track main function usage
            self.main_fn_usage[mf_idx] = self.main_fn_usage.get(mf_idx, 0) + 1
            
            # Get depth
            depth = self._get_node_depth(ltree_name)
            
            # Get node_dict data and resolve function fields
            node_dict = node_data.get('node_dict', {})
            resolved_data = self._resolve_function_fields(node_dict) if node_dict else {}
            
            # Build resolved node
            resolved_node = {
                'mf': mf_idx,
                'if': if_idx,
                'af': af_idx,
                'tf': tf_idx,
                'links': resolved_links,
                'parent': parent_index,
                'depth': depth,
                'data': resolved_data
            }
            
            self.resolved_nodes[array_index] = resolved_node
        
        print(f"  Resolved {len(self.resolved_nodes)} nodes")
        print(f"  Max index: {self.max_index}")
    
    def _extract_tables(self) -> None:
        """Extract event and bitmask tables."""
        # Look for event_string_table_kb
        for ltree_name, node_data in self.raw_data.items():
            if not isinstance(node_data, dict):
                continue
            
            parts = ltree_name.split('.')
            if len(parts) >= 2 and parts[1] == 'event_string_table_kb':
                node_dict = node_data.get('node_dict', {})
                if node_dict:
                    # Sort by index value
                    sorted_events = sorted(node_dict.items(), key=lambda x: x[1])
                    self.events = [e[0] for e in sorted_events]
                break
        
        # Look for bitmask_table_kb
        for ltree_name, node_data in self.raw_data.items():
            if not isinstance(node_data, dict):
                continue
            
            parts = ltree_name.split('.')
            if len(parts) >= 2 and parts[1] == 'bitmask_table_kb':
                node_dict = node_data.get('node_dict', {})
                if node_dict:
                    # Sort by bit number
                    sorted_bitmasks = sorted(node_dict.items(), key=lambda x: x[1])
                    self.bitmasks = [b[0] for b in sorted_bitmasks]
                break
        
        print(f"  Events: {len(self.events)}")
        print(f"  Bitmasks: {len(self.bitmasks)}")
    
    def _build_kb_info(self) -> None:
        """Build knowledge base info array."""
        for kb_name, kb_path in self.kb_log_dict.items():
            # Skip special tables
            if kb_name in ('event_string_table_kb', 'bitmask_table_kb'):
                continue
            
            # Find root node and count
            root_index = None
            start_index = None
            end_index = 0
            max_depth = 0
            
            for ltree_name in self.ltree_to_index:
                parts = ltree_name.split('.')
                if len(parts) >= 2 and parts[1] == kb_name:
                    if ltree_name in self.filtered_nodes:
                        continue
                    
                    idx = self.ltree_to_index[ltree_name]
                    depth = self._get_node_depth(ltree_name)
                    
                    if start_index is None or idx < start_index:
                        start_index = idx
                    if root_index is None or idx < root_index:
                        root_index = idx
                    
                    end_index = max(end_index, idx + 1)
                    max_depth = max(max_depth, depth)
            
            if root_index is None:
                continue
            
            # Get metadata
            metadata = self.kb_metadata.get(kb_name, {})
            mem_factor = metadata.get('node_memory_factor', 10)
            
            kb_info = {
                'name': kb_name,
                'root': root_index,
                'start': start_index or 0,
                'count': end_index - (start_index or 0),
                'max_depth': max_depth,
                'mem_factor': mem_factor
            }
            
            self.kb_info.append(kb_info)
        
        print(f"  Knowledge bases: {len(self.kb_info)}")
        for kb in self.kb_info:
            print(f"    {kb['name']}: root={kb['root']}, count={kb['count']}, depth={kb['max_depth']}")
    
    def _build_final_json(self) -> Dict:
        """Build the final JSON structure."""
        # Build nodes array (with gaps for filtered nodes)
        nodes_array = []
        for i in range(self.max_index + 1):
            if i in self.resolved_nodes:
                nodes_array.append(self.resolved_nodes[i])
            else:
                # Placeholder for filtered/missing node
                nodes_array.append({
                    'mf': 0,
                    'if': 0,
                    'af': 0,
                    'tf': 0,
                    'links': [],
                    'parent': 0xFFFF,
                    'depth': 0,
                    'data': None  # null signals invalid node
                })
        
        # Build main function usage array
        main_usage = [self.main_fn_usage.get(i, 0) for i in range(len(self.main_functions))]
        
        return {
            'meta': {
                'version': '1.0',
                'total_nodes': len(self.resolved_nodes),
                'array_size': self.max_index + 1
            },
            'nodes': nodes_array,
            'functions': {
                'main': self.main_functions,
                'one_shot': self.one_shot_functions,
                'boolean': self.boolean_functions,
                'main_usage': main_usage
            },
            'events': self.events,
            'bitmasks': self.bitmasks,
            'kb_info': self.kb_info
        }
    
    def _print_summary(self) -> None:
        """Print resolution summary."""
        print("\n" + "=" * 70)
        print("Resolution Summary")
        print("=" * 70)
        print(f"Total operational nodes: {len(self.resolved_nodes)}")
        print(f"Array size: {self.max_index + 1}")
        print(f"Filtered nodes: {len(self.filtered_nodes)}")
        print(f"\nFunction tables:")
        print(f"  Main: {len(self.main_functions)} functions")
        print(f"  One-shot: {len(self.one_shot_functions)} functions")
        print(f"  Boolean: {len(self.boolean_functions)} functions")
        print(f"\nMain function usage:")
        for i, (name, count) in enumerate(zip(self.main_functions, 
                [self.main_fn_usage.get(i, 0) for i in range(len(self.main_functions))])):
            if count > 0:
                print(f"  [{i}] {name}: {count} uses")
    
    def save(self, output_file: Path, compact: bool = False) -> None:
        """
        Resolve and save to output file.
        
        Args:
            output_file: Path to write resolved JSON
            compact: If True, no indentation
        """
        result = self.resolve()
        
        with open(output_file, 'w') as f:
            if compact:
                json.dump(result, f, separators=(',', ':'))
            else:
                json.dump(result, f, indent=2)
        
        print(f"\nSaved to: {output_file}")
        print(f"File size: {output_file.stat().st_size} bytes")


# =============================================================================
# Test
# =============================================================================

if __name__ == "__main__":
    from chain_tree_json import ChainTreeJson
    
    print("Creating test configuration...")
    print("=" * 70)
    
    # Create raw JSON using DSL
    raw_file = Path("chaintree_raw.json")
    ct = ChainTreeJson(raw_file)
    
    ct.add_kb("test_kb")
    ct.select_kb("test_kb")
    ct.set_kb_metadata("test_kb", "node_memory_factor", 15)
    
    ct.start_assembly()
    
    # Build a test tree
    root = ct.add_node_element(
        "GATE", "root",
        main_function_name="CFL_GATE_MAIN",
        initialization_function_name="CFL_GATE_INIT",
        aux_function_name="CFL_NULL",
        termination_function_name="CFL_GATE_TERM",
        node_data={"priority": 1},
        links_flag=False
    )
    
    col = ct.add_node_element(
        "COLUMN", "col1",
        main_function_name="CFL_COLUMN_MAIN",
        initialization_function_name="CFL_COLUMN_INIT",
        aux_function_name="CFL_NULL",
        termination_function_name="CFL_COLUMN_TERM",
        node_data={"auto_start": True}
    )
    
    # Add leaves
    for i in range(3):
        ct.add_leaf_element(
            "LEAF", f"leaf{i}",
            main_function_name="CFL_LEAF_MAIN",
            initialization_function_name="CFL_NULL",
            aux_function_name="CFL_CHECK_READY" if i == 0 else "CFL_NULL",
            termination_function_name="CFL_NULL",
            node_data={"leaf_id": i, "timeout": 1000 * (i + 1)}
        )
    
    ct.pop_node_element(col)
    
    # Second column
    col2 = ct.add_node_element(
        "COLUMN", "col2",
        main_function_name="CFL_COLUMN_MAIN",
        initialization_function_name="CFL_COLUMN_INIT",
        aux_function_name="CFL_NULL",
        termination_function_name="CFL_COLUMN_TERM",
        node_data={"auto_start": False}
    )
    
    ct.add_leaf_element(
        "LEAF", "leaf_final",
        main_function_name="CFL_LEAF_MAIN",
        initialization_function_name="CFL_NULL",
        aux_function_name="CFL_NULL",
        termination_function_name="CFL_NULL",
        node_data={}
    )
    
    ct.pop_node_element(col2)
    ct.pop_node_element(root)
    
    # Register some events
    ct.register_event("START")
    ct.register_event("STOP")
    ct.register_event("TIMEOUT")
    
    # Register bitmasks
    ct.register_bitmask("ACTIVE")
    ct.register_bitmask("ERROR")
    
    ct.check_for_balance_ltree()
    ct.leave_kb()
    ct.generate_json()
    
    print(f"\nRaw JSON created: {raw_file}")
    
    # Now resolve it
    print("\n")
    resolver = ChainTreeJsonResolver(raw_file)
    resolved_file = Path("chaintree_resolved.json")
    resolver.save(resolved_file)
    
    # Show the resolved output
    print("\n" + "=" * 70)
    print("Resolved JSON:")
    print("=" * 70)
    with open(resolved_file, 'r') as f:
        print(f.read())