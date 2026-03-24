"""
Stage 2: Node Index Builder

Builds node ordering and index mappings from ChainTree YAML data.
Preserves original YAML indices for operational nodes.
Filters out non-operational metadata nodes used only for function cataloging.
"""

from typing import Dict, List, Set, Tuple, Optional

from .stage1_handle import ChainTreeHandle


class NodeIndexBuilder:
    """
    Build node ordering and index mappings.
    
    Preserves original YAML indices for operational nodes.
    Filters out non-operational metadata nodes used only for function cataloging.
    """
    
    # Node labels that indicate function definition metadata (not operational)
    METADATA_NODE_LABELS = {
        'virtual_functions',
        'complete_functions',
        'main_functions',
        'one_shot_functions',
        'boolean_functions',
    }
    
    def __init__(self, handle: ChainTreeHandle):
        self.handle = handle
        
        # Node ordering by KB (preserves original indices)
        self.kb_node_order: Dict[str, List[str]] = {}   # kb_name -> [ltree_names in BFS order]
        self.kb_start_index: Dict[str, int] = {}         # kb_name -> min original index
        self.kb_end_index: Dict[str, int] = {}            # kb_name -> max original index + 1
        
        # Use ORIGINAL indices from YAML
        self.ltree_to_final_index: Dict[str, int] = {}   # ltree_name -> original yaml index
        self.final_index_to_ltree: Dict[int, str] = {}   # SPARSE: original_index -> ltree_name
        
        # Track filtered nodes
        self.filtered_nodes: Set[str] = set()
        
        # Max index for array sizing
        self.max_index: int = 0
    
    def get_node_depth(self, ltree_name: str) -> int:
        """
        Calculate tree depth relative to the KB's root node.
        
        Root node = depth 0, direct children = depth 1, etc.
        Depth is measured in pairs (every 2 ltree segments = 1 depth level).
        """
        parts = ltree_name.split('.')
        
        if len(parts) < 2:
            return 0
        
        kb_name = parts[1]
        root_ltree = self.handle.get_kb_root_node(kb_name)
        
        if not root_ltree:
            return max(0, len(parts) - 3)
        
        root_parts = len(root_ltree.split('.'))
        depth = len(parts) - root_parts
        depth = int(depth / 2)
        return max(0, depth)
    
    def _is_metadata_node(self, ltree_name: str) -> bool:
        """Check if a node is a function definition metadata node (should be filtered)."""
        node_data = self.handle.get_node_data(ltree_name)
        if not node_data:
            return False
        
        # Check node label
        label = node_data.get('label', '')
        if label in self.METADATA_NODE_LABELS:
            return True
        
        # Check if parent is a metadata node (entire subtree should be filtered)
        parent_ltree = self.handle.get_node_parent(ltree_name)
        if parent_ltree and parent_ltree in self.filtered_nodes:
            return True
        
        return False
    
    def build(self) -> None:
        """Build the node ordering for all knowledge bases, preserving ORIGINAL YAML indices."""
        total_filtered = 0
        
        for kb_name in self.handle.get_kb_names():
            all_nodes = self.handle.traverse_kb_breadth_first(kb_name)
            
            operational_nodes = []
            kb_indices = []
            
            for ltree_name in all_nodes:
                if self._is_metadata_node(ltree_name):
                    self.filtered_nodes.add(ltree_name)
                    total_filtered += 1
                else:
                    operational_nodes.append(ltree_name)
                    
                    original_index = self.handle.get_node_index(ltree_name)
                    if original_index is not None:
                        kb_indices.append(original_index)
                        self.ltree_to_final_index[ltree_name] = original_index
                        self.final_index_to_ltree[original_index] = ltree_name
                        self.max_index = max(self.max_index, original_index)
            
            self.kb_node_order[kb_name] = operational_nodes
            
            if kb_indices:
                self.kb_start_index[kb_name] = min(kb_indices)
                self.kb_end_index[kb_name] = max(kb_indices) + 1
        
        if total_filtered > 0:
            print(f"  Filtered out {total_filtered} function definition metadata nodes")
    
    # =========================================================================
    # Accessors
    # =========================================================================
    
    def get_node_final_index(self, ltree_name: str) -> int:
        return self.ltree_to_final_index[ltree_name]
    
    def get_node_by_index(self, index: int) -> Optional[str]:
        return self.final_index_to_ltree.get(index)
    
    def get_kb_range(self, kb_name: str) -> Tuple[int, int]:
        start = self.kb_start_index.get(kb_name, 0)
        end = self.kb_end_index.get(kb_name, 0)
        return (start, end)
    
    def get_total_nodes(self) -> int:
        return len(self.ltree_to_final_index)
    
    def get_max_index(self) -> int:
        return self.max_index
    
    def get_array_size(self) -> int:
        return self.max_index + 1
    
    def print_summary(self) -> None:
        print("=" * 70)
        print("Stage 2: Node Index Builder Summary")
        print("=" * 70)
        print(f"Total operational nodes: {self.get_total_nodes()}")
        print(f"Array size needed: {self.get_array_size()} (indices 0..{self.max_index})")
        print(f"Filtered nodes create {self.get_array_size() - self.get_total_nodes()} gaps in array")
        
        for kb_name in self.kb_node_order.keys():
            start, end = self.get_kb_range(kb_name)
            count = len(self.kb_node_order[kb_name])
            print(f"  {kb_name}: original indices [{start}..{end-1}] ({count} nodes)")