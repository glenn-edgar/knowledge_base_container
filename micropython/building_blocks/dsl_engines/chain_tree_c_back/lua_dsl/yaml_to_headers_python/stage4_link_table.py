"""
Stage 4: Link Table Builder

Builds flat arrays of child node indices for each parent node.
Skips filtered metadata children from Stage 2.
"""

from typing import Dict, List

from .stage1_handle import ChainTreeHandle
from .stage2_node_index import NodeIndexBuilder


class LinkTableBuilder:
    """
    Build link tables for node parent-child relationships.
    
    Creates a flat array of child indices with per-node offset+count.
    """
    
    def __init__(self, handle: ChainTreeHandle, node_builder: NodeIndexBuilder):
        self.handle = handle
        self.node_builder = node_builder
        
        # Link table data
        self.link_table: List[int] = []               # Flat array of child indices
        self.node_link_info: Dict[str, Dict] = {}     # ltree_name -> {link_start, link_count}
    
    def build(self) -> None:
        """Build the link table for all operational nodes, skipping filtered children."""
        
        for ltree_name in self.node_builder.ltree_to_final_index.keys():
            children = self.handle.get_node_children(ltree_name)
            
            # Filter out children that were excluded from node array
            operational_children = []
            for child_ltree in children:
                if child_ltree in self.node_builder.filtered_nodes:
                    continue
                if child_ltree not in self.node_builder.ltree_to_final_index:
                    continue
                operational_children.append(child_ltree)
            
            link_start = len(self.link_table)
            link_count = len(operational_children)
            
            self.node_link_info[ltree_name] = {
                'link_start': link_start,
                'link_count': link_count
            }
            
            for child_ltree in operational_children:
                child_index = self.node_builder.get_node_final_index(child_ltree)
                self.link_table.append(child_index)
    
    # =========================================================================
    # Accessors
    # =========================================================================
    
    def get_node_link_info(self, ltree_name: str) -> Dict:
        return self.node_link_info.get(ltree_name, {'link_start': 0, 'link_count': 0})
    
    def get_link_table_size(self) -> int:
        return len(self.link_table)
    
    def print_summary(self) -> None:
        print("=" * 70)
        print("Stage 4: Link Table Builder Summary")
        print("=" * 70)
        print(f"Total link entries: {self.get_link_table_size()}")
        
        max_children = max([info['link_count'] for info in self.node_link_info.values()] or [0])
        nodes_with_children = sum(1 for info in self.node_link_info.values() if info['link_count'] > 0)
        
        print(f"Nodes with children: {nodes_with_children}")
        print(f"Maximum children per node: {max_children}")