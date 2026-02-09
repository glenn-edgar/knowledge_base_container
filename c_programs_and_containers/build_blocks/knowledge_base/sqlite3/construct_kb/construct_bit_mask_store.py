"""
Construct Bit Mask Store - SQLite3 Compatible
Manages bit mask construction and storage with knowledge base integration
"""

import json
from .bit_mask_operations import BitMaskOperations


class Construct_Bit_Mask_Store:
    """
    High-level interface for creating and managing bit mask entries
    with knowledge base integration.
    """
    
    def __init__(self, conn, construct_kb,upload_flag=False):
        """
        Initialize the bit mask store.
        
        Args:
            conn: Active SQLite3 connection object
            construct_kb: Knowledge base construct object for node management
        """
        self.bit_mask_operations = BitMaskOperations(conn)
        self.construct_kb = construct_kb 
        self.conn = conn
        self.upload_flag = upload_flag
        if self.upload_flag == False:
            self.bit_mask_operations.create_table()
        self.bit_mask_flags = {}
        
    def clear_flags(self):
        """Clear all registered bit mask flags."""
        self.bit_mask_flags = {}
        
    def add_flag(self, flag_name, bit_position, flag_description):
        """
        Register a flag definition for the bit mask.
        
        Args:
            flag_name: Unique name for the flag
            bit_position: Bit position (0-63) for this flag
            flag_description: Human-readable description of the flag
        """
        self.bit_mask_flags[flag_name] = {
            'bit': bit_position, 
            'description': flag_description
        }
        
    def create_bit_mask_entry(self, user_name, name, mask_size, bit_mask, description=""):
        """
        Create a new bit mask entry with validation and knowledge base integration.
        
        Args:
            user_name: User creating this mask entry
            name: Name identifier for the bit mask
            mask_size: Number of bits in the mask (1-64)
            bit_mask: Initial mask value
            description: Optional description for the mask entry
            
        Raises:
            TypeError: If arguments have incorrect types
            ValueError: If mask_size, bit_mask, or flag definitions are invalid
        """
        # Type validation
        if not isinstance(name, str):
            raise TypeError("name must be a string")
        if not isinstance(mask_size, int):
            raise TypeError("mask_size must be an integer")
        if not isinstance(bit_mask, int):
            raise TypeError("bit_mask must be an integer")
            
        # Range validation
        if mask_size < 1 or mask_size > 64:
            raise ValueError("mask_size must be between 1 and 64")
        if bit_mask < 0 or bit_mask > 2**mask_size - 1:
            raise ValueError("bit_mask must be between 0 and 2**mask_size - 1")
        if mask_size != len(self.bit_mask_flags):
            raise ValueError("bit_mask size must be equal to the number of flags")
            
        # Check for duplicate bit positions
        temp_mask = {}
        for i in range(mask_size):
            temp_mask[i] = 0
            
        for flag_name, flag_data in self.bit_mask_flags.items():
            if temp_mask[flag_data['bit']] == 1:
                raise ValueError(f"Duplicate bit position {flag_data['bit']} for flag '{flag_name}'")
            temp_mask[flag_data['bit']] = 1
            
        # Generate ltree-style node name
        label = "KB_BIT_MASK"
        ltree_node_name = '.'.join(
            self.construct_kb.path[self.construct_kb.working_kb]
        ) + "." + label + "." + name
        ltree_node_name = ltree_node_name.replace(".", "_").lower()
    
        # Create the bit mask entry in the database
        self.bit_mask_operations.create_entry(ltree_node_name, bit_mask)
        
        # Prepare node properties for knowledge base
        node_properties = {
            "user_name": user_name,
            "mask_size": mask_size,
            "bit_mask": bit_mask,
            "flag_dictionary": json.dumps(self.bit_mask_flags),
            'record_id': ltree_node_name                   
        }
        
        # Add to knowledge base
        self.construct_kb.add_info_node(
            label, 
            name, 
            node_properties, 
            {}, 
            description=description
        )