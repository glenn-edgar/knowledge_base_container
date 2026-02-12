"""
SQLite3 KB Bit Structures
Handles bit structures for the knowledge base with SQLite backend
"""

import time
import json
import sqlite3
from dataclasses import dataclass, field
from typing import Dict, Optional, Any


from .bit_mask_operations import BitMaskOperations
from .bit_s_expression import KB_BIT_DATA
from .bit_s_expression import SExpressionProcessor


class KB_Bit_Structures(SExpressionProcessor):
    """
    A class to handle bit structures for the knowledge base.
    
    Table schema:
        CREATE TABLE bit_mask_table (
            node_id TEXT PRIMARY KEY,
            bit_mask INTEGER NOT NULL DEFAULT 0
        )
    """
    
    def __init__(self, kb_search, database):
        """
        Initialize KB Bit Structures handler.
        
        Args:
            kb_search: Knowledge base search object
            database: Database connection/handler object
        """
        SExpressionProcessor.__init__(self)
        self.kb_search = kb_search
        self.database = database
        self.conn = self.kb_search.conn
        self.bit_mask_operations = BitMaskOperations(self.conn)
        
        
    def find_bit_structure_id(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find a single bit structure id for given parameters. Raises error if 0 or multiple bit structures found.
        
        Args:
            kb (str, optional): Knowledge base to search in
            node_name (str, optional): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            dict: Single matching bit structure record with field names
            
        Raises:
            ValueError: If no bit structure or multiple bit structures found
        """
        results = self.find_bit_structure_ids(kb, node_name, properties, node_path)
        
        if len(results) == 0:
            raise ValueError(
                f"No bit structure found matching parameters: "
                f"name={node_name}, properties={properties}, path={node_path}"
            )
        if len(results) > 1:
            raise ValueError(
                f"Multiple bit structures ({len(results)}) found matching parameters: "
                f"name={node_name}, properties={properties}, path={node_path}"
            )
        
        return results[0]
    
    def find_bit_structure_ids(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find all bit structure ids matching the given parameters.
        
        Args:
            kb (str, optional): Knowledge base to search in
            node_name (str, optional): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            list: List of matching bit structure records as dictionaries
            
        Raises:
            ValueError: If no bit structures found
        """
        try:
            # Clear previous filters and build new query
            self.kb_search.clear_filters()
            self.kb_search.search_label("KB_BIT_MASK")
            
            if kb is not None:
                self.kb_search.search_kb(kb)
                
            if node_name is not None:
                self.kb_search.search_name(node_name)
                
            if properties is not None and isinstance(properties, dict):
                for key, value in properties.items():
                    self.kb_search.search_property_value(key, value)
                    
            if node_path is not None:
                self.kb_search.search_path(node_path)
            
            # Execute query and get results
            node_ids = self.kb_search.execute_query()
            
            if not node_ids or len(node_ids) == 0:
                raise ValueError(
                    f"No bit structures found matching parameters: "
                    f"name={node_name}, properties={properties}, path={node_path}"
                )
            
            return node_ids
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error finding bit structure IDs: {str(e)}")
        
        
    def find_assemble_bit_data(self, table_dict_rows, clear_flag_data: bool = False, user_names=None):
        """
        Extract and assemble bit data from bit structure query results.
        
        Args:
            table_dict_rows (list): List of result dictionaries
            clear_flag_data (bool): If True, clear the flag data (set all bits to 0)
            user_names (list, optional): Optional list to assign user_names to the bit data
            
        Returns:
            dict: Dictionary mapping user_name to KB_BIT_DATA objects
            
        Raises:
            ValueError: If user_names length doesn't match table_dict_rows length
        """
        if not table_dict_rows:
            return {}
            
 
                
        return_values = {}
        for row in table_dict_rows:
            if clear_flag_data:
                self.bit_mask_operations.set_bit_mask(row['properties']['record_id'], 0, -1)
                
            data_class = self.assemble_bit_data(row)
            
            user_name = data_class.user_name
            return_values[user_name] = data_class
            
        if user_names is not None:
            if len(user_names) != len(table_dict_rows):
                raise ValueError(
                    f"Number of user names ({len(user_names)}) must match "
                    f"number of table dict rows ({len(table_dict_rows)})"
                )
            for i in range(len(user_names)):
                return_values[user_names[i]] = return_values[user_names[i]]
        
        return return_values

    
    
    def assemble_bit_data(self, row):
        return_data = KB_BIT_DATA()
        row_properties = json.loads(row['properties'])
        return_data.user_name = row_properties['user_name']
        return_data.flags = json.loads(row_properties['flag_dictionary'])
        return_data.bit_size = row_properties['mask_size']
        return_data.node_id = row_properties['record_id']
        for flag_name, flag_data in return_data.flags.items():
            return_data.flags_mask[flag_name] = 1<<int(flag_data['bit'])
            
                  
        bit_mask = self.bit_mask_operations.get_bit_mask(row_properties['record_id'])
        return_data.bit_mask = bit_mask
        for flag_name, flag_data in return_data.flags.items():
            return_data.flag_data[flag_name] = bit_mask & return_data.flags_mask[flag_name]
            return_data.flag_change[flag_name] = False
        return return_data
    # ... rest of code
    def get_bit_mask(self, node_id: str) -> int:
        """
        Get the bit mask value for a node.
        
        Args:
            node_id: Node identifier
            
        Returns:
            int: Current bit mask value
        """
        return self.bit_mask_operations.get_bit_mask(node_id)
    
    def set_bit_mask(self, node_id: str, new_bits: int, change_mask: int = -1) -> bool:
        """
        Set specific bits in the bit mask.
        
        Args:
            node_id: Node identifier
            new_bits: New bit values
            change_mask: Mask indicating which bits to change (default: all bits)
            
        Returns:
            bool: True if successful
        """
        return self.bit_mask_operations.set_bit_mask(node_id, new_bits, change_mask)
        
    def set_all_ones(self, node_id: str):
        """
        Set all bits to 1 for a node.
        
        Args:
            node_id: Node identifier
        """
        self.bit_mask_operations.set_bit_mask(node_id, -1, -1)
    
    def set_all_zeros(self, node_id: str):
        """
        Set all bits to 0 for a node.
        
        Args:
            node_id: Node identifier
        """
        self.bit_mask_operations.set_bit_mask(node_id, 0, -1)
    
    def set_flag_data(self, data_class, flag_data: dict):
        """
        Set specific flag values in the bit mask.
        
        Args:
            data_class: KB_BIT_DATA object containing flag definitions
            flag_data: Dictionary mapping flag names to values (0 or 1)
            
        Raises:
            ValueError: If flag not found or flag value not 0 or 1
        """
        mask = 0
        change_mask = 0

        for flag_name, flag_value in flag_data.items():
            if flag_name not in data_class.flags:
                raise ValueError(f"Flag '{flag_name}' not found in data class")
            if flag_value not in [0, 1]:
                raise ValueError(f"Flag value {flag_value} must be 0 or 1")
                
            if flag_value == 1:
                mask |= data_class.flags_mask[flag_name]
            else:
                mask &= ~data_class.flags_mask[flag_name]
            change_mask |= data_class.flags_mask[flag_name]
        
        self.bit_mask_operations.set_bit_mask(data_class.node_id, mask, change_mask)
        
    def get_flag_data(self, data_class):
        """
        Read current flag values from the database and update data class.
        
        Args:
            data_class: KB_BIT_DATA object to update
            
        Returns:
            dict: Dictionary of current flag values
            
        Raises:
            ValueError: If flag not found in data class
        """
        bit_mask = self.bit_mask_operations.get_bit_mask(data_class.node_id)
        data_class.bit_mask = bit_mask
        
        for flag_name, flag_info in data_class.flags.items():
            if flag_name not in data_class.flags:
                raise ValueError(f"Flag '{flag_name}' not found in data class")
                
            flag_value = bit_mask & data_class.flags_mask[flag_name]
            
            # Check if flag changed
            if flag_value != data_class.flag_data[flag_name]:
                data_class.flag_change[flag_name] = True
            else:
                data_class.flag_change[flag_name] = False
            
            # Store as 0 or 1
            if flag_value != 0:
                data_class.flag_data[flag_name] = 1
            else:
                data_class.flag_data[flag_name] = 0
                
        return data_class.flag_data