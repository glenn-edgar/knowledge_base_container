"""
SQLite3 Bit Mask Operations Class
Manages bit masks and flag registers for distributed node control systems
"""

import json
import sqlite3
from typing import Dict, Optional, Any


class BitMaskOperations:
    """
    Manages bit mask operations for distributed control system nodes.
    Provides methods to create, read, and update bit masks and flag dictionaries
    stored in SQLite3.
    """
    
    def __init__(self, conn: sqlite3.Connection, bit_mask_table_name: str = "bit_mask_table"):
        """
        Initialize the bit mask operations handler.
        Creates the table if it doesn't exist.
        
        Args:
            conn: Active SQLite3 connection object
            bit_mask_table_name: Name of the table to use for bit mask storage
        """
        self.conn = conn
        self.table_name = bit_mask_table_name
        
    
    def create_table(self):
        """
        Creates a fresh bit mask table, dropping any existing table with the same name.
        Table schema:
            - node_id: TEXT PRIMARY KEY (unique identifier for node)
            - bit_mask: INTEGER (64-bit integer mask)
        
        """
        # Drop existing table if it exists
        drop_table_query = f"DROP TABLE IF EXISTS {self.table_name}"
        
        # Create fresh table
        create_table_query = f"""
            CREATE TABLE {self.table_name} (
                node_id TEXT PRIMARY KEY,
                bit_mask INTEGER NOT NULL DEFAULT 0
            )
        """
        
        cursor = self.conn.cursor()
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        self.conn.commit()
    
    def create_entry(self, node_id: str, bit_mask: int = 0) -> bool:
        """
        Create a new entry in the bit mask table.
        
        Args:
            node_id: Unique identifier for the node
            bit_mask: Initial bit mask value (default: 0)
        
        Returns:
            True if entry created successfully, False otherwise
        
        Raises:
            sqlite3.IntegrityError: If node_id already exists
        """
        insert_query = f"""
            INSERT INTO {self.table_name} (node_id, bit_mask)
            VALUES (?, ?)
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(insert_query, (node_id, bit_mask))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError as e:
            self.conn.rollback()
            raise sqlite3.IntegrityError(
                f"Node ID '{node_id}' already exists in table '{self.table_name}'"
            ) from e
        except Exception as e:
            self.conn.rollback()
            raise
    
    def get_bit_mask(self, node_id: str) -> Optional[int]:
        """
        Retrieve the bit mask value for a given node.
        
        Args:
            node_id: Unique identifier for the node
        
        Returns:
            64-bit integer bit mask value, or None if node not found
        """
        select_query = f"""
            SELECT bit_mask FROM {self.table_name}
            WHERE node_id = ?
        """
        
        cursor = self.conn.cursor()
        cursor.execute(select_query, (node_id,))
        result = cursor.fetchone()
        
        if result is None:
            return None
        
        return result[0]
    
    def set_bit_mask(self, node_id: str, new_bits: int, change_mask: int = -1) -> bool:
        """
        Atomically update specific bits in the bit_mask for a given node.

        This performs the following SQL-level operation:
        new_mask = (current_mask & (~change_mask)) | (new_bits & change_mask)
        
        Args:
            node_id: Unique identifier for the node
            new_bits: The integer containing the new bit values to apply.
            change_mask: A 64-bit mask where each '1' indicates
                        a bit to be updated using the value from new_bits.
                        Defaults to -1 (all 64 bits, 0xFFF...FFF)
                        for a full overwrite.
        
        Returns:
            True if the update was successful (row affected), False otherwise.
        
        Raises:
            ValueError: If new_bits or change_mask is out of the
                        valid 64-bit signed integer range.
        """
        # Define 64-bit signed integer limits
        SIGNED_64_BIT_MIN = -9223372036854775808
        SIGNED_64_BIT_MAX = 9223372036854775807
        
        # Validate both inputs are within the 64-bit signed integer range
        if not (SIGNED_64_BIT_MIN <= new_bits <= SIGNED_64_BIT_MAX):
            raise ValueError(f"new_bits must be a valid 64-bit integer, got {new_bits}")
            
        if not (SIGNED_64_BIT_MIN <= change_mask <= SIGNED_64_BIT_MAX):
            raise ValueError(f"change_mask must be a valid 64-bit integer, got {change_mask}")

        # This single SQL query performs the read, modify, and write atomically.
        # 1. (bit_mask & (~?)) -> Clears the bits we want to change.
        # 2. (? & ?) -> Isolates the new bits to be set.
        # 3. (... | ...) -> Combines the two, updating the mask.
        update_query = f"""
            UPDATE {self.table_name}
            SET bit_mask = (bit_mask & (~?)) | (? & ?)
            WHERE node_id = ?
        """
        
        # The parameters are: (change_mask, new_bits, change_mask, node_id)
        params = (change_mask, new_bits, change_mask, node_id)
        
        rows_affected = 0
        try:
            cursor = self.conn.cursor()
            cursor.execute(update_query, params)
            rows_affected = cursor.rowcount
            self.conn.commit()
        except Exception:
            # Rollback in case of any SQL error
            self.conn.rollback()
            raise  # Re-raise the exception after rollback
            
        return rows_affected > 0

    
    def get_entry(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve complete entry for a given node.
        
        Args:
            node_id: Unique identifier for the node
        
        Returns:
            Dictionary with keys: node_id, bit_mask
            Returns None if node not found
        """
        select_query = f"""
            SELECT node_id, bit_mask
            FROM {self.table_name}
            WHERE node_id = ?
        """
        
        cursor = self.conn.cursor()
        cursor.execute(select_query, (node_id,))
        result = cursor.fetchone()
        
        if result is None:
            return None
        
        return {
            'node_id': result[0],
            'bit_mask': result[1],
        }
    
    def delete_entry(self, node_id: str) -> bool:
        """
        Delete an entry from the bit mask table.
        
        Args:
            node_id: Unique identifier for the node
        
        Returns:
            True if entry deleted, False if node not found
        """
        delete_query = f"""
            DELETE FROM {self.table_name}
            WHERE node_id = ?
        """
        
        cursor = self.conn.cursor()
        cursor.execute(delete_query, (node_id,))
        rows_affected = cursor.rowcount
        
        self.conn.commit()
        return rows_affected > 0
    
    '''
    Dictionary is not allowed to be modified after creation
    def update_flag_dictionary(self, node_id: str, flag_dictionary: Dict[str, Any]) -> bool:
        """
        Update the flag dictionary for a given node.
        
        Args:
            node_id: Unique identifier for the node
            flag_dictionary: New flag dictionary to store
        
        Returns:
            True if update successful, False if node not found
        """
        update_query = f"""
            UPDATE {self.table_name}
            SET flag_regist = ?
            WHERE node_id = ?
        """
        
        cursor = self.conn.cursor()
        cursor.execute(update_query, (json.dumps(flag_dictionary), node_id))
        rows_affected = cursor.rowcount
        
        self.conn.commit()
        return rows_affected > 0
    '''
    
    def list_all_nodes(self) -> list:
        """
        List all node IDs in the table.
        
        Returns:
            List of all node_id strings
        """
        select_query = f"""
            SELECT node_id FROM {self.table_name}
            ORDER BY node_id
        """
        
        cursor = self.conn.cursor()
        cursor.execute(select_query)
        return [row[0] for row in cursor.fetchall()]