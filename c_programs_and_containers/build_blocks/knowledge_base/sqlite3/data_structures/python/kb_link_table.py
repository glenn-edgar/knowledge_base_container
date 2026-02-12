import sqlite3


class KB_Link_Table:
    """
    A class to handle link table operations for the knowledge base.
    SQLite version.
    """
    
    def __init__(self, conn, cursor, base_table):
        """
        Initialize the KB_Link_Table class.
        
        Args:
            conn: Database connection object (sqlite3.Connection)
            cursor: Database cursor object (sqlite3.Cursor)
            base_table: Base name for the table (will be suffixed with '_link')
        """
        self.conn = conn
        self.cursor = cursor
        self.base_table = base_table + "_link"
        
        # Ensure row factory is set for dictionary-like access
        if self.conn.row_factory != sqlite3.Row:
            self.conn.row_factory = sqlite3.Row
    
    def find_records_by_link_name(self, link_name, kb=None):
        """
        Find records by link_name, optionally filtered by knowledge_base.
        
        Args:
            link_name (str): The link name to search for
            kb (str, optional): Knowledge base to filter by. If None, search all.
            
        Returns:
            list: List of matching records as dictionaries
        """
        if kb is None:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE link_name = ?
            """
            self.cursor.execute(query, (link_name,))
        else:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE link_name = ? AND parent_node_kb = ?
            """
            self.cursor.execute(query, (link_name, kb))
        
        # Get all rows and convert to list of dicts
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]
    
    def find_records_by_node_path(self, node_path, kb=None):
        """
        Find records by node_path (parent_path), optionally filtered by knowledge_base.
        
        Args:
            node_path (str): The node path (parent_path) to search for
            kb (str, optional): Knowledge base to filter by. If None, search all.
            
        Returns:
            list: List of matching records as dictionaries
        """
        if kb is None:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE parent_path = ?
            """
            self.cursor.execute(query, (node_path,))
        else:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE parent_path = ? AND parent_node_kb = ?
            """
            self.cursor.execute(query, (node_path, kb))
        
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]
    
    def find_all_link_names(self):
        """
        Get all unique link names from the table.
        
        Returns:
            list: List of all unique link names
        """
        query = f"SELECT DISTINCT link_name FROM {self.base_table} ORDER BY link_name"
        self.cursor.execute(query)
        
        rows = self.cursor.fetchall()
        
        return_value = []
        for row in rows:
            # Handle both sqlite3.Row and dict access
            link_name = row["link_name"] if isinstance(row, sqlite3.Row) else row.get("link_name")
            if link_name is not None:
                return_value.append(link_name)
        
        return return_value
    
    def find_all_node_names(self):
        """
        Get all unique parent paths (node names) from the table.
        
        Returns:
            list: List of all unique parent paths
        """
        query = f"SELECT DISTINCT parent_path FROM {self.base_table} ORDER BY parent_path"
        self.cursor.execute(query)
        
        rows = self.cursor.fetchall()
        
        return_value = []
        for row in rows:
            # Handle both sqlite3.Row and dict access
            parent_path = row["parent_path"] if isinstance(row, sqlite3.Row) else row.get("parent_path")
            if parent_path is not None:
                return_value.append(parent_path)
        
        return return_value


# Example usage:
"""
import sqlite3

# Connect to database
conn = sqlite3.connect('your_database.db')
conn.row_factory = sqlite3.Row  # Enable dictionary-like access
cursor = conn.cursor()

# Create instance
link_table = KB_Link_Table(conn, cursor, "my_database")

# Use the methods
records = link_table.find_records_by_link_name("example_link")
node_records = link_table.find_records_by_node_path("root.docs", kb="kb1")
all_links = link_table.find_all_link_names()
all_nodes = link_table.find_all_node_names()

# Close connection when done
conn.close()
"""

