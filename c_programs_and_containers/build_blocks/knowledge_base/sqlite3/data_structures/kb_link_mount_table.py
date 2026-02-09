import sqlite3

class KB_Link_Mount_Table:
    def __init__(self, conn, cursor, database):
        """
        Initialize the LinkMountTable class.
        
        Args:
            conn: Database connection object
            cursor: Database cursor object
            database: Base name for the table (will be suffixed with '_link_mount')
        """
        self.conn = conn
        self.cursor = cursor
        self.base_table = database + "_link_mount"
        
        # Enable dictionary-like row access
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
    
    def find_records_by_link_name(self, link_name, kb=None):
        """
        Find records by link_name, optionally filtered by parent_node_kb.
        
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
                WHERE link_name = ? AND knowledge_base = ?
            """
            self.cursor.execute(query, (link_name, kb))
        
        # Convert sqlite3.Row objects to dictionaries
        return [dict(row) for row in self.cursor.fetchall()]
    
    def find_records_by_mount_path(self, mount_path, kb=None):
        """
        Find records by mount_path, optionally filtered by knowledge_base.
        
        Args:
            mount_path (str): The mount path to search for
            kb (str, optional): Knowledge base to filter by. If None, search all.
            
        Returns:
            list: List of matching records as dictionaries
        """
        if kb is None:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE mount_path = ?
            """
            self.cursor.execute(query, (mount_path,))
        else:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE mount_path = ? AND knowledge_base = ?
            """
            self.cursor.execute(query, (mount_path, kb))
        
        # Convert sqlite3.Row objects to dictionaries
        return [dict(row) for row in self.cursor.fetchall()]
    
    def find_all_link_names(self):
        """
        Get all unique link names from the table.
        
        Returns:
            list: List of all unique link names
        """
        query = f"SELECT DISTINCT link_name FROM {self.base_table} ORDER BY link_name"
        self.cursor.execute(query)
        return [row["link_name"] for row in self.cursor.fetchall()]
    
    def find_all_mount_paths(self):
        """
        Get all unique mount paths from the table.
        
        Returns:
            list: List of all unique mount paths
        """
        query = f"SELECT DISTINCT mount_path FROM {self.base_table} ORDER BY mount_path"
        self.cursor.execute(query)
        return [row["mount_path"] for row in self.cursor.fetchall()]


# Example usage:
"""
import sqlite3

# Connect to database
conn = sqlite3.connect('your_database.db')
cursor = conn.cursor()

# Create instance
link_mount_table = KB_Link_Mount_Table(conn, cursor, "my_database")

# Use the methods
records = link_mount_table.find_records_by_link_name("example_link")
mount_records = link_mount_table.find_records_by_mount_path("root.docs.folder", kb="kb1")
all_links = link_mount_table.find_all_link_names()
all_mounts = link_mount_table.find_all_mount_paths()

# Don't forget to close the connection when done
conn.close()
"""

