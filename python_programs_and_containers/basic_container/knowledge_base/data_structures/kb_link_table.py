class KB_Link_Table:
    def __init__(self, conn, cursor, base_table):
        """
        Initialize the LinkTable class.
        
        Args:
            conn: Database connection object
            cursor: Database cursor object
            base_table: Base name for the table (will be suffixed with '_link')
        """
        self.conn = conn
        self.cursor = cursor
        self.base_table = base_table + "_link"
    
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
                WHERE link_name = %s
            """
            self.cursor.execute(query, (link_name,))
        else:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE link_name = %s AND parent_node_kb = %s
            """
            self.cursor.execute(query, (link_name, kb))
        
        # Get column names from cursor description
        rows = self.cursor.fetchall()
        return rows
    
    def find_records_by_node_path(self, node_path, kb=None):
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
                WHERE parent_path = %s
            """
            self.cursor.execute(query, (node_path,))
        else:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE parent_path = %s AND parent_node_kb = %s
            """
            self.cursor.execute(query, (node_path, kb))
        
        return self.cursor.fetchall()
    
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
        
            return_value.append(row["link_name"])
        return return_value
    
    def find_all_node_names(self):
        """
        Get all unique mount points from the table.
        
        Returns:
            list: List of all unique mount points
        """
        query = f"SELECT DISTINCT parent_path FROM {self.base_table} ORDER BY parent_path"
        self.cursor.execute(query)
        return [row["parent_path"] for row in self.cursor.fetchall()]

# Example usage:
"""
import psycopg2

# Connect to database
conn = psycopg2.connect(
    host="localhost",
    database="your_db",
    user="your_user",
    password="your_password"
)
cursor = conn.cursor()

# Create instance
link_table = LinkTable(conn, cursor, "my_database")

# Use the methods
records = link_table.find_records_by_link_name("example_link")
mount_records = link_table.find_records_by_mount_path("root.docs", kb="kb1")
all_links = link_table.find_all_link_names()
all_mounts = link_table.find_all_mount_points()
"""