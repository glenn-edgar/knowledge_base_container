import psycopg2
from psycopg2 import sql

import json
from typing import Optional, Dict, Any


class KnowledgeBaseManager:
    def __init__(self, table_name: str, connection_params: Dict[str, Any]):
        """
        Initialize the KnowledgeBaseManager with database connection parameters.
        
        Args:
            connection_params: Dictionary containing database connection parameters
                             (host, database, user, password, port)
        """
        self.connection_params = connection_params
        
        self.table_name = table_name
        self._connect()
        self._create_tables()
        
   
 
        
    def _connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            # Enable ltree extension if not already enabled
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS ltree;")
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            raise
            
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
    def _delete_table(self, table_name, schema='public'):
        """
        Deletes a specified table from the PostgreSQL database using an existing connection.
        
        Args:
            table_name (str): Name of the table to delete.
            schema (str): Schema name where the table resides (default: 'public').
        
        Returns:
            bool: True if the table was deleted successfully, False otherwise.
        """
        try:
            # Create a cursor from the existing connection
            cursor = self.cursor

            # Construct and execute DROP TABLE command
            drop_query = f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;"
            cursor.execute(drop_query)
    
         

        except psycopg2.Error as e:
            print(f"Error deleting table {schema}.{table_name}: {e}")
            raise
            
    def _create_tables(self):
        """
        Create knowledge base tables with the supplied table name.
        """
        
        self._delete_table(self.table_name)
        self._delete_table(f"{self.table_name}_info")
        self._delete_table(f"{self.table_name}_link")
        self._delete_table(f"{self.table_name}_link_mount")
        
        try:
            # Create knowledge base table
            kb_table_query = sql.SQL("""
                CREATE TABLE {} (
                    id SERIAL PRIMARY KEY,
                    knowledge_base VARCHAR NOT NULL,
                    label VARCHAR NOT NULL,
                    name VARCHAR NOT NULL,
                    properties JSON,
                    data JSON,
                    has_link BOOLEAN DEFAULT FALSE,
                    has_link_mount BOOLEAN DEFAULT FALSE,
                    path LTREE UNIQUE
                )
            """).format(sql.Identifier(self.table_name))
            
            # Create information table
            info_table_query = sql.SQL("""
                CREATE TABLE {} (
                    id SERIAL PRIMARY KEY,
                    knowledge_base VARCHAR NOT NULL UNIQUE,
                    description VARCHAR
                )
            """).format(sql.Identifier(f"{self.table_name}_info"))
            
            # Create link table
            link_table_query = sql.SQL("""
                CREATE TABLE {} (
                    id SERIAL PRIMARY KEY,
                    link_name VARCHAR NOT NULL,
                    parent_node_kb VARCHAR NOT NULL,
                    parent_path LTREE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(link_name, parent_node_kb, parent_path)
                )
            """).format(sql.Identifier(f"{self.table_name}_link"))
            
            # Create link mount table
            link_mount_table_query = sql.SQL("""
                CREATE TABLE {} (
                    id SERIAL PRIMARY KEY,
                    link_name VARCHAR NOT NULL UNIQUE,
                    knowledge_base VARCHAR NOT NULL,
                    mount_path LTREE NOT NULL,
                    description VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(knowledge_base, mount_path)
                )
            """).format(sql.Identifier(f"{self.table_name}_link_mount"))
            
            # Execute table creation
            self.cursor.execute(kb_table_query)
            self.cursor.execute(info_table_query)
            self.cursor.execute(link_table_query)
            self.cursor.execute(link_mount_table_query)
            
            # Create indexes for better performance
            
            # === Main Knowledge Base Table Indexes ===
            # Index on knowledge_base field for faster lookups
            kb_index_query = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (knowledge_base)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_kb"),
                sql.Identifier(self.table_name)
            )
            
            # GiST index on path for ltree operations
            path_index_query = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (path)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_path"),
                sql.Identifier(self.table_name)
            )
            
            # Index on label for search operations
            label_index_query = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (label)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_label"),
                sql.Identifier(self.table_name)
            )
            
            # Index on name for search operations
            name_index_query = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (name)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_name"),
                sql.Identifier(self.table_name)
            )
            
            # Index on has_link flag for filtering
            has_link_index_query = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (has_link)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_has_link"),
                sql.Identifier(self.table_name)
            )
            
            # Index on has_link_mount flag for filtering
            has_link_mount_index_query = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (has_link_mount)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_has_link_mount"),
                sql.Identifier(self.table_name)
            )
            
            # Composite index for common queries (knowledge_base + path)
            kb_path_composite_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (knowledge_base, path)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_kb_path"),
                sql.Identifier(self.table_name)
            )
            
            # === Info Table Indexes ===
            # knowledge_base already has UNIQUE constraint, but adding explicit index for clarity
            info_kb_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (knowledge_base)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_info_kb"),
                sql.Identifier(f"{self.table_name}_info")
            )
            
            # === Link Table Indexes ===
            # Index on link_name for fast link lookups
            link_name_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (link_name)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_link_name"),
                sql.Identifier(f"{self.table_name}_link")
            )
            
            # Index on parent_node_kb for filtering by knowledge base
            link_parent_kb_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (parent_node_kb)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_link_parent_kb"),
                sql.Identifier(f"{self.table_name}_link")
            )
            
            # GiST index on parent_path for ltree operations
            link_parent_path_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (parent_path)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_link_parent_path"),
                sql.Identifier(f"{self.table_name}_link")
            )
            
            # Index on created_at for temporal queries
            link_created_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (created_at)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_link_created"),
                sql.Identifier(f"{self.table_name}_link")
            )
            
            # Composite index for common query patterns (link_name + parent_node_kb)
            link_composite_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (link_name, parent_node_kb)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_link_composite"),
                sql.Identifier(f"{self.table_name}_link")
            )
            
            # === Link Mount Table Indexes ===
            # Index on link_name (already has UNIQUE constraint but adding for explicit fast lookups)
            mount_link_name_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (link_name)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_mount_link_name"),
                sql.Identifier(f"{self.table_name}_link_mount")
            )
            
            # Index on knowledge_base for fast lookups
            mount_kb_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (knowledge_base)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_mount_kb"),
                sql.Identifier(f"{self.table_name}_link_mount")
            )
            
            # GiST index on mount_path for ltree operations
            mount_path_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (mount_path)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_mount_path"),
                sql.Identifier(f"{self.table_name}_link_mount")
            )
            
            # Index on created_at for temporal queries
            mount_created_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (created_at)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_mount_created"),
                sql.Identifier(f"{self.table_name}_link_mount")
            )
            
            # Composite index for the unique constraint (knowledge_base + mount_path)
            mount_composite_index = sql.SQL("""
                CREATE INDEX IF NOT EXISTS {} ON {} (knowledge_base, mount_path)
            """).format(
                sql.Identifier(f"idx_{self.table_name}_mount_composite"),
                sql.Identifier(f"{self.table_name}_link_mount")
            )
            
            # Execute all index creation queries
            indexes = [
                # Main table indexes
                kb_index_query,
                path_index_query,
                label_index_query,
                name_index_query,
                has_link_index_query,
                has_link_mount_index_query,
                kb_path_composite_index,
                # Info table indexes
                info_kb_index,
                # Link table indexes
                link_name_index,
                link_parent_kb_index,
                link_parent_path_index,
                link_created_index,
                link_composite_index,
                # Mount table indexes
                mount_link_name_index,
                mount_kb_index,
                mount_path_index,
                mount_created_index,
                mount_composite_index
            ]
            
            for index_query in indexes:
                self.cursor.execute(index_query)
            
            self.conn.commit()
          
            
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error creating tables: {e}")
            raise
            
    def add_kb(self,  kb_name: str, description: Optional[str] = None):
        """
        Add a knowledge base entry to the information table.
        
        Args:
            table_name: Base table name
            kb_name: Name of the knowledge base
            description: Optional description of the knowledge base
        """
        if not isinstance(kb_name, str):
            raise TypeError("kb_name must be a string")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        
        
        try:
            info_table = f"{self.table_name}_info"
            query = sql.SQL("""
                INSERT INTO {} (knowledge_base, description)
                VALUES (%s, %s)
                ON CONFLICT (knowledge_base) DO NOTHING
            """).format(sql.Identifier(info_table))
            
            self.cursor.execute(query, (kb_name, description))
            self.conn.commit()
        
            
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error adding knowledge base: {e}")
            raise
        
    
        
            
    def add_node(self,  kb_name: str, label: str, name: str, 
                 properties: Optional[Dict] = None, data: Optional[Dict] = None, 
                 path: str = ''):
        """
        Add a node to the knowledge base.
        
    
            kb_name: Name of the knowledge base
            label: Label for the node
            name: Name of the node
            properties: Optional JSON properties
            data: Optional JSON data
            path: LTREE path for the node
        """
        if not isinstance(kb_name, str):
            raise TypeError("kb_name must be a string")
        if not isinstance(label, str):
            raise TypeError("label must be a string")
        if not isinstance(name, str):
            raise TypeError("name must be a string")
        if not isinstance(path, str):
            raise TypeError("path must be a string")
        if not isinstance(properties, dict):
            raise TypeError("properties must be a dictionary")
        if not isinstance(data, dict):
            raise TypeError("data must be a dictionary")
        
        try:
            if not isinstance(kb_name, str):
                raise TypeError("kb_name must be a string")
            if not isinstance(label, str):
                raise TypeError("label must be a string")
            if not isinstance(name, str):
                raise TypeError("name must be a string")
            if not isinstance(path, str):
                raise TypeError("path must be a string")
            if not isinstance(properties, dict):
                raise TypeError("properties must be a dictionary")
            if not isinstance(data, dict):
                raise TypeError("data must be a dictionary")
            
            # Check if kb_name exists in info table
            info_table = f"{self.table_name}_info"
            check_query = sql.SQL("""
                SELECT 1 FROM {} WHERE knowledge_base = %s
            """).format(sql.Identifier(info_table))
            
            self.cursor.execute(check_query, (kb_name,))
            if not self.cursor.fetchone():
                raise ValueError(f"Knowledge base '{kb_name}' not found in info table")
            
            # Prepend kb_name to path to ensure uniqueness
            full_path = path
            
            
            # Convert dictionaries to JSON strings
            properties_json = json.dumps(properties) if properties else None
            data_json = json.dumps(data) if data else None
            
            # Insert node
            insert_query = sql.SQL("""
                INSERT INTO {} (knowledge_base, label, name, properties, data, has_link, path)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(insert_query, 
                              (kb_name, label, name, properties_json, data_json, False, full_path))
            self.conn.commit()

            
        except psycopg2.Error as e:
         
            print(f"Error adding node: {e}")
            raise
        except ValueError as e:
            print(f"Validation error: {e}")
            raise
            
    def add_link(self,  parent_kb: str, parent_path: str, 
                 link_name: str):
        """
        Add a link between two nodes in the knowledge base.
        
        Args:
            
            parent_kb: Parent node's knowledge base name
            parent_path: Parent node's path
            child_kb: Child node's knowledge base name
            child_path: Child node's path
            
        """
       
        if not isinstance(parent_kb, str):
            raise TypeError("parent_kb must be a string")
        if not isinstance(parent_path, str):
            raise TypeError("parent_path must be a string")
        if not isinstance(link_name, str):
            raise TypeError("link_name must be a string")
        
        
        try:
           
            
            # Check if both knowledge bases exist
            info_table = f"{self.table_name}_info"
            kb_check_query = sql.SQL("""
                SELECT knowledge_base FROM {} WHERE knowledge_base IN (%s)
            """).format(sql.Identifier(info_table))
            
            self.cursor.execute(kb_check_query, (parent_kb,))
            found_kbs = [row[0] for row in self.cursor.fetchall()]
            
            if parent_kb not in found_kbs:
                raise ValueError(f"Parent knowledge base '{parent_kb}' not found")
          
            
            # Check if parent node exist in the knowledge base
            node_check_query = sql.SQL("""
                SELECT path FROM {} WHERE path IN (%s)
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(node_check_query, (parent_path,))
            found_paths = [row[0] for row in self.cursor.fetchall()]
            
            if parent_path not in found_paths:
                raise ValueError(f"Parent node with path '{parent_path}' not found")
            # Check if link name already exists
            link_name_exists_query = sql.SQL("""
                SELECT link_name FROM {} WHERE link_name = %s
            """).format(sql.Identifier(f"{self.table_name}_link_mount"))
            
            self.cursor.execute(link_name_exists_query, (link_name,))
            if not self.cursor.fetchone():
                raise ValueError(f"Link name '{link_name}' already exists in link_mount table")
       
            link_table = f"{self.table_name}_link"
            link_insert_query = sql.SQL("""
                INSERT INTO {} (parent_node_kb, parent_path, link_name)
                VALUES (%s, %s, %s)
                    
                """).format(sql.Identifier(link_table))
            
            self.cursor.execute(link_insert_query, 
                              (parent_kb, parent_path, link_name))
            
            # Update has_link flag for parent node
            update_query = sql.SQL("""
                UPDATE {} SET has_link = TRUE WHERE path = %s
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(update_query, (parent_path,))
            
            self.conn.commit()
           
            
        except psycopg2.Error as e:
            
            print(f"Error adding link: {e}")
            raise
        except ValueError as e:
            print(f"Validation error: {e}")
            raise

    def add_link_mount(self, knowledge_base, path, link_mount_name, description = ""):
        """
        Add a link node by verifying prerequisites and updating the has_link_mount flag.
        
        Args:
            knowledge_base (str): The knowledge base identifier
            path (str): The LTREE path in the knowledge base
            link_mount_name (str): The name of the link mount
            description (str): The description of the link mount
        Returns:
            tuple: (knowledge_base, mount_path) on successful completion
            
        Raises:
            ValueError: If any validation fails (knowledge base not found, path not found, etc.)
            RuntimeError: If database operations fail (insert/update failures)
            psycopg2.Error: If database connection/transaction fails
        """
        
        
        if not isinstance(knowledge_base, str):
            raise TypeError("knowledge_base must be a string")
        if not isinstance(path, str):
            raise TypeError("path must be a string")
        if not isinstance(link_mount_name, str):
            raise TypeError("link_mount_name must be a string")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        
        try:
        
            # Step 1: Verify that knowledge_base exists in info table
            info_check_query = sql.SQL("""
                SELECT knowledge_base FROM {} WHERE knowledge_base = %s
            """).format(sql.Identifier(f"{self.table_name}_info"))
            
            self.cursor.execute(info_check_query, (knowledge_base,))
            if not self.cursor.fetchone():
                raise ValueError(f"Knowledge base '{knowledge_base}' does not exist in info table")
            
            # Step 2: Verify that the path exists for the given knowledge base in main table
            path_check_query = sql.SQL("""
                SELECT id FROM {} WHERE knowledge_base = %s AND path = %s
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(path_check_query, (knowledge_base, path))
            node_record = self.cursor.fetchone()
            if not node_record:
                raise ValueError(f"Path '{path}' does not exist for knowledge base '{knowledge_base}'")
            
           
            
            # Step 3: Verify that link_name does not already exist in link_mount table
            link_name_exists_query = sql.SQL("""
                SELECT link_name FROM {} WHERE link_name = %s
            """).format(sql.Identifier(f"{self.table_name}_link_mount"))
            
            self.cursor.execute(link_name_exists_query, (link_mount_name,))
            if self.cursor.fetchone():
                raise ValueError(f"Link name '{link_mount_name}' already exists in link_mount table")
            
            # Step 4: Insert a record in the link_mount table with knowledge_base, path, link_mount_name
            insert_link_mount_record_query = sql.SQL("""
                INSERT INTO {} (link_name, knowledge_base, mount_path, description)
                VALUES (%s, %s, %s, %s)
            """).format(sql.Identifier(f"{self.table_name}_link_mount"))
            
            self.cursor.execute(insert_link_mount_record_query, (link_mount_name, knowledge_base, path, description))
            
            if self.cursor.rowcount == 0:
                raise RuntimeError(f"Failed to insert record with link_name '{link_mount_name}', knowledge_base '{knowledge_base}', path '{path}' into link_mount table")
            
            # Step 5: Verify that entry with knowledge_base and mount_path exists in main table
            mount_entry_check_query = sql.SQL("""
                SELECT id FROM {} WHERE knowledge_base = %s AND path = %s
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(mount_entry_check_query, (knowledge_base, path))
            mount_entry_record = self.cursor.fetchone()
            if not mount_entry_record:
                raise ValueError(f"Entry with knowledge_base '{knowledge_base}' and mount_path '{path}' does not exist in main table")
            
         
            
            # Step 7: Set the has_link_mount field to true for the original node
            update_query = sql.SQL("""
                UPDATE {} SET has_link_mount = TRUE 
                WHERE knowledge_base = %s AND path = %s
            """).format(sql.Identifier(self.table_name))
            
            self.cursor.execute(update_query, (knowledge_base, path))
            
            # Check if the update was successful
            if self.cursor.rowcount == 0:
                raise RuntimeError(f"No rows were updated for knowledge_base '{knowledge_base}' and path '{path}'")
            
            # Commit the transaction
            self.conn.commit()
            
           
            return (knowledge_base, path)
            
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Database error in add_link_node: {e}")
            raise
        except Exception as e:
            self.conn.rollback()
            print(f"Unexpected error in add_link_node: {e}")
            raise 

# Example usage
if __name__ == "__main__":
    password = input("Enter PostgreSQL password: ")
    # Database connection parameters
    conn_params = {
        'host': 'localhost',
        'database': 'knowledge_base',
        'user': 'gedgar',
        'password': password,
        'port': 5432
    }
    
    # Using context manager
    kb_manager = KnowledgeBaseManager('knowledge_base', conn_params)
    # Create tables
    print("starting unit test")
    
    # Add knowledge bases
    kb_manager.add_kb( 'kb1', 'First knowledge base')
    kb_manager.add_kb('kb2', 'Second knowledge base')
    
    # Add nodes
    kb_manager.add_node( 'kb1', 'person', 'John Doe',
                        {'age': 30}, {'email': 'john@example.com'}, 'people.john')
    kb_manager.add_node('kb2', 'person', 'Jane Smith',
                        {'age': 25}, {'email': 'jane@example.com'}, 'people.jane')
    
    
    kb_manager.add_link_mount('kb1', 'people.john', 'link1', 'link1 description')
    
    # Add link
    kb_manager.add_link( 'kb1', 'people.john','link1')
    kb_manager.disconnect()
    print("ending unit test")

