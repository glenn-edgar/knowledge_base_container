import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt

class Construct_Status_Table:
    """
    This class is designed to construct a status table with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a PostgreSQL database and sets up the schema.
    """
    def __init__(self, conn, cursor,construct_kb,database):
        self.conn = conn
        self.cursor = cursor
        self.construct_kb = construct_kb
        self.database = database
        self.table_name = self.database + "_status"
        print(f"database: {self.database}")
       
        self._setup_schema()
        
    def _setup_schema(self):
        """
        Sets up the database schema (tables, functions, etc.).
    
        # Use psycopg2.sql module to construct SQL queries safely. This prevents SQL injection.
        # ltree extension needs to be created.
        """
        # Create extensions
        create_extensions_script = sql.SQL("""
            CREATE EXTENSION IF NOT EXISTS ltree;
        """)
        self.cursor.execute(create_extensions_script)
        
        # Create the table with dynamic name
        query = sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE").format(
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(query)
        create_table_script = sql.SQL("""
            CREATE TABLE  {table_name} (
                id SERIAL PRIMARY KEY,
                data JSON,
                path LTREE UNIQUE
            );
        """).format(table_name=sql.Identifier(self.table_name))
        
        self.cursor.execute(create_table_script)
        
        # Create indexes optimized for equal read/write workload
        # Primary key index is automatically created for 'id'
        
        # Index for the path column (already unique, but explicit index for performance)
        create_path_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIST (path);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_path_gist"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_path_index)
        
      
        # Additional B-tree index on path for exact lookups and ordering
        create_path_btree_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (path);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_path_btree"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_path_btree_index)
        
        self.conn.commit()  # Commit all changes
        print(f"Status table '{self.table_name}' created with optimized indexes.")

    def add_status_field(self, status_key, properties, description,initial_data):
        """
        Add a new status field to the knowledge base
        
        Args:
            status_key (str): The key/name of the status field
            initial_properties (dict): Initial properties for the status field
            description (str): The description of the status field
            initial_data (dict): Initial data for the status field
            
        Raises:
            TypeError: If status_key is not a string or initial_properties is not a dictionary
        """
        if not isinstance(status_key, str):
            raise TypeError("status_key must be a string")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        if not isinstance(initial_data, dict):
            raise TypeError("initial_data must be a dictionary")
            
        if  properties == None:
            initial_properties = {}
        if not isinstance(properties, dict):
            raise TypeError("properties must be a dictionary")
       
        
        print(f"Added status field '{status_key}' with properties: {properties} and data: {initial_data}")
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_STATUS_FIELD", status_key, properties, initial_data,description)
        
        
        
        return {
            "status": "success",
            "message": f"Status field '{status_key}' added successfully",
            "properties": properties,
            "data": initial_data
        }
    
    def check_installation(self):     
        """
        Synchronize the knowledge_base and status_table based on paths.
        - Remove entries from status_table that don't exist in knowledge_base with label "KB_STATUS_FIELD"
        - Add entries to status_table for paths in knowledge_base that don't exist in status_table
        """
        # Construct the dynamic table name
        
        
        # Get all paths from status_table
        get_paths_query = sql.SQL("""
            SELECT path FROM {table_name};
        """).format(table_name=sql.Identifier(self.table_name))
        
        self.cursor.execute(get_paths_query)
        all_paths = [row[0] for row in self.cursor.fetchall()]
        
        # Get specified paths (paths with label "KB_STATUS_FIELD") from knowledge_table
        
        self.cursor.execute(sql.SQL("""
            SELECT path FROM {table_name} 
            WHERE label = 'KB_STATUS_FIELD';
            """).format(table_name=sql.Identifier(self.database )))
        specified_paths_data = self.cursor.fetchall()
        specified_paths = [row[0] for row in specified_paths_data]
        print(f"specified_paths: {specified_paths}")
        
        # Find missing_paths: paths in specified_paths that are not in all_paths
        missing_paths = [path for path in specified_paths if path not in all_paths]
        print(f"missing_paths: {missing_paths}")
        
        # Find not_specified_paths: paths in all_paths that are not in specified_paths
        not_specified_paths = [path for path in all_paths if path not in specified_paths]
        print(f"not_specified_paths: {not_specified_paths}")
        
        # Process not_specified_paths: remove entries from status_table
        delete_query = sql.SQL("""
            DELETE FROM {table_name}
            WHERE path = %s;
        """).format(table_name=sql.Identifier(self.table_name))
        
        for path in not_specified_paths:
            print(f"deleting path: {path}")
            self.cursor.execute(delete_query, (path,))
        
        # Process missing_paths: add entries to status_table
        insert_query = sql.SQL("""
            INSERT INTO {table_name} 
            (data, path)
            VALUES (%s, %s);
        """).format(table_name=sql.Identifier(self.table_name))
        
        for path in missing_paths:
            print(f"inserting path: {path}")
            self.cursor.execute(insert_query, ('{}', path))
        
        # Commit the changes
        self.conn.commit()
        
        return {
            "missing_paths_added": len(missing_paths),
            "not_specified_paths_removed": len(not_specified_paths)
        }
    