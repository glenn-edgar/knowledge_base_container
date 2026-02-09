import sqlite3
import json


class Construct_Status_Table:
    """
    This class is designed to construct a status table with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a SQLite database and sets up the schema.
    """
    def __init__(self, conn, cursor, construct_kb, database,upload_flag=False):
        self.conn = conn
        self.cursor = cursor
        self.construct_kb = construct_kb
        self.database = database
        self.table_name = self.database + "_status"
        print(f"database: {self.database}")
        self.upload_flag = upload_flag
        if self.upload_flag == False:
            self._setup_schema()
        
    def _setup_schema(self):
        """
        Sets up the database schema (tables, functions, etc.).
        """
        # Drop existing table if it exists
        query = f"DROP TABLE IF EXISTS {self.table_name}"
        self.cursor.execute(query)
        
        # Create the status table with dynamic name
        create_table_script = f"""
            CREATE TABLE {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT,
                path TEXT UNIQUE
            )
        """
        
        self.cursor.execute(create_table_script)
        
        # Create indexes optimized for equal read/write workload
        # Primary key index is automatically created for 'id'
        
        # Index for the path column (already unique, but explicit index for performance)
        create_path_index = f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_path ON {self.table_name} (path)
        """
        self.cursor.execute(create_path_index)
        
        self.conn.commit()  # Commit all changes
        print(f"Status table '{self.table_name}' created with optimized indexes.")

    def add_status_field(self, status_key, properties, description, initial_data):
        """
        Add a new status field to the knowledge base
        
        Args:
            status_key (str): The key/name of the status field
            properties (dict): Properties for the status field
            description (str): The description of the status field
            initial_data (dict): Initial data for the status field
            
        Raises:
            TypeError: If status_key is not a string or properties/initial_data are not dictionaries
        """
        if not isinstance(status_key, str):
            raise TypeError("status_key must be a string")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        if not isinstance(initial_data, dict):
            raise TypeError("initial_data must be a dictionary")
            
        if properties == None:
            initial_properties = {}
        else:
            initial_properties = properties
            
        if not isinstance(initial_properties, dict):
            raise TypeError("properties must be a dictionary")
       
        print(f"Added status field '{status_key}' with properties: {initial_properties} and data: {initial_data}")
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_STATUS_FIELD", status_key, initial_properties, initial_data, description)
        
        return {
            "status": "success",
            "message": f"Status field '{status_key}' added successfully",
            "properties": initial_properties,
            "data": initial_data
        }
    
    def check_installation(self):     
        """
        Synchronize the knowledge_base and status_table based on paths.
        - Remove entries from status_table that don't exist in knowledge_base with label "KB_STATUS_FIELD"
        - Add entries to status_table for paths in knowledge_base that don't exist in status_table
        """
        # Get all paths from status_table
        get_paths_query = f"SELECT path FROM {self.table_name}"
        
        self.cursor.execute(get_paths_query)
        all_paths = [row[0] for row in self.cursor.fetchall()]
        
        # Get specified paths (paths with label "KB_STATUS_FIELD") from knowledge_table
        query = f"""
            SELECT path FROM {self.database} 
            WHERE label = 'KB_STATUS_FIELD'
        """
        self.cursor.execute(query)
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
        delete_query = f"""
            DELETE FROM {self.table_name}
            WHERE path = ?
        """
        
        for path in not_specified_paths:
            print(f"deleting path: {path}")
            self.cursor.execute(delete_query, (path,))
        
        # Process missing_paths: add entries to status_table
        insert_query = f"""
            INSERT INTO {self.table_name} 
            (data, path)
            VALUES (?, ?)
        """
        
        for path in missing_paths:
            print(f"inserting path: {path}")
            self.cursor.execute(insert_query, (json.dumps({}), path))
        
        # Commit the changes
        self.conn.commit()
        
        return {
            "missing_paths_added": len(missing_paths),
            "not_specified_paths_removed": len(not_specified_paths)
        }

