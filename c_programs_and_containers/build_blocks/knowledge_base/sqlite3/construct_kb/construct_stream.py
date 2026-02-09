import sqlite3
import json


class Construct_Stream_Table:
    """
    This class is designed to construct a stream table with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a SQLite database and sets up the schema.
    """
    def __init__(self, conn, cursor, construct_kb, database,upload_flag=False):
        self.conn = conn
        self.cursor = cursor
        self.construct_kb = construct_kb
        self.database = database
        self.table_name = self.database + "_stream"
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
        
        # Create the stream table
        create_table_script = f"""
            CREATE TABLE {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT,
                recorded_at TEXT DEFAULT (datetime('now')),
                valid INTEGER DEFAULT 0,
                data TEXT
            )
        """
        self.cursor.execute(create_table_script)
        
        # Create indexes optimized for read/write operations
        # Primary key index on 'id' is automatically created
        
        # Index on path for ltree operations (hierarchical queries)
        create_path_index = f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_path ON {self.table_name} (path)
        """
        self.cursor.execute(create_path_index)
        
        # Index on recorded_at for time-based queries and ordering
        create_recorded_at_index = f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_recorded_at ON {self.table_name} (recorded_at)
        """
        self.cursor.execute(create_recorded_at_index)
        
        # Descending index on recorded_at for recent-first queries
        create_recorded_at_desc_index = f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_recorded_at_desc 
            ON {self.table_name} (recorded_at DESC)
        """
        self.cursor.execute(create_recorded_at_desc_index)
        
        # Composite index on path and recorded_at for stream queries by path and time
        create_path_time_index = f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_path_recorded_at 
            ON {self.table_name} (path, recorded_at)
        """
        self.cursor.execute(create_path_time_index)
        
        self.conn.commit()
        print(f"Stream table '{self.table_name}' created with optimized indexes.")
    
    def add_stream_field(self, stream_key, stream_length, description):
        """
        Add a new stream field to the knowledge base
        
        Args:
            stream_key (str): The key/name of the stream field
            stream_length (int): The length of the stream
            description (str): The description of the stream field
            
        Raises:
            TypeError: If stream_key is not a string or stream_length is not an integer
        """
        if not isinstance(stream_key, str):
            raise TypeError("stream_key must be a string")
        
        if not isinstance(stream_length, int):
            raise TypeError("stream_length must be an integer")
        
        properties = {"stream_length": stream_length}
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_STREAM_FIELD", stream_key, properties, {}, description)
        
        return {
            "stream": "success",
            "message": f"stream field '{stream_key}' added successfully",
            "properties": properties,
            "data": description
        }
        
        
    def _remove_invalid_stream_fields(self, invalid_stream_paths, chunk_size=500):
        """
        Removes all database entries with paths that match any in the invalid_stream_paths array.
        Processes the deletion in chunks to avoid SQL statement limitations.
        
        Args:
            invalid_stream_paths (list): Array of paths that should be removed from the database
            chunk_size (int): Maximum number of paths to process in a single query
        """
        if not invalid_stream_paths:
            return  # Nothing to do if array is empty
        
        # Process in chunks to avoid SQL limitations
        for i in range(0, len(invalid_stream_paths), chunk_size):
            # Get current chunk
            chunk = invalid_stream_paths[i:i + chunk_size]
            
            # Construct placeholders for SQL IN clause
            placeholders = ','.join(['?'] * len(chunk))
            
            # Delete entries with paths in current chunk
            delete_query = f"""
                DELETE FROM {self.table_name}
                WHERE path IN ({placeholders})
            """
            
            self.cursor.execute(delete_query, chunk)
        
        # Commit after all chunks are processed
        self.conn.commit()
        
        
    def _manage_stream_table(self, specified_stream_paths, specified_stream_length):
        """
        Manages the number of records in stream_table to match specified stream lengths for each path.
        Removes older records first if necessary and adds new ones with NULL for JSON data.
        
        Args:
            specified_stream_paths (list): Array of valid paths
            specified_stream_length (list): Array of corresponding lengths for each path
        """
        # Iterate through the arrays of paths and lengths
        for i in range(len(specified_stream_paths)):
            path = specified_stream_paths[i]
            target_length = specified_stream_length[i]
            
            stream_field_prompt = f"""
                SELECT COUNT(*) FROM {self.table_name} WHERE path = ?
            """

            self.cursor.execute(stream_field_prompt, (path,))
  
            current_count = self.cursor.fetchone()[0]
            
            # Calculate the difference
            diff = target_length - current_count
           
            if diff < 0:
                # Need to remove records (oldest first) for this path
                query = f"""
                    DELETE FROM {self.table_name}
                    WHERE path = ? AND rowid IN (
                        SELECT rowid 
                        FROM {self.table_name}
                        WHERE path = ?
                        ORDER BY recorded_at ASC 
                        LIMIT ?
                    )
                """

                # Execute the query with parameter bindings
                self.cursor.execute(query, (path, path, abs(diff)))
                
            elif diff > 0:
                # Need to add records for this path
                for _ in range(diff):
                    query = f"""
                        INSERT INTO {self.table_name} (path, recorded_at, data, valid)
                        VALUES (?, datetime('now'), ?, 0)
                    """

                    self.cursor.execute(query, (path, json.dumps({})))
                        
        # Commit all changes at once
        self.conn.commit()
        
    def check_installation(self):     
        """
        Synchronize the knowledge_base and stream_table based on paths.
        - Remove entries from stream_table that don't exist in knowledge_base with label "KB_STREAM_FIELD"
        - Add entries to stream_table for paths in knowledge_base that don't exist in stream_table
        """
        
        # Get all paths from stream_table
        stream_paths_query = f"SELECT DISTINCT path FROM {self.table_name}"
        
        self.cursor.execute(stream_paths_query)
        unique_stream_paths = [row[0] for row in self.cursor.fetchall()]
        
        # Get specified paths (paths with label "KB_STREAM_FIELD") from knowledge_table
        knowledge_query = f"""
            SELECT path, label, name, properties FROM {self.database} 
            WHERE label = 'KB_STREAM_FIELD'
        """
        
        self.cursor.execute(knowledge_query)
        specified_stream_data = self.cursor.fetchall()
        
        specified_stream_paths = [row[0] for row in specified_stream_data]
        specified_stream_length = []
        
        # Parse JSON properties to get stream_length
        for row in specified_stream_data:
            props_json = row[3]
            if props_json:
                props = json.loads(props_json)
                specified_stream_length.append(props.get('stream_length', 0))
            else:
                specified_stream_length.append(0)
        
        print(f"specified_stream_paths: {specified_stream_paths}")
        print(f"specified_stream_length: {specified_stream_length}")
        
        invalid_stream_paths = [path for path in unique_stream_paths if path not in specified_stream_paths]
        missing_stream_paths = [path for path in specified_stream_paths if path not in unique_stream_paths]
        print(f"invalid_stream_paths: {invalid_stream_paths}")
        print(f"missing_stream_paths: {missing_stream_paths}")
        
        self._remove_invalid_stream_fields(invalid_stream_paths)
        self._manage_stream_table(specified_stream_paths, specified_stream_length)

