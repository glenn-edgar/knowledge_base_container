import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt

class Construct_Stream_Table:
    """
    This class is designed to construct a stream table with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a PostgreSQL database and sets up the schema.
    """
    def __init__(self, conn, cursor,construct_kb,database):
        self.conn = conn
        self.cursor = cursor
        self.construct_kb = construct_kb
        self.database = database
        self.table_name = self.database + "_stream"
        self._setup_schema()

    def _setup_schema(self):
        """
        Sets up the database schema (tables, functions, etc.).

        # Use psycopg2.sql module to construct SQL queries safely. This prevents SQL injection.
        # ltree extension needs to be created.
        """
        create_extensions_script = sql.SQL("""
            CREATE EXTENSION IF NOT EXISTS ltree;
        """)
        query = sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE").format(
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(query)
        self.cursor.execute(create_extensions_script)
        query = sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE").format(
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(query)
        
        # Create the stream table
        create_table_script = sql.SQL("""
            CREATE TABLE  {table_name}(
                id SERIAL PRIMARY KEY,
                path LTREE,
                recorded_at TIMESTAMPTZ DEFAULT NOW(),
                valid BOOLEAN DEFAULT FALSE,
                data JSONB
            );
        """).format(table_name=sql.Identifier(self.table_name))
        self.cursor.execute(create_table_script)
        
        # Create indexes optimized for read/write operations
        # Primary key index on 'id' is automatically created
        
        # GIST index for ltree path operations (hierarchical queries)
        create_path_gist_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} USING GIST (path);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_path_gist"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_path_gist_index)
        
        # B-tree index on path for exact lookups and sorting
        create_path_btree_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (path);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_path_btree"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_path_btree_index)
        
        # Index on recorded_at for time-based queries and ordering
        create_recorded_at_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (recorded_at);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_recorded_at"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_recorded_at_index)
        
        # Descending index on recorded_at for recent-first queries
        create_recorded_at_desc_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (recorded_at DESC);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_recorded_at_desc"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_recorded_at_desc_index)
        
        # Composite index on path and recorded_at for stream queries by path and time
        create_path_time_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (path, recorded_at);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_path_recorded_at"),
            table_name=sql.Identifier(self.table_name)
        )
    
    def add_stream_field(self, stream_key, stream_length, description):
        """
        Add a new stream field to the knowledge base
        
        Args:
            stream_key (str): The key/name of the stream field
            stream_length (int): The length of the stream
          
            
        Raises:
            TypeError: If stream_key is not a string or properties is not a dictionary
        """
        if not isinstance(stream_key, str):
            raise TypeError("stream_key must be a string")
        
        if not isinstance(stream_length, int):
            raise TypeError("stream_length must be an integer")
        properties = {"stream_length": stream_length}
       
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_STREAM_FIELD", stream_key, properties, {},description)
        
        
        
        return {
            "stream": "success",
            "message": "stream field '{stream_key}' added successfully",
            "properties": properties,
            "data": description
        }
        
        
    def _remove_invalid_stream_fields(self, invalid_stream_paths, chunk_size=500):
        """
        Removes all database entries with paths that match any in the invalid_stream_paths array.
        Processes the deletion in chunks to avoid SQL statement limitations.
        
        Args:
            invalid_stream_paths (list): Array of LTREE paths that should be removed from the database
            chunk_size (int): Maximum number of paths to process in a single query
        """
        if not invalid_stream_paths:
            return  # Nothing to do if array is empty
        
        # Process in chunks to avoid SQL limitations
        for i in range(0, len(invalid_stream_paths), chunk_size):
            # Get current chunk
            chunk = invalid_stream_paths[i:i + chunk_size]
            
            # Construct placeholders for SQL IN clause
            placeholders = sql.SQL(',').join([sql.Placeholder()] * len(chunk))
            
            # Delete entries with paths in current chunk
            delete_query = sql.SQL("""
                DELETE FROM {table_name}
                WHERE path IN ({placeholders});
            """).format(
                table_name=sql.Identifier(self.table_name), 
                placeholders=placeholders
            )
            
            self.cursor.execute(delete_query, chunk)
        
        # Commit after all chunks are processed
        self.conn.commit()
        
        
    def _manage_stream_table(self, specified_stream_paths, specified_stream_length):
        """
        Manages the number of records in stream_table.job_table to match specified stream lengths for each path.
        Removes older records first if necessary and adds new ones with None for JSON data.
        
        Args:
            specified_stream_paths (list): Array of valid LTREE paths
            specified_stream_length (list): Array of corresponding lengths for each path
        """
        # Iterate through the arrays of paths and lengths
        for i in range(len(specified_stream_paths)):
            path = specified_stream_paths[i]
            target_length = specified_stream_length[i]
            
            stream_field_prompt = sql.SQL("""
                SELECT COUNT(*) FROM {table_name} WHERE path = {path};
            """).format(table_name=sql.Identifier(self.table_name), path=sql.Literal(path))

            self.cursor.execute(stream_field_prompt, (path,))
  
            current_count = self.cursor.fetchone()[0]
            
            # Calculate the difference
            diff = target_length - current_count
           
            if diff < 0:
                # Need to remove records (oldest first) for this path
                query = sql.SQL("""
                    DELETE FROM {table}
                    WHERE path = %s AND recorded_at IN (
                        SELECT recorded_at 
                        FROM {table}
                        WHERE path = %s
                        ORDER BY recorded_at ASC 
                        LIMIT %s
                    );
                """).format(
                    table=sql.Identifier(self.table_name)
                )

                # Execute the query with parameter bindings
                self.cursor.execute(query, (path, path, abs(diff)))
                
            elif diff > 0:
                # Need to add records for this path
                for _ in range(diff):
                    query = sql.SQL("""
                        INSERT INTO {table_name} (path, recorded_at, data, valid)
                        VALUES (%s, CURRENT_TIMESTAMP, %s, FALSE);
                    """).format(table_name=sql.Identifier(self.table_name))

                    self.cursor.execute(query, (path, '{}'))  # '{}' is passed as a string literal
                        
        # Commit all changes at once
        self.conn.commit()
        
    def check_installation(self):     
        """
        Synchronize the knowledge_base and stream_table based on paths.
        - Remove entries from stream_table that don't exist in knowledge_base with label "KB_STREAM_FIELD"
        - Add entries to stream_table for paths in knowledge_base that don't exist in stream_table
        """
        
        # Get all paths from stream_table
        stream_paths_query = sql.SQL("""
            SELECT DISTINCT path::text FROM {table_name}; 
        """).format(table_name=sql.Identifier(self.table_name))
        
        self.cursor.execute(stream_paths_query)
        unique_stream_paths = [row[0] for row in self.cursor.fetchall()]
        
        
        # Get specified paths (paths with label "KB_STREAM_FIELD") from knowledge_table
        knowledge_query = sql.SQL("""
            SELECT path, label, name, properties FROM {table_name} 
            WHERE label = 'KB_STREAM_FIELD';
        """).format(table_name=sql.Identifier(self.database))
        
        self.cursor.execute(knowledge_query)
        specified_stream_data = self.cursor.fetchall()
        
    
        
        specified_stream_paths = [row[0] for row in specified_stream_data]
        specified_stream_length = [row[3]['stream_length'] for row in specified_stream_data]
        print(f"specified_stream_paths: {specified_stream_paths}")
        print(f"specified_stream_length: {specified_stream_length}")
        
        invalid_stream_paths = [path for path in unique_stream_paths if path not in specified_stream_paths]
        missing_stream_paths = [path for path in specified_stream_paths if path not in unique_stream_paths]
        print(f"invalid_stream_paths: {invalid_stream_paths}")
        print(f"missing_stream_paths: {missing_stream_paths}")
        
        
        self._remove_invalid_stream_fields(invalid_stream_paths)
        self._manage_stream_table(specified_stream_paths, specified_stream_length)
        