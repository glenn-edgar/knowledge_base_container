import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt  


class Construct_Job_Table:
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
        self.table_name = self.database + "_job"
 # Execute the SQL script to set up the schema
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
        query = sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE").format(
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(query)
        # Create the job table with dynamic name
        create_table_script = sql.SQL("""
            CREATE TABLE  {table_name}(
                id SERIAL PRIMARY KEY,
                path LTREE,
                schedule_at TIMESTAMPTZ DEFAULT NOW(),
                started_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ DEFAULT NOW(),
                is_active BOOLEAN DEFAULT FALSE,
                valid BOOLEAN DEFAULT FALSE,
                data JSONB
            );
        """).format(table_name=sql.Identifier(self.table_name))
        
        self.cursor.execute(create_table_script)
        
        # Create indexes optimized for read/write operations
        # Primary key index on 'id' is automatically created
        
        # GIST index for ltree path operations
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
        
        # Index on schedule_at for job scheduling queries
        create_schedule_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (schedule_at);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_schedule_at"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_schedule_index)
        
        # Index on is_active for filtering active jobs
        create_active_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (is_active);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_is_active"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_active_index)
        
        # Index on valid for filtering valid jobs
        create_valid_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (valid);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_valid"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_valid_index)
        
        # Composite index on is_active and schedule_at for common job queries
        create_composite_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (is_active, schedule_at);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_active_schedule"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_composite_index)
        
        # Index on started_at for tracking job execution times
        create_started_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (started_at);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_started_at"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_started_index)
        
        # Index on completed_at for tracking job completion times
        create_completed_index = sql.SQL("""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (completed_at);
        """).format(
            index_name=sql.Identifier(f"idx_{self.table_name}_completed_at"),
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(create_completed_index)
        
        self.conn.commit()  # Commit all changes
        print(f"Job table '{self.table_name}' created with optimized indexes.")

    def add_job_field(self, job_key, job_length,description):
        """
        Add a new stream field to the knowledge base
        
        Args:
            job_key (str): The key/name of the stream field
            job_length (int): The length of the job
            description (str): The description of the job queue
            
        Raises:
            TypeError: If stream_key is not a string or properties is not a dictionary
        """
        if not isinstance(job_key, str):
            raise TypeError("job_key must be a string")
        
        if not isinstance(job_length, int):
            raise TypeError("job_length must be an integer")
        properties = {'job_length': job_length}
    
    
        data = {}
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_JOB_QUEUE", job_key, properties, data,description)
        
        print(f"Added job field '{job_key}' with properties: {properties} and data: {data}")
        
        return {
            "job": "success",
            "message": f"job field '{job_key}' added successfully",
            "properties": properties,
            "data":data
    }
        
    def _manage_job_table(self, specified_job_paths, specified_job_length):
        """
        Manages the number of records in job_table.job_table to match specified job lengths for each path.
        Removes older records first if necessary and adds new ones with None for JSON data.
        
        Args:
            specified_job_paths (list): Array of valid LTREE paths
            specified_job_length (list): Array of corresponding lengths for each path
        """
        print(f"specified_job_paths: {specified_job_paths}")
        print(f"specified_job_length: {specified_job_length}")
    
        # Iterate through the arrays of paths and lengths
        for i in range(len(specified_job_paths)):
            path = specified_job_paths[i]
            target_length = specified_job_length[i]
            
            # Get current count for this path
            count_query = sql.SQL("SELECT COUNT(*) FROM {table_name} WHERE path = %s;").format(
                table_name=sql.Identifier(self.table_name)
            )
            self.cursor.execute(count_query, (path,))
            current_count = self.cursor.fetchone()[0]
            print(f"current_count: {current_count}")
    
            # Calculate the difference
            diff = target_length - current_count
            
            if diff < 0:
                # Need to remove records (oldest first) for this path
                # Fixed: Use completed_at consistently and correct subquery structure
                delete_query = sql.SQL("""
                    DELETE FROM {table_name}
                    WHERE path = %s AND completed_at IN (
                        SELECT completed_at 
                        FROM {table_name} 
                        WHERE path = %s
                        ORDER BY completed_at ASC 
                        LIMIT %s
                    );
                """).format(table_name=sql.Identifier(self.table_name))
                
                self.cursor.execute(delete_query, (path, path, abs(diff)))
                
            elif diff > 0:
                # Need to add records for this path
                insert_query = sql.SQL("""
                    INSERT INTO {table_name} (path, data)
                    VALUES (%s, %s);
                """).format(table_name=sql.Identifier(self.table_name))
                
                for _ in range(diff):
                    self.cursor.execute(insert_query, (path, None))
        
        # Commit all changes
        self.conn.commit()
        print("Job table management completed.")
        
        
    def _remove_invalid_job_fields(self, invalid_job_paths, chunk_size=500):
        """
        Removes all database entries with paths that match any in the invalid_job_paths array.
        Processes the deletion in chunks to avoid SQL statement limitations.
        
        Args:
            invalid_job_paths (list): Array of LTREE paths that should be removed from the database
            chunk_size (int): Maximum number of paths to process in a single query
        """
        if not invalid_job_paths:
            return  # Nothing to do if array is empty
        
        # Process in chunks to avoid SQL limitations
        for i in range(0, len(invalid_job_paths), chunk_size):
            # Get current chunk
            chunk = invalid_job_paths[i:i + chunk_size]
            
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
            
    def check_installation(self):     
        """
        Synchronize the knowledge_base and job_table based on paths.
        - Remove entries from job_table that don't exist in knowledge_base with label "KB_JOB_QUEUE"
        - Add entries to stream_table for paths in knowledge_base that don't exist in stream_table
        """
        
        query = sql.SQL("""
                SELECT DISTINCT path::text FROM {table_name}
                
            """).format(table_name=sql.Identifier(self.table_name))

        self.cursor.execute(query)
        results = self.cursor.fetchall()

        unique_job_paths = [row[0] for row in results]
        print(f"unique_job_paths: {unique_job_paths}")
       
      
    
        self.cursor.execute(sql.SQL("""
            SELECT path, label, name,properties FROM {table_name} 
            WHERE label = 'KB_JOB_QUEUE';
            """).format(table_name=sql.Identifier(self.database)))
        specified_job_data = self.cursor.fetchall()
        specified_job_paths = [row[0] for row in specified_job_data]
        specified_job_length = [row[3]['job_length'] for row in specified_job_data]
        
        print(f"specified_job_paths: {specified_job_paths}")
        print(f"specified_job_length: {specified_job_length}")
        
        
        invalid_job_paths = [path for path in unique_job_paths if path not in specified_job_paths]
        #missing_job_paths = [path for path in specified_job_paths if path not in unique_job_paths]
 
        self._remove_invalid_job_fields(invalid_job_paths)
        self._manage_job_table( specified_job_paths,specified_job_length)
        
     
 