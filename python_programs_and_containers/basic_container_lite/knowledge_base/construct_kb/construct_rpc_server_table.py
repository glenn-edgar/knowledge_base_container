import psycopg2
import json
from psycopg2 import sql
from psycopg2.extensions import adapt
import uuid

class Construct_RPC_Server_Table:
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
        self.table_name = self.database + "_rpc_server"
 # Execute the SQL script to set up the schema
        self._setup_schema()

    def _setup_schema(self):
        """
        Sets up the database schema (tables, functions, etc.).
   
        # Use psycopg2.sql module to construct SQL queries safely. This prevents SQL injection.
        # ltree extension needs to be created.
        """
        query = sql.SQL("DROP TABLE IF EXISTS {table_name} CASCADE").format(
            table_name=sql.Identifier(self.table_name)
        )
        self.cursor.execute(query)
        # Create the knowledge_base table
        create_table_script = sql.SQL("""
        CREATE TABLE  {table_name} (
            id SERIAL PRIMARY KEY,
            server_path LTREE NOT NULL,
            
            -- Request information
            request_id UUID NOT NULL DEFAULT gen_random_uuid(),
            rpc_action TEXT NOT NULL DEFAULT 'none',
            request_payload JSONB NOT NULL,
            request_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            
            -- Tag to prevent duplicate transactions
            transaction_tag TEXT NOT NULL,
            
            -- Status tracking
            state TEXT NOT NULL DEFAULT 'empty'
                CHECK (state IN ('empty', 'new_job', 'processing')),
            
            -- Additional useful fields
            priority INTEGER NOT NULL DEFAULT 0,
            
            -- New fields as requested
            processing_timestamp TIMESTAMPTZ DEFAULT NULL,
            completed_timestamp TIMESTAMPTZ DEFAULT NULL,
            rpc_client_queue LTREE
        );
        """).format(table_name=sql.Identifier(self.table_name))


        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("rpc_server table created.")

    def add_rpc_server_field(self, rpc_server_key,queue_depth, description):
        """
        Add a new status field to the knowledge base
        
        Args:
            rpc_server_key (str): The key/name of the status field
            queue_depth (int): The length of the rpc_server
            description (str): The description of the rpc_server
            
        Raises:
            TypeError: If status_key is not a string or properties is not a dictionary
        """
        if not isinstance(rpc_server_key, str):
            raise TypeError("rpc_server_key must be a string")
        
        if not isinstance(queue_depth, int):
            raise TypeError("queue_depth must be an integer")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        properties = {'queue_depth': queue_depth}
 
        data = {}
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_RPC_SERVER_FIELD", rpc_server_key, properties, data,description)

        print(f"Added rpc_server field '{rpc_server_key}' with properties: {properties} and data: {data}")
        
        return {
            "status": "success",
            "message": f"RPC server field '{rpc_server_key}' added successfully",
            "properties": properties,
            "data": description
        }
            
    def remove_unspecified_entries(self, specified_server_paths):
        """
        Remove entries from rpc_server_table where the server_path is not in the specified list,
        handling large lists of paths efficiently.
        
        Args:
            specified_server_paths (list): List of valid server_paths to keep
            
        Returns:
            int: Number of deleted records
        """
        try:
            if not specified_server_paths:
                print("Warning: No server_paths specified. No entries will be removed.")
                return 0
            
            # Filter out None values and convert all paths to strings
            valid_paths = []
            for path in specified_server_paths:
                if path is not None:
                    valid_paths.append(str(path))
            
            if not valid_paths:
                print("Warning: No valid server_paths found after filtering. No entries will be removed.")
                return 0
            
            print(f"Processing {len(valid_paths)} valid server paths")
            
            # Create a temporary table to store valid paths for efficient processing
            # Don't use explicit transaction control to avoid set_session conflicts
            self.cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS valid_server_paths (path text)
            """)
            
            # Clear the table in case it already exists
            self.cursor.execute("DELETE FROM valid_server_paths")
            
            # Insert paths in batches to avoid parameter limits
            batch_size = 1000
            for i in range(0, len(valid_paths), batch_size):
                batch = valid_paths[i:i+batch_size]
                args = [(path,) for path in batch]
                try:
                    self.cursor.executemany("""
                        INSERT INTO valid_server_paths VALUES (%s)
                    """, args)
                except Exception as batch_error:
                    print(f"Error inserting batch {i//batch_size + 1}: {batch_error}")
                    print(f"Problematic batch: {batch}")
                    raise
            
            # Set state to empty for remaining records before deleting unspecified ones
            update_query = sql.SQL("""
                UPDATE {table_name}
                SET state = 'empty'
                WHERE server_path::text IN (
                    SELECT path FROM valid_server_paths
                )
            """).format(table_name=sql.Identifier(self.table_name))
            
            self.cursor.execute(update_query)
            
            # Delete entries where server_path is not in our temp table
            delete_query = sql.SQL("""
                DELETE FROM {table_name}
                WHERE server_path::text NOT IN (
                    SELECT path FROM valid_server_paths
                )
            """).format(table_name=sql.Identifier(self.table_name))
            
            self.cursor.execute(delete_query)
            deleted_count = self.cursor.rowcount
            
            # Commit the transaction
            self.conn.commit()
            
            # Clean up the temporary table
            try:
                self.cursor.execute("DROP TABLE IF EXISTS valid_server_paths")
            except Exception as cleanup_error:
                print(f"Warning: Could not clean up temporary table: {cleanup_error}")
            
            print(f"Removed {deleted_count} unspecified entries from {self.table_name}")
            return deleted_count
            
        except Exception as e:
            print(f"Error in remove_unspecified_entries: {e}")
            # Roll back in case of error - but don't force autocommit changes
            try:
                if hasattr(self, 'conn') and self.conn:
                    self.conn.rollback()
            except Exception as rollback_error:
                print(f"Warning: Could not rollback transaction: {rollback_error}")
            raise Exception(f"Error in remove_unspecified_entries: {e}")
        import uuid

    def adjust_queue_length(self, specified_server_paths, specified_queue_lengths):
        """
        Adjust the number of records for multiple server paths to match their specified queue lengths.
        
        Args:
            specified_server_paths (list): List of server paths to adjust
            specified_queue_lengths (list): List of desired queue lengths corresponding to each server path
            
        Returns:
            dict: A dictionary with server paths as keys and operation results as values
        """
        results = {}
        
        try:
            if len(specified_server_paths) != len(specified_queue_lengths):
                raise ValueError("Mismatch between paths and lengths lists")
                
            for i, server_path in enumerate(specified_server_paths):
                try:
                    target_length = int(specified_queue_lengths[i])
                    
                    # Get current count
                    count_query = sql.SQL("""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE server_path::text = %s
                    """).format(table_name=sql.Identifier(self.table_name))
                    
                    self.cursor.execute(count_query, (server_path,))
                    current_count = self.cursor.fetchone()[0]
                    
                    # Set state to empty for all records with this server_path
                    update_query = sql.SQL("""
                        UPDATE {table_name}
                        SET state = 'empty'
                        WHERE server_path::text = %s
                    """).format(table_name=sql.Identifier(self.table_name))
                    
                    self.cursor.execute(update_query, (server_path,))
                    
                    if current_count > target_length:
                        # Need to remove excess records - remove oldest first
                        delete_query = sql.SQL("""
                            DELETE FROM {table_name}
                            WHERE id IN (
                                SELECT id FROM {table_name}
                                WHERE server_path::text = %s
                                ORDER BY request_timestamp ASC
                                LIMIT %s
                            )
                        """).format(table_name=sql.Identifier(self.table_name))
                        
                        self.cursor.execute(delete_query, (server_path, current_count - target_length))
                        
                        results[server_path] = {
                            'action': 'removed',
                            'count': current_count - target_length,
                            'new_total': target_length
                        }
                        
                    elif current_count < target_length:
                        # Need to add placeholder records
                        records_to_add = target_length - current_count
                        
                        insert_query = sql.SQL("""
                            INSERT INTO {table_name} (
                                server_path, request_payload, transaction_tag, state
                            ) VALUES (
                                %s, %s, %s, 'empty'
                            )
                        """).format(table_name=sql.Identifier(self.table_name))
                        
                        for _ in range(records_to_add):
                            self.cursor.execute(insert_query, (
                                server_path,  # Use server_path as server_path for placeholders
                                '{}',         # Empty JSON object
                                f"placeholder_{uuid.uuid4()}"  # Unique transaction tag
                            ))
                        
                        results[server_path] = {
                            'action': 'added',
                            'count': records_to_add,
                            'new_total': target_length
                        }
                        
                    else:
                        results[server_path] = {
                            'action': 'unchanged',
                            'count': 0,
                            'new_total': current_count
                        }
                        
                except Exception as path_error:
                    print(f"Error adjusting queue for path {server_path}: {path_error}")
                    results[server_path] = {'error': str(path_error)}
            
            # Commit all changes
            self.conn.commit()
            return results
            
        except Exception as e:
            print(f"Error in adjust_queue_length: {e}")
            if hasattr(self, 'conn') and self.conn:
                self.conn.rollback()
            raise Exception(f"Error in adjust_queue_length: {e}")
        
        
    def restore_default_values(self):
        """
        Restore default values for all fields in rpc_server_table except for server_path.
        
        This method will:
        1. Generate a unique UUID for request_id for each record
        2. Set rpc_action to 'none'
        3. Set request_payload to an empty JSON object
        4. Set request_timestamp to current time
        5. Generate a new transaction_tag
        6. Set state to 'empty'
        7. Set priority to 0
        8. Clear processing_timestamp (set to NULL)
        9. Clear completed_timestamp (set to NULL)
        10. Clear rpc_client_queue (set to NULL)
        
        Returns:
            int: Number of records updated
        """
        # Update all records with default values while preserving server_path
        update_query = sql.SQL("""
            UPDATE {table_name}
            SET 
                request_id = gen_random_uuid(),
                rpc_action = 'none',
                request_payload = '{{}}'::jsonb,
                request_timestamp = NOW(),
                transaction_tag = CONCAT('reset_', gen_random_uuid()::text),
                state = 'empty',
                priority = 0,
                processing_timestamp = NULL,
                completed_timestamp = NULL,
                rpc_client_queue = NULL
            
            RETURNING id
        """).format(table_name=sql.Identifier(self.table_name))
        
        self.cursor.execute(update_query)
        
        # Get count of updated records using RETURNING clause
        updated_count = len(self.cursor.fetchall())
        
        print(f"Restored default values for {updated_count} records")
        return updated_count
        
    def check_installation(self):     

        
            # Get specified paths (paths with label "KB_RPC_SERVER_FIELD") from knowledge_table
            query = sql.SQL("""
                SELECT path, properties FROM {table_name} 
                WHERE label = 'KB_RPC_SERVER_FIELD';
            """).format(table_name=sql.Identifier(self.database))
            
            self.cursor.execute(query)
            specified_paths_data = self.cursor.fetchall()
            
            paths = []
            lengths = []
            for row in specified_paths_data:
                paths.append(row[0])
                properties = row[1]
                lengths.append(properties['queue_depth'])
            print(f"paths: {paths}", f"lengths: {lengths}")

            self.remove_unspecified_entries(paths)
            self.adjust_queue_length(paths, lengths)
            self.restore_default_values()
            
  