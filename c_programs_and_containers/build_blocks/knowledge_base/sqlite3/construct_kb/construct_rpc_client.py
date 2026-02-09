import sqlite3
import json
import uuid


class Construct_RPC_Client_Table:
    """
    This class is designed to construct a rpc_client table with header
    and info nodes, using a stack-based approach to manage the path. It also
    manages a connection to a SQLite database and sets up the schema.
    """
    def __init__(self, conn, cursor, construct_kb, database,upload_flag=False):
        self.conn = conn
        self.cursor = cursor
        self.construct_kb = construct_kb
        self.database = database
        self.table_name = self.database + "_rpc_client"
        # Execute the SQL script to set up the schema
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
        
        # Create the rpc_client table
        create_table_script = f"""
            CREATE TABLE {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Reference to the request
                request_id TEXT NOT NULL,
                
                -- Path to identify the RPC client queue for routing responses
                client_path TEXT NOT NULL,
                server_path TEXT NOT NULL,
                
                -- Response information
                transaction_tag TEXT NOT NULL DEFAULT 'none',
                rpc_action TEXT NOT NULL DEFAULT 'none',

                response_payload TEXT NOT NULL,
                response_timestamp TEXT NOT NULL DEFAULT (datetime('now')),
                
                -- Boolean to identify new/unprocessed results
                is_new_result INTEGER NOT NULL DEFAULT 0
            )
        """
        self.cursor.execute(create_table_script)
        self.conn.commit()  # Commit the changes
        print("rpc_client table created.")

    def add_rpc_client_field(self, rpc_client_key, queue_depth, description):
        """
        Add a new rpc_client field to the knowledge base
        
        Args:
            rpc_client_key (str): The key/name of the rpc_client field
            queue_depth (int): The depth of the queue
            description (str): The description of the rpc_client field
            
        Raises:
            TypeError: If rpc_client_key is not a string or queue_depth is not an integer
        """
        if not isinstance(rpc_client_key, str):
            raise TypeError("rpc_client_key must be a string")
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        if not isinstance(queue_depth, int):
            raise TypeError("queue_depth must be an integer")
        
        properties = {"queue_depth": queue_depth}
        
        # Add the node to the knowledge base
        self.construct_kb.add_info_node("KB_RPC_CLIENT_FIELD", rpc_client_key, properties, {}, description)
        
        print(f"Added rpc_client field '{rpc_client_key}' with properties: {properties}")
        
        return {
            "rpc_client": "success",
            "message": f"rpc_client field '{rpc_client_key}' added successfully",
            "properties": properties,
            "data": description
        }
        
    def remove_unspecified_entries(self, specified_client_paths):
        """
        Remove entries from rpc_client_table where the client_path is not in the specified list,
        handling large lists of paths efficiently.
        
        Args:
            specified_client_paths (list): List of valid client_paths to keep
            
        Returns:
            int: Number of deleted records
        """
        try:
            if not specified_client_paths:
                print("Warning: No client_paths specified. No entries will be removed.")
                return 0
            
            # Filter out None values and convert all paths to strings
            valid_paths = []
            for path in specified_client_paths:
                if path is not None:
                    valid_paths.append(str(path))
            
            if not valid_paths:
                print("Warning: No valid client_paths found after filtering. No entries will be removed.")
                return 0
            
            print(f"Processing {len(valid_paths)} valid client paths")
            
            # Create a temporary table to store valid paths for efficient processing
            self.cursor.execute("""
                CREATE TEMP TABLE IF NOT EXISTS valid_client_paths (path TEXT)
            """)
            
            # Clear the table in case it already exists
            self.cursor.execute("DELETE FROM valid_client_paths")
            
            # Insert paths in batches to avoid parameter limits
            batch_size = 1000
            for i in range(0, len(valid_paths), batch_size):
                batch = valid_paths[i:i+batch_size]
                args = [(path,) for path in batch]
                try:
                    self.cursor.executemany("""
                        INSERT INTO valid_client_paths VALUES (?)
                    """, args)
                except Exception as batch_error:
                    print(f"Error inserting batch {i//batch_size + 1}: {batch_error}")
                    print(f"Problematic batch: {batch}")
                    raise
            
            # Delete entries where client_path is not in our temp table
            delete_query = f"""
                DELETE FROM {self.table_name}
                WHERE client_path NOT IN (
                    SELECT path FROM valid_client_paths
                )
            """
            
            self.cursor.execute(delete_query)
            deleted_count = self.cursor.rowcount
            
            # Commit the transaction
            self.conn.commit()
            
            # Clean up the temporary table
            try:
                self.cursor.execute("DROP TABLE IF EXISTS valid_client_paths")
            except Exception as cleanup_error:
                print(f"Warning: Could not clean up temporary table: {cleanup_error}")
            
            print(f"Removed {deleted_count} unspecified entries from {self.table_name}")
            return deleted_count
            
        except Exception as e:
            print(f"Error in remove_unspecified_entries: {e}")
            # Roll back in case of error
            try:
                if hasattr(self, 'conn') and self.conn:
                    self.conn.rollback()
            except Exception as rollback_error:
                print(f"Warning: Could not rollback transaction: {rollback_error}")
            raise Exception(f"Error in remove_unspecified_entries: {e}")
        
        
    def adjust_queue_length(self, specified_client_paths, specified_queue_lengths):
        """
        Adjust the number of records for multiple client paths to match their specified queue lengths.
        
        Args:
            specified_client_paths (list): List of client paths to adjust
            specified_queue_lengths (list): List of desired queue lengths corresponding to each client path
            
        Returns:
            dict: A dictionary with client paths as keys and operation results as values
        """
        if len(specified_client_paths) != len(specified_queue_lengths):
            raise ValueError("The specified_client_paths and specified_queue_lengths lists must be of equal length")
        
        results = {}
        
        for i, client_path in enumerate(specified_client_paths):
            queue_length = specified_queue_lengths[i]
            
            if queue_length < 0:
                results[client_path] = {"error": "Invalid queue length (negative)"}
                continue
            
            # Count current records
            count_query = f"""
                SELECT COUNT(*) 
                FROM {self.table_name}
                WHERE client_path = ?
            """
            
            self.cursor.execute(count_query, (client_path,))
            current_count = self.cursor.fetchone()[0]
            
            path_result = {'added': 0, 'removed': 0}
            
            # Remove excess records
            if current_count > queue_length:
                records_to_remove = current_count - queue_length
                delete_query = f"""
                    DELETE FROM {self.table_name}
                    WHERE id IN (
                        SELECT id
                        FROM {self.table_name}
                        WHERE client_path = ?
                        ORDER BY response_timestamp ASC
                        LIMIT ?
                    )
                """

                self.cursor.execute(delete_query, (client_path, records_to_remove))
                path_result['removed'] = self.cursor.rowcount
            
            # Add missing records
            elif current_count < queue_length:
                records_to_add = queue_length - current_count
                insert_query = f"""
                    INSERT INTO {self.table_name} (
                        request_id, client_path, server_path,
                        transaction_tag, rpc_action,
                        response_payload, response_timestamp, is_new_result
                    )
                    VALUES (?, ?, ?, ?, ?, ?, datetime('now'), 0)
                """

                for _ in range(records_to_add):
                    self.cursor.execute(insert_query, (
                        str(uuid.uuid4()),
                        client_path,
                        client_path,              # default server_path = client_path
                        'none',                   # default transaction_tag
                        'none',                   # default rpc_action
                        json.dumps({})            # empty JSON payload
                    ))
                    path_result['added'] += 1
            
            results[client_path] = path_result

        return results
                
    def restore_default_values(self):
        """
        Restore default values for all fields in rpc_client_table except for client_path.
        
        This method will:
        1. Generate a unique UUID for request_id for each record
        2. Set server_path to match client_path
        3. Set transaction_tag to 'none'
        4. Set rpc_action to 'none'
        5. Set response_payload to an empty JSON object
        6. Set response_timestamp to current time
        7. Set is_new_result to FALSE (0)
        
        Returns:
            int: Number of records updated
        """
        # First, get all the IDs to update (to count them)
        select_query = f"SELECT id, client_path FROM {self.table_name}"
        self.cursor.execute(select_query)
        records = self.cursor.fetchall()
        
        # Update each record with a unique UUID
        update_query = f"""
            UPDATE {self.table_name}
            SET 
                request_id = ?,
                server_path = client_path,
                transaction_tag = 'none',
                rpc_action = 'none',
                response_payload = ?,
                response_timestamp = datetime('now'),
                is_new_result = 0
            WHERE id = ?
        """
        
        updated_count = 0
        for record in records:
            record_id = record[0]
            self.cursor.execute(update_query, (
                str(uuid.uuid4()),  # Unique UUID for each record
                json.dumps({}),     # Empty JSON object
                record_id
            ))
            updated_count += 1
        
        return updated_count
            
    def check_installation(self):     
        
        try:
            query = f"""
                SELECT path, properties FROM {self.database} 
                WHERE label = 'KB_RPC_CLIENT_FIELD'
            """
            
            self.cursor.execute(query)
            specified_paths_data = self.cursor.fetchall()
            
            paths = []
            lengths = []
            print("specified_paths_data", specified_paths_data)
            
            for row in specified_paths_data:
                paths.append(row[0])
                properties_json = row[1]
                if properties_json:
                    properties = json.loads(properties_json)
                    lengths.append(properties.get('queue_depth', 0))
                else:
                    lengths.append(0)
            
        except Exception as e:
            raise Exception(f"Error retrieving knowledge base fields: {str(e)}")
        
        self.remove_unspecified_entries(paths)
        self.adjust_queue_length(paths, lengths)
        self.restore_default_values()