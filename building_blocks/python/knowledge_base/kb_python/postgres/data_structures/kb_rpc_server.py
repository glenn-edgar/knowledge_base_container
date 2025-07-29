import uuid
from datetime import datetime, timezone
import json
import time
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter, AsIs

class NoMatchingRecordError(Exception):
    pass

class KB_RPC_Server:
    """
    A class to handle the RPC server for the knowledge base.
    """
    def __init__(self, kb_search, database):
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        self.cursor = self.kb_search.cursor 
        self.base_table = f"{database}_rpc_server"
        
    
    def find_rpc_server_id(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find the node id for a given node name, properties, node path, and data.
        """
    
        result = self.find_rpc_server_ids(kb, node_name, properties, node_path)
        if len(result) == 0:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        if len(result) > 1:
            raise ValueError(f"Multiple nodes found matching path parameters: {node_name}, {properties}, {node_path}")
        return result
    
    def find_rpc_server_ids(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find the node id for a given node name, properties, node path :
        """
       
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_RPC_SERVER_FIELD")
        if kb is not None:
            self.kb_search.search_kb(kb)
        if node_name is not None:
            self.kb_search.search_name(node_name)
        if properties is not None:
            for key in properties:
                self.kb_search.search_property_value(key, properties[key])
        if node_path is not None:
            self.kb_search.search_path(node_path)
        node_ids = self.kb_search.execute_query()
        
        if node_ids is None:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        if len(node_ids) == 0:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        return node_ids
    
    def find_rpc_server_table_keys(self, key_data):
       
            
        return_values = []
        for key in key_data:
            
            return_values.append(key['path'])
        return return_values    
    
 

    def list_jobs_job_types(self, server_path: str, state: str) -> list[dict]:
        """
        List records in the table where server_path matches and state matches the given value.

        Args:
            server_path (str): The server path in ltree format (e.g., 'root.node1.node2').
            state (str): One of 'empty', 'new_job', or 'processing'.

        Returns:
            list[dict]: List of matching job records as dictionaries.

        Raises:
            ValueError: If inputs are invalid.
            psycopg2.Error: On database query issues.
        """
        # Validate server_path
        if not isinstance(server_path, str) or not server_path or not self._is_valid_ltree(server_path):
            raise ValueError("server_path must be a non-empty valid ltree string (e.g., 'root.node1')")

        # Validate state
        allowed_states = {'empty', 'new_job', 'processing'}
        if state not in allowed_states:
            raise ValueError(f"state must be one of {allowed_states}")

        try:
            if self.conn.closed:
                raise psycopg2.Error("Database connection is closed")

            table_ident = sql.Identifier(*self.base_table.split('.'))

            query = sql.SQL("""
                SELECT *
                FROM {table}
                WHERE server_path = %s::ltree
                  AND state = %s
                ORDER BY priority DESC, request_timestamp ASC
            """).format(table=table_ident)

            self.cursor.execute("BEGIN;")
            self.cursor.execute(query, (server_path, state))
            rows = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            results = [dict(zip(columns, row)) for row in rows]

            self.conn.commit()
            return results

        except psycopg2.Error as e:
            self.conn.rollback()
            raise psycopg2.Error(f"Database error in list_jobs_job_types: {str(e)}")

            
    def count_all_jobs(self, server_path):
        """
        Count all records in the table where server_path matches.
        """
        return_value = {}
        return_value["empty_jobs"] = self.count_empty_jobs(server_path)
        return_value["new_jobs"] = self.count_new_jobs(server_path)
        return_value["processing_jobs"] = self.count_processing_jobs(server_path)
        return return_value
      
    
 
    def count_processing_jobs(self, server_path):
        """
        Count records in the table where server_path matches and state is 'processing'.
        """
        return self.count_jobs_job_types(server_path, 'processing')

    def count_new_jobs(self, server_path):
        """
        Count records in the table where server_path matches and state is 'new_job'.
        """
        return self.count_jobs_job_types(server_path, 'new_job')
    
    
    def count_empty_jobs(self, server_path):
        """
        Count records in the table where server_path matches and state is 'empty'.
        """
        return self.count_jobs_job_types(server_path, 'empty')

    def count_jobs_job_types(self, server_path: str, state: str) -> int:
        """
        Count records in the table where server_path matches specified state.

        Args:
            server_path (str): The server path to match against (ltree format, e.g., 'root.node1.node2').
            state (str): The state to match against ('empty', 'new_job', 'processing', 'completed_job').

        Returns:
            int: The number of records that match the criteria.

        Raises:
            ValueError: If server_path is invalid or not in ltree format.
            psycopg2.Error: For database errors.
        """
        if not server_path or not isinstance(server_path, str) or not self._is_valid_ltree(server_path):
            raise ValueError("server_path must be a valid ltree format (e.g., 'root.node1.node2')")

        valid_states = {'empty', 'new_job', 'processing', 'completed_job'}
        if state not in valid_states:
            raise ValueError(f"state must be one of: {', '.join(valid_states)}")

        try:
            if self.conn.closed:
                raise psycopg2.Error("Database connection is closed")

           

            query = sql.SQL("""
                SELECT COUNT(*) AS job_count
                FROM {table}
                WHERE server_path = %s::ltree
                  AND state = %s
            """).format(table=sql.Identifier(self.base_table))

            self.cursor.execute("BEGIN;")
            self.cursor.execute(query, (server_path, state))
            count_dic = dict(self.cursor.fetchone())
            count = count_dic['job_count']
            self.conn.commit()
            return count

        except psycopg2.Error as e:
            self.conn.rollback()
            raise psycopg2.Error(f"Database error in count_jobs_job_types: {str(e)}")




    def push_rpc_queue(self, server_path, request_id, rpc_action, request_payload, transaction_tag,
                    priority=0, rpc_client_queue=None, max_retries=5, wait_time=0.5):
        """
        Push a request to the RPC queue.

        Args:
            server_path (str): The server path in ltree format (e.g. 'root.node1.node2')
            request_id (str): UUID for the request
            rpc_action (str): RPC action name
            request_payload (dict): JSON-serializable payload for the request
            transaction_tag (str): Tag to prevent duplicate transactions
            priority (int): Priority of the request (higher number = higher priority)
            rpc_client_queue (str): Client queue in ltree format (e.g. 'client.queue1')
            max_retries (int, optional): Maximum number of retries for transaction conflicts
            wait_time (float, optional): Initial wait time between retries in seconds

        Returns:
            dict: The updated record

        Raises:
            ValueError: If any parameters fail validation
            NoMatchingRecordError: If no matching record is found to update
            psycopg2.Error: For database errors
            RuntimeError: If max retries exceeded
        """
        # Validate server_path (ltree format)
        if not server_path or not isinstance(server_path, str) or not self._is_valid_ltree(server_path):
            raise ValueError("server_path must be a valid ltree format (e.g. 'root.node1.node2')")

        # Validate request_id (UUID)
        try:
            if not request_id:
                request_id = str(uuid.uuid4())
            else:
                request_id = str(uuid.UUID(request_id))
        except (ValueError, AttributeError, TypeError):
            raise ValueError("request_id must be a valid UUID string or None")

        # Validate rpc_action
        if not rpc_action or not isinstance(rpc_action, str):
            raise ValueError("rpc_action must be a non-empty string")

        # Validate request_payload (JSON-serializable)
        if request_payload is None:
            raise ValueError("request_payload cannot be None")
        try:
            json.dumps(request_payload)
        except (TypeError, OverflowError):
            raise ValueError("request_payload must be JSON-serializable")

        # Validate transaction_tag
        if not transaction_tag or not isinstance(transaction_tag, str):
            raise ValueError("transaction_tag must be a non-empty string")

        # Validate rpc_client_queue (ltree format)
        if rpc_client_queue is not None and (not isinstance(rpc_client_queue, str) or
                                            not self._is_valid_ltree(rpc_client_queue)):
            raise ValueError("rpc_client_queue must be None or a valid ltree format")

        # Validate priority
        if not isinstance(priority, int):
            raise ValueError("priority must be an integer")

        # Create table identifier
        table_parts = self.base_table.split('.')
        if len(table_parts) == 2:
            # Schema-qualified table name
            table_ref = sql.SQL("{}.{}").format(
                sql.Identifier(table_parts[0]),  # schema
                sql.Identifier(table_parts[1])   # table
            )
        else:
            # Just table name
            table_ref = sql.Identifier(table_parts[0])

        # Process with retry logic for transaction conflicts
        attempt = 0
        current_wait = wait_time
        max_wait = 8  # Cap maximum wait time at 8 seconds

        while attempt < max_retries:
            try:
                # Ensure connection is valid
                if self.conn.closed:
                    raise Exception("Database connection is closed")

                # Explicitly begin transaction
                self.cursor.execute("BEGIN")
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                # Acquire advisory lock (schema-specific)
                lock_key = hash(f"{self.base_table}:{server_path}")
                self.cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))

                # Find the earliest completed/failed record with is_new_result=True
                query = sql.SQL("""
                    SELECT id FROM {table}
                    WHERE state = 'empty'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1
                    FOR UPDATE
                """).format(table=table_ref)
                
                self.cursor.execute(query)
                record = self.cursor.fetchone()
              
                if not record:
                    self.conn.rollback()
                    raise NoMatchingRecordError("No matching record found with state = 'empty'")

                record_id = record['id']

                # Update the record
                update_query = sql.SQL("""
                    UPDATE {table}
                    SET server_path = %s,
                        request_id = %s,
                        rpc_action = %s,
                        request_payload = %s,
                        transaction_tag = %s,
                        priority = %s,
                        rpc_client_queue = %s,
                        state = 'new_job',
                        request_timestamp = NOW() AT TIME ZONE 'UTC',
                        completed_timestamp = NULL
                    WHERE id = %s
                    RETURNING *
                """).format(table=table_ref)
                
                self.cursor.execute(update_query, (server_path, request_id, rpc_action, json.dumps(request_payload),
                                                transaction_tag, priority, rpc_client_queue, record_id))
                result = self.cursor.fetchone()

                if not result:
                    self.conn.rollback()
                    raise Exception("Failed to update record in RPC queue")

                # Commit the transaction
                self.conn.commit()

                # Convert result to dictionary
                columns = [desc[0] for desc in self.cursor.description]
                result_dict = dict(zip(columns, result))
                return result_dict

            except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as e:
                self.conn.rollback()
                attempt += 1
                if attempt < max_retries:
                    sleep_time = min(current_wait * (2 ** attempt), max_wait)  # Exponential backoff with cap
                    time.sleep(sleep_time)
                else:
                    raise RuntimeError(f"Failed to push to RPC queue after {max_retries} retries: {str(e)}")
            except psycopg2.Error as e:
                self.conn.rollback()
                raise psycopg2.Error(f"Database error in push_rpc_queue: {str(e)}")
            except NoMatchingRecordError:
                self.conn.rollback()
                raise
            finally:
                # No need to reset transaction isolation level (it resets automatically after COMMIT/ROLLBACK)
                pass
    
    def _is_valid_ltree(self, path):
        """
        Validate if a string is a valid ltree path.
        
        Args:
            path (str): The path to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not path or not isinstance(path, str):
            return False
        
        # Basic ltree validation - each label must start with a letter or underscore
        # and contain only letters, numbers, and underscores
        parts = path.split('.')
        if not parts:
            return False
        
        for part in parts:
            if not part:
                return False
            if not (part[0].isalpha() or part[0] == '_'):
                return False
            if not all(c.isalnum() or c == '_' for c in part):
                return False
        
        return True
    
    def peak_server_queue(self, server_path, retries=5, wait_time=1):
        """
        Finds and processes one pending record from the server queue.

        Args:
            server_path: The server path to search for records
            retries: Number of retry attempts if transaction conflicts occur
            wait_time: Initial wait time in seconds between retries (uses exponential backoff)

        Returns:
            dict: Record as dictionary with column names as keys, or None if no record is found.

        Raises:
            Exception: If operation fails after retries or due to other errors.
        """
        attempt = 0
        

        while attempt < retries:
            try:
                if self.conn.closed:
                    raise Exception("Database connection is closed")

                self.cursor.execute("BEGIN")
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                # Select one pending job with SELECT *
                select_query = sql.SQL("""
                    SELECT *
                    FROM {table}
                    WHERE server_path = %s
                      AND state = 'new_job'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """).format(table=sql.Identifier(self.base_table))

                self.cursor.execute(select_query, (server_path,))
                row = self.cursor.fetchone()

                if not row:
                    self.conn.rollback()
                    return None
                
                record_dict = dict(row)
                
                
             
                # Update the record status
                update_query = sql.SQL("""
                    UPDATE {table}
                    SET state = 'processing',
                        processing_timestamp = NOW() AT TIME ZONE 'UTC'
                    WHERE id = %s
                    RETURNING id
                """).format(table=sql.Identifier(self.base_table))

                self.cursor.execute(update_query, (record_dict['id'],))
                if not self.cursor.fetchone():
                    self.conn.rollback()
                    raise Exception(f"Failed to update state to 'processing' for id: {record_dict['id']}")

                self.conn.commit()
                return record_dict

            except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as e:
                self.conn.rollback()
                attempt += 1
                if attempt < retries:
                    time.sleep(wait_time * (2 ** attempt))  # Exponential backoff
                else:
                    raise Exception(f"Failed to peak server queue after {retries} attempts: {str(e)}")
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error in peak_server_queue: {str(e)}")

        return None

    
    def mark_job_completion(self, server_path, id, retries=5, wait_time=1):
        """
        Marks a job as completed in the server queue.
        """
        attempt = 0
    

        while attempt < retries:
            try:
                if self.conn.closed:
                    raise Exception("Database connection is closed")

                self.cursor.execute("BEGIN")
                self.cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                verify_query = sql.SQL("""
                    SELECT id FROM {table}
                    WHERE id = %s
                      AND server_path = %s
                      AND state = 'processing'
                    FOR UPDATE
                """).format(table=sql.Identifier(self.base_table))

                self.cursor.execute(verify_query, (id, server_path))
                record = self.cursor.fetchone()

                if not record:
                    self.conn.rollback()
                    return False

                update_query = sql.SQL("""
                    UPDATE {table}
                    SET state = 'empty',
                        completed_timestamp = NOW() AT TIME ZONE 'UTC'
                    WHERE id = %s
                    RETURNING id
                """).format(table=sql.Identifier(self.base_table))

                self.cursor.execute(update_query, (id,))
                updated = self.cursor.fetchone()

                self.conn.commit()
                return True if updated else False

            except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected) as e:
                self.conn.rollback()
                attempt += 1
                if attempt < retries:
                    time.sleep(wait_time * (2 ** attempt))
                else:
                    raise Exception(f"Failed to mark job as completed after {retries} attempts: {str(e)}")
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error in mark_job_completion: {str(e)}")

        return False

    
    def clear_server_queue(self, server_path, max_retries=3, retry_delay=1):
        """
        Clear the reply queue by resetting records matching the specified server_path.
        """
        retry_count = 0
        row_count = 0
        original_autocommit = self.conn.autocommit
        
        while retry_count < max_retries:
            try:
                if self.conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                    self.conn.rollback()
                self.conn.autocommit = False

                with self.conn:
                    lock_query = sql.SQL("""
                        SELECT 1 FROM {table}
                        WHERE server_path = %s::ltree
                        FOR UPDATE NOWAIT
                    """).format(table=sql.Identifier(self.base_table))

                    self.cursor.execute(lock_query, (server_path,))

                    update_query = sql.SQL("""
                        UPDATE {table}
                        SET request_id = gen_random_uuid(),
                            request_payload = '{{}}',
                            completed_timestamp = CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                            state = 'empty',
                            rpc_client_queue = NULL
                        WHERE server_path = %s::ltree
                    """).format(table=sql.Identifier(self.base_table))

                    self.cursor.execute(update_query, (server_path,))
                    row_count = self.cursor.rowcount
                    self.conn.commit()
                    return row_count

            except psycopg2.errors.LockNotAvailable:
                self.conn.rollback()
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to acquire lock after {max_retries} attempts for server path: {server_path}")

            except psycopg2.Error as e:
                self.conn.rollback()
                raise Exception(f"Failed to clear reply queue for {server_path}: {str(e)}")

            finally:
                self.conn.autocommit = original_autocommit

        return row_count
