import uuid
from datetime import datetime, timezone
import json
import time
import sqlite3


class NoMatchingRecordError(Exception):
    pass


class KB_RPC_Server:
    """
    A class to handle the RPC server for the knowledge base.
    SQLite version.
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
        Find the node id for a given node name, properties, node path.
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
            sqlite3.Error: On database query issues.
        """
        # Validate server_path
        if not isinstance(server_path, str) or not server_path or not self._is_valid_ltree(server_path):
            raise ValueError("server_path must be a non-empty valid ltree string (e.g., 'root.node1')")

        # Validate state
        allowed_states = {'empty', 'new_job', 'processing'}
        if state not in allowed_states:
            raise ValueError(f"state must be one of {allowed_states}")

        try:
            query = f"""
                SELECT *
                FROM {self.base_table}
                WHERE server_path = ?
                  AND state = ?
                ORDER BY priority DESC, request_timestamp ASC
            """

            self.cursor.execute("BEGIN")
            self.cursor.execute(query, (server_path, state))
            rows = self.cursor.fetchall()
            
            # Convert sqlite3.Row objects to dictionaries
            results = [dict(row) for row in rows]

            self.conn.commit()
            return results

        except sqlite3.Error as e:
            self.conn.rollback()
            raise sqlite3.Error(f"Database error in list_jobs_job_types: {str(e)}")

            
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
            sqlite3.Error: For database errors.
        """
        if not server_path or not isinstance(server_path, str) or not self._is_valid_ltree(server_path):
            raise ValueError("server_path must be a valid ltree format (e.g., 'root.node1.node2')")

        valid_states = {'empty', 'new_job', 'processing', 'completed_job'}
        if state not in valid_states:
            raise ValueError(f"state must be one of: {', '.join(valid_states)}")

        try:
            query = f"""
                SELECT COUNT(*) AS job_count
                FROM {self.base_table}
                WHERE server_path = ?
                  AND state = ?
            """

            self.cursor.execute("BEGIN")
            self.cursor.execute(query, (server_path, state))
            result = self.cursor.fetchone()
            count = result['job_count'] if result else 0
            self.conn.commit()
            return count

        except sqlite3.Error as e:
            self.conn.rollback()
            raise sqlite3.Error(f"Database error in count_jobs_job_types: {str(e)}")


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
            sqlite3.Error: For database errors
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

        # Process with retry logic for transaction conflicts
        attempt = 0
        current_wait = wait_time
        max_wait = 8  # Cap maximum wait time at 8 seconds

        while attempt < max_retries:
            try:
                # Begin immediate transaction to get write lock
                self.cursor.execute("BEGIN IMMEDIATE")

                # Find the earliest empty record
                query = f"""
                    SELECT id FROM {self.base_table}
                    WHERE state = 'empty'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1
                """
                
                self.cursor.execute(query)
                record = self.cursor.fetchone()
              
                if not record:
                    self.conn.rollback()
                    raise NoMatchingRecordError("No matching record found with state = 'empty'")

                record_id = record['id']

                # Get current timestamp
                current_timestamp = datetime.now(timezone.utc).isoformat()

                # Update the record
                update_query = f"""
                    UPDATE {self.base_table}
                    SET server_path = ?,
                        request_id = ?,
                        rpc_action = ?,
                        request_payload = ?,
                        transaction_tag = ?,
                        priority = ?,
                        rpc_client_queue = ?,
                        state = 'new_job',
                        request_timestamp = ?,
                        completed_timestamp = NULL
                    WHERE id = ?
                    RETURNING *
                """
                
                self.cursor.execute(update_query, (
                    server_path, request_id, rpc_action, json.dumps(request_payload),
                    transaction_tag, priority, rpc_client_queue, current_timestamp, record_id
                ))
                result = self.cursor.fetchone()

                if not result:
                    self.conn.rollback()
                    raise Exception("Failed to update record in RPC queue")

                # Commit the transaction
                self.conn.commit()

                # Convert result to dictionary
                result_dict = dict(result)
                return result_dict

            except sqlite3.OperationalError as e:
                # Handle database locked errors
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    attempt += 1
                    if attempt < max_retries:
                        sleep_time = min(current_wait * (2 ** attempt), max_wait)  # Exponential backoff with cap
                        time.sleep(sleep_time)
                    else:
                        raise RuntimeError(f"Failed to push to RPC queue after {max_retries} retries: {str(e)}")
                else:
                    raise sqlite3.Error(f"Database error in push_rpc_queue: {str(e)}")
            except sqlite3.Error as e:
                self.conn.rollback()
                raise sqlite3.Error(f"Database error in push_rpc_queue: {str(e)}")
            except NoMatchingRecordError:
                self.conn.rollback()
                raise

        # Should not reach here
        raise RuntimeError(f"Failed to push to RPC queue after {max_retries} retries")
    
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
        
        Note: SQLite doesn't support FOR UPDATE SKIP LOCKED like PostgreSQL.
        This implementation uses BEGIN IMMEDIATE for database-level locking.

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
                # Begin immediate transaction to get write lock
                self.cursor.execute("BEGIN IMMEDIATE")

                # Select one pending job
                select_query = f"""
                    SELECT *
                    FROM {self.base_table}
                    WHERE server_path = ?
                      AND state = 'new_job'
                    ORDER BY priority DESC, request_timestamp ASC
                    LIMIT 1
                """

                self.cursor.execute(select_query, (server_path,))
                row = self.cursor.fetchone()

                if not row:
                    self.conn.rollback()
                    return None
                
                record_dict = dict(row)
                
                # Get current timestamp
                current_timestamp = datetime.now(timezone.utc).isoformat()
             
                # Update the record status
                update_query = f"""
                    UPDATE {self.base_table}
                    SET state = 'processing',
                        processing_timestamp = ?
                    WHERE id = ?
                    RETURNING id
                """

                self.cursor.execute(update_query, (current_timestamp, record_dict['id']))
                if not self.cursor.fetchone():
                    self.conn.rollback()
                    raise Exception(f"Failed to update state to 'processing' for id: {record_dict['id']}")

                self.conn.commit()
                return record_dict

            except sqlite3.OperationalError as e:
                # Handle database locked errors
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    attempt += 1
                    if attempt < retries:
                        time.sleep(wait_time * (2 ** attempt))  # Exponential backoff
                    else:
                        raise Exception(f"Failed to peak server queue after {retries} attempts: {str(e)}")
                else:
                    raise Exception(f"Error in peak_server_queue: {str(e)}")
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
                # Begin immediate transaction to get write lock
                self.cursor.execute("BEGIN IMMEDIATE")

                verify_query = f"""
                    SELECT id FROM {self.base_table}
                    WHERE id = ?
                      AND server_path = ?
                      AND state = 'processing'
                """

                self.cursor.execute(verify_query, (id, server_path))
                record = self.cursor.fetchone()

                if not record:
                    self.conn.rollback()
                    return False

                # Get current timestamp
                current_timestamp = datetime.now(timezone.utc).isoformat()

                update_query = f"""
                    UPDATE {self.base_table}
                    SET state = 'empty',
                        completed_timestamp = ?
                    WHERE id = ?
                    RETURNING id
                """

                self.cursor.execute(update_query, (current_timestamp, id))
                updated = self.cursor.fetchone()

                self.conn.commit()
                return True if updated else False

            except sqlite3.OperationalError as e:
                # Handle database locked errors
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    attempt += 1
                    if attempt < retries:
                        time.sleep(wait_time * (2 ** attempt))
                    else:
                        raise Exception(f"Failed to mark job as completed after {retries} attempts: {str(e)}")
                else:
                    raise Exception(f"Error in mark_job_completion: {str(e)}")
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error in mark_job_completion: {str(e)}")

        return False

    
    def clear_server_queue(self, server_path, max_retries=3, retry_delay=1):
        """
        Clear the server queue by resetting records matching the specified server_path.
        """
        retry_count = 0
        row_count = 0
        
        while retry_count < max_retries:
            try:
                # Begin immediate transaction to get write lock
                self.cursor.execute("BEGIN IMMEDIATE")

                # Generate new UUID for each cleared record
                new_uuid = str(uuid.uuid4())
                current_timestamp = datetime.now(timezone.utc).isoformat()

                update_query = f"""
                    UPDATE {self.base_table}
                    SET request_id = ?,
                        request_payload = ?,
                        completed_timestamp = ?,
                        state = 'empty',
                        rpc_client_queue = NULL
                    WHERE server_path = ?
                """

                self.cursor.execute(update_query, (new_uuid, '{}', current_timestamp, server_path))
                row_count = self.cursor.rowcount
                self.conn.commit()
                return row_count

            except sqlite3.OperationalError as e:
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(retry_delay)
                    else:
                        raise Exception(f"Failed to acquire lock after {max_retries} attempts for server path: {server_path}")
                else:
                    raise Exception(f"Failed to clear server queue for {server_path}: {str(e)}")

            except sqlite3.Error as e:
                self.conn.rollback()
                raise Exception(f"Failed to clear server queue for {server_path}: {str(e)}")

        return row_count