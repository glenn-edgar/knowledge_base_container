import time
import uuid
import json
from datetime import datetime, timezone
import sqlite3


class KB_RPC_Client:
    """
    A class to handle the RPC client for the knowledge base.
    SQLite version.
    """
    def __init__(self, kb_search, database):
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        self.cursor = self.kb_search.cursor 
        self.base_table = f"{database}_rpc_client"
        
    def find_rpc_client_id(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find the node id for a given node name, properties, node path, and data.
        """
        result = self.find_rpc_client_ids(kb, node_name, properties, node_path)
        if len(result) == 0:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        if len(result) > 1:
            raise ValueError(f"Multiple nodes found matching path parameters: {node_name}, {properties}, {node_path}")
        return result
    
    def find_rpc_client_ids(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find the node id for a given node name, properties, node path.
        """
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_RPC_CLIENT_FIELD")
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
    
    def find_rpc_client_keys(self, key_data):
        """
        Extract key values from key_data.
        """
        return_values = []
        for key in key_data:
            return_values.append(key['path'])
        return return_values    
    
    
    def find_free_slots(self, client_path):
        """
        Find the number of free slots (records with is_new_result=0/FALSE) for a given client_path.
        This is a point-in-time snapshot and may change immediately after reading.
        
        Args:
            client_path (str): LTree compatible path for client
            
        Returns:
            int: Number of free slots available at the time of query
            
        Raises:
            Exception: If no records exist for the specified client_path
        """
        try:
            # SQLite supports COUNT(*) FILTER (WHERE ...) since version 3.30.0
            query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE is_new_result = 0) as free_slots
                FROM {self.base_table} 
                WHERE client_path = ?
            """
            
            self.cursor.execute(query, (client_path,))
            result = self.cursor.fetchone()
            
            if result:
                total_records = result['total_records'] if isinstance(result, sqlite3.Row) else result[0]
                free_slots = result['free_slots'] if isinstance(result, sqlite3.Row) else result[1]
            else:
                total_records = 0
                free_slots = 0
            
            if total_records == 0:
                raise Exception(f"No records found for client_path: {client_path}")
            
            return free_slots
            
        except sqlite3.Error as e:
            raise Exception(f"Database error when finding free slots: {str(e)}")
                  

    def find_queued_slots(self, client_path):
        """
        Find the number of queued slots (records with is_new_result=1/TRUE) for a given client_path.
        This is a point-in-time snapshot for monitoring purposes.
        
        Args:
            client_path (str): LTree compatible path for client
            
        Returns:
            int: Number of queued slots available for the client_path
            
        Raises:
            Exception: If no records exist for the specified client_path
        """
        try:
            # Single query to get both total and queued slots
            query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE is_new_result = 1) as queued_slots
                FROM {self.base_table} 
                WHERE client_path = ?
            """
            
            self.cursor.execute(query, (client_path,))
            result = self.cursor.fetchone()
            
            if result:
                total_records = result['total_records'] if isinstance(result, sqlite3.Row) else result[0]
                queued_slots = result['queued_slots'] if isinstance(result, sqlite3.Row) else result[1]
            else:
                total_records = 0
                queued_slots = 0
            
            if total_records == 0:
                raise Exception(f"No records found for client_path: {client_path}")
            
            return queued_slots
            
        except sqlite3.Error as e:
            raise Exception(f"Database error when finding queued slots: {str(e)}")
        
            
    def peak_and_claim_reply_data(self,
                                client_path: str,
                                max_retries: int = 3,
                                retry_delay: float = 1.0
                                ) -> dict:
        """
        Atomically fetch and mark the next available reply as processed.
        
        Note: SQLite doesn't support FOR UPDATE SKIP LOCKED. This implementation
        uses BEGIN IMMEDIATE for database-level locking with retry logic.

        Args:
            client_path (str): ltree path of the client
            max_retries (int): number of retry attempts if database is locked
            retry_delay (float): delay in seconds between retries

        Returns:
            dict: remaining data as dict, or None if no new results found

        Raises:
            RuntimeError: If lock could not be acquired within retries
        """
        attempt = 0

        while attempt < max_retries:
            try:
                # Begin immediate transaction for write lock
                self.cursor.execute("BEGIN IMMEDIATE")
                
                # Find the next available record
                select_query = f"""
                    SELECT *
                    FROM {self.base_table}
                    WHERE client_path = ?
                    AND is_new_result = 1
                    ORDER BY response_timestamp ASC
                    LIMIT 1
                """
                
                self.cursor.execute(select_query, (client_path,))
                row = self.cursor.fetchone()

                if row:
                    # Convert to dict
                    if isinstance(row, sqlite3.Row):
                        data = dict(row)
                        row_id = data['id']
                    else:
                        # Fallback if Row factory not set
                        col_names = [desc[0] for desc in self.cursor.description]
                        data = dict(zip(col_names, row))
                        row_id = data['id']
                    
                    # Update the record to mark as processed
                    update_query = f"""
                        UPDATE {self.base_table}
                        SET is_new_result = 0
                        WHERE id = ?
                    """
                    self.cursor.execute(update_query, (row_id,))
                    
                    self.conn.commit()
                    return data

                # Check if any matching unclaimed rows exist
                check_query = f"""
                    SELECT EXISTS (
                        SELECT 1 FROM {self.base_table}
                        WHERE client_path = ? AND is_new_result = 1
                    )
                """

                self.cursor.execute(check_query, (client_path,))
                exists = self.cursor.fetchone()[0]

                if not exists:
                    self.conn.rollback()
                    return None

                self.conn.rollback()
                attempt += 1
                time.sleep(retry_delay)

            except sqlite3.OperationalError as e:
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    attempt += 1
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Database error in peak_and_claim_reply_data: {str(e)}")

        raise RuntimeError(f"Could not lock a new-reply row after {max_retries} attempts")
        
        
    def clear_reply_queue(self,
                        client_path: str,
                        max_retries: int = 3,
                        retry_delay: float = 1.0
                        ) -> int:
        """
        Clear the reply queue by resetting records matching the specified client_path.

        For each matching record:
        - Sets a unique UUID for request_id
        - Sets server_path equal to client_path
        - Resets response_payload to empty JSON object
        - Updates response_timestamp to current UTC time
        - Sets is_new_result to FALSE (0)

        Includes record locking with retries to handle concurrent access.

        Args:
            client_path (str): The client path to match for clearing records
            max_retries (int): Maximum number of retries for acquiring the lock
            retry_delay (float): Delay in seconds between retry attempts

        Returns:
            int: Number of records updated
        """
        attempt = 0
        while attempt < max_retries:
            try:
                # Begin immediate transaction for write lock
                self.cursor.execute("BEGIN IMMEDIATE")

                # Get all matching records
                select_query = f"""
                    SELECT id
                    FROM {self.base_table}
                    WHERE client_path = ?
                """

                self.cursor.execute(select_query, (client_path,))
                rows = self.cursor.fetchall()

                if not rows:
                    self.conn.commit()
                    return 0

                updated = 0
                current_timestamp = datetime.now(timezone.utc).isoformat()
                
                for row in rows:
                    row_id = row['id'] if isinstance(row, sqlite3.Row) else row[0]
                    new_uuid = str(uuid.uuid4())

                    update_query = f"""
                        UPDATE {self.base_table}
                        SET
                            request_id         = ?,
                            server_path        = ?,
                            response_payload   = ?,
                            response_timestamp = ?,
                            is_new_result      = 0
                        WHERE id = ?
                    """

                    self.cursor.execute(update_query, (
                        new_uuid,
                        client_path,
                        json.dumps({}),
                        current_timestamp,
                        row_id
                    ))
                    updated += self.cursor.rowcount

                self.conn.commit()
                return updated

            except sqlite3.OperationalError as e:
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    attempt += 1
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Database error in clear_reply_queue: {str(e)}")

        raise RuntimeError(f"Could not acquire lock after {max_retries} retries")


    def push_and_claim_reply_data(self, client_path, request_uuid, server_path,
                                rpc_action, transaction_tag, reply_data,
                                max_retries=3, retry_delay=1):
        """
        Atomically claim and update the earliest matching record with is_new_result=0/FALSE.
        
        Note: SQLite doesn't support FOR UPDATE SKIP LOCKED. This uses BEGIN IMMEDIATE.
        
        Args:
            client_path (str): LTree client path
            request_uuid (str): Request UUID
            server_path (str): LTree server path
            rpc_action (str): RPC action name
            transaction_tag (str): Transaction tag
            reply_data (dict): Reply data (to be stored as JSON)
            max_retries (int): Max retries on lock conflict
            retry_delay (float): Delay between retries
        
        Raises:
            Exception: On failure after all retries or if no matching record found.
        """
        attempt = 0
        last_error = None
        request_uuid = str(request_uuid) if not isinstance(request_uuid, str) else request_uuid
    

        while attempt <= max_retries:
            try:
                # Begin immediate transaction for write lock
                self.cursor.execute("BEGIN IMMEDIATE")

                # Find the first available record
                select_query = f"""
                    SELECT id
                    FROM {self.base_table}
                    WHERE client_path = ?
                    AND is_new_result = 0
                    ORDER BY response_timestamp ASC
                    LIMIT 1
                """
                
                self.cursor.execute(select_query, (client_path,))
                result = self.cursor.fetchone()
                
                if not result:
                    self.conn.rollback()
                    raise Exception("No available record with is_new_result=FALSE found")
                
                record_id = result['id'] if isinstance(result, sqlite3.Row) else result[0]
                
                # Get current timestamp
                current_timestamp = datetime.now(timezone.utc).isoformat()
                
                # Update the record
                update_query = f"""
                    UPDATE {self.base_table}
                    SET request_id        = ?,
                        server_path       = ?,
                        rpc_action        = ?,
                        transaction_tag   = ?,
                        response_payload  = ?,
                        is_new_result     = 1,
                        response_timestamp = ?
                    WHERE id = ?
                    RETURNING id
                """

                self.cursor.execute(update_query, (
                    request_uuid,
                    server_path,
                    rpc_action,
                    transaction_tag,
                    json.dumps(reply_data),
                    current_timestamp,
                    record_id
                ))

                result = self.cursor.fetchone()
                if not result:
                    self.conn.rollback()
                    raise Exception("Failed to update record")

                self.conn.commit()
                return  # success

            except sqlite3.OperationalError as e:
                self.conn.rollback()
                last_error = e
                if "database is locked" in str(e).lower():
                    attempt += 1
                    if attempt > max_retries:
                        raise Exception(f"Failed after {max_retries} retries: {str(last_error)}")
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Database error in push_and_claim_reply_data: {str(e)}")

            except Exception as e:
                self.conn.rollback()
                raise e

        raise Exception(f"Failed after {max_retries} retries: {str(last_error)}")


    def list_waiting_jobs(self, client_path=None):
        """
        List all rows where is_new_result is TRUE (1), optionally filtered by client_path.

        Args:
            client_path (str, optional): If provided, filter results to this client_path

        Returns:
            list: A list of dictionaries, each containing the data for one waiting job

        Raises:
            Exception: If a database error occurs
        """
        try:
            if client_path is None:
                query = f"""
                    SELECT id, request_id, client_path, server_path, 
                        response_payload, response_timestamp, is_new_result
                    FROM {self.base_table}
                    WHERE is_new_result = 1
                    ORDER BY response_timestamp ASC
                """
                params = ()
            else:
                query = f"""
                    SELECT id, request_id, client_path, server_path, 
                        response_payload, response_timestamp, is_new_result
                    FROM {self.base_table}
                    WHERE is_new_result = 1 AND client_path = ?
                    ORDER BY response_timestamp ASC
                """
                params = (client_path,)

            self.cursor.execute(query, params)
            records = self.cursor.fetchall()

            result = []
            for record in records:
                # Convert sqlite3.Row to dict
                if isinstance(record, sqlite3.Row):
                    record_dict = dict(record)
                else:
                    column_names = [desc[0] for desc in self.cursor.description]
                    record_dict = dict(zip(column_names, record))

                # Convert UUID to string if needed
                if record_dict.get('request_id') is not None:
                    record_dict['request_id'] = str(record_dict['request_id'])

                # Convert timestamp if it's a datetime object (though in SQLite it's usually a string)
                if isinstance(record_dict.get('response_timestamp'), datetime):
                    record_dict['response_timestamp'] = record_dict['response_timestamp'].isoformat()

                # Ensure path fields are strings
                for path_key in ('client_path', 'server_path'):
                    if record_dict.get(path_key) is not None:
                        record_dict[path_key] = str(record_dict[path_key])

                result.append(record_dict)

            return result

        except sqlite3.Error as e:
            raise Exception(f"Database error when listing waiting jobs: {str(e)}")