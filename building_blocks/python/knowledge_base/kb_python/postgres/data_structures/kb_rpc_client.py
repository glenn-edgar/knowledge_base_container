import time
import uuid
import json
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError
from psycopg2 import errors
from psycopg2 import sql
import psycopg2.extras
psycopg2.extras.register_uuid()
from psycopg2 import errors
class KB_RPC_Client:
    """
    A class to handle the RPC client for the knowledge base.
    """
    def __init__(self, kb_search,database):
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        self.cursor = self.kb_search.cursor 
        self.base_table = f"{database}_rpc_client"
        
    def find_rpc_client_id(self,kb=None,node_name=None, properties=None, node_path=None):
        """
        Find the node id for a given node name, properties, node path, and data.
        """
      
        result = self.find_node_ids(kb,node_name, properties, node_path)
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
        Find the number of free slots (records with is_new_result=FALSE) for a given client_path.
        This is a point-in-time snapshot and may change immediately after reading.
        
        Args:
            client_path (str): LTree compatible path for client
            
        Returns:
            int: Number of free slots available at the time of query
            
        Raises:
            Exception: If no records exist for the specified client_path
        """
        try:
            with self.conn.cursor() as cursor:
                # Use READ COMMITTED isolation level for consistent snapshot
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                
                query = sql.SQL("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(*) FILTER (WHERE is_new_result = FALSE) as free_slots
                    FROM {table} 
                    WHERE client_path = %s
                """).format(table=sql.Identifier(self.base_table))
                
                cursor.execute(query, (client_path,))
                result = cursor.fetchone()
                
                total_records = result[0]
                free_slots = result[1]
                
                if total_records == 0:
                    raise Exception(f"No records found for client_path: {client_path}")
                
                return free_slots
                
        except psycopg2.Error as e:
            raise Exception(f"Database error when finding free slots: {str(e)}")
                  

    def find_queued_slots(self, client_path):
        """
        Find the number of queued slots (records with is_new_result=TRUE) for a given client_path.
        This is a point-in-time snapshot for monitoring purposes.
        
        Args:
            client_path (str): LTree compatible path for client
            
        Returns:
            int: Number of queued slots available for the client_path
            
        Raises:
            Exception: If no records exist for the specified client_path
        """
        try:
            with self.conn.cursor() as cursor:
                # Single query to get both total and queued slots
                query = sql.SQL("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(*) FILTER (WHERE is_new_result = TRUE) as queued_slots
                    FROM {table} 
                    WHERE client_path = %s
                """).format(table=sql.Identifier(self.base_table))
                
                cursor.execute(query, (client_path,))
                result = cursor.fetchone()
                
                total_records = result[0]
                queued_slots = result[1]
                
                if total_records == 0:
                    raise Exception(f"No records found for client_path: {client_path}")
                
                return queued_slots
                
        except psycopg2.Error as e:
            raise Exception(f"Database error when finding queued slots: {str(e)}")
        
            
    def peak_and_claim_reply_data(self,
                                client_path: str,
                                max_retries: int = 3,
                                retry_delay: float = 1.0
                                ) -> tuple[int, dict]:
        """
        Atomically fetch and mark the next available reply as processed.

        Args:
            client_path (str): ltree path of the client
            max_retries (int): number of retry attempts if row is locked
            retry_delay (float): delay in seconds between retries

        Returns:
            tuple[int, dict]: (record ID, remaining data as dict)

        Raises:
            Exception: If no new result is found for client_path
            RuntimeError: If lock could not be acquired within retries
        """
        attempt = 0
        table_ident = sql.Identifier(*self.base_table.split('.'))

        while attempt < max_retries:
            try:
                with self.conn.cursor() as cur:
                    update_query = sql.SQL("""
                        UPDATE {table}
                        SET is_new_result = FALSE
                        WHERE id = (
                            SELECT id
                            FROM {table}
                            WHERE client_path = %s
                            AND is_new_result = TRUE
                            ORDER BY response_timestamp ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT 1
                        )
                        RETURNING *
                    """).format(table=table_ident)

                    cur.execute(update_query, (client_path,))
                    row = cur.fetchone()

                    if row:
                        col_names = [desc.name for desc in cur.description]
                        data = dict(zip(col_names, row))
                        
                        self.conn.commit()
                        return  data

                    # Check if any matching unclaimed rows exist
                    check_query = sql.SQL("""
                        SELECT EXISTS (
                            SELECT 1 FROM {table}
                            WHERE client_path = %s AND is_new_result = TRUE
                        )
                    """).format(table=table_ident)

                    cur.execute(check_query, (client_path,))
                    exists = cur.fetchone()[0]

                    if not exists:
                        return None

                    self.conn.rollback()
                    attempt += 1
                    time.sleep(retry_delay)

            except errors.LockNotAvailable:
                self.conn.rollback()
                attempt += 1
                time.sleep(retry_delay)

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
        - Sets is_new_result to FALSE

        Includes record locking with retries to handle concurrent access.

        Args:
            client_path (ltree value): The client path to match for clearing records
            max_retries (int): Maximum number of retries for acquiring the lock
            retry_delay (float): Delay in seconds between retry attempts

        Returns:
            int: Number of records updated
        """
        attempt = 0
        while attempt < max_retries:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("BEGIN;")

                    # Dynamically construct the SELECT ... FOR UPDATE statement
                    select_query = sql.SQL("""
                        SELECT id
                        FROM {table}
                        WHERE client_path = %s
                        FOR UPDATE NOWAIT
                    """).format(table=sql.Identifier(*self.base_table.split('.')))

                    cur.execute(select_query, (client_path,))
                    rows = cur.fetchall()

                    if not rows:
                        cur.execute("COMMIT;")
                        return 0

                    updated = 0
                    for (row_id,) in rows:
                        new_uuid = str(uuid.uuid4())

                        # Dynamically construct the UPDATE statement
                        update_query = sql.SQL("""
                            UPDATE {table}
                            SET
                                request_id         = %s,
                                server_path        = %s,
                                response_payload   = %s,
                                response_timestamp = NOW(),
                                is_new_result      = FALSE
                            WHERE id = %s
                        """).format(table=sql.Identifier(*self.base_table.split('.')))

                        cur.execute(update_query, (
                            new_uuid,
                            client_path,
                            json.dumps({}),
                            row_id
                        ))
                        updated += cur.rowcount

                    cur.execute("COMMIT;")
                    return updated

            except errors.LockNotAvailable:
                self.conn.rollback()
                attempt += 1
                time.sleep(retry_delay)

        raise RuntimeError(f"Could not acquire lock after {max_retries} retries")

    def push_and_claim_reply_data(self, client_path, request_uuid, server_path,
                                rpc_action, transaction_tag, reply_data,
                                max_retries=3, retry_delay=1):
        """
        Atomically claim and update the earliest matching record with is_new_result=FALSE.
        
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
        table_ident = sql.Identifier(*self.base_table.split('.'))

        while attempt <= max_retries:
            try:
                with self.conn.cursor() as cur:
                    # Begin transaction
                    cur.execute("BEGIN;")

                    # Combine SELECT and UPDATE atomically with locking
                    query = sql.SQL("""
                        WITH candidate AS (
                            SELECT id
                            FROM {table}
                            WHERE client_path = %s
                            AND is_new_result = FALSE
                            ORDER BY response_timestamp ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT 1
                        )
                        UPDATE {table}
                        SET request_id        = %s,
                            server_path       = %s,
                            rpc_action        = %s,
                            transaction_tag   = %s,
                            response_payload  = %s,
                            is_new_result     = TRUE,
                            response_timestamp = CURRENT_TIMESTAMP
                        FROM candidate
                        WHERE {table}.id = candidate.id
                        RETURNING {table}.id
                    """).format(table=table_ident)

                    cur.execute(query, (
                        client_path,
                        request_uuid,
                        server_path,
                        rpc_action,
                        transaction_tag,
                        json.dumps(reply_data)
                    ))

                    result = cur.fetchone()
                    if not result:
                        self.conn.rollback()
                        raise Exception("No available record with is_new_result=FALSE found")

                    self.conn.commit()
                    return  # success

            except (OperationalError, DatabaseError) as e:
                self.conn.rollback()
                last_error = e
                attempt += 1
                if attempt > max_retries:
                    raise Exception(f"Failed after {max_retries} retries: {str(last_error)}")
                time.sleep(retry_delay)

            except Exception as e:
                self.conn.rollback()
                raise e

        raise Exception(f"Failed after {max_retries} retries: {str(last_error)}")

    def list_waiting_jobs(self, client_path=None):
        """
        List all rows where is_new_result is TRUE, optionally filtered by client_path.

        Args:
            client_path (str, optional): If provided, filter results to this client_path

        Returns:
            list: A list of dictionaries, each containing the data for one waiting job

        Raises:
            Exception: If a database error occurs
        """
        try:
            with self.conn.cursor() as cursor:
                table_ident = sql.Identifier(*self.base_table.split('.'))
                
                if client_path is None:
                    query = sql.SQL("""
                        SELECT id, request_id, client_path, server_path, 
                            response_payload, response_timestamp, is_new_result
                        FROM {table}
                        WHERE is_new_result = TRUE
                        ORDER BY response_timestamp ASC
                    """).format(table=table_ident)
                    params = ()
                else:
                    query = sql.SQL("""
                        SELECT id, request_id, client_path, server_path, 
                            response_payload, response_timestamp, is_new_result
                        FROM {table}
                        WHERE is_new_result = TRUE AND client_path = %s
                        ORDER BY response_timestamp ASC
                    """).format(table=table_ident)
                    params = (client_path,)

                cursor.execute(query, params)
                column_names = [desc[0] for desc in cursor.description]
                records = cursor.fetchall()

                result = []
                for record in records:
                    record_dict = dict(zip(column_names, record))

                    if record_dict.get('request_id') is not None:
                        record_dict['request_id'] = str(record_dict['request_id'])

                    if isinstance(record_dict.get('response_timestamp'), datetime):
                        record_dict['response_timestamp'] = record_dict['response_timestamp'].isoformat()

                    for path_key in ('client_path', 'server_path'):
                        if record_dict.get(path_key) is not None:
                            record_dict[path_key] = str(record_dict[path_key])

                    result.append(record_dict)

                return result

        except psycopg2.Error as e:
            raise Exception(f"Database error when listing waiting jobs: {str(e)}")