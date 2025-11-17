
import time
import json
from psycopg2.extras import RealDictCursor


class KB_Stream:
    """
    A class to handle stream data for the knowledge base.
    Stream table rows are pre-allocated and new entries replace the oldest entries.
    Always returns dictionaries with field names instead of tuples.
    
    Table Schema:
    - id SERIAL PRIMARY KEY
    - path LTREE
    - recorded_at TIMESTAMPTZ DEFAULT NOW()
    - data JSONB
    """
    
    def __init__(self, kb_search, database):
        """
        Initialize the KB_Stream object.
        
        Args:
            kb_search: An instance of KB_Search class
            database (str): The base database name
        """
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        # Create our own cursor with RealDictCursor to ensure dictionary results
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.base_table = f"{database}_stream"
    
    def _execute_query(self, query, params=None):
        """
        Execute a query and return results as dictionaries.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            list: List of dictionaries with field names
        """
        self.cursor.execute(query, params or ())
        return self.cursor.fetchall()
    
    def _execute_single(self, query, params=None):
        """
        Execute a query and return a single result as a dictionary.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            dict or None: Single dictionary with field names or None
        """
        self.cursor.execute(query, params or ())
        return self.cursor.fetchone()
        
    def find_stream_id(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find a single stream node id for given parameters. Raises error if 0 or multiple nodes found.
        
        Args:
            node_name (str, optional): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            dict: Single matching stream record with field names
            
        Raises:
            ValueError: If no node or multiple nodes found
        """
        
        
        results = self.find_stream_ids(kb, node_name, properties, node_path)
        
        if len(results) == 0:
            raise ValueError(f"No stream node found matching parameters: name={node_name}, properties={properties}, path={node_path}")
        if len(results) > 1:
            raise ValueError(f"Multiple stream nodes ({len(results)}) found matching parameters: name={node_name}, properties={properties}, path={node_path}")
        
        return results[0]
    
    def find_stream_ids(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find all stream node ids matching the given parameters.
        
        Args:
            node_name (str, optional): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            list: List of matching stream records as dictionaries
            
        Raises:
            ValueError: If no nodes found
        """
        
        
        try:
            # Clear previous filters and build new query
            self.kb_search.clear_filters()
            self.kb_search.search_label("KB_STREAM_FIELD")
            
            if kb is not None:
                self.kb_search.search_kb(kb)
                
            if node_name is not None:
                self.kb_search.search_name(node_name)
            if properties is not None and isinstance(properties, dict):
                for key, value in properties.items():
                    self.kb_search.search_property_value(key, value)
            if node_path is not None:
                self.kb_search.search_path(node_path)
            
            # Execute query and get results
            node_ids = self.kb_search.execute_query()
            
            if not node_ids or len(node_ids) == 0:
                raise ValueError(f"No stream nodes found matching parameters: name={node_name}, properties={properties}, path={node_path}")
            
            return node_ids
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error finding stream node IDs: {str(e)}")
    
    def find_stream_table_keys(self, key_data):
        """
        Extract path values from stream query results.
        
        Args:
            key_data (list): List of result dictionaries
            
        Returns:
            list: List of path values
        """
        if not key_data:
            return []
        
        return_values = []
        for row in key_data:
            # Since we always use RealDictCursor, row will always be a dictionary
            path = row.get('path')
            if path is not None:
                return_values.append(path)
        
        return return_values
    
    def push_stream_data(self, path, data, max_retries=3, retry_delay=1.0):
        """
        Find the oldest record (by recorded_at) for the given path,
        update it with new data, fresh timestamp, and set valid=TRUE.
        This implements a true circular buffer pattern that ignores the valid status
        and always replaces the oldest record by time.

        Args:
            path (str): The path in LTREE format.
            data (dict): The JSON-serializable data to write.
            max_retries (int): Max attempts if rows are locked.
            retry_delay (float): Seconds to wait between retries.

        Returns:
            dict: Dictionary containing the updated record information

        Raises:
            ValueError: If inputs are invalid
            Exception: If no records exist for this path.
            RuntimeError: If all matching rows are locked after retries.
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")
        
        for attempt in range(1, max_retries + 1):
            try:
                # 1) ensure there's at least one record to update
                count_query = f"""
                    SELECT COUNT(*) as count
                      FROM {self.base_table}
                     WHERE path = %s
                """
                count_result = self._execute_single(count_query, (path,))
                total = count_result['count'] if count_result else 0
                
                if total == 0:
                    raise Exception(f"No records found for path='{path}'. Records must be pre-allocated for stream tables.")

                # 2) try to lock the oldest record regardless of valid status (true circular buffer)
                select_query = f"""
                    SELECT id, recorded_at, valid
                      FROM {self.base_table}
                     WHERE path = %s
                     ORDER BY recorded_at ASC
                     FOR UPDATE SKIP LOCKED
                     LIMIT 1
                """
                row = self._execute_single(select_query, (path,))

                if not row:
                    # All rows are currently locked
                    self.conn.rollback()
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise RuntimeError(
                            f"Could not lock any row for path='{path}' after {max_retries} attempts"
                        )

                record_id = row['id']
                old_recorded_at = row['recorded_at']
                was_valid = row['valid']

                # 3) perform the update with valid=TRUE (always overwrites oldest record)
                update_query = f"""
                    UPDATE {self.base_table}
                       SET data         = %s,
                           recorded_at  = NOW(),
                           valid        = TRUE
                     WHERE id = %s
                     RETURNING id, path, recorded_at, data, valid
                """
                updated_row = self._execute_single(update_query, (json.dumps(data, separators=(',', ':')), record_id))

                if not updated_row:
                    self.conn.rollback()
                    raise Exception(f"Failed to update record id={record_id}")

                self.conn.commit()
                
                return {
                    'id': updated_row['id'],
                    'path': updated_row['path'],
                    'recorded_at': updated_row['recorded_at'],
                    'data': updated_row['data'],
                    'valid': updated_row['valid'],
                    'previous_recorded_at': old_recorded_at,
                    'was_previously_valid': was_valid,
                    'operation': 'circular_buffer_replace'
                }

            except Exception as e:
                try:
                    self.conn.rollback()
                except:
                    pass
                
                if isinstance(e, (ValueError, RuntimeError)) or "No records found" in str(e):
                    raise
                
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise Exception(f"Error pushing stream data for path '{path}': {str(e)}")

        # Should never reach here
        raise RuntimeError("Unexpected error in push_stream_data")
    
    def get_latest_stream_data(self, path):
        """
        Get the most recent valid stream data for a given path.
        
        Args:
            path (str): The path to search for in LTREE format
            
        Returns:
            dict or None: Dictionary containing the latest valid stream data, or None if not found
            
        Raises:
            ValueError: If path is invalid
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            query = f"""
                SELECT id, path, recorded_at, data, valid
                FROM {self.base_table}
                WHERE path = %s AND valid = TRUE
                ORDER BY recorded_at DESC
                LIMIT 1
            """
            
            result = self._execute_single(query, (path,))
            return dict(result) if result else None
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error getting latest stream data for path '{path}': {str(e)}")
    
    def get_stream_data_count(self, path, include_invalid=False):
        """
        Count the number of valid stream entries for a given path.
        
        Args:
            path (str): The path to search for in LTREE format
            include_invalid (bool): If True, count all records; if False, count only valid records
            
        Returns:
            int: Number of stream entries for the given path
            
        Raises:
            ValueError: If path is invalid
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            if include_invalid:
                query = f"""
                    SELECT COUNT(*) as count
                    FROM {self.base_table}
                    WHERE path = %s
                """
            else:
                query = f"""
                    SELECT COUNT(*) as count
                    FROM {self.base_table}
                    WHERE path = %s AND valid = TRUE
                """
            
            result = self._execute_single(query, (path,))
            return result['count'] if result else 0
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error counting stream data for path '{path}': {str(e)}")
    
    def clear_stream_data(self, path, older_than=None):
        """
        Clear stream data for a given path by setting the valid field to FALSE.
        If older_than is None, all records for the path are marked as invalid.
        If older_than is specified, only records older than that time are marked as invalid.
        
        Args:
            path (str): The path to clear data for
            older_than (datetime, optional): Only clear data older than this time
            
        Returns:
            dict: Dictionary with results including count of cleared records
            
        Raises:
            ValueError: If path is invalid
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            if older_than is not None:
                update_query = f"""
                    UPDATE {self.base_table}
                    SET valid = FALSE
                    WHERE path = %s
                    AND recorded_at < %s
                    AND valid = TRUE
                    RETURNING id, recorded_at
                """
                params = (path, older_than)
                operation_desc = f"older than {older_than}"
            else:
                update_query = f"""
                    UPDATE {self.base_table}
                    SET valid = FALSE
                    WHERE path = %s
                    AND valid = TRUE
                    RETURNING id, recorded_at
                """
                params = (path,)
                operation_desc = "all records"
            
            cleared_records = self._execute_query(update_query, params)
            
            self.conn.commit()
            
            return {
                'success': True,
                'cleared_count': len(cleared_records),
                'cleared_records': cleared_records,
                'path': path,
                'operation': f"Cleared {operation_desc}"
            }
            
        except Exception as e:
            try:
                self.conn.rollback()
            except:
                pass
            
            if isinstance(e, ValueError):
                raise
            
            error_msg = f"Error clearing stream data for path '{path}': {str(e)}"
            return {
                'success': False,
                'cleared_count': 0,
                'error': error_msg,
                'path': path
            }
    
    def list_stream_data(self, path, limit=None, offset=0, recorded_after=None, recorded_before=None, order='ASC'):
        """
        List valid stream data for a given path with optional filtering and pagination.
        Only returns records where valid=TRUE.
        
        Args:
            path (str): The path to search for in LTREE format
            limit (int, optional): Maximum number of records to return
            offset (int, optional): Number of records to skip
            recorded_after (datetime, optional): Only include data recorded after this time
            recorded_before (datetime, optional): Only include data recorded before this time
            order (str): Sort order - 'ASC' for oldest first, 'DESC' for newest first
        
        Returns:
            list: A list of dictionaries containing valid stream data with field names
            
        Raises:
            ValueError: If path is invalid or order is not ASC/DESC
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        if order.upper() not in ['ASC', 'DESC']:
            raise ValueError("Order must be 'ASC' or 'DESC'")
        
        try:
            # Build the base query - only return valid records
            query = f"""
                SELECT id, path, recorded_at, data, valid
                FROM {self.base_table}
                WHERE path = %s AND valid = TRUE
            """
            
            params = [path]
            
            # Add optional time-based filters
            if recorded_after is not None:
                query += " AND recorded_at >= %s"
                params.append(recorded_after)
                
            if recorded_before is not None:
                query += " AND recorded_at <= %s"
                params.append(recorded_before)
                
            # Add ordering
            query += f" ORDER BY recorded_at {order.upper()}"
            
            # Add optional pagination
            if limit is not None and limit > 0:
                query += " LIMIT %s"
                params.append(limit)
                
            if offset > 0:
                query += " OFFSET %s"
                params.append(offset)
                
            rows = self._execute_query(query, params)
            return [dict(row) for row in rows]
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error listing stream data for path '{path}': {str(e)}")
    
    def get_stream_data_range(self, path, start_time, end_time):
        """
        Get valid stream data within a specific time range.
        Only returns records where valid=TRUE.
        
        Args:
            path (str): The path to search for in LTREE format
            start_time (datetime): Start of the time range
            end_time (datetime): End of the time range
            
        Returns:
            list: List of dictionaries containing valid stream data within the time range
            
        Raises:
            ValueError: If inputs are invalid
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        if not start_time or not end_time:
            raise ValueError("Both start_time and end_time must be provided")
        if start_time >= end_time:
            raise ValueError("start_time must be before end_time")
        
        try:
            query = f"""
                SELECT id, path, recorded_at, data, valid
                FROM {self.base_table}
                WHERE path = %s
                AND recorded_at >= %s
                AND recorded_at <= %s
                AND valid = TRUE
                ORDER BY recorded_at ASC
            """
            
            results = self._execute_query(query, (path, start_time, end_time))
            return [dict(row) for row in results]
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error getting stream data range for path '{path}': {str(e)}")
    
    def get_stream_statistics(self, path, include_invalid=False):
        """
        Get comprehensive statistics for stream data at a given path.
        By default, only considers valid records unless include_invalid=True.
        
        Args:
            path (str): The path to get statistics for
            include_invalid (bool): If True, include invalid records in statistics
            
        Returns:
            dict: Dictionary containing various stream statistics
            
        Raises:
            ValueError: If path is invalid
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            if include_invalid:
                stats_query = f"""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN valid = TRUE THEN 1 END) as valid_records,
                        COUNT(CASE WHEN valid = FALSE THEN 1 END) as invalid_records,
                        MIN(CASE WHEN valid = TRUE THEN recorded_at END) as earliest_valid_recorded,
                        MAX(CASE WHEN valid = TRUE THEN recorded_at END) as latest_valid_recorded,
                        MIN(recorded_at) as earliest_recorded_overall,
                        MAX(recorded_at) as latest_recorded_overall,
                        AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) as avg_interval_seconds_all,
                        AVG(CASE WHEN valid = TRUE THEN EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at))) END) as avg_interval_seconds_valid
                    FROM {self.base_table}
                    WHERE path = %s
                """
            else:
                stats_query = f"""
                    SELECT 
                        COUNT(*) as valid_records,
                        MIN(recorded_at) as earliest_recorded,
                        MAX(recorded_at) as latest_recorded,
                        AVG(EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (ORDER BY recorded_at)))) as avg_interval_seconds
                    FROM {self.base_table}
                    WHERE path = %s AND valid = TRUE
                """
            
            result = self._execute_single(stats_query, (path,))
            
            if result is None:
                if include_invalid:
                    return {
                        'total_records': 0,
                        'valid_records': 0,
                        'invalid_records': 0,
                        'earliest_valid_recorded': None,
                        'latest_valid_recorded': None,
                        'earliest_recorded_overall': None,
                        'latest_recorded_overall': None,
                        'avg_interval_seconds_all': None,
                        'avg_interval_seconds_valid': None
                    }
                else:
                    return {
                        'valid_records': 0,
                        'earliest_recorded': None,
                        'latest_recorded': None,
                        'avg_interval_seconds': None
                    }
            
            return dict(result)
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error getting stream statistics for path '{path}': {str(e)}")
    
    def get_stream_data_by_id(self, record_id):
        """
        Retrieve a specific stream record by its ID.
        
        Args:
            record_id (int): The ID of the record to retrieve
            
        Returns:
            dict or None: Dictionary containing record details with field names, or None if not found
            
        Raises:
            ValueError: If record_id is invalid
            Exception: If there's an error executing the query
        """
        if not record_id or not isinstance(record_id, int):
            raise ValueError("record_id must be a valid integer")
        
        try:
            query = f"""
                SELECT id, path, recorded_at, data
                FROM {self.base_table}
                WHERE id = %s
            """
            
            return self._execute_single(query, (record_id,))
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error retrieving stream record with id {record_id}: {str(e)}")
    
  
#