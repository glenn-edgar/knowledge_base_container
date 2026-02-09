import time
import json
import sqlite3
from datetime import datetime


class KB_Stream:
    """
    A class to handle stream data for the knowledge base.
    Stream table rows are pre-allocated and new entries replace the oldest entries.
    Always returns dictionaries with field names instead of tuples.
    
    Table Schema:
    - id INTEGER PRIMARY KEY AUTOINCREMENT
    - path TEXT
    - recorded_at TEXT (ISO8601 format)
    - data TEXT (JSON)
    - valid INTEGER (0 or 1, default 1)
    """
    
    def __init__(self, kb_search, database,reset_flag = False):
        """
        Initialize the KB_Stream object.
        
        Args:
            kb_search: An instance of KB_Search class (SQLite version)
            database (str): The base database name
        """
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        # Set row factory to return dictionary-like results
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
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
            kb (str, optional): Knowledge base name to search for
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
            kb (str, optional): Knowledge base name to search for
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
            # Row will be a sqlite3.Row object with dictionary-like access
            path = row['path'] if 'path' in row.keys() else None
            if path is not None:
                return_values.append(path)
        
        return return_values
    
    def push_stream_data(self, path, data, max_retries=3, retry_delay=1.0):
        """
        Find the oldest record (by recorded_at) for the given path,
        update it with new data, fresh timestamp, and set valid=1.
        This implements a true circular buffer pattern that ignores the valid status
        and always replaces the oldest record by time.
        
        Note: SQLite doesn't support FOR UPDATE SKIP LOCKED like PostgreSQL.
        We use BEGIN IMMEDIATE to get an exclusive lock on the database.

        Args:
            path (str): The path in LTREE format.
            data (dict): The JSON-serializable data to write.
            max_retries (int): Max attempts if database is locked.
            retry_delay (float): Seconds to wait between retries.

        Returns:
            dict: Dictionary containing the updated record information

        Raises:
            ValueError: If inputs are invalid
            Exception: If no records exist for this path.
            RuntimeError: If database is locked after retries.
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")
        
        for attempt in range(1, max_retries + 1):
            try:
                # Begin an immediate transaction to lock the database
                self.cursor.execute("BEGIN IMMEDIATE")
                
                # 1) ensure there's at least one record to update
                count_query = f"""
                    SELECT COUNT(*) as count
                    FROM {self.base_table}
                    WHERE path = ?
                """
                count_result = self._execute_single(count_query, (path,))
                total = count_result['count'] if count_result else 0
                
                if total == 0:
                    self.conn.rollback()
                    raise Exception(f"No records found for path='{path}'. Records must be pre-allocated for stream tables.")

                # 2) Get the oldest record (true circular buffer - ignores valid status)
                select_query = f"""
                    SELECT id, recorded_at, valid
                    FROM {self.base_table}
                    WHERE path = ?
                    ORDER BY recorded_at ASC
                    LIMIT 1
                """
                row = self._execute_single(select_query, (path,))

                if not row:
                    self.conn.rollback()
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise RuntimeError(
                            f"Could not find any row for path='{path}' after {max_retries} attempts"
                        )

                record_id = row['id']
                old_recorded_at = row['recorded_at']
                was_valid = row['valid']

                # 3) perform the update with valid=1 (always overwrites oldest record)
                # Use ISO8601 format for timestamp
                current_time = datetime.utcnow().isoformat()
                update_query = f"""
                    UPDATE {self.base_table}
                    SET data = ?,
                        recorded_at = ?,
                        valid = 1
                    WHERE id = ?
                """
                self.cursor.execute(update_query, (
                    json.dumps(data, separators=(',', ':')),
                    current_time,
                    record_id
                ))

                # Get the updated record
                verify_query = f"""
                    SELECT id, path, recorded_at, data, valid
                    FROM {self.base_table}
                    WHERE id = ?
                """
                updated_row = self._execute_single(verify_query, (record_id,))

                if not updated_row:
                    self.conn.rollback()
                    raise Exception(f"Failed to update record id={record_id}")

                self.conn.commit()
                
                return {
                    'id': updated_row['id'],
                    'path': updated_row['path'],
                    'recorded_at': updated_row['recorded_at'],
                    'data': json.loads(updated_row['data']) if isinstance(updated_row['data'], str) else updated_row['data'],
                    'valid': bool(updated_row['valid']),
                    'previous_recorded_at': old_recorded_at,
                    'was_previously_valid': bool(was_valid),
                    'operation': 'circular_buffer_replace'
                }

            except sqlite3.OperationalError as e:
                # Handle database locked errors
                try:
                    self.conn.rollback()
                except:
                    pass
                
                if "database is locked" in str(e).lower():
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise RuntimeError(f"Database locked after {max_retries} attempts")
                else:
                    raise
                    
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
                WHERE path = ? AND valid = 1
                ORDER BY recorded_at DESC
                LIMIT 1
            """
            
            result = self._execute_single(query, (path,))
            if result:
                # Parse JSON data if it's stored as string
                result_dict = dict(result)
                if isinstance(result_dict.get('data'), str):
                    result_dict['data'] = json.loads(result_dict['data'])
                result_dict['valid'] = bool(result_dict['valid'])
                return result_dict
            return None
            
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
                    WHERE path = ?
                """
            else:
                query = f"""
                    SELECT COUNT(*) as count
                    FROM {self.base_table}
                    WHERE path = ? AND valid = 1
                """
            
            result = self._execute_single(query, (path,))
            return result['count'] if result else 0
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error counting stream data for path '{path}': {str(e)}")
    
    def clear_stream_data(self, path, older_than=None):
        """
        Clear stream data for a given path by setting the valid field to 0 (FALSE).
        If older_than is None, all records for the path are marked as invalid.
        If older_than is specified, only records older than that time are marked as invalid.
        
        Args:
            path (str): The path to clear data for
            older_than (str or datetime, optional): Only clear data older than this time (ISO8601 format)
            
        Returns:
            dict: Dictionary with results including count of cleared records
            
        Raises:
            ValueError: If path is invalid
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            # Convert datetime to ISO8601 string if needed
            if older_than is not None and isinstance(older_than, datetime):
                older_than = older_than.isoformat()
            
            if older_than is not None:
                # First get the records that will be cleared
                select_query = f"""
                    SELECT id, recorded_at
                    FROM {self.base_table}
                    WHERE path = ?
                    AND recorded_at < ?
                    AND valid = 1
                """
                cleared_records = self._execute_query(select_query, (path, older_than))
                
                # Now update them
                update_query = f"""
                    UPDATE {self.base_table}
                    SET valid = 0
                    WHERE path = ?
                    AND recorded_at < ?
                    AND valid = 1
                """
                self.cursor.execute(update_query, (path, older_than))
                operation_desc = f"older than {older_than}"
            else:
                # First get the records that will be cleared
                select_query = f"""
                    SELECT id, recorded_at
                    FROM {self.base_table}
                    WHERE path = ?
                    AND valid = 1
                """
                cleared_records = self._execute_query(select_query, (path,))
                
                # Now update them
                update_query = f"""
                    UPDATE {self.base_table}
                    SET valid = 0
                    WHERE path = ?
                    AND valid = 1
                """
                self.cursor.execute(update_query, (path,))
                operation_desc = "all records"
            
            self.conn.commit()
            
            # Convert sqlite3.Row objects to dicts
            cleared_records_list = [dict(row) for row in cleared_records]
            
            return {
                'success': True,
                'cleared_count': len(cleared_records_list),
                'cleared_records': cleared_records_list,
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
        Only returns records where valid=1.
        
        Args:
            path (str): The path to search for in LTREE format
            limit (int, optional): Maximum number of records to return
            offset (int, optional): Number of records to skip
            recorded_after (str or datetime, optional): Only include data recorded after this time (ISO8601)
            recorded_before (str or datetime, optional): Only include data recorded before this time (ISO8601)
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
            # Convert datetime objects to ISO8601 strings if needed
            if recorded_after is not None and isinstance(recorded_after, datetime):
                recorded_after = recorded_after.isoformat()
            if recorded_before is not None and isinstance(recorded_before, datetime):
                recorded_before = recorded_before.isoformat()
            
            # Build the base query - only return valid records
            query = f"""
                SELECT id, path, recorded_at, data, valid
                FROM {self.base_table}
                WHERE path = ? AND valid = 1
            """
            
            params = [path]
            
            # Add optional time-based filters
            if recorded_after is not None:
                query += " AND recorded_at >= ?"
                params.append(recorded_after)
                
            if recorded_before is not None:
                query += " AND recorded_at <= ?"
                params.append(recorded_before)
                
            # Add ordering
            query += f" ORDER BY recorded_at {order.upper()}"
            
            # Add optional pagination
            if limit is not None and limit > 0:
                query += " LIMIT ?"
                params.append(limit)
                
            if offset > 0:
                query += " OFFSET ?"
                params.append(offset)
                
            rows = self._execute_query(query, params)
            
            # Convert to list of dicts and parse JSON data
            result_list = []
            for row in rows:
                row_dict = dict(row)
                if isinstance(row_dict.get('data'), str):
                    row_dict['data'] = json.loads(row_dict['data'])
                row_dict['valid'] = bool(row_dict['valid'])
                result_list.append(row_dict)
            
            return result_list
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error listing stream data for path '{path}': {str(e)}")
    
    def get_stream_data_range(self, path, start_time, end_time):
        """
        Get valid stream data within a specific time range.
        Only returns records where valid=1.
        
        Args:
            path (str): The path to search for in LTREE format
            start_time (str or datetime): Start of the time range (ISO8601 format)
            end_time (str or datetime): End of the time range (ISO8601 format)
            
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
        
        # Convert datetime objects to ISO8601 strings if needed
        if isinstance(start_time, datetime):
            start_time = start_time.isoformat()
        if isinstance(end_time, datetime):
            end_time = end_time.isoformat()
        
        if start_time >= end_time:
            raise ValueError("start_time must be before end_time")
        
        try:
            query = f"""
                SELECT id, path, recorded_at, data, valid
                FROM {self.base_table}
                WHERE path = ?
                AND recorded_at >= ?
                AND recorded_at <= ?
                AND valid = 1
                ORDER BY recorded_at ASC
            """
            
            results = self._execute_query(query, (path, start_time, end_time))
            
            # Convert to list of dicts and parse JSON data
            result_list = []
            for row in results:
                row_dict = dict(row)
                if isinstance(row_dict.get('data'), str):
                    row_dict['data'] = json.loads(row_dict['data'])
                row_dict['valid'] = bool(row_dict['valid'])
                result_list.append(row_dict)
            
            return result_list
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error getting stream data range for path '{path}': {str(e)}")
    
    def get_stream_statistics(self, path, include_invalid=False):
        """
        Get comprehensive statistics for stream data at a given path.
        By default, only considers valid records unless include_invalid=True.
        
        Note: SQLite doesn't have LAG window function in all versions, so we compute
        average intervals differently using a self-join approach.
        
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
                # Basic stats query
                stats_query = f"""
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN valid = 1 THEN 1 ELSE 0 END) as valid_records,
                        SUM(CASE WHEN valid = 0 THEN 1 ELSE 0 END) as invalid_records,
                        MIN(CASE WHEN valid = 1 THEN recorded_at END) as earliest_valid_recorded,
                        MAX(CASE WHEN valid = 1 THEN recorded_at END) as latest_valid_recorded,
                        MIN(recorded_at) as earliest_recorded_overall,
                        MAX(recorded_at) as latest_recorded_overall
                    FROM {self.base_table}
                    WHERE path = ?
                """
                result = self._execute_single(stats_query, (path,))
                
                if result is None or result['total_records'] == 0:
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
                
                # Compute average intervals using self-join
                # For all records
                interval_query_all = f"""
                    SELECT AVG(
                        (julianday(t1.recorded_at) - julianday(t2.recorded_at)) * 86400.0
                    ) as avg_seconds
                    FROM {self.base_table} t1
                    JOIN {self.base_table} t2 ON t1.path = t2.path
                    WHERE t1.path = ?
                    AND t1.id > t2.id
                    AND t1.id = (
                        SELECT MIN(id) FROM {self.base_table} 
                        WHERE path = t1.path AND id > t2.id
                    )
                """
                interval_result_all = self._execute_single(interval_query_all, (path,))
                avg_interval_all = interval_result_all['avg_seconds'] if interval_result_all else None
                
                # For valid records only
                interval_query_valid = f"""
                    SELECT AVG(
                        (julianday(t1.recorded_at) - julianday(t2.recorded_at)) * 86400.0
                    ) as avg_seconds
                    FROM {self.base_table} t1
                    JOIN {self.base_table} t2 ON t1.path = t2.path
                    WHERE t1.path = ?
                    AND t1.valid = 1 AND t2.valid = 1
                    AND t1.id > t2.id
                    AND t1.id = (
                        SELECT MIN(id) FROM {self.base_table} 
                        WHERE path = t1.path AND id > t2.id AND valid = 1
                    )
                """
                interval_result_valid = self._execute_single(interval_query_valid, (path,))
                avg_interval_valid = interval_result_valid['avg_seconds'] if interval_result_valid else None
                
                result_dict = dict(result)
                result_dict['avg_interval_seconds_all'] = avg_interval_all
                result_dict['avg_interval_seconds_valid'] = avg_interval_valid
                
                return result_dict
            else:
                # Stats for valid records only
                stats_query = f"""
                    SELECT 
                        COUNT(*) as valid_records,
                        MIN(recorded_at) as earliest_recorded,
                        MAX(recorded_at) as latest_recorded
                    FROM {self.base_table}
                    WHERE path = ? AND valid = 1
                """
                result = self._execute_single(stats_query, (path,))
                
                if result is None or result['valid_records'] == 0:
                    return {
                        'valid_records': 0,
                        'earliest_recorded': None,
                        'latest_recorded': None,
                        'avg_interval_seconds': None
                    }
                
                # Compute average interval for valid records
                interval_query = f"""
                    SELECT AVG(
                        (julianday(t1.recorded_at) - julianday(t2.recorded_at)) * 86400.0
                    ) as avg_seconds
                    FROM {self.base_table} t1
                    JOIN {self.base_table} t2 ON t1.path = t2.path
                    WHERE t1.path = ?
                    AND t1.valid = 1 AND t2.valid = 1
                    AND t1.id > t2.id
                    AND t1.id = (
                        SELECT MIN(id) FROM {self.base_table} 
                        WHERE path = t1.path AND id > t2.id AND valid = 1
                    )
                """
                interval_result = self._execute_single(interval_query, (path,))
                avg_interval = interval_result['avg_seconds'] if interval_result else None
                
                result_dict = dict(result)
                result_dict['avg_interval_seconds'] = avg_interval
                
                return result_dict
            
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
                SELECT id, path, recorded_at, data, valid
                FROM {self.base_table}
                WHERE id = ?
            """
            
            result = self._execute_single(query, (record_id,))
            if result:
                result_dict = dict(result)
                if isinstance(result_dict.get('data'), str):
                    result_dict['data'] = json.loads(result_dict['data'])
                result_dict['valid'] = bool(result_dict['valid'])
                return result_dict
            return None
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error retrieving stream record with id {record_id}: {str(e)}")

