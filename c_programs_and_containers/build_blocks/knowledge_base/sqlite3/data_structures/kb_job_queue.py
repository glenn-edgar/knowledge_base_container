import time
import json
import sqlite3
from datetime import datetime, timezone


class KB_Job_Queue:
    """
    A class to handle job queue operations for the knowledge base.
    Always returns dictionaries with field names instead of tuples.
    Performance-optimized with proper error handling and dynamic table names.
    SQLite version.
    
    Table Schema:
    - id INTEGER PRIMARY KEY AUTOINCREMENT
    - path TEXT
    - schedule_at TEXT (ISO8601 format)
    - started_at TEXT (ISO8601 format)
    - completed_at TEXT (ISO8601 format)
    - is_active INTEGER (0 or 1, default 0)
    - valid INTEGER (0 or 1, default 0)
    - data TEXT (JSON)
    """
    
    def __init__(self, kb_search, database):
        """
        Initialize the KB_Job_Queue object.
        
        Args:
            kb_search: An instance of KB_Search class (SQLite version)
            database (str): The base database name
        """
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        # Set row factory to return dictionary-like results
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.base_table = f"{database}_job"
    
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
    
    def find_job_id(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find a single job id for given parameters. Raises error if 0 or multiple jobs found.
        
        Args:
            kb (str, optional): Knowledge base name
            node_name (str, optional): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            dict: Single matching job record with field names
            
        Raises:
            ValueError: If no job or multiple jobs found
        """
        results = self.find_job_ids(kb, node_name, properties, node_path)
        
        if len(results) == 0:
            raise ValueError(f"No job found matching parameters: name={node_name}, properties={properties}, path={node_path}")
        if len(results) > 1:
            raise ValueError(f"Multiple jobs ({len(results)}) found matching parameters: name={node_name}, properties={properties}, path={node_path}")
        
        return results[0]
    
    def find_job_ids(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find all job ids matching the given parameters.
        
        Args:
            kb (str, optional): Knowledge base name
            node_name (str, optional): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            list: List of matching job records as dictionaries
            
        Raises:
            ValueError: If no jobs found
        """
        try:
            # Clear previous filters and build new query
            self.kb_search.clear_filters()
            self.kb_search.search_label("KB_JOB_QUEUE")
            
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
                raise ValueError(f"No jobs found matching parameters: name={node_name}, properties={properties}, path={node_path}")
            
            return node_ids
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error finding job IDs: {str(e)}")
    
    def find_job_paths(self, table_dict_rows):
        """
        Extract path values from job query results.
        
        Args:
            table_dict_rows (list): List of result dictionaries
            
        Returns:
            list: List of path values
        """
        if not table_dict_rows:
            return []
        
        return_values = []
        for row in table_dict_rows:
            # Row will be a sqlite3.Row object with dictionary-like access
            path = row['path'] if 'path' in row.keys() else None
            if path is not None:
                return_values.append(path)
        
        return return_values
    
    def get_queued_number(self, path):
        """
        Count the number of job entries where valid is true (1) for a given path.
        
        Args:
            path (str): The path to search for in LTREE format
            
        Returns:
            int: Number of valid jobs for the given path
            
        Raises:
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            query = f"""
                SELECT COUNT(*) as count
                FROM {self.base_table}
                WHERE path = ?
                AND valid = 1
            """
            
            result = self._execute_single(query, (path,))
            return result['count'] if result else 0
            
        except Exception as e:
            raise Exception(f"Error counting queued jobs for path '{path}': {str(e)}")
        
    def get_free_number(self, path):
        """
        Count the number of job entries where valid is false (0) for a given path.
        
        Args:
            path (str): The path to search for in LTREE format
            
        Returns:
            int: Number of invalid jobs for the given path
            
        Raises:
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            query = f"""
                SELECT COUNT(*) as count
                FROM {self.base_table}
                WHERE path = ?
                AND valid = 0
            """
            
            result = self._execute_single(query, (path,))
            return result['count'] if result else 0
        
        except Exception as e:
            raise Exception(f"Error counting free jobs for path '{path}': {str(e)}")
        
    def peak_job_data(self, path, max_retries=3, retry_delay=1):
        """
        Find the job with the earliest schedule_at time for a given path where 
        valid is true and is_active is false, update its started_at timestamp to current time,
        set is_active to true, and return the job information.
        
        Note: SQLite doesn't support FOR UPDATE SKIP LOCKED. This uses BEGIN IMMEDIATE.

        Args:
            path (str): The path to search for in LTREE format
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries

        Returns:
            dict/None: Dictionary containing job info with keys (id, data, schedule_at) or None if no jobs found
            
        Raises:
            ValueError: If path is invalid
            RuntimeError: If unable to obtain lock after max_retries
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        attempt = 0
        while attempt < max_retries:
            try:
                # Begin immediate transaction for write lock
                self.cursor.execute("BEGIN IMMEDIATE")
                
                # Get current timestamp
                current_timestamp = datetime.now(timezone.utc).isoformat()
                
                # Find eligible job
                # SQLite sorts NULL values differently - handle with COALESCE or separate logic
                find_query = f"""
                    SELECT id, data, schedule_at
                    FROM {self.base_table}
                    WHERE path = ?
                        AND valid = 1
                        AND is_active = 0
                        AND (schedule_at IS NULL OR schedule_at <= ?)
                    ORDER BY 
                        CASE WHEN schedule_at IS NULL THEN 0 ELSE 1 END,  -- NULL first
                        schedule_at ASC
                    LIMIT 1
                """
                result = self._execute_single(find_query, (path, current_timestamp))

                if result is None:
                    self.conn.rollback()
                    return None

                job_id = result['id']

                # Update with additional safety check
                update_query = f"""
                    UPDATE {self.base_table}
                    SET started_at = ?,
                        is_active = 1
                    WHERE id = ?
                        AND is_active = 0
                        AND valid = 1
                    RETURNING id, started_at
                """
                update_result = self._execute_single(update_query, (current_timestamp, job_id))

                if update_result is None:
                    # Job state changed between SELECT and UPDATE
                    self.conn.rollback()
                    attempt += 1
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    continue

                self.conn.commit()
                
                # Parse JSON data if stored as string
                data = result['data']
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError:
                        pass
                
                return {
                    'id': result['id'],
                    'data': data,
                    'schedule_at': result['schedule_at'],
                    'started_at': update_result['started_at']
                }

            except sqlite3.OperationalError as e:
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    attempt += 1
                    if attempt < max_retries:
                        time.sleep(retry_delay * (1.5 ** attempt))  # Exponential backoff
                    else:
                        raise RuntimeError(
                            f"Could not lock and claim a job for path='{path}' after {max_retries} retries"
                        )
                else:
                    raise Exception(f"Database error peeking job data for path '{path}': {str(e)}")

            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error peeking job data for path '{path}': {str(e)}")

        raise RuntimeError(
            f"Could not lock and claim a job for path='{path}' after {max_retries} retries"
        )
    
    def mark_job_completed(self, job_id, max_retries=3, retry_delay=1.0):
        """
        For a record matching the given id, set completed_at to current time,
        set valid to FALSE (0), and set is_active to FALSE (0). Protects against
        parallel transactions with retries.

        Args:
            job_id (int): The ID of the job record
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries

        Returns:
            dict: Dictionary with success status and job info
            
        Raises:
            ValueError: If job_id is invalid
            Exception: If no matching record is found
            RuntimeError: If unable to obtain lock after max_retries
        """
        if not job_id or not isinstance(job_id, int):
            raise ValueError("job_id must be a valid integer")
        
        attempt = 0
        while attempt < max_retries:
            try:
                # Begin immediate transaction for write lock
                self.cursor.execute("BEGIN IMMEDIATE")
                
                # Check if row exists
                lock_query = f"""
                    SELECT id
                    FROM {self.base_table}
                    WHERE id = ?
                """
                row = self._execute_single(lock_query, (job_id,))

                if not row:
                    self.conn.rollback()
                    raise Exception(f"No job found with id={job_id}")

                # Get current timestamp
                current_timestamp = datetime.now(timezone.utc).isoformat()

                # Perform the update to mark completion
                update_query = f"""
                    UPDATE {self.base_table}
                    SET completed_at = ?,
                        valid = 0,
                        is_active = 0
                    WHERE id = ?
                    RETURNING id, completed_at
                """
                result = self._execute_single(update_query, (current_timestamp, job_id))
                
                if not result:
                    self.conn.rollback()
                    raise Exception(f"Failed to mark job {job_id} as completed")

                self.conn.commit()
                return {
                    'success': True,
                    'job_id': result['id'],
                    'completed_at': result['completed_at']
                }

            except sqlite3.OperationalError as e:
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    attempt += 1
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
                        raise RuntimeError(f"Could not lock job id={job_id} after {max_retries} attempts")
                else:
                    raise Exception(f"Database error marking job {job_id} as completed: {str(e)}")

            except Exception as e:
                self.conn.rollback()
                if "No job found" in str(e) or "Failed to mark job" in str(e):
                    raise
                raise Exception(f"Error marking job {job_id} as completed: {str(e)}")

        raise RuntimeError(f"Could not lock job id={job_id} after {max_retries} attempts")
    
    def push_job_data(self, path, data, max_retries=3, retry_delay=1):
        """
        Find an available record (valid=0/False) for the given path with the earliest completed_at time,
        update it with new data, and prepare it for scheduling.

        Args:
            path (str): The path in LTREE format
            data (dict): The JSON data to insert
            max_retries (int): Maximum number of retries in case of lock conflicts
            retry_delay (float): Delay in seconds between retries

        Returns:
            dict: Dictionary containing the updated job information
            
        Raises:
            ValueError: If inputs are invalid
            Exception: If no available record is found or if locks aren't obtained after retries
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")

        for attempt in range(1, max_retries + 1):
            try:
                # Begin immediate transaction for write lock
                self.cursor.execute("BEGIN IMMEDIATE")
                
                # Find an available slot
                select_sql = f"""
                    SELECT id
                    FROM {self.base_table}
                    WHERE path = ?
                    AND valid = 0
                    ORDER BY completed_at ASC
                    LIMIT 1
                """
                
                row = self._execute_single(select_sql, (path,))
                
                if not row:
                    self.conn.rollback()
                    raise Exception(f"No available job slot for path '{path}'")

                job_id = row['id']
                
                # Get current timestamp
                current_timestamp = datetime.now(timezone.utc).isoformat()
                
                # Update the record
                update_sql = f"""
                    UPDATE {self.base_table}
                    SET data = ?,
                        schedule_at = ?,
                        started_at  = ?,
                        completed_at = ?,
                        valid = 1,
                        is_active = 0
                    WHERE id = ?
                    RETURNING id, schedule_at, data
                """
                
                updated = self._execute_single(update_sql, (
                    json.dumps(data, separators=(',', ':')),
                    current_timestamp,
                    current_timestamp,
                    current_timestamp,
                    job_id
                ))
                
                if not updated:
                    self.conn.rollback()
                    raise Exception(f"Failed to update job slot for path '{path}'")
                
                self.conn.commit()
                
                # Parse JSON data if needed
                returned_data = updated['data']
                if isinstance(returned_data, str):
                    try:
                        returned_data = json.loads(returned_data)
                    except json.JSONDecodeError:
                        pass
                
                return {
                    'job_id': updated['id'],
                    'schedule_at': updated['schedule_at'],
                    'data': returned_data
                }

            except sqlite3.OperationalError as e:
                self.conn.rollback()
                if "database is locked" in str(e).lower():
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise Exception(f"Could not acquire lock for path '{path}' after {max_retries} attempts")
                else:
                    raise Exception(f"Database error pushing job data for path '{path}': {str(e)}")
                    
            except Exception as e:
                self.conn.rollback()
                if isinstance(e, ValueError):
                    raise
                raise Exception(f"Error pushing job data for path '{path}': {str(e)}")
        
    def list_pending_jobs(self, path, limit=None, offset=0):
        """
        List all jobs for a given path where valid is True (1) and is_active is False (0),
        ordered by schedule_at with earliest first.
        
        Args:
            path (str): The path to search for in LTREE format
            limit (int, optional): Maximum number of jobs to return
            offset (int, optional): Number of jobs to skip
        
        Returns:
            list: A list of dictionaries containing all job details with field names
            
        Raises:
            ValueError: If path is invalid
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            # Build the query with optional LIMIT and OFFSET
            query = f"""
                SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
                FROM {self.base_table}
                WHERE path = ?
                AND valid = 1
                AND is_active = 0
                ORDER BY schedule_at ASC
            """
            
            params = [path]
            
            if limit is not None and limit > 0:
                query += " LIMIT ?"
                params.append(limit)
                
            if offset > 0:
                query += " OFFSET ?"
                params.append(offset)
            
            results = self._execute_query(query, params)
            
            # Convert to list of dicts and parse JSON data
            result_list = []
            for row in results:
                row_dict = dict(row)
                if isinstance(row_dict.get('data'), str):
                    try:
                        row_dict['data'] = json.loads(row_dict['data'])
                    except json.JSONDecodeError:
                        pass
                result_list.append(row_dict)
            
            return result_list
        
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error listing pending jobs for path '{path}': {str(e)}")
        
    def list_active_jobs(self, path, limit=None, offset=0):
        """
        List all jobs for a given path where valid is True (1) and is_active is True (1),
        ordered by started_at with earliest first.
        
        Args:
            path (str): The path to search for in LTREE format
            limit (int, optional): Maximum number of jobs to return
            offset (int, optional): Number of jobs to skip
        
        Returns:
            list: A list of dictionaries containing all job details with field names
            
        Raises:
            ValueError: If path is invalid
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            # Build the query with optional LIMIT and OFFSET
            query = f"""
                SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
                FROM {self.base_table}
                WHERE path = ?
                AND valid = 1
                AND is_active = 1
                ORDER BY started_at ASC
            """
            
            params = [path]
            
            if limit is not None and limit > 0:
                query += " LIMIT ?"
                params.append(limit)
                
            if offset > 0:
                query += " OFFSET ?"
                params.append(offset)
            
            results = self._execute_query(query, params)
            
            # Convert to list of dicts and parse JSON data
            result_list = []
            for row in results:
                row_dict = dict(row)
                if isinstance(row_dict.get('data'), str):
                    try:
                        row_dict['data'] = json.loads(row_dict['data'])
                    except json.JSONDecodeError:
                        pass
                result_list.append(row_dict)
            
            return result_list
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error listing active jobs for path '{path}': {str(e)}")
        
    def clear_job_queue(self, path):
        """
        Clear all jobs for a given path by marking them as completed and invalid.
        
        Args:
            path (str): The path to clear jobs for
            
        Returns:
            dict: Dictionary with results including count of cleared jobs
            
        Raises:
            ValueError: If path is invalid
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            # Begin immediate transaction for exclusive access
            self.cursor.execute("BEGIN IMMEDIATE")
            
            # Get current timestamp
            current_timestamp = datetime.now(timezone.utc).isoformat()
            
            # Update all jobs for this path
            update_query = f"""
                UPDATE {self.base_table}
                SET schedule_at = ?,
                    started_at = ?,
                    completed_at = ?,
                    is_active = 0,
                    valid = 0,
                    data = ?
                WHERE path = ?
                RETURNING id, completed_at
            """
            
            # Execute the update
            self.cursor.execute(update_query, (
                current_timestamp,
                current_timestamp,
                current_timestamp,
                '{}',
                path
            ))
            
            results = self.cursor.fetchall()
            results_list = [dict(row) for row in results]
            
            # Commit the transaction
            self.conn.commit()
            
            return {
                'success': True,
                'cleared_count': len(results_list),
                'cleared_jobs': results_list
            }
            
        except Exception as e:
            # Rollback in case of error
            try:
                self.conn.rollback()
            except:
                pass
            
            if isinstance(e, ValueError):
                raise
            
            error_msg = f"Error in clear_job_queue for path '{path}': {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
            
    
    def get_job_statistics(self, path):
        """
        Get comprehensive statistics for jobs at a given path.
        
        Args:
            path (str): The path to get statistics for
            
        Returns:
            dict: Dictionary containing various job statistics with field names
            
        Raises:
            ValueError: If path is invalid
            Exception: If there's an error executing the query
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            # SQLite version using julianday for time calculations
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN valid = 1 AND is_active = 0 THEN 1 ELSE 0 END) as pending_jobs,
                    SUM(CASE WHEN valid = 1 AND is_active = 1 THEN 1 ELSE 0 END) as active_jobs,
                    SUM(CASE WHEN valid = 0 THEN 1 ELSE 0 END) as completed_jobs,
                    MIN(schedule_at) as earliest_scheduled,
                    MAX(completed_at) as latest_completed,
                    AVG(
                        (julianday(completed_at) - julianday(started_at)) * 86400.0
                    ) as avg_processing_time_seconds
                FROM {self.base_table}
                WHERE path = ?
            """
            
            result = self._execute_single(stats_query, (path,))
            
            if result is None or result['total_jobs'] == 0:
                return {
                    'total_jobs': 0,
                    'pending_jobs': 0,
                    'active_jobs': 0,
                    'completed_jobs': 0,
                    'earliest_scheduled': None,
                    'latest_completed': None,
                    'avg_processing_time_seconds': None
                }
            
            return dict(result)
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error getting job statistics for path '{path}': {str(e)}")
    
    def get_job_by_id(self, job_id):
        """
        Retrieve a specific job by its ID.
        
        Args:
            job_id (int): The ID of the job to retrieve
            
        Returns:
            dict or None: Dictionary containing job details with field names, or None if not found
            
        Raises:
            ValueError: If job_id is invalid
            Exception: If there's an error executing the query
        """
        if not job_id or not isinstance(job_id, int):
            raise ValueError("job_id must be a valid integer")
        
        try:
            query = f"""
                SELECT id, path, schedule_at, started_at, completed_at, is_active, valid, data
                FROM {self.base_table}
                WHERE id = ?
            """
            
            result = self._execute_single(query, (job_id,))
            
            if result:
                result_dict = dict(result)
                # Parse JSON data if stored as string
                if isinstance(result_dict.get('data'), str):
                    try:
                        result_dict['data'] = json.loads(result_dict['data'])
                    except json.JSONDecodeError:
                        pass
                return result_dict
            
            return None
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error retrieving job with id {job_id}: {str(e)}")
    
    def close(self):
        """
        Close the job queue's cursor (the connection remains open for kb_search).
        """
        if self.cursor and self.cursor != self.kb_search.cursor:
            self.cursor.close()

