import time
import json
import psycopg2
from psycopg2 import errors, sql
from psycopg2.extras import RealDictCursor


class KB_Job_Queue:
    """
    A class to handle job queue operations for the knowledge base.
    Always returns dictionaries with field names instead of tuples.
    Performance-optimized with proper error handling and dynamic table names.
    
    Table Schema:
    - id SERIAL PRIMARY KEY
    - path LTREE
    - schedule_at TIMESTAMPTZ DEFAULT NOW()
    - started_at TIMESTAMPTZ DEFAULT NOW()
    - completed_at TIMESTAMPTZ DEFAULT NOW()
    - is_active BOOLEAN DEFAULT FALSE
    - valid BOOLEAN DEFAULT FALSE
    - data JSONB
    """
    
    def __init__(self, kb_search, database):
        """
        Initialize the KB_Job_Queue object.
        
        Args:
            kb_search: An instance of KB_Search class
            database (str): The base database name
        """
        self.kb_search = kb_search
        self.conn = self.kb_search.conn
        # Create our own cursor with RealDictCursor to ensure dictionary results
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
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
            # Since we always use RealDictCursor, row will always be a dictionary
            path = row.get('path')
            if path is not None:
                return_values.append(path)
        
        return return_values
    
    def get_queued_number(self, path):
        """
        Count the number of job entries where valid is true for a given path.
        
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
                WHERE path = %s
                AND valid = TRUE
            """
            
            result = self._execute_single(query, (path,))
            return result['count'] if result else 0
            
        except Exception as e:
            raise Exception(f"Error counting queued jobs for path '{path}': {str(e)}")
        
    def get_free_number(self, path):
        """
        Count the number of job entries where valid is false for a given path.
        
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
                WHERE path = %s
                AND valid = FALSE
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
                # Use NOWAIT for immediate lock failure (optional - current SKIP LOCKED is good)
                find_query = f"""
                    SELECT id, data, schedule_at
                    FROM {self.base_table}
                    WHERE path = %s
                        AND valid = TRUE
                        AND is_active = FALSE
                        AND (schedule_at IS NULL OR schedule_at <= NOW())  -- Don't claim future jobs
                    ORDER BY schedule_at ASC NULLS FIRST  -- Handle NULL schedule_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                """
                result = self._execute_single(find_query, (path,))

                if result is None:
                    self.conn.rollback()
                    return None

                job_id = result['id']

                # Update with additional safety check
                update_query = f"""
                    UPDATE {self.base_table}
                    SET started_at = NOW(),
                        is_active = TRUE
                    WHERE id = %s
                        AND is_active = FALSE  -- Double-check it's still available
                        AND valid = TRUE       -- Ensure it wasn't invalidated
                    RETURNING id, started_at
                """
                update_result = self._execute_single(update_query, (job_id,))

                if update_result is None:
                    # Job state changed between SELECT and UPDATE
                    self.conn.rollback()
                    attempt += 1
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    continue

                self.conn.commit()
                return {
                    'id': result['id'],
                    'data': result['data'],
                    'schedule_at': result['schedule_at'],
                    'started_at': update_result.get('started_at')  # Include when it was started
                }

            except errors.LockNotAvailable:
                self.conn.rollback()
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay * (1.5 ** attempt))  # Exponential backoff

            except (OperationalError, InterfaceError) as e:
                # Handle transient connection errors
                self.conn.rollback()
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)
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
        set valid to FALSE, and set is_active to FALSE. Protects against
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
                # Begin transaction and try to lock the specific row
                lock_query = f"""
                    SELECT id
                      FROM {self.base_table}
                     WHERE id = %s
                     FOR UPDATE NOWAIT
                """
                row = self._execute_single(lock_query, (job_id,))

                # if no row, nothing to complete
                if not row:
                    self.conn.rollback()
                    raise Exception(f"No job found with id={job_id}")

                # perform the update to mark completion
                update_query = f"""
                    UPDATE {self.base_table}
                       SET completed_at = NOW(),
                           valid        = FALSE,
                           is_active    = FALSE
                     WHERE id = %s
                     RETURNING id, completed_at
                """
                result = self._execute_single(update_query, (job_id,))
                
                if not result:
                    self.conn.rollback()
                    raise Exception(f"Failed to mark job {job_id} as completed")

                # commit and return success
                self.conn.commit()
                return {
                    'success': True,
                    'job_id': result['id'],
                    'completed_at': result['completed_at']
                }

            except errors.LockNotAvailable:
                # another transaction holds the lock: rollback and retry
                self.conn.rollback()
                attempt += 1
                if attempt < max_retries:
                    time.sleep(retry_delay)

            except Exception as e:
                # rollback and propagate any other error
                self.conn.rollback()
                if "No job found" in str(e) or "Failed to mark job" in str(e):
                    raise
                raise Exception(f"Error marking job {job_id} as completed: {str(e)}")

        # if we exhaust retries without locking, raise an error
        raise RuntimeError(f"Could not lock job id={job_id} after {max_retries} attempts")
    
    def push_job_data(self, path, data, max_retries=3, retry_delay=1):
        """
        Find an available record (valid=False) for the given path with the earliest completed_at time,
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

        select_sql = f"""
            SELECT id
            FROM {self.base_table}
            WHERE path = %s
            AND valid = FALSE
            ORDER BY completed_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """
        
        update_sql = f"""
            UPDATE {self.base_table}
            SET data = %s,
                schedule_at = timezone('UTC', now()),
                started_at  = timezone('UTC', now()),
                completed_at= timezone('UTC', now()),
                valid      = TRUE,
                is_active  = FALSE
            WHERE id = %s
            RETURNING id, schedule_at, data
        """

        for attempt in range(1, max_retries + 1):
            try:
                # try to grab a row
                row = self._execute_single(select_sql, (path,))
                
                if not row:
                    self.conn.rollback()
                    raise Exception(f"No available job slot for path '{path}'")

                job_id = row['id']
                
                # update it
                updated = self._execute_single(update_sql, (json.dumps(data, separators=(',', ':')), job_id))
                
                if not updated:
                    self.conn.rollback()
                    raise Exception(f"Failed to update job slot for path '{path}'")
                
                self.conn.commit()
                
                return {
                    'job_id': updated['id'],
                    'schedule_at': updated['schedule_at'],
                    'data': updated['data']
                }

            except (psycopg2.OperationalError, psycopg2.errors.LockNotAvailable) as e:
                self.conn.rollback()
                # Lock not available → retry
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise Exception(f"Could not acquire lock for path '{path}' after {max_retries} attempts") from e
            except Exception as e:
                self.conn.rollback()
                if isinstance(e, ValueError):
                    raise
                raise Exception(f"Error pushing job data for path '{path}': {str(e)}")
        
    def list_pending_jobs(self, path, limit=None, offset=0):
        """
        List all jobs for a given path where valid is True and is_active is False,
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
                WHERE path = %s
                AND valid = TRUE
                AND is_active = FALSE
                ORDER BY schedule_at ASC
            """
            
            params = [path]
            
            if limit is not None and limit > 0:
                query += " LIMIT %s"
                params.append(limit)
                
            if offset > 0:
                query += " OFFSET %s"
                params.append(offset)
                
            return self._execute_query(query, params)
        
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error listing pending jobs for path '{path}': {str(e)}")
        
    def list_active_jobs(self, path, limit=None, offset=0):
        """
        List all jobs for a given path where valid is True and is_active is True,
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
                WHERE path = %s
                AND valid = TRUE
                AND is_active = TRUE
                ORDER BY started_at ASC
            """
            
            params = [path]
            
            if limit is not None and limit > 0:
                query += " LIMIT %s"
                params.append(limit)
                
            if offset > 0:
                query += " OFFSET %s"
                params.append(offset)
                
            return self._execute_query(query, params)
            
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
            # First acquire a lock on the table to prevent concurrent access
            self.cursor.execute(f"LOCK TABLE {self.base_table} IN EXCLUSIVE MODE;")
            
            # Prepare the query to update only rows with matching path
            update_query = f"""
                UPDATE {self.base_table}
                SET schedule_at = NOW(),
                    started_at = NOW(),
                    completed_at = NOW(),
                    is_active = %s,
                    valid = %s,
                    data = %s
                WHERE path = %s
                RETURNING id, completed_at;
            """
            
            # Execute the update with our parameters
            results = self._execute_query(update_query, (False, False, '{}', path))
            
            # Commit the transaction which will automatically release all locks
            self.conn.commit()
            
            return {
                'success': True,
                'cleared_count': len(results),
                'cleared_jobs': results
            }
            
        except Exception as e:
            # Rollback in case of error, which will also release the lock
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
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_jobs,
                    COUNT(CASE WHEN valid = TRUE AND is_active = FALSE THEN 1 END) as pending_jobs,
                    COUNT(CASE WHEN valid = TRUE AND is_active = TRUE THEN 1 END) as active_jobs,
                    COUNT(CASE WHEN valid = FALSE THEN 1 END) as completed_jobs,
                    MIN(schedule_at) as earliest_scheduled,
                    MAX(completed_at) as latest_completed,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_processing_time_seconds
                FROM {self.base_table}
                WHERE path = %s
            """
            
            result = self._execute_single(stats_query, (path,))
            
            if result is None:
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
                WHERE id = %s
            """
            
            return self._execute_single(query, (job_id,))
            
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


#