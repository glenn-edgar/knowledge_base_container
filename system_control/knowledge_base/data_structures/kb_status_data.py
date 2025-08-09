from typing import Dict, Any, Tuple,Union,List
from psycopg2 import sql, OperationalError, InterfaceError
import json
import time

class KB_Status_Data:
    """
    A class to handle the status data for the knowledge base.
    Performance-optimized with proper error handling and dynamic table names.
    """
    
    def __init__(self, kb_search, database):
        """
        Initialize the KB_Status_Data object.
        
        Args:
            kb_search: An instance of KB_Search class
            database (str): The base database name
        """
        self.kb_search = kb_search
        self.base_table = f"{database}_status"
    
    def find_node_id(self, kb, node_name, properties=None, node_path=None):
        """
        Find a single node id for given parameters. Raises error if 0 or multiple nodes found.
        
        Args:
            kb (str): Knowledge base name
            
            node_name (str): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            dict: Single matching node record
            
        Raises:
            ValueError: If no node or multiple nodes found
        """
        
        
        results = self.find_node_ids(kb, node_name, properties, node_path)
        
        if len(results) == 0:
            raise ValueError(f"No node found matching parameters: kb={kb}, name={node_name}, properties={properties}, path={node_path}")
        if len(results) > 1:
            raise ValueError(f"Multiple nodes ({len(results)}) found matching parameters: kb={kb}, name={node_name}, properties={properties}, path={node_path}")
        
        return results[0]
    
    def find_node_ids(self, kb=None,  node_name=None, properties=None, node_path=None):
        """
        Find all node ids matching the given parameters.
        
        Args:
            kb (str, optional): Knowledge base name
            node_name (str, optional): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            list: List of matching node records
            
        Raises:
            ValueError: If no nodes found
        """
        
        
        try:
            # Clear previous filters and build new query
            self.kb_search.clear_filters()
            self.kb_search.search_label("KB_STATUS_FIELD")
            
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
                raise ValueError(f"No nodes found matching parameters: kb={kb}, name={node_name}, properties={properties}, path={node_path}")
            
            return node_ids
            
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise Exception(f"Error finding node IDs: {str(e)}")
    
 
    def get_status_data(self, path):
        """
        Retrieve status data for a given path with performance optimization.
        
        Args:
            path (str): The path to search for
            
        Returns:
            tuple: (data_dict, path) where data_dict is a Python dictionary
            
        Raises:
            Exception: If no data is found or a database error occurs
        """
        if not path:
            raise ValueError("Path cannot be empty or None")
        
        try:
            # Use parameterized query with the dynamic table name
            query = f"""
            SELECT data, path
            FROM {self.base_table}
            WHERE path = %s
            LIMIT 1
            """
            
            self.kb_search.cursor.execute(query, (path,))
            result = self.kb_search.cursor.fetchone()
            
            if result is None:
                raise Exception(f"No data found for path: {path}")
            
            # Extract data and path from result (handle both dict and tuple)
            if isinstance(result, dict):
                data = result['data']
                path_value = result['path']
            else:
                data, path_value = result
            
            # Handle JSON conversion if needed
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError as e:
                    raise Exception(f"Failed to decode JSON data for path '{path}': {str(e)}")
            
            return data, path_value
            
        except Exception as e:
            if str(e).startswith(("No data found", "Failed to decode", "Path cannot be")):
                raise
            raise Exception(f"Error retrieving status data for path '{path}': {str(e)}")
    
    def get_multiple_status_data(self, paths):
        """
        Performance-optimized method to retrieve status data for multiple paths in a single query.
        
        Args:
            paths (list): List of paths to search for
            
        Returns:
            dict: Dictionary mapping paths to their data
        """
        if not paths:
            return {}
        
        if not isinstance(paths, (list, tuple)):
            paths = [paths]
        
        try:
            # Use IN clause for efficient bulk retrieval
            placeholders = ','.join(['%s'] * len(paths))
            query = f"""
            SELECT data, path
            FROM {self.base_table}
            WHERE path IN ({placeholders})
            """
            
            self.kb_search.cursor.execute(query, tuple(paths))
            results = self.kb_search.cursor.fetchall()
            
            data_dict = {}
            for result in results:
                if isinstance(result, dict):
                    data = result['data']
                    path_value = result['path']
                else:
                    data, path_value = result
                
                # Handle JSON conversion if needed
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError:
                        # Log warning but continue with string data
                        print(f"Warning: Failed to decode JSON for path '{path_value}'")
                
                data_dict[path_value] = data
            
            return data_dict
            
        except Exception as e:
            raise Exception(f"Error retrieving multiple status data: {str(e)}")


    def set_status_data(self, path: str, data: Dict[str, Any], 
                    retry_count: int = 3, retry_delay: float = 1.0) -> Tuple[bool, str]:
        """
        Update status data for a given path with performance optimization using UPSERT.
        
        Args:
            path (str): The path to update
            data (dict): The data to store
            retry_count (int): Number of retries on failure (default: 3)
            retry_delay (float): Delay between retries in seconds (default: 1.0)
            
        Returns:
            tuple: (bool, str) indicating success/failure and a message
            
        Raises:
            ValueError: If inputs are invalid
            Exception: If the update fails after all retries
        """
        # Input validation
        if not path:
            raise ValueError("Path cannot be empty or None")
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")
        if retry_count < 0:
            raise ValueError("Retry count must be non-negative")
        if retry_delay < 0:
            raise ValueError("Retry delay must be non-negative")
        
        # Convert data to JSON string once, outside the retry loop
        json_data = json.dumps(data, separators=(',', ':'))
        
        # Prepare the query
        upsert_query = sql.SQL("""
            INSERT INTO {table} (path, data)
            VALUES (%s, %s)
            ON CONFLICT (path)
            DO UPDATE SET data = EXCLUDED.data
            RETURNING path, (xmax = 0) AS was_inserted
        """).format(
            table=sql.Identifier(self.base_table)
        )
        
        last_error = None
        attempt = 0
        
        while attempt <= retry_count:
            try:
                self.kb_search.cursor.execute(upsert_query, (path, json_data))
                result = self.kb_search.cursor.fetchone()
                
                # Commit the transaction
                self.kb_search.conn.commit()
                
                if result is not None:
                    if isinstance(result, dict):
                        returned_path = result['path']
                        was_inserted = result['was_inserted']
                    else:
                        returned_path, was_inserted = result
                    
                    operation = "inserted" if was_inserted else "updated"
                    return True, f"Successfully {operation} data for path: {returned_path}"
                else:
                    self.kb_search.conn.rollback()
                    raise Exception("Database operation completed but no result was returned")
                    
            except (OperationalError, InterfaceError) as e:
                # These are typically transient errors that may succeed on retry
                last_error = e
                
                try:
                    self.kb_search.conn.rollback()
                except Exception:
                    pass
                
                if attempt < retry_count:
                    time.sleep(retry_delay)
                    attempt += 1
                else:
                    break
                    
            except ValueError:
                # Don't retry on validation errors
                raise
                
            except Exception as e:
                # For other errors, attempt rollback but don't retry
                last_error = e
                
                try:
                    self.kb_search.conn.rollback()
                except Exception:
                    pass
                
                raise Exception(f"Error setting status data for path '{path}': {str(e)}")
        
        # If we've exhausted all retries
        error_msg = f"Failed to set status data for path '{path}' after {retry_count + 1} attempts"
        if last_error:
            error_msg += f": {str(last_error)}"
        
        raise Exception(error_msg)
    
    def set_multiple_status_data(self, path_data_pairs: Union[Dict[str, Dict], List[Tuple[str, Dict]]], 
                                retry_count: int = 3, retry_delay: float = 1.0) -> Tuple[bool, str, Dict]:
        """
        Performance-optimized method to update multiple path-data pairs in a single transaction.
        
        Args:
            path_data_pairs (dict or list): Dictionary of {path: data} or list of (path, data) tuples
            retry_count (int): Number of retries on failure (default: 3)
            retry_delay (float): Delay between retries in seconds (default: 1.0)
            
        Returns:
            tuple: (bool, str, dict) success flag, message, and results per path
        """
        if not path_data_pairs:
            raise ValueError("path_data_pairs cannot be empty")
        if retry_count < 0:
            raise ValueError("Retry count must be non-negative")
        if retry_delay < 0:
            raise ValueError("Retry delay must be non-negative")
        
        # Normalize input to list of tuples
        if isinstance(path_data_pairs, dict):
            pairs = list(path_data_pairs.items())
        else:
            pairs = list(path_data_pairs)
        
        # Validate all inputs first
        for path, data in pairs:
            if not path:
                raise ValueError(f"Path cannot be empty or None")
            if not isinstance(data, dict):
                raise ValueError(f"Data for path '{path}' must be a dictionary")
        
        # Pre-convert all data to JSON to avoid doing it multiple times on retries
        json_pairs = [(path, json.dumps(data, separators=(',', ':'))) for path, data in pairs]
        
        # Prepare the query with safe table name injection
        upsert_query = sql.SQL("""
            INSERT INTO {table} (path, data)
            VALUES (%s, %s)
            ON CONFLICT (path)
            DO UPDATE SET data = EXCLUDED.data
            RETURNING path, (xmax = 0) AS was_inserted
        """).format(
            table=sql.Identifier(self.base_table)
        )
        
        last_error = None
        attempt = 0
        
        while attempt <= retry_count:
            try:
                results = {}
                
                # Use batch operations within a transaction
                for path, json_data in json_pairs:
                    self.kb_search.cursor.execute(upsert_query, (path, json_data))
                    result = self.kb_search.cursor.fetchone()
                    
                    if result:
                        if isinstance(result, dict):
                            returned_path = result['path']
                            was_inserted = result['was_inserted']
                        else:
                            returned_path, was_inserted = result
                        
                        operation = "inserted" if was_inserted else "updated"
                        results[returned_path] = operation
                    else:
                        results[path] = "failed"
                
                # Commit all changes
                self.kb_search.conn.commit()
                
                success_count = len([r for r in results.values() if r != "failed"])
                return True, f"Successfully processed {success_count}/{len(pairs)} records", results
                
            except (OperationalError, InterfaceError) as e:
                # These are typically transient errors that may succeed on retry
                last_error = e
                
                try:
                    self.kb_search.conn.rollback()
                except Exception:
                    pass
                
                if attempt < retry_count:
                    time.sleep(retry_delay)
                    attempt += 1
                else:
                    break
                    
            except ValueError:
                # Don't retry on validation errors
                raise
                
            except Exception as e:
                # For other errors, attempt rollback but don't retry
                last_error = e
                
                try:
                    self.kb_search.conn.rollback()
                except Exception:
                    pass
                
                raise Exception(f"Error setting multiple status data: {str(e)}")
        
        # If we've exhausted all retries
        error_msg = f"Failed to set multiple status data after {retry_count + 1} attempts"
        if last_error:
            error_msg += f": {str(last_error)}"
        
        raise Exception(error_msg)
    
  
