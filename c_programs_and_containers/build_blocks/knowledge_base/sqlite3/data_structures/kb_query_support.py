import sqlite3
import json

class KB_Search:
    """
    A class to handle SQL filtering for the knowledge_base table in SQLite.
    Uses Common Table Expressions (CTEs) for reentrant queries.
    Always selects all columns from the knowledge_base table.
    Requires the ltree SQLite extension to be loaded.
    """
    
    def __init__(self, db_path, database, ltree_extension_path=None):
        """
        Initializes the KB_Search object and connects to the SQLite database.
        Loads the ltree extension.

        Args:
            db_path (str): Path to the SQLite database file.
            database (str): The name of the table to query.
            ltree_extension_path (str): Path to the ltree extension WITHOUT extension suffix
                                       (e.g., './ltree' or '/usr/local/lib/ltree')
                                       SQLite automatically adds .so/.dll/.dylib
                                       If None, will search common locations
        """
        self.path = []  # Stack to keep track of the path (levels/nodes)
        self.db_path = db_path
        self.base_table = database
        self.link_table = self.base_table + "_link"
        self.link_mount_table = self.base_table + "_link_mount"
        self.filters = []
        self.results = None
        self.path_values = {}
        self.conn = None
        self.cursor = None

        # Determine ltree extension path
        if ltree_extension_path is None:
            # Search common locations
            import sys
            import os
            search_paths = [
                './ltree',
                '/usr/local/lib/ltree',
                '/usr/lib/ltree',
            ]
            
            # Check which file exists
            suffix = '.so'
        
           
            for path in search_paths:
                if os.path.exists(path + suffix):
                    ltree_extension_path = path
                    break
            else:
                # Default to ./ltree if none found
                ltree_extension_path = './ltree'
                
        
        
        self.ltree_extension_path = ltree_extension_path
        self._connect()  # Establish the database connection during initialization
        
    def _connect(self):
        """
        Establishes a connection to the SQLite database and loads the ltree extension.
        This is a helper method called by __init__.
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            
            # Enable extension loading
            self.conn.enable_load_extension(True)
            
            # Load the ltree extension
            try:
                self.conn.load_extension(self.ltree_extension_path)
            except sqlite3.OperationalError as e:
                print(f"Warning: Could not load ltree extension from {self.ltree_extension_path}: {e}")
                print("Continuing without ltree extension - path matching may not work")
            
            # Disable extension loading for security
            self.conn.enable_load_extension(False)
            
            # Set row factory to return dictionaries
            self.conn.row_factory = sqlite3.Row
            
            self.cursor = self.conn.cursor()
        
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
        
    def disconnect(self):
        """
        Closes the connection to the SQLite database.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
        self.cursor = None
        self.conn = None    
        
    def get_conn_and_cursor(self):
        """
        Get the database connection and cursor.
        
        Returns:
            tuple: (success_flag, connection, cursor)
        """
        if not self.conn or not self.cursor:
            raise ValueError("Not connected to database. Call _connect() first.")
        return True, self.conn, self.cursor
    
    def clear_filters(self):
        """
        Clear all filters and reset the query state.
        """
        self.filters = []
        self.results = None
    
    def search_kb(self, knowledge_base):
        """
        Add a filter to search for rows matching the specified knowledge_base.
        
        Args:
            knowledge_base: The knowledge_base name to search for.
        """
        self.filters.append({
            "condition": "knowledge_base = :knowledge_base",
            "params": {"knowledge_base": knowledge_base}
        })
        
    def search_label(self, label):
        """
        Add a filter to search for rows matching the specified label.
        
        Args:
            label: The label value to match
        """
        self.filters.append({
            "condition": "label = :label",
            "params": {"label": label}
        })
    
    def search_name(self, name):
        """
        Add a filter to search for rows matching the specified name.
        
        Args:
            name: The name value to match
        """
        self.filters.append({
            "condition": "name = :name",
            "params": {"name": name}
        })
    
    def search_property_key(self, key):
        """
        Add a filter to search for rows where the properties JSON field contains the specified key.
        
        Args:
            key: The JSON key to check for existence
        """
        # SQLite JSON functions: use json_type to check if key exists
        self.filters.append({
            "condition": "json_type(properties, :json_path) IS NOT NULL",
            "params": {"json_path": f"$.{key}"}
        })
    
    def search_property_value(self, key, value):
        """
        Add a filter to search for rows where the properties JSON field contains 
        the specified key with the specified value.
        
        Args:
            key: The JSON key to search
            value: The value to match for the key
        """
        # Use json_extract to get the value and compare
        self.filters.append({
            "condition": "json_extract(properties, :json_path) = :json_value",
            "params": {
                "json_path": f"$.{key}",
                "json_value": value
            }
        })
    
    def search_starting_path(self, starting_path):
        """
        Add a filter to search for rows matching all descendants of the specified starting path.
        Uses ltree_ancestor function: ltree_ancestor(child, parent) returns 1 if child is descendant of parent.
        """
        self.filters.append({
            "condition": "ltree_ancestor(path, :starting_path) = 1",
            "params": {"starting_path": starting_path}
        })
    
    def search_path(self, path_expression):
        """
        Add a filter to search for rows matching the specified LTREE path expression.
        Uses ltree_match function from the extension.
        
        Args:
            path_expression: The LTREE path expression to match
                Examples:
                - 'docs.technical' for exact match
                - 'docs.*' for all immediate children of docs
                - 'docs.*{1,3}' for children up to 3 levels deep
                - 'docs.technical.*' for all children of docs.technical
                - '*.technical.*' for any path containing 'technical'
        """
        self.filters.append({
            "condition": "ltree_match(path, :path_expr) = 1",
            "params": {"path_expr": path_expression}
        })
    
    def search_has_link(self):
        """
        Add a filter to search for rows where the has_link field is TRUE.
        """
        self.filters.append({
            "condition": "has_link = 1",
            "params": {}
        })
    
    def search_has_link_mount(self):
        """
        Add a filter to search for rows where the has_link_mount field is TRUE.
        """
        self.filters.append({
            "condition": "has_link_mount = 1",
            "params": {}
        })
    
    def execute_query(self):
        """
        Execute the progressive query with all added filters using CTEs.
        
        Returns:
            list: Query results as list of dictionaries
        """
        if not self.conn or not self.cursor:
            raise ValueError("Not connected to database. Call _connect() first.")
            
        # Always select all columns
        column_str = "*"
        
        # If no filters, execute a simple query
        if not self.filters:
            query = f"SELECT {column_str} FROM {self.base_table}"
            self.cursor.execute(query)
            self.results = [dict(row) for row in self.cursor.fetchall()]
            return self.results
        
        # Start building the CTE query
        cte_parts = []
        combined_params = {}
        
        # Initial CTE starts with the base table
        cte_parts.append(f"base_data AS (SELECT {column_str} FROM {self.base_table})")
        
        # Process each filter in sequence, building a chain of CTEs
        for i, filter_info in enumerate(self.filters):
            condition = filter_info.get('condition', '')
            params = filter_info.get('params', {})
            
            # Update the combined parameters dictionary with prefixed parameter names
            prefixed_params = {}
            prefixed_condition = condition
            
            for param_name, param_value in params.items():
                prefixed_name = f"p{i}_{param_name}"
                prefixed_params[prefixed_name] = param_value
                # Replace parameter placeholder in the condition
                prefixed_condition = prefixed_condition.replace(
                    f":{param_name}", 
                    f":{prefixed_name}"
                )
            
            combined_params.update(prefixed_params)
            
            # Define the CTE name for this step
            cte_name = f"filter_{i}"
            prev_cte = "base_data" if i == 0 else f"filter_{i-1}"
            
            # Build this CTE part
            if condition:
                cte_query = f"{cte_name} AS (SELECT {column_str} FROM {prev_cte} WHERE {prefixed_condition})"
            else:
                cte_query = f"{cte_name} AS (SELECT {column_str} FROM {prev_cte})"
            
            cte_parts.append(cte_query)
        
        # Build the final query with all CTEs
        with_clause = "WITH " + ",\n".join(cte_parts)
        final_select = f"SELECT {column_str} FROM filter_{len(self.filters)-1}"
        
        # Combine into the complete query
        final_query = f"{with_clause}\n{final_select}"
        
        try:
            # Execute the query with the combined parameters
            self.cursor.execute(final_query, combined_params)
            self.results = [dict(row) for row in self.cursor.fetchall()]
            
            return self.results
        except Exception as e:
            print(f"Error executing query: {e}")
            print(f"Query: {final_query}")
            print(f"Parameters: {combined_params}")
            raise
        
        
    def find_path_values(self, key_data):
        """
        Extract path values from query results.
        
        Args:
            key_data (list): List of result dictionaries
            
        Returns:
            list: List of path values
        """
        if not key_data:
            return []
        if not isinstance(key_data, list):
            key_data = [key_data]
        
        return_values = []
        for row in key_data:
            path = row['path']
            return_values.append(path)
        
        return return_values
    
    def get_results(self):
        """
        Get the results of the last executed query.
        
        Returns:
            List of dictionaries representing the query results,
            or empty list if no results or query hasn't been executed
        """
        return self.results if self.results else []
    
    def find_description(self, key_data):
        """
        Extract description from properties field of query results.
        
        Args:
            key_data: Single row dictionary or list of row dictionaries
            
        Returns:
            list: List of dictionaries mapping path to description
        """
        if isinstance(key_data, dict):
            key_data = [key_data]
        
        return_values = []
        
        for row in key_data:
            properties_str = row.get('properties', '{}')
            # Parse JSON string if it's a string
            if isinstance(properties_str, str):
                try:
                    properties = json.loads(properties_str)
                except (json.JSONDecodeError, TypeError):
                    properties = {}
            else:
                properties = properties_str if properties_str else {}
            
            description = properties.get('description', '') if properties else ''
            path = row.get('path', '')
            return_values.append({path: description})
        
        return return_values
   
    def find_description_paths(self, path_array):
        """
        Find data for specified paths in the knowledge base.
        
        Args:
            path_array (str or list): A single path or list of paths to search for
            
        Returns:
            dict: A dictionary mapping paths to their corresponding data values
            
        Raises:
            Exception: If a database error occurs
        """
        # Normalize input to always be a list
        if not isinstance(path_array, list):
            path_array = [path_array]
        
        # Handle empty input case
        if not path_array:
            return {}
        
        return_values = {}
        
        try:
            # Optimize with single query for multiple paths
            if len(path_array) == 1:
                query_string = f'''
                SELECT path, data
                FROM {self.base_table}
                WHERE path = ?;
                '''
                self.cursor.execute(query_string, (path_array[0],))
            else:
                # Use IN clause for multiple paths
                placeholders = ','.join(['?'] * len(path_array))
                query_string = f'''
                SELECT path, data
                FROM {self.base_table}
                WHERE path IN ({placeholders});
                '''
                self.cursor.execute(query_string, tuple(path_array))
            
            results = self.cursor.fetchall()
            
            # Build results dictionary
            found_paths = set()
            for row in results:
                path = row['path']
                data = row['data']
                return_values[path] = data
                found_paths.add(path)
            
            # Add None for paths that weren't found
            for path in path_array:
                if path not in found_paths:
                    return_values[path] = None
                    
            return return_values
            
        except Exception as e:
            # Handle errors gracefully
            raise Exception(f"Error retrieving data for paths: {str(e)}")

    def decode_link_nodes(self, path):
        """
        Decode an ltree path into knowledge base name and node link/name pairs.
        
        The path format is expected to be: kb.link1.name1.link2.name2.link3.name3...
        where:
        - kb: knowledge base identifier (first element)
        - link/name pairs: alternating link identifiers and node names
        
        Args:
            path (str): The ltree path to decode (e.g., 'kb_main.uuid1.node1.uuid2.node2')
        
        Returns:
            tuple: (kb_name, node_pairs)
                - kb_name (str): The knowledge base identifier
                - node_pairs (list): List of [node_link, node_name] pairs
        
        Raises:
            ValueError: If path format is invalid (odd number of elements after kb)
            
        Example:
            >>> kb, nodes = self.decode_link_nodes('kb_main.uuid1.parent.uuid2.child')
            >>> print(kb)  # 'kb_main'
            >>> print(nodes)  # [['uuid1', 'parent'], ['uuid2', 'child']]
        """
        if not path or not isinstance(path, str):
            raise ValueError("Path must be a non-empty string")
        
        path_parts = path.split('.')
        
        # Need at least kb name, and then pairs of (link, name)
        # So minimum is 3 elements: kb.link.name
        if len(path_parts) < 3:
            raise ValueError(f"Path must have at least 3 elements (kb.link.name), got {len(path_parts)}")
        
        # After removing kb (first element), remaining elements should be even
        # (pairs of link and name)
        remaining_parts = len(path_parts) - 1
        if remaining_parts % 2 != 0:
            raise ValueError(f"Bad path format: after kb identifier, must have even number of elements (link/name pairs), got {remaining_parts} elements")
        
        # Extract knowledge base name (first element)
        kb = path_parts[0]
        
        # Initialize result list for node pairs
        result = []
        
        # Process pairs of (node_link, node_name) starting from index 1
        for i in range(1, len(path_parts), 2):
            node_link = path_parts[i]
            node_name = path_parts[i + 1]
            result.append([node_link, node_name])
        
        return kb, result
