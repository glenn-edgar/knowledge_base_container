#!/usr/bin/env python3
"""
LTree JSONB Database Operations
JSONB operations including queues on ltree-based document database.
Provides mainstream PostgreSQL JSONB operators and queue abstractions.
"""

import json
from typing import Optional, List, Dict, Any, Union
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from psycopg2 import sql


class QueueOperationError(Exception):
    """Raised when a queue operation fails."""
    pass


class KB_Document_Table:
    """
    JSONB operations on ltree document database with queue support.
    Assumes records already exist and are managed by another class.
    
    Provides:
    - Core JSONB operations (get, set, delete, contains, etc.)
    - Array operations (append, prepend, remove, etc.)
    - Queue/Stack abstractions (enqueue, dequeue, push, pop)
    - Path-based queries using standard PostgreSQL JSONB operators
    """
    
    def __init__(self, 
                 conn, 
                 kb_search,
                 database):
        """
        Initialize the JSONB database operations.
        
        Args:
            conn: psycopg2 connection object
            kb_search: Knowledge base search object
            database: Database name prefix
        """
        self.conn = conn
        self.database = database
        self.table_name = database + "_document"
        self.kb_search = kb_search
        
    def find_document_id(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find a single job id for given parameters. Raises error if 0 or multiple jobs found.
        
        Args:
            kb: Knowledge base filter
            node_name (str, optional): Node name to search for
            properties (dict, optional): Properties to match
            node_path (str, optional): LTREE path to match
            
        Returns:
            dict: Single matching job record with field names
            
        Raises:
            ValueError: If no job or multiple jobs found
        """
        
        results = self.find_document_ids(kb, node_name, properties, node_path)
        
        if len(results) == 0:
            raise ValueError(f"No job found matching parameters: name={node_name}, properties={properties}, path={node_path}")
        if len(results) > 1:
            raise ValueError(f"Multiple jobs ({len(results)}) found matching parameters: name={node_name}, properties={properties}, path={node_path}")
        
        return results[0]
    
    def find_document_ids(self, kb=None, node_name=None, properties=None, node_path=None):
        """
        Find all job ids matching the given parameters.
        
        Args:
            kb: Knowledge base filter
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
            self.kb_search.search_label("KB_JSONB_FIELD")
            
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
            raise Exception(f"Error finding document IDs: {str(e)}")
    
    def find_document_paths(self, table_dict_rows):
        """
        Extract path values from document query results.
        
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
    
    # ===== Core JSONB Operations =====
    
    def jsonb_get(self,
                  ltree_path: str,
                  json_path: str,
                  as_text: bool = False,
                  doc_type: Optional[str] = None) -> Any:
        """
        Get a value from JSONB field using -> or ->> operators.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path in format "field" or "field.subfield"
                      Use "" or "{}" to get entire data field
            as_text: If True, use ->> (text), else use -> (JSON)
            doc_type: Optional document type filter
            
        Returns:
            The value at the JSON path, or None if not found
        """
        type_filter = "AND type = %s" if doc_type else ""
        
        # Special case: empty path or "{}" means get entire data field
        if json_path == "" or json_path == "{}":
            params = [ltree_path]
            if doc_type:
                params.append(doc_type)
            
            # Get entire data as JSONB (as_text doesn't apply here)
            accessor = "data"
        else:
            # Convert dot notation to PostgreSQL path operators
            path_parts = json_path.split('.')
            
            # Build the accessor chain and params list
            # SQL format: SELECT {accessor} FROM table WHERE ltree = %s
            # Params order: [accessor_params..., ltree_path, doc_type?]
            if len(path_parts) == 1:
                # Single key: use -> or ->>
                operator = "->>" if as_text else "->"
                accessor = f"data {operator} %s"
                params = [path_parts[0], ltree_path]
            else:
                # Nested path: use #> or #>>
                operator = "#>>" if as_text else "#>"
                accessor = f"data {operator} %s::text[]"
                params = [path_parts, ltree_path]
            
            if doc_type:
                params.append(doc_type)
        
        select_sql = f"""
        SELECT {accessor} as value
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else None
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to get JSONB value: {e}")
    
    def jsonb_set(self,
                  ltree_path: str,
                  json_path: str,
                  value: Any,
                  doc_type: Optional[str] = None,
                  create_missing: bool = True) -> bool:
        """
        Set a value in JSONB field using jsonb_set().
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path in format "field" or "field.subfield"
                      Use "" or "{}" to replace entire data field
            value: Value to set (will be JSON encoded)
            doc_type: Optional document type filter
            create_missing: Create path if it doesn't exist
            
        Returns:
            True if successful, False if document not found
        """
        type_filter = "AND type = %s" if doc_type else ""
        
        # Special case: empty path or "{}" means replace entire data field
        if json_path == "" or json_path == "{}":
            params = [Json(value), ltree_path]
            if doc_type:
                params.append(doc_type)
            
            update_sql = f"""
            UPDATE {self.table_name}
            SET data = %s::jsonb,
            updated_at = CURRENT_TIMESTAMP
            WHERE ltree = %s::ltree
            {type_filter}
            RETURNING id;
            """
        else:
            # Normal path: use jsonb_set
            path_parts = json_path.split('.')
            params = [path_parts, Json(value), ltree_path]
            if doc_type:
                params.append(doc_type)
            
            update_sql = f"""
            UPDATE {self.table_name}
            SET data = jsonb_set(
                data,
                %s::text[],
                %s::jsonb,
                {str(create_missing).lower()}
            ),
            updated_at = CURRENT_TIMESTAMP
            WHERE ltree = %s::ltree
            {type_filter}
            RETURNING id;
            """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to set JSONB value: {e}")
    
    def jsonb_delete_key(self,
                        ltree_path: str,
                        key: str,
                        doc_type: Optional[str] = None) -> bool:
        """
        Delete a key from JSONB using - operator.
        
        Args:
            ltree_path: The document ltree path
            key: Key to delete
            doc_type: Optional document type filter
            
        Returns:
            True if successful
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [key, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = data - %s,
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to delete JSONB key: {e}")
    
    def jsonb_delete_path(self,
                         ltree_path: str,
                         json_path: str,
                         doc_type: Optional[str] = None) -> bool:
        """
        Delete a nested path from JSONB using #- operator.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to delete (e.g., "address.city")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = data #- %s::text[],
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to delete JSONB path: {e}")
    
    # ===== Existence & Search Operations =====
    
    def jsonb_has_key(self,
                     ltree_path: str,
                     key: str,
                     doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB has a key using ? operator.
        
        Args:
            ltree_path: The document ltree path
            key: Key to check
            doc_type: Optional document type filter
            
        Returns:
            True if key exists
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [key, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data ? %s as has_key
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB key: {e}")
    
    def jsonb_has_any_keys(self,
                          ltree_path: str,
                          keys: List[str],
                          doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB has any of the specified keys using ?| operator.
        
        Args:
            ltree_path: The document ltree path
            keys: List of keys to check
            doc_type: Optional document type filter
            
        Returns:
            True if any key exists
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [keys, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data ?| %s::text[] as has_any
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB keys: {e}")
    
    def jsonb_has_all_keys(self,
                          ltree_path: str,
                          keys: List[str],
                          doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB has all of the specified keys using ?& operator.
        
        Args:
            ltree_path: The document ltree path
            keys: List of keys to check
            doc_type: Optional document type filter
            
        Returns:
            True if all keys exist
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [keys, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data ?& %s::text[] as has_all
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB keys: {e}")
    
    def jsonb_contains(self,
                      ltree_path: str,
                      contained: Dict[str, Any],
                      doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB contains an object using @> operator.
        
        Args:
            ltree_path: The document ltree path
            contained: Object that should be contained
            doc_type: Optional document type filter
            
        Returns:
            True if contained
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [Json(contained), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data @> %s::jsonb as contains
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB containment: {e}")
    
    def jsonb_contained_by(self,
                          ltree_path: str,
                          container: Dict[str, Any],
                          doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB is contained by an object using <@ operator.
        
        Args:
            ltree_path: The document ltree path
            container: Object that should contain the data
            doc_type: Optional document type filter
            
        Returns:
            True if contained by
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [Json(container), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT data <@ %s::jsonb as contained_by
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSONB containment: {e}")
    
    # ===== Path Query Operations =====
    
    def jsonb_path_exists(self,
                         ltree_path: str,
                         json_path_query: str,
                         doc_type: Optional[str] = None) -> bool:
        """
        Check if a JSON path exists using jsonb_path_exists().
        
        Example: '$.address.city ? (@ == "LA")'
        
        Args:
            ltree_path: The document ltree path
            json_path_query: JSON path query
            doc_type: Optional document type filter
            
        Returns:
            True if path exists
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [json_path_query, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT jsonb_path_exists(data, %s::jsonpath) as exists
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check JSON path: {e}")
    
    def jsonb_path_query(self,
                        ltree_path: str,
                        json_path_query: str,
                        doc_type: Optional[str] = None) -> List[Any]:
        """
        Query JSON path using jsonb_path_query_array().
        
        Example: '$.items[*].price'
        
        Args:
            ltree_path: The document ltree path
            json_path_query: JSON path query
            doc_type: Optional document type filter
            
        Returns:
            Array of matching values
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [json_path_query, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT jsonb_path_query_array(data, %s::jsonpath) as results
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else []
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to query JSON path: {e}")
    
    def jsonb_query(self,
                   ltree_path: str,
                   jsonb_filter: Dict[str, Any],
                   doc_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Query a document with JSONB containment filter using @>.
        
        Args:
            ltree_path: The document ltree path
            jsonb_filter: JSONB filter using @> containment operator
            doc_type: Optional document type filter
            
        Returns:
            The document data if found and matches filter, None otherwise
        """
        type_filter = "AND type = %s" if doc_type else ""
        params = [ltree_path, Json(jsonb_filter)]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT id, ltree::text as ltree, type, data
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        AND data @> %s::jsonb
        {type_filter};
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return dict(result) if result else None
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to query JSONB: {e}")
    
    # ===== Array Operations =====
    
    def jsonb_array_append(self,
                          ltree_path: str,
                          json_path: str,
                          item: Any,
                          doc_type: Optional[str] = None) -> bool:
        """
        Append an item to a JSONB array using || operator.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            item: Item to append
            doc_type: Optional document type filter
            
        Returns:
            True if successful, False if document not found
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, path_parts, Json(item), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = jsonb_set(
            data,
            %s::text[],
            COALESCE(data #> %s::text[], '[]'::jsonb) || %s::jsonb,
            true
        ),
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to append to JSONB array: {e}")
    
    def jsonb_array_prepend(self,
                           ltree_path: str,
                           json_path: str,
                           item: Any,
                           doc_type: Optional[str] = None) -> bool:
        """
        Prepend an item to a JSONB array using || operator.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            item: Item to prepend
            doc_type: Optional document type filter
            
        Returns:
            True if successful, False if document not found
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, Json(item), path_parts, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = jsonb_set(
            data,
            %s::text[],
            %s::jsonb || COALESCE(data #> %s::text[], '[]'::jsonb),
            true
        ),
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter}
        RETURNING id;
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(update_sql, params)
                result = cur.fetchone()
                self.conn.commit()
                return result is not None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to prepend to JSONB array: {e}")
    
    def jsonb_array_remove_index(self,
                                 ltree_path: str,
                                 json_path: str,
                                 index: int,
                                 doc_type: Optional[str] = None) -> Optional[Any]:
        """
        Remove an item from a JSONB array by index and return it.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            index: Index to remove (0-based)
            doc_type: Optional document type filter
            
        Returns:
            The removed item, or None if not found
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        
        # First get the item
        select_params = [path_parts, index, ltree_path]
        if doc_type:
            select_params.append(doc_type)
        
        select_sql = f"""
        SELECT (data #> %s::text[]) -> %s as item
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        # Then remove it
        update_params = [path_parts, path_parts, index, ltree_path]
        if doc_type:
            update_params.append(doc_type)
        
        update_sql = f"""
        UPDATE {self.table_name}
        SET data = jsonb_set(
            data,
            %s::text[],
            (data #> %s::text[]) - %s,
            true
        ),
        updated_at = CURRENT_TIMESTAMP
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                # Get the item first
                cur.execute(select_sql, select_params)
                result = cur.fetchone()
                removed_item = result[0] if result else None
                
                # Then remove it if it exists
                if removed_item is not None:
                    cur.execute(update_sql, update_params)
                
                self.conn.commit()
                return removed_item
        except psycopg2.Error as e:
            self.conn.rollback()
            raise RuntimeError(f"Failed to remove from JSONB array: {e}")
    
    def jsonb_array_contains(self,
                            ltree_path: str,
                            json_path: str,
                            item: Any,
                            doc_type: Optional[str] = None) -> bool:
        """
        Check if JSONB array contains an item using @> operator.
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            item: Item to check for
            doc_type: Optional document type filter
            
        Returns:
            True if array contains item
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, Json([item]), ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT (data #> %s::text[]) @> %s::jsonb as contains
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                result = cur.fetchone()
                return result[0] if result else False
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check array containment: {e}")
    
    def jsonb_array_elements(self,
                            ltree_path: str,
                            json_path: str,
                            doc_type: Optional[str] = None) -> List[Any]:
        """
        Expand JSONB array elements using jsonb_array_elements().
        
        Args:
            ltree_path: The document ltree path
            json_path: JSON path to the array
            doc_type: Optional document type filter
            
        Returns:
            List of array elements
        """
        path_parts = json_path.split('.')
        
        type_filter = "AND type = %s" if doc_type else ""
        params = [path_parts, ltree_path]
        if doc_type:
            params.append(doc_type)
        
        select_sql = f"""
        SELECT jsonb_array_elements(data #> %s::text[]) as element
        FROM {self.table_name}
        WHERE ltree = %s::ltree
        {type_filter};
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(select_sql, params)
                results = cur.fetchall()
                return [row[0] for row in results]
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to expand array elements: {e}")
    
    # ===== Queue Operations (High-Level Abstractions) =====
    
    def enqueue(self,
                ltree_path: str,
                item: Any,
                queue_path: str = "items",
                doc_type: Optional[str] = None) -> bool:
        """
        Add an item to the end of the queue (append) - FIFO.
        
        Args:
            ltree_path: The document ltree path
            item: Item to add to queue
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
            
        Raises:
            QueueOperationError: If operation fails
        """
        try:
            result = self.jsonb_array_append(ltree_path, queue_path, item, doc_type)
            if not result:
                raise QueueOperationError(f"Document not found: {ltree_path}")
            return True
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to enqueue: {e}")
    
    def dequeue(self,
                ltree_path: str,
                queue_path: str = "items",
                doc_type: Optional[str] = None) -> Optional[Any]:
        """
        Remove and return the first item from the queue (FIFO).
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            The dequeued item, or None if queue is empty
            
        Raises:
            QueueOperationError: If operation fails
        """
        try:
            item = self.jsonb_array_remove_index(ltree_path, queue_path, 0, doc_type)
            return item
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to dequeue: {e}")
    
    def peek(self,
             ltree_path: str,
             queue_path: str = "items",
             doc_type: Optional[str] = None,
             index: int = 0) -> Optional[Any]:
        """
        View an item in the queue without removing it.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            index: Index to peek at (default: 0 for first item)
            
        Returns:
            The item at the specified index, or None if not found
        """
        queue = self.jsonb_get(ltree_path, queue_path, as_text=False, doc_type=doc_type)
        if queue and isinstance(queue, list) and 0 <= index < len(queue):
            return queue[index]
        return None
    
    def size(self,
             ltree_path: str,
             queue_path: str = "items",
             doc_type: Optional[str] = None) -> int:
        """
        Get the number of items in the queue.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            Number of items in queue (0 if queue doesn't exist)
        """
        queue = self.jsonb_get(ltree_path, queue_path, as_text=False, doc_type=doc_type)
        if queue and isinstance(queue, list):
            return len(queue)
        return 0
    
    def is_empty(self,
                 ltree_path: str,
                 queue_path: str = "items",
                 doc_type: Optional[str] = None) -> bool:
        """
        Check if the queue is empty.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            True if queue is empty or doesn't exist
        """
        return self.size(ltree_path, queue_path, doc_type) == 0
    
    def clear(self,
              ltree_path: str,
              queue_path: str = "items",
              doc_type: Optional[str] = None) -> bool:
        """
        Remove all items from the queue.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
            
        Raises:
            QueueOperationError: If operation fails
        """
        try:
            result = self.jsonb_set(ltree_path, queue_path, [], doc_type, create_missing=True)
            if not result:
                raise QueueOperationError(f"Document not found: {ltree_path}")
            return True
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to clear queue: {e}")
    
    def get_all(self,
                ltree_path: str,
                queue_path: str = "items",
                doc_type: Optional[str] = None) -> List[Any]:
        """
        Get all items in the queue without modifying it.
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            List of all items in queue (empty list if queue doesn't exist)
        """
        queue = self.jsonb_get(ltree_path, queue_path, as_text=False, doc_type=doc_type)
        if queue and isinstance(queue, list):
            return queue
        return []
    
    def push(self,
             ltree_path: str,
             item: Any,
             queue_path: str = "items",
             doc_type: Optional[str] = None) -> bool:
        """
        Push an item to the front of the queue - for stack/priority operations (LIFO).
        
        Args:
            ltree_path: The document ltree path
            item: Item to push
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
            
        Raises:
            QueueOperationError: If operation fails
        """
        try:
            result = self.jsonb_array_prepend(ltree_path, queue_path, item, doc_type)
            if not result:
                raise QueueOperationError(f"Document not found: {ltree_path}")
            return True
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to push: {e}")
    
    def pop(self,
            ltree_path: str,
            queue_path: str = "items",
            doc_type: Optional[str] = None) -> Optional[Any]:
        """
        Remove and return the last item from the queue - for stack operations (LIFO).
        
        Args:
            ltree_path: The document ltree path
            queue_path: JSON path to the queue array (default: "items")
            doc_type: Optional document type filter
            
        Returns:
            The popped item, or None if queue is empty
            
        Raises:
            QueueOperationError: If operation fails
        """
        size = self.size(ltree_path, queue_path, doc_type)
        if size == 0:
            return None
        
        try:
            item = self.jsonb_array_remove_index(ltree_path, queue_path, size - 1, doc_type)
            return item
        except RuntimeError as e:
            raise QueueOperationError(f"Failed to pop: {e}")
    
    def get_metadata(self,
                     ltree_path: str,
                     metadata_path: str = "metadata",
                     doc_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get queue metadata.
        
        Args:
            ltree_path: The document ltree path
            metadata_path: JSON path to metadata (default: "metadata")
            doc_type: Optional document type filter
            
        Returns:
            Metadata dict or None if not found
        """
        return self.jsonb_get(ltree_path, metadata_path, as_text=False, doc_type=doc_type)
    
    def set_metadata(self,
                     ltree_path: str,
                     metadata: Dict[str, Any],
                     metadata_path: str = "metadata",
                     doc_type: Optional[str] = None) -> bool:
        """
        Set queue metadata.
        
        Args:
            ltree_path: The document ltree path
            metadata: Metadata to set
            metadata_path: JSON path to metadata (default: "metadata")
            doc_type: Optional document type filter
            
        Returns:
            True if successful
        """
        return self.jsonb_set(ltree_path, metadata_path, metadata, doc_type)