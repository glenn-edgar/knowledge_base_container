import sqlite3
import json
import yaml
from typing import Optional, Dict, Any, List
from datetime import datetime, date
from decimal import Decimal


class KnowledgeBaseYAMLHandlerSQLite:
    """
    Standalone class for exporting and importing knowledge base data to/from YAML.
    SQLite3 version.
    Does not create or modify table structures.
    
    Import Modes:
    -------------
    1. Skip conflicts (default): import_from_yaml(file)
       - Inserts new records only
       - Skips records that would conflict with existing data
       
    2. Update existing: import_from_yaml(file, update_existing=True)
       - Inserts new records
       - Updates existing records when conflicts occur (upsert)
       - Preserves id and created_at fields
       
    3. Clear and import: import_from_yaml(file, clear_existing=True)
       - Deletes all existing data first (or filtered by kb_name)
       - Then inserts all records from file
       - Use with caution!
       
    Note: clear_existing=True takes precedence over update_existing
    
    Link Flags:
    -----------
    The has_link and has_link_mount flags are now exported and can be imported.
    After import, these flags are automatically recalculated based on the actual
    link and link_mount table contents to ensure consistency.
    """
    
    def __init__(self, table_name: str, db_path: str):
        """
        Initialize the YAML handler with database path.
        
        Args:
            table_name: Base name of the knowledge base tables
            db_path: Path to SQLite database file
        """
        self.table_name = table_name
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            # Use Row factory to get dict-like access
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise
            
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        
    # ========== EXPORT METHODS ==========
    
    def export_table_data(self, table_name: str, 
                         where_clause: Optional[str] = None,
                         order_by: Optional[str] = None,
                         exclude_columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Export records from a table.
        
        Args:
            table_name: Name of the table to export
            where_clause: Optional WHERE clause (without WHERE keyword)
            order_by: Optional ORDER BY clause (without ORDER BY keyword)
            exclude_columns: List of column names to exclude from export
            
        Returns:
            List of dictionaries, one per row
        """
        query = f"SELECT * FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        
        # Convert Row objects to dicts and serialize special types
        records = []
        for row in rows:
            record = dict(row)
            serialized_record = self._serialize_record(record)
            
            # Exclude specified columns
            if exclude_columns:
                serialized_record = {
                    k: v for k, v in serialized_record.items() 
                    if k not in exclude_columns
                }
            
            records.append(serialized_record)
        
        return records

    def export_all_kb_data(self, order_by_path: bool = True) -> Dict[str, List[Dict[str, Any]]]:
        """
        Export all knowledge base related tables.
        Now includes has_link and has_link_mount flags in exports.
        
        Args:
            order_by_path: If True, order main table and links by path/created_at
            
        Returns:
            Dictionary mapping table names to lists of records
        """
        tables = {
            self.table_name: self.export_table_data(
                self.table_name, 
                order_by='path' if order_by_path else None
                # No longer excluding has_link and has_link_mount
            ),
            f"{self.table_name}_info": self.export_table_data(
                f"{self.table_name}_info"
            ),
            f"{self.table_name}_link": self.export_table_data(
                f"{self.table_name}_link",
                order_by='created_at' if order_by_path else None
            ),
            f"{self.table_name}_link_mount": self.export_table_data(
                f"{self.table_name}_link_mount",
                order_by='created_at' if order_by_path else None
            )
        }
        
        return tables

    def export_kb_by_name(self, kb_name: str) -> Dict[str, Any]:
        """
        Export all data for a specific knowledge base.
        Now includes has_link and has_link_mount flags in node exports.
        
        Args:
            kb_name: Name of the knowledge base to export
            
        Returns:
            Dictionary with all related records for this KB
        """
        return {
            'knowledge_base': kb_name,
            'nodes': self.export_table_data(
                self.table_name,
                where_clause=f"knowledge_base = '{kb_name}'",
                order_by='path'
                # No longer excluding has_link and has_link_mount
            ),
            'info': self.export_table_data(
                f"{self.table_name}_info",
                where_clause=f"knowledge_base = '{kb_name}'"
            ),
            'links': self.export_table_data(
                f"{self.table_name}_link",
                where_clause=f"parent_node_kb = '{kb_name}'",
                order_by='created_at'
            ),
            'link_mounts': self.export_table_data(
                f"{self.table_name}_link_mount",
                where_clause=f"knowledge_base = '{kb_name}'",
                order_by='created_at'
            )
        }

    def export_to_yaml(self, filename: str, 
                      kb_name: Optional[str] = None,
                      include_metadata: bool = True) -> None:
        """
        Export knowledge base data to YAML file.
        
        Args:
            filename: Output YAML filename
            kb_name: If provided, export only this KB; otherwise export all
            include_metadata: If True, include record counts
        """
        if kb_name:
            data = self.export_kb_by_name(kb_name)
        else:
            data = self.export_all_kb_data()
        
        self.save_to_yaml(data, filename, include_metadata)
        print(f"Exported data to {filename}")

    def save_to_yaml(self, data: Any, filename: str, 
                     include_metadata: bool = True) -> None:
        """
        Save data to YAML file.
        
        Args:
            data: Data to save (list or dict)
            filename: Output YAML filename
            include_metadata: If True, include record counts and export timestamp
        """
        output = {}
        
        if include_metadata:
            output['metadata'] = {
                'exported_at': datetime.now().isoformat(),
                'table_name': self.table_name,
                'database_type': 'sqlite3'
            }
        
        if isinstance(data, dict):
            # Add record counts for multi-table exports
            if include_metadata and any(isinstance(v, list) for v in data.values()):
                output['record_counts'] = {
                    k: len(v) if isinstance(v, list) else 1 
                    for k, v in data.items()
                }
            output['data'] = data
        else:
            output['data'] = data
        
        with open(filename, 'w') as f:
            yaml.dump(output, f, default_flow_style=False, 
                     sort_keys=False, allow_unicode=True)

    def _serialize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert SQLite types to YAML-serializable types.
        
        Args:
            record: Dictionary containing a database row
            
        Returns:
            Dictionary with serialized values
        """
        serialized = {}
        for key, value in record.items():
            if value is None:
                serialized[key] = None
            elif isinstance(value, (datetime, date)):
                serialized[key] = value.isoformat()
            elif isinstance(value, Decimal):
                serialized[key] = float(value)
            elif isinstance(value, bytes):
                serialized[key] = value.hex()
            elif isinstance(value, str):
                # Try to parse as JSON if it looks like JSON
                # SQLite stores JSON as TEXT
                if value.startswith('{') or value.startswith('['):
                    try:
                        serialized[key] = json.loads(value)
                    except (json.JSONDecodeError, ValueError):
                        serialized[key] = value
                else:
                    serialized[key] = value
            else:
                # Handles int, float, bool
                serialized[key] = value
        
        return serialized

    # ========== IMPORT METHODS ==========
    
    def load_from_yaml(self, filename: str) -> Dict[str, Any]:
        """
        Load data from YAML file.
        
        Args:
            filename: Input YAML filename
            
        Returns:
            Dictionary containing the loaded data
        """
        with open(filename, 'r') as f:
            content = yaml.safe_load(f)
        
        # Handle both wrapped (with metadata) and unwrapped formats
        if isinstance(content, dict) and 'data' in content:
            return content['data']
        return content

    def import_from_yaml(self, filename: str, 
                        clear_existing: bool = False,
                        update_existing: bool = False,
                        kb_name_filter: Optional[str] = None,
                        recalculate_flags: bool = True) -> Dict[str, int]:
        """
        Import knowledge base data from YAML file.
        
        Args:
            filename: Input YAML filename
            clear_existing: If True, delete existing data before import
            update_existing: If True, update existing records on conflict (upsert)
                           If False, skip records that conflict with existing data
            kb_name_filter: If provided, only import data for this KB
            recalculate_flags: If True, recalculate has_link and has_link_mount 
                             flags after import based on link table contents.
                             Recommended to keep True to ensure data consistency.
            
        Returns:
            Dictionary with counts of imported records per table
            
        Note: clear_existing takes precedence over update_existing
        """
        data = self.load_from_yaml(filename)
        counts = {}
        
        try:
            # Handle single KB export format
            if 'knowledge_base' in data:
                kb_filter = data['knowledge_base']
                if kb_name_filter and kb_filter != kb_name_filter:
                    print(f"Skipping KB {kb_filter} (filter: {kb_name_filter})")
                    return counts
                    
                tables_data = {
                    f"{self.table_name}_info": data.get('info', []),
                    self.table_name: data.get('nodes', []),
                    f"{self.table_name}_link": data.get('links', []),
                    f"{self.table_name}_link_mount": data.get('link_mounts', [])
                }
            else:
                # Handle full export format
                tables_data = data
            
            # Clear existing data if requested
            if clear_existing:
                self._clear_tables(kb_name_filter)
            
            # Import in proper order (info first, then nodes, then links)
            import_order = [
                f"{self.table_name}_info",
                self.table_name,
                f"{self.table_name}_link_mount",
                f"{self.table_name}_link"
            ]
            
            for table in import_order:
                if table in tables_data:
                    records = tables_data[table]
                    if not isinstance(records, list):
                        records = [records]
                    
                    count = self._import_table_data(table, records, kb_name_filter, update_existing)
                    counts[table] = count
            
            # Recalculate has_link and has_link_mount flags if requested
            if recalculate_flags:
                flag_counts = self._recalculate_link_flags(kb_name_filter)
                print(f"Recalculated link flags: {flag_counts}")
            
            self.conn.commit()
            print(f"Successfully imported data from {filename}")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error importing data: {e}")
            raise
        
        return counts

    def clear_tables(self, kb_name_filter: Optional[str] = None) -> Dict[str, int]:
        """
        Clear existing data from tables.
        Deletes in proper order to maintain referential integrity.
        
        Args:
            kb_name_filter: If provided, only clear data for this KB
            
        Returns:
            Dictionary with counts of deleted records per table
            
        Example:
            # Clear all data
            counts = handler.clear_tables()
            
            # Clear only specific KB
            counts = handler.clear_tables(kb_name_filter='kb1')
        """
        try:
            counts = {}
            
            if kb_name_filter:
                where = f"WHERE knowledge_base = ?"
                where_parent = f"WHERE parent_node_kb = ?"
                params = (kb_name_filter,)
            else:
                where = ""
                where_parent = ""
                params = ()
            
            # Delete in reverse order (links first, then nodes, then info)
            if kb_name_filter:
                self.cursor.execute(f"DELETE FROM {self.table_name}_link {where_parent}", params)
            else:
                self.cursor.execute(f"DELETE FROM {self.table_name}_link")
            counts[f"{self.table_name}_link"] = self.cursor.rowcount
            
            if kb_name_filter:
                self.cursor.execute(f"DELETE FROM {self.table_name}_link_mount {where}", params)
            else:
                self.cursor.execute(f"DELETE FROM {self.table_name}_link_mount")
            counts[f"{self.table_name}_link_mount"] = self.cursor.rowcount
            
            if kb_name_filter:
                self.cursor.execute(f"DELETE FROM {self.table_name} {where}", params)
            else:
                self.cursor.execute(f"DELETE FROM {self.table_name}")
            counts[self.table_name] = self.cursor.rowcount
            
            if kb_name_filter:
                self.cursor.execute(f"DELETE FROM {self.table_name}_info {where}", params)
            else:
                self.cursor.execute(f"DELETE FROM {self.table_name}_info")
            counts[f"{self.table_name}_info"] = self.cursor.rowcount
            
            self.conn.commit()
            
            return counts
            
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"Error clearing tables: {e}")
            raise

    def _clear_tables(self, kb_name_filter: Optional[str] = None):
        """Internal method to clear tables (called by import_from_yaml)."""
        self.clear_tables(kb_name_filter)

    def _import_table_data(self, table_name: str, records: List[Dict[str, Any]], 
                          kb_name_filter: Optional[str] = None,
                          update_existing: bool = False) -> int:
        """
        Import records into a table.
        Now allows has_link and has_link_mount to be imported from YAML.
        
        Args:
            table_name: Name of the table
            records: List of record dictionaries
            kb_name_filter: If provided, only import records for this KB
            update_existing: If True, update existing records on conflict
            
        Returns:
            Number of records imported/updated
        """
        if not records:
            return 0
        
        # Define conflict columns for each table (for SQLite UPSERT)
        conflict_targets = {
            self.table_name: 'path',
            f"{self.table_name}_info": 'knowledge_base',
            f"{self.table_name}_link": 'link_name, parent_node_kb, parent_path',
            f"{self.table_name}_link_mount": 'link_name'
        }
        
        count = 0
        for record in records:
            # Filter by KB name if specified
            if kb_name_filter:
                kb_field = None
                if 'knowledge_base' in record:
                    kb_field = 'knowledge_base'
                elif 'parent_node_kb' in record:
                    kb_field = 'parent_node_kb'
                
                if kb_field and record[kb_field] != kb_name_filter:
                    continue
            
            # Remove only auto-generated id field
            # Keep has_link and has_link_mount - they'll be recalculated after import
            record = {k: v for k, v in record.items() if k not in ['id']}
            
            # Handle JSON fields - convert dicts/lists to JSON strings for SQLite
            if table_name == self.table_name:
                for json_field in ['properties', 'data']:
                    if json_field in record:
                        # If it's a dict or list, convert to JSON string
                        if isinstance(record[json_field], (dict, list)):
                            record[json_field] = json.dumps(record[json_field])
                        # If it's already a string, leave it as is
            
            # Build INSERT query
            columns = list(record.keys())
            values = [record[col] for col in columns]
            
            # SQLite uses ? placeholders
            placeholders = ', '.join(['?'] * len(columns))
            columns_sql = ', '.join(columns)
            
            # Get the conflict target for this table
            conflict_target = conflict_targets.get(table_name, 'id')
            
            if update_existing:
                # Build UPDATE clause for all columns except the conflict target
                update_cols = [col for col in columns if col not in ['id', 'created_at']]
                update_set = ', '.join([f"{col} = excluded.{col}" for col in update_cols])
                
                # SQLite UPSERT syntax
                query = f"""
                    INSERT INTO {table_name} ({columns_sql})
                    VALUES ({placeholders})
                    ON CONFLICT({conflict_target}) DO UPDATE SET
                    {update_set}
                """
            else:
                # SQLite INSERT OR IGNORE
                query = f"""
                    INSERT OR IGNORE INTO {table_name} ({columns_sql})
                    VALUES ({placeholders})
                """
            
            self.cursor.execute(query, values)
            count += self.cursor.rowcount
        
        return count

    def _recalculate_link_flags(self, kb_name_filter: Optional[str] = None) -> Dict[str, int]:
        """
        Recalculate has_link and has_link_mount flags based on link tables.
        This ensures the flags are consistent with actual link data.
        
        Args:
            kb_name_filter: If provided, only recalculate for this KB
            
        Returns:
            Dictionary with counts of updated records
        """
        counts = {}
        
        if kb_name_filter:
            where_clause = f"WHERE knowledge_base = ?"
            params = (kb_name_filter,)
        else:
            where_clause = ""
            params = ()
        
        # Reset all flags to False (for filtered KB if specified)
        if kb_name_filter:
            self.cursor.execute(f"""
                UPDATE {self.table_name} 
                SET has_link = 0, has_link_mount = 0
                {where_clause}
            """, params)
        else:
            self.cursor.execute(f"""
                UPDATE {self.table_name} 
                SET has_link = 0, has_link_mount = 0
            """)
        counts['reset'] = self.cursor.rowcount
        
        # Set has_link = TRUE for nodes that have entries in link table
        if kb_name_filter:
            kb_condition = f"AND kb.knowledge_base = ?"
            self.cursor.execute(f"""
                UPDATE {self.table_name} AS kb
                SET has_link = 1
                WHERE EXISTS (
                    SELECT 1 FROM {self.table_name}_link AS link
                    WHERE kb.path = link.parent_path
                    AND kb.knowledge_base = link.parent_node_kb
                    {kb_condition}
                )
            """, params)
        else:
            self.cursor.execute(f"""
                UPDATE {self.table_name}
                SET has_link = 1
                WHERE EXISTS (
                    SELECT 1 FROM {self.table_name}_link AS link
                    WHERE {self.table_name}.path = link.parent_path
                    AND {self.table_name}.knowledge_base = link.parent_node_kb
                )
            """)
        counts['has_link_set'] = self.cursor.rowcount
        
        # Set has_link_mount = TRUE for nodes that have entries in link_mount table
        if kb_name_filter:
            self.cursor.execute(f"""
                UPDATE {self.table_name} AS kb
                SET has_link_mount = 1
                WHERE EXISTS (
                    SELECT 1 FROM {self.table_name}_link_mount AS mount
                    WHERE kb.path = mount.mount_path
                    AND kb.knowledge_base = mount.knowledge_base
                    {kb_condition}
                )
            """, params)
        else:
            self.cursor.execute(f"""
                UPDATE {self.table_name}
                SET has_link_mount = 1
                WHERE EXISTS (
                    SELECT 1 FROM {self.table_name}_link_mount AS mount
                    WHERE {self.table_name}.path = mount.mount_path
                    AND {self.table_name}.knowledge_base = mount.knowledge_base
                )
            """)
        counts['has_link_mount_set'] = self.cursor.rowcount
        
        return counts


# Example usage
if __name__ == "__main__":
    # SQLite database pat
    import sys
    if len(sys.argv) < 3:
        print("Usage: python knowledge_base_yaml_handler.py <database_file.db>,<yaml_file_name>")
        print("Example: python knowledge_base_yaml_handler.py knowledge_base.db","export_db.yaml")
        exit(1)
    db_path = sys.argv[1]
    yaml_file_name = sys.argv[2]
    
    # Using context manager for automatic connection handling
    print("\n=== EXPORT Example ===")
    with KnowledgeBaseYAMLHandlerSQLite('knowledge_base', db_path) as handler:
        # Export all data (now includes has_link and has_link_mount)
        handler.export_to_yaml(yaml_file_name)
        
        # Export specific KB
        #handler.export_to_yaml('kb_export_kb1.yaml', kb_name='kb1')
        
        # Export without metadata
        #andler.export_to_yaml('kb_export_simple.yaml', include_metadata=False)
    
    
    print("\n=== IMPORT Example ===")
    with KnowledgeBaseYAMLHandlerSQLite('knowledge_base', db_path) as handler:
        # Import from file (skip conflicts - default behavior)
        # Flags are automatically recalculated after import
        counts = handler.import_from_yaml(yaml_file_name)
        print(f"Import counts (skip conflicts): {counts}")
        exit(0)
        # Import and update existing records (upsert)
        
        
        # Import and replace all existing data for specific KB
        # counts = handler.import_from_yaml('kb_export_kb1.yaml', 
        #                                   clear_existing=True,
        #                                   kb_name_filter='kb1')
        # print(f"Import counts (clear and import): {counts}")