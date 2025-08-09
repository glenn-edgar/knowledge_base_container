import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor


class File_Table:
    def __init__(self, postgres_connection, table_name):
        """
        Initialize File_table with database connection and table name.
        
        Args:
            postgres_connection: Active psycopg2 connection object
            table_name: Name of the table to create/use
        """
        self.conn = postgres_connection
        self.table_name = table_name
        
        self._create_file_table()
    
    def _create_file_table(self):
        """Create the file table with the specified schema."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            volume VARCHAR(255) NOT NULL,
            file_path LTREE NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            file_extension VARCHAR(16),
            content TEXT,
            file_size BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(volume, file_path, file_name, file_extension)
        );
        
        -- Create indexes for efficient queries
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_volume ON {self.table_name}(volume);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_file_path ON {self.table_name} USING GIST(file_path);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_file_name ON {self.table_name}(file_name);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_file_extension ON {self.table_name}(file_extension);
        """
        
        with self.conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            self.conn.commit()
        
    def _parse_file_path(self, file_path):
        """
        Parse file path to extract ltree path, filename, and extension.
        
        Args:
            file_path: String in format "xx/xx/xx/filename.ext"
            
        Returns:
            tuple: (ltree_path, file_name, file_extension)
        """
        path_obj = Path(file_path)
        
        # Get filename and extension
        file_name = path_obj.stem
        file_extension = path_obj.suffix.lstrip('.')  # Remove leading dot
        
        # Convert parent directory path to ltree format (replace / with .)
        parent_path = str(path_obj.parent)
        if parent_path == '.' or parent_path == '':
            # File is in root directory - use 'root' as the ltree path
            ltree_path = 'root'
        else:
            # Replace / with . for ltree format
            ltree_path = parent_path.replace('/', '.')
        
        return ltree_path, file_name, file_extension

    def store_record(self, update, volume, file_path, data):
        """
        Store file data in the database.
        
        Args:
            update: Boolean - if True, allow updates to existing records; if False, only insert new records
            volume: String - volume identifier
            file_path: String - file path in format "xx/xx/xx/filename.ext"
            data: String - file content
            
        Raises:
            Exception: If database operation fails
        """
        ltree_path, file_name, file_extension = self._parse_file_path(file_path)
        file_size = len(data.encode('utf-8')) if data else 0
        
        with self.conn.cursor() as cursor:
            # Check if record exists
            check_sql = f"""
            SELECT id FROM {self.table_name} 
            WHERE volume = %s AND file_path = %s AND file_name = %s AND file_extension = %s
            """
            cursor.execute(check_sql, (volume, ltree_path, file_name, file_extension))
            existing_record = cursor.fetchone()
            
            if existing_record:
                if update:
                    # Update existing record
                    update_sql = f"""
                    UPDATE {self.table_name} 
                    SET content = %s, file_size = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE volume = %s AND file_path = %s AND file_name = %s AND file_extension = %s
                    """
                    cursor.execute(update_sql, (data, file_size, volume, ltree_path, file_name, file_extension))
                else:
                    # update=False means skip existing records silently
                    pass
            else:
                # Record doesn't exist, insert new record (allowed for both update=True and update=False)
                insert_sql = f"""
                INSERT INTO {self.table_name} 
                (volume, file_path, file_name, file_extension, content, file_size)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (volume, ltree_path, file_name, file_extension, data, file_size))
            
            self.conn.commit()
        
    def retrieve_file(self, sql_statement):
        """
        Execute SQL statement and return results as a list of dictionaries.
        
        Args:
            sql_statement: String - SQL SELECT statement
            
        Returns:
            list: List of dictionaries representing the query results
            
        Raises:
            Exception: If SQL execution fails
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql_statement)
            results = cursor.fetchall()
            
            # Convert RealDictRow objects to regular dictionaries
            return [dict(row) for row in results]
        
        
    def delete_by_volume(self, volume):
        """
        Delete all entries matching the specified volume.
        
        Args:
            volume: String - volume identifier to delete
            
        Returns:
            int: Number of rows deleted
            
        Raises:
            Exception: If database operation fails
        """
        delete_sql = f"""
        DELETE FROM {self.table_name} 
        WHERE volume = %s
        """
        
        with self.conn.cursor() as cursor:
            cursor.execute(delete_sql, (volume,))
            rows_deleted = cursor.rowcount
            self.conn.commit()
            
            return rows_deleted

    def update_content(self, volume, file_path, file_name, file_extension, content):
        """
        Update the content field for a specific file record.
        
        Args:
            volume: String - volume identifier
            file_path: String - ltree path (directory structure)
            file_name: String - name of the file without extension
            file_extension: String - file extension without dot
            content: String - new content to store
            
        Returns:
            int: Number of rows updated (0 if record not found, 1 if updated)
            
        Raises:
            Exception: If database operation fails
        """
        file_size = len(content.encode('utf-8')) if content else 0
        
        update_sql = f"""
        UPDATE {self.table_name} 
        SET content = %s, file_size = %s, updated_at = CURRENT_TIMESTAMP
        WHERE volume = %s AND file_path = %s AND file_name = %s AND file_extension = %s
        """
        
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql, (content, file_size, volume, file_path, file_name, file_extension))
            rows_updated = cursor.rowcount
            self.conn.commit()
            
            return rows_updated
        
    def record_exists(self, volume, file_path, file_name, file_extension):
        """
        Check if a record exists for the given parameters.
        
        Args:
            volume: String - volume identifier
            file_path: String - ltree path (directory structure)
            file_name: String - name of the file without extension
            file_extension: String - file extension without dot
            
        Returns:
            tuple: (bool, int|None) - (True/False if record exists, record ID or None)
            
        Raises:
            Exception: If database operation fails
        """
        check_sql = f"""
        SELECT id FROM {self.table_name} 
        WHERE volume = %s AND file_path = %s AND file_name = %s AND file_extension = %s
        LIMIT 1
        """
        
        with self.conn.cursor() as cursor:
            cursor.execute(check_sql, (volume, file_path, file_name, file_extension))
            result = cursor.fetchone()
            
            if result is not None:
                return True, result[0]  # Return True and the record ID
            else:
                return False, None      # Return False and None
            
            
    # content = call_back_function(content) is a function that takes the following arguments: content
    # content is the data of the file
    # it returns the modified content
    # if call_back_function is None, the content is not modified
    def export_files_to_disk(self, volume, base_path=None, call_back_function=None):
        
        """
        Export all files from the database for a specific volume to disk.
        
        Args:
            volume: String - volume identifier to export
            base_path: String - optional override for base directory path. If None, uses path from volume table
            
        Returns:
            dict: Summary with counts of files processed, created, and any errors
            
        Raises:
            Exception: If database operation or file writing fails
        """
        import os
        from pathlib import Path
        
        # Initialize counters for summary
        summary = {
            'files_processed': 0,
            'files_created': 0,
            'directories_created': 0,
            'errors': []
        }
        
        # Get base path from volume table if not provided
        if base_path is None:
            volume_sql = f"""
            SELECT base_path FROM volume 
            WHERE volume = %s
            """
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(volume_sql, (volume,))
                    volume_result = cursor.fetchone()
                    if volume_result is None:
                        raise Exception(f"Volume '{volume}' not found in volume table")
                    base_path = volume_result[0]
            except Exception as e:
                raise Exception(f"Error retrieving base path from volume table: {str(e)}")

        # Retrieve all files for the specified volume
        select_sql = f"""
        SELECT volume, file_path, file_name, file_extension, content, file_size
        FROM {self.table_name}
        WHERE volume = %s
        ORDER BY file_path, file_name
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_sql, (volume,))
                results = cursor.fetchall()
                
                for row in results:
                    summary['files_processed'] += 1
                    
                    
                    try:
                        # Extract file information
                        file_path_ltree = row['file_path']
                        file_name = row['file_name']
                        file_extension = row['file_extension']
                        content = row['content'] or ''  # Handle None conte
                        
                        # Convert ltree path back to filesystem path
                        if file_path_ltree == 'root':
                            # File is in root directory
                            relative_path = ''
                        else:
                            # Convert ltree format (dots) back to filesystem format (slashes)
                            relative_path = file_path_ltree.replace('.', os.sep)
                        
                        # Construct full filename with extension
                        if file_extension:
                            full_filename = f"{file_name}.{file_extension}"
                        else:
                            full_filename = file_name
                        
                        
                        if relative_path:
                            full_file_path = os.path.join(base_path, relative_path, full_filename)
                            directory_path = os.path.join(base_path, relative_path)
                        else:
                            full_file_path = os.path.join(base_path, full_filename)
                            directory_path = base_path
                        
                    
                        # Create directory structure if it doesn't exist
                        if not os.path.exists(directory_path):
                            os.makedirs(directory_path, exist_ok=True)
                            summary['directories_created'] += 1
                        
                        if call_back_function is not None:
                            content = call_back_function(content)
                        
                        # Write file content to disk
                        with open(full_file_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        
                        summary['files_created'] += 1
                        
                    except Exception as e:
                        raise e
        
        except Exception as e:
            raise Exception(f"Database error while exporting files: {str(e)}")
        
        return summary

    def export_single_file_to_disk(self, volume, record_id, base_path=None):
        """
        Export a single file record from the database to disk.
        
        Args:
            volume: String - volume identifier
            record_id: Int - ID of the specific record to export
            base_path: String - optional override for base directory path. If None, uses path from volume table
            
        Returns:
            str: Full path where the file was created
            
        Raises:
            Exception: If record not found, database operation fails, or file writing fails
        """
        import os
        from pathlib import Path
        
        # Get base path from volume table if not provided
        if base_path is None:
            volume_sql = f"""
            SELECT base_path FROM volume 
            WHERE volume = %s
            """
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(volume_sql, (volume,))
                    volume_result = cursor.fetchone()
                    if volume_result is None:
                        raise Exception(f"Volume '{volume}' not found in volume table")
                    base_path = volume_result[0]
            except Exception as e:
                raise Exception(f"Error retrieving base path from volume table: {str(e)}")
        
        # Retrieve the specific file record
        select_sql = f"""
        SELECT volume, file_path, file_name, file_extension, content, file_size
        FROM {self.table_name}
        WHERE volume = %s AND id = %s
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_sql, (volume, record_id))
                row = cursor.fetchone()
                
                if row is None:
                    raise Exception(f"Record with ID {record_id} not found for volume '{volume}'")
                
                # Extract file information
                file_path_ltree = row['file_path']
                file_name = row['file_name']
                file_extension = row['file_extension']
                content = row['content'] or ''  # Handle None content
                
                # Convert ltree path back to filesystem path
                if file_path_ltree == 'root':
                    # File is in root directory
                    relative_path = ''
                else:
                    # Convert ltree format (dots) back to filesystem format (slashes)
                    relative_path = file_path_ltree.replace('.', os.sep)
                
                # Construct full filename with extension
                if file_extension:
                    full_filename = f"{file_name}.{file_extension}"
                else:
                    full_filename = file_name
                
                # Create full file path
                if relative_path:
                    full_file_path = os.path.join(base_path, relative_path, full_filename)
                    directory_path = os.path.join(base_path, relative_path)
                else:
                    full_file_path = os.path.join(base_path, full_filename)
                    directory_path = base_path
                
                # Create directory structure if it doesn't exist
                if not os.path.exists(directory_path):
                    os.makedirs(directory_path, exist_ok=True)
                
                # Write file content to disk
                with open(full_file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                return full_file_path
        
        except Exception as e:
            if "Record with ID" in str(e) and "not found" in str(e):
                raise  # Re-raise record not found exception as-is
            else:
                raise Exception(f"Error exporting file: {str(e)}")