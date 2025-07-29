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

    def store_file(self, update, volume, file_path, data):
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
            bool: True if record exists, False otherwise
            
        Raises:
            Exception: If database operation fails
        """
        check_sql = f"""
        SELECT 1 FROM {self.table_name} 
        WHERE volume = %s AND file_path = %s AND file_name = %s AND file_extension = %s
        LIMIT 1
        """
        
        with self.conn.cursor() as cursor:
            cursor.execute(check_sql, (volume, file_path, file_name, file_extension))
            result = cursor.fetchone()
            
            return result is not None