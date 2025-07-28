
"""        
Here's the categorization of functions by what they affect:
Database Only Functions (14)
Read/Query Operations:

search_files(ltree_query, volume=None)
get_file_tree(root_path=None, volume=None)
get_volumes()
get_volume_stats(volume=None)

Delete Operations:

delete_file_by_id(file_id)
delete_file_by_path(ltree_path, volume=None)
delete_files_by_pattern(ltree_pattern, volume=None)
delete_volume(volume)

Update/Create Operations:

update_file_content(file_id, new_content)
update_file_content_by_path(ltree_path, new_content, volume=None)
create_new_file(ltree_path, file_name, content, volume=None)

Volume Management:

add_volume(volume_name, base_path) - Only updates in-memory mapping
remove_volume_mapping(volume_name) - Only updates in-memory mapping
get_volume_base_path(volume) - Only reads in-memory mapping

Disk Only Functions (0)

No functions only affect disk without database interaction

Both Database and Disk Functions (4)

Read from ast import Assign
from Disk → Store in Database:

_store_file(volume_name, file_path) - Reads file from disk, stores in DB
read_directory_recursive(directory_path, file_extensions=['.txt'], volume_name) - Reads files from disk, stores in DB

Read from Database → Write to Disk:

write_file_to_disk(file_id, output_path) - Reads from DB, writes to disk
write_files_by_pattern_to_disk(ltree_pattern, output_base_path, volume=None) - Reads from DB, writes to disk

Summary

Database Only: 14 functions
Disk Only: 0 functions
Database + Disk: 4 functions

The Assign is primarily database-centric, with disk operations mainly serving as input (reading files to store) or output (exporting stored files) operations.

"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
from typing import Optional, List, Dict, Any

class PostgresFileStorage:
    """
    A class to recursively read text files and store them in PostgreSQL 
    with ltree support for hierarchical path queries.
    """
    
    def __init__(self, connection: psycopg2.extensions.connection, 
                 table_name: str = "text_files"):
        """
        Initialize the PostgresFileStorage class.
        
        Args:
            connection: Active psycopg2 connection object
            table_name: Name of the table to store files (default: "text_files")
          
        """
        self.conn = connection
        self.table_name = table_name
        self.volume_base_paths = {}
        self.default_volume = None
        
        
        # Ensure ltree extension is available
        self._ensure_ltree_extension()
        
        # Create table if it doesn't exist
        self._create_table()
        self.existing_volumes = self.get_volumes()
        self.default_volume = self.existing_volumes[0] if self.existing_volumes else None
        
    def _ensure_ltree_extension(self):
        """Ensure the ltree extension is installed in the database."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS ltree;")
                self.conn.commit()
            
        except Exception as e:
            raise e
    
    def _create_table(self):
        """Create the text files table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            volume VARCHAR(255) NOT NULL,
            file_path LTREE NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            content TEXT,
            file_size BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(volume, file_path)
        );
        
        -- Create indexes for efficient ltree queries
        CREATE INDEX IF NOT EXISTS {self.table_name}_file_path_idx ON {self.table_name} USING GIST (file_path);
        CREATE INDEX IF NOT EXISTS {self.table_name}_file_path_btree_idx ON {self.table_name} USING BTREE (file_path);
        CREATE INDEX IF NOT EXISTS {self.table_name}_volume_idx ON {self.table_name} (volume);
        CREATE INDEX IF NOT EXISTS {self.table_name}_volume_path_idx ON {self.table_name} (volume, file_path);
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.conn.commit()

        except Exception as e:
            raise e
    
    def _path_to_ltree(self, file_path: Path, volume: str) -> str:
        """
        Convert a file path to ltree format.
        
        Args:
            file_path: Path object representing the file location
            volume: Volume name to determine the appropriate base path
            
        Returns:
            String representation suitable for ltree column
        """
        # Get base path for this volume
        base_path = self.volume_base_paths.get(volume)
        if base_path:
            base_path = Path(base_path)
            try:
                relative_path = file_path.relative_to(base_path)
            except ValueError:
                # If file_path is not relative to base_path, use full path
                relative_path = file_path
        else:
            relative_path = file_path
        
        # Convert path parts to ltree format
        # Replace invalid characters and convert to lowercase
        parts = []
        for part in relative_path.parts:
            # Remove file extension for the final part
            if part == relative_path.parts[-1]:
                part = relative_path.stem
            
            # Clean part for ltree (alphanumeric and underscore only)
            clean_part = ''.join(c if c.isalnum() else '_' for c in part.lower())
            # Ensure it doesn't start with a number
            if clean_part and clean_part[0].isdigit():
                clean_part = 'f_' + clean_part
            parts.append(clean_part)
        
        return '.'.join(parts) if parts else 'root'
    
    def _read_file_content(self, file_path: Path) -> tuple[str, int]:
        """
        Read the content of a text file.
        
        Args:
            file_path: Path to the text file
            
        Returns:
            Tuple of (content, file_size)
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                file_size = file_path.stat().st_size
                return content, file_size
        except UnicodeDecodeError:
            # Try with different encoding
            try:
                with open(file_path, 'r', encoding='latin-1') as file:
                    content = file.read()
                    file_size = file_path.stat().st_size
                    return content, file_size
            except Exception as e:
                raise e
        except Exception as e:
            raise e
        
        
    
    def _store_file(self, volume_name: str, file_path: Path) -> bool:
        """
        Store a single file in the database.
        
        Args:
            file_path: Path to the file
            content: Optional pre-read content (if None, will read from file)
            volume: Volume name (if None, uses default_volume)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            
            content, file_size = self._read_file_content(file_path)
            
            
            
            ltree_path = self._path_to_ltree(file_path, volume_name)
            file_name = file_path.name
            
            insert_sql = f"""
            INSERT INTO {self.table_name} (volume, file_path, file_name, content, file_size)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (volume, file_path) 
            DO UPDATE SET 
                content = EXCLUDED.content,
                file_size = EXCLUDED.file_size,
                updated_at = CURRENT_TIMESTAMP;
            """
            
            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, (volume_name, ltree_path, file_name, content, file_size))
                self.conn.commit()
                
            
                return True
                
        except Exception as e:
            
            self.conn.rollback()
            raise e
    
    def read_directory_recursive(self, directory_path: str,
                                volume_name: str,
                                file_extensions: List[str] = ['.txt']) -> int:
        """
        Recursively read all text files from a directory and store them in the database.
        
        Args:
            directory_path: Path to the directory to scan
            file_extensions: List of file extensions to process (default: ['.txt'])
            volume_name: Volume name for all files in this directory
            
        Returns:
            Number of files successfully processed
        """
        directory = Path(directory_path)
        if not directory.exists() or not directory.is_dir():
            raise ValueError(f"Directory does not exist or is not a directory: {directory_path}")
        
        
        
        # Walk through all files recursively
        for file_path in directory.rglob('*'):
            if file_path.is_file() and file_path.suffix.lower() in file_extensions:
                if self.store_file(file_path, volume=volume_name):
                    processed_count += 1
        
        
        return processed_count
    
    def search_files(self, ltree_query: str, volume: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search files using ltree queries.
        
        Args:
            ltree_query: Ltree query string (e.g., 'documents.*', '*.config')
            volume: Optional volume name to filter results
            
        Returns:
            List of dictionaries containing file information
        """
        if volume:
            search_sql = f"""
            SELECT id, volume, file_path, file_name, content, file_size, created_at, updated_at
            FROM {self.table_name}
            WHERE file_path ~ %s AND volume = %s
            ORDER BY volume, file_path;
            """
            params = (ltree_query, volume)
        else:
            search_sql = f"""
            SELECT id, volume, file_path, file_name, content, file_size, created_at, updated_at
            FROM {self.table_name}
            WHERE file_path ~ %s
            ORDER BY volume, file_path;
            """
            params = (ltree_query,)
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(search_sql, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            raise e
    
    def get_file_tree(self, root_path: str, 
                      volume_name: str) -> List[Dict[str, Any]]:
        """
        Get hierarchical file tree structure.
        
        Args:
            root_path: root path to filter results
            volume_name: name of the volume to get the file tree from
            
        Returns:
            List of dictionaries with file tree information
        """
        where_conditions = []
        params = []
        
        if root_path:
            where_conditions.append("file_path <@ %s")
            params.append(root_path)
        
        if volume_name:
            where_conditions.append("volume = %s")
            params.append(volume_name)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        tree_sql = f"""
        SELECT volume, file_path, file_name, nlevel(file_path) as depth
        FROM {self.table_name}
        {where_clause}
        ORDER BY volume, file_path;
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(tree_sql, tuple(params))
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            raise e
            
    
    def delete_file_by_id(self, file_id: int) -> bool:
        """
        Delete a specific file by its ID.
        
        Args:
            file_id: The ID of the file to delete
            
        Returns:
            True if file was deleted, False otherwise
        """
        delete_sql = f"DELETE FROM {self.table_name} WHERE id = %s;"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_sql, (file_id,))
                deleted = cursor.rowcount > 0
                self.conn.commit()
                return deleted
        except Exception as e:
            self.conn.rollback()
            raise e
    
    def delete_file_by_path(self, ltree_path: str, volume_name: str) -> bool:
        """
        Delete a specific file by its ltree path.
        
        Args:
            ltree_path: The ltree path of the file to delete
            volume_name: name of the volume to delete the file from
            
        Returns:
            True if file was deleted, False otherwise
        """
        
        delete_sql = f"DELETE FROM {self.table_name} WHERE file_path = %s AND volume = %s;"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_sql, (ltree_path, volume_name))
                deleted = cursor.rowcount > 0
                self.conn.commit()
                
                return deleted
        except Exception as e:
            self.conn.rollback()
            raise e

    def delete_files_by_pattern(self, ltree_pattern: str, volume_name: str) -> int:
        """
        Delete files matching an ltree pattern.
        
        Args:
            ltree_pattern: Ltree pattern to match files for deletion
            volume_name: name of the volume to delete the files from
            
        Returns:
            Number of files deleted
        """
        
        delete_sql = f"""
        DELETE FROM {self.table_name}
        WHERE file_path ~ %s AND volume = %s;
        """
        params = (ltree_pattern, volume_name)

    
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_sql, params)
                deleted_count = cursor.rowcount
                self.conn.commit()
                volume_info = f" in volume '{volume_name}'" if volume_name else ""
        
                return deleted_count
        except Exception as e:
            self.conn.rollback()
            raise e
        
    def delete_volume(self, volume: str) -> int:
        """
        Delete all files in a specific volume.
        
        Args:
            volume: Volume name to delete
            
        Returns:
            Number of files deleted
        """
        delete_sql = f"DELETE FROM {self.table_name} WHERE volume = %s;"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_sql, (volume,))
                deleted_count = cursor.rowcount
                self.conn.commit()
                
                return deleted_count
        except Exception as e:
            self.conn.rollback()
            raise e
    
    def update_file_content(self, file_id: int, new_content: str) -> bool:
        """
        Update the content of an existing file by ID.
        
        Args:
            file_id: The ID of the file to update
            new_content: The new content for the file
            
        Returns:
            True if update was successful, False otherwise
        """
        update_sql = f"""
        UPDATE {self.table_name} 
        SET content = %s, 
            file_size = %s, 
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s;
        """
        
        try:
            file_size = len(new_content.encode('utf-8'))
            with self.conn.cursor() as cursor:
                cursor.execute(update_sql, (new_content, file_size, file_id))
                updated = cursor.rowcount > 0
                self.conn.commit()
              
                return updated
        except Exception as e:
            self.conn.rollback()
            raise e
    
    def update_file_content_by_path(self, ltree_path: str, new_content: str, 
                                   volume_name: str) -> bool:
        """
        Update the content of an existing file by ltree path.
        
        Args:
            ltree_path: The ltree path of the file to update
            new_content: The new content for the file
            volume: Optional volume name (if None, uses default_volume)
            
        Returns:
            True if update was successful, False otherwise
        """
    
        update_sql = f"""
        UPDATE {self.table_name} 
        SET content = %s, 
            file_size = %s, 
            updated_at = CURRENT_TIMESTAMP
        WHERE file_path = %s AND volume = %s;
        """
        
        try:
            file_size = len(new_content.encode('utf-8'))
            with self.conn.cursor() as cursor:
                cursor.execute(update_sql, (new_content, file_size, ltree_path, volume_name))
                return cursor.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            raise e
    
    def create_new_file(self, ltree_path: str, file_name: str, content: str,
                       volume_name: str) -> Optional[int]:
        """
        Create a new file entry in the database.
        
        Args:
            ltree_path: The ltree path for the new file
            file_name: The name of the file
            content: The content of the file
            volume_name: name of the volume to create the file in,
            
        Returns:
            The ID of the newly created file, or None if creation failed
        """
        
        insert_sql = f"""
        INSERT INTO {self.table_name} (volume, file_path, file_name, content, file_size)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """
        
        try:
            file_size = len(content.encode('utf-8'))
            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, (volume_name, ltree_path, file_name, content, file_size))
                new_id = cursor.fetchone()[0]
                self.conn.commit()
              
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            raise e
        except Exception as e:
            
            self.conn.rollback()
            raise e
    
    def write_file_to_disk(self, file_id: int, output_path: str) -> bool:
        """
        Write a file from the database to disk.
        
        Args:
            file_id: The ID of the file to write
            output_path: The filesystem path where to write the file
            
        Returns:
            True if write was successful, False otherwise
        """
        select_sql = f"""
        SELECT volume, file_name, content 
        FROM {self.table_name} 
        WHERE id = %s;
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_sql, (file_id,))
                result = cursor.fetchone()
                
                if not result:
                    self.logger.error(f"No file found with ID: {file_id}")
                    return False
                
                volume, file_name, content = result
                output_file_path = Path(output_path) / volume / file_name
                
                # Create directory if it doesn't exist
                output_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                
                return True
                
        except Exception as e:
            raise e
    
    def write_files_by_pattern_to_disk(self,volume_name: str, ltree_pattern: str, output_base_path: str) -> int:
        """
        Write multiple files matching a pattern to disk, preserving directory structure.
        
        Args:
            ltree_pattern: Ltree pattern to match files
            output_base_path: Base path where files will be written
            volume_name: name of the volume to write the files from
            
        Returns:
            Number of files successfully written
        """
    
        select_sql = f"""
        SELECT volume, file_path, file_name, content 
        FROM {self.table_name} 
        WHERE file_path ~ %s AND volume = %s
        ORDER BY volume, file_path;
        """
        params = (ltree_pattern, volume_name)

    
        written_count = 0
        base_path = Path(output_base_path)
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_sql, params)
                results = cursor.fetchall()
                
                for volume_name, file_path, file_name, content in results:
                    # Convert ltree path back to filesystem path
                    ltree_parts = file_path.split('.')
                    file_dir = base_path / volume_name / Path(*ltree_parts[:-1]) if len(ltree_parts) > 1 else base_path / volume_name
                    output_file_path = file_dir / file_name
                    
                    # Create directory if it doesn't exist
                    output_file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    try:
                        with open(output_file_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        written_count += 1
                    except Exception as write_error:
                        raise write_error
                
                
                return written_count
                
        except Exception as e:
            raise e
    
    def get_volumes(self) -> List[str]:
        """
        Get a list of all volumes in the database.
        
        Returns:
            List of volume names
        """
        select_sql = f"SELECT DISTINCT volume FROM {self.table_name} ORDER BY volume;"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_sql)
                results = cursor.fetchall()
                return [row[0] for row in results]
        except Exception as e:
            raise e
    
    def get_volume_stats(self, volume: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics for a volume or all volumes.
        
        Args:
            volume: Optional volume name (if None, gets stats for all volumes)
            
        Returns:
            Dictionary with volume statistics
        """
        if volume:
            stats_sql = f"""
            SELECT 
                volume,
                COUNT(*) as file_count,
                SUM(file_size) as total_size,
                AVG(file_size) as avg_size,
                MIN(created_at) as oldest_file,
                MAX(created_at) as newest_file
            FROM {self.table_name}
            WHERE volume = %s
            GROUP BY volume;
            """
            params = (volume,)
        else:
            stats_sql = f"""
            SELECT 
                volume,
                COUNT(*) as file_count,
                SUM(file_size) as total_size,
                AVG(file_size) as avg_size,
                MIN(created_at) as oldest_file,
                MAX(created_at) as newest_file
            FROM {self.table_name}
            GROUP BY volume
            ORDER BY volume;
            """
            params = ()
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(stats_sql, params)
                results = cursor.fetchall()
                if volume:
                    return dict(results[0]) if results else {}
                else:
                    return {row['volume']: dict(row) for row in results}
        except Exception as e:
            raise e
    

        
        
    
 
    
    
    
    def delete_volume(self, volume: str) -> int:
        """
        Delete all files in a specific volume.
        
        Args:
            volume: Volume name to delete
            
        Returns:
            Number of files deleted
        """
        delete_sql = f"DELETE FROM {self.table_name} WHERE volume = %s;"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_sql, (volume,))
                deleted_count = cursor.rowcount
                self.conn.commit()
                
                return deleted_count
        except Exception as e:
            
            self.conn.rollback()
            raise e





# Example usage
if __name__ == "__main__":
    import psycopg2
    
    # Configure logging
    
    password = os.getenv('POSTGRES_PASSWORD')
    #print(password)
    # Database connection parameters
    db_params = {
        'host': 'localhost',
        'database': 'knowledge_base',
        'user': 'gedgar',
        'password': password,
        'port': 5432
    }
    
    
    # Create database connection
    conn = psycopg2.connect(**db_params)
    

    # Initialize the file storage system
    file_storage = PostgresFileStorage(
        connection=conn,
    
    )
    exit()
  