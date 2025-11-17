import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor


class Volume_Manager:
    def __init__(self, postgres_connector, volume_table_name):
        """
        Initialize Volume_Manager with database connection and table name.
        
        Args:
            postgres_connector: Active psycopg2 connection object
            volume_table_name: Name of the volume table to create/use
        """
        self.conn = postgres_connector
        self.volume_table_name = volume_table_name
        self._create_volume_table()
        self.existing_volumes = self._get_existing_volumes()
    
    def _create_volume_table(self):
        """Create the volume table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.volume_table_name} (
            id SERIAL PRIMARY KEY,
            volume_name VARCHAR(255) UNIQUE NOT NULL,
            volume_path TEXT NOT NULL,
            volume_description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create index for efficient volume name lookups
        CREATE INDEX IF NOT EXISTS idx_{self.volume_table_name}_volume_name 
        ON {self.volume_table_name}(volume_name);
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_sql)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error creating volume table: {str(e)}")
        
    def _get_existing_volumes(self):
        """Retrieve all existing volume names from the database"""
        select_sql = f"SELECT volume_name FROM {self.volume_table_name};"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_sql)
                results = cursor.fetchall()
            return {row[0] for row in results}
        except Exception as e:
            raise Exception(f"Error retrieving existing volumes: {str(e)}")
    
    def check_volume(self, volume_name):
        """
        Check if a volume exists in the database
        
        Args:
            volume_name (str): Name of the volume to check
            
        Returns:
            bool: True if volume exists, False otherwise
        """
        return volume_name in self.existing_volumes
    
    def add_volume(self, volume_name, volume_path, volume_description):
        """
        Add a new volume to the database
        
        Args:
            volume_name (str): Unique name for the volume
            volume_path (str): Filesystem path for the volume
            volume_description (str): Description of the volume
            
        Raises:
            ValueError: If volume already exists
            Exception: If path doesn't exist or database operation fails
        """
        if self.check_volume(volume_name):
            raise ValueError(f"Volume '{volume_name}' already exists")
        
        # Validate that the volume path exists and is a directory
        path_obj = Path(volume_path)
        if not path_obj.exists():
            raise Exception(f"Volume path does not exist: {volume_path}")
        if not path_obj.is_dir():
            raise Exception(f"Volume path is not a directory: {volume_path}")
        
        # Convert to absolute path for consistency
        absolute_path = str(path_obj.absolute())
        
        insert_sql = f"""
        INSERT INTO {self.volume_table_name} (volume_name, volume_path, volume_description)
        VALUES (%s, %s, %s);
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_sql, (volume_name, absolute_path, volume_description))
            self.conn.commit()
            self.existing_volumes.add(volume_name)
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            if "duplicate key" in str(e).lower():
                raise ValueError(f"Volume '{volume_name}' already exists")
            else:
                raise Exception(f"Database integrity error: {str(e)}")
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error adding volume '{volume_name}': {str(e)}")
    
    def delete_volume(self, volume_name):
        """
        Delete a volume from the database
        
        Args:
            volume_name (str): Name of the volume to delete
            
        Returns:
            int: Number of rows deleted (0 if volume didn't exist, 1 if deleted)
            
        Raises:
            Exception: If database operation fails
        """
        delete_sql = f"DELETE FROM {self.volume_table_name} WHERE volume_name = %s;"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_sql, (volume_name,))
                rows_deleted = cursor.rowcount
            self.conn.commit()
            
            if rows_deleted > 0:
                self.existing_volumes.discard(volume_name)
                
            return rows_deleted
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error deleting volume '{volume_name}': {str(e)}")
    
    def delete_all_volumes(self):
        """
        Delete all volumes from the database
        
        Returns:
            int: Number of volumes deleted
            
        Raises:
            Exception: If database operation fails
        """
        delete_all_sql = f"DELETE FROM {self.volume_table_name};"
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_all_sql)
                rows_deleted = cursor.rowcount
            self.conn.commit()
            self.existing_volumes.clear()
            return rows_deleted
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error deleting all volumes: {str(e)}")
        
    def get_volume_path(self, volume_name):
        """
        Retrieve the volume_path for a given volume_name
        
        Args:
            volume_name (str): The name of the volume to look up
            
        Returns:
            str: The volume path if found
            
        Raises:
            Exception: If volume not found or database error occurs
        """
        select_sql = f"""
        SELECT volume_path 
        FROM {self.volume_table_name} 
        WHERE volume_name = %s;
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_sql, (volume_name,))
                result = cursor.fetchone()
                
                if result:
                    return result[0]  # Return the volume_path
                else:
                    raise Exception(f"Volume '{volume_name}' not found")
                    
        except Exception as e:
            if f"Volume '{volume_name}' not found" in str(e):
                raise  # Re-raise volume not found exception
            else:
                raise Exception(f"Error retrieving volume path for '{volume_name}': {str(e)}")
    
    def get_all_volumes(self):
        """
        Retrieve all volumes from the database
        
        Returns:
            list: List of dictionaries containing volume information
            
        Raises:
            Exception: If database operation fails
        """
        select_all_sql = f"""
        SELECT id, volume_name, volume_path, volume_description, created_at, updated_at
        FROM {self.volume_table_name} 
        ORDER BY volume_name;
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_all_sql)
                results = cursor.fetchall()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            raise Exception(f"Error retrieving all volumes: {str(e)}")
    
    def get_volume_info(self, volume_name):
        """
        Get detailed information for a specific volume
        
        Args:
            volume_name (str): Name of the volume
            
        Returns:
            dict: Volume information including all fields
            
        Raises:
            Exception: If volume not found or database error occurs
        """
        select_sql = f"""
        SELECT id, volume_name, volume_path, volume_description, created_at, updated_at
        FROM {self.volume_table_name}
        WHERE volume_name = %s;
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_sql, (volume_name,))
                result = cursor.fetchone()
                
                if result:
                    return dict(result)
                else:
                    raise Exception(f"Volume '{volume_name}' not found")
                    
        except Exception as e:
            if f"Volume '{volume_name}' not found" in str(e):
                raise  # Re-raise volume not found exception
            else:
                raise Exception(f"Error retrieving volume info for '{volume_name}': {str(e)}")
    
    def update_volume(self, volume_name, volume_path=None, volume_description=None):
        """
        Update volume information
        
        Args:
            volume_name (str): Name of the volume to update
            volume_path (str, optional): New path for the volume
            volume_description (str, optional): New description for the volume
            
        Returns:
            int: Number of rows updated (0 if volume not found, 1 if updated)
            
        Raises:
            Exception: If database operation fails or path validation fails
        """
        if not self.check_volume(volume_name):
            return 0  # Volume doesn't exist
        
        update_fields = []
        update_values = []
        
        if volume_path is not None:
            # Validate the new path
            path_obj = Path(volume_path)
            if not path_obj.exists():
                raise Exception(f"Volume path does not exist: {volume_path}")
            if not path_obj.is_dir():
                raise Exception(f"Volume path is not a directory: {volume_path}")
            
            update_fields.append("volume_path = %s")
            update_values.append(str(path_obj.absolute()))
        
        if volume_description is not None:
            update_fields.append("volume_description = %s")
            update_values.append(volume_description)
        
        if not update_fields:
            return 0  # Nothing to update
        
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        update_values.append(volume_name)
        
        update_sql = f"""
        UPDATE {self.volume_table_name}
        SET {', '.join(update_fields)}
        WHERE volume_name = %s;
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_sql, update_values)
                rows_updated = cursor.rowcount
            self.conn.commit()
            return rows_updated
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error updating volume '{volume_name}': {str(e)}")
    
    def refresh_existing_volumes(self):
        """
        Refresh the cached list of existing volumes from the database
        
        This is useful if volumes might have been added/removed by other processes
        """
        self.existing_volumes = self._get_existing_volumes()
    
    def volume_exists(self, volume_name):
        """
        Alias for check_volume for consistency with other classes
        
        Args:
            volume_name (str): Name of the volume to check
            
        Returns:
            bool: True if volume exists, False otherwise
        """
        return self.check_volume(volume_name)