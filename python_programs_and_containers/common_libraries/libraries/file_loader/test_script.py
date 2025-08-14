#!/usr/bin/env python3
"""
Test script for the file_loader module.
Run this file directly to test the functionality.
"""

import os
import sys
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

# Import the classes directly (no relative imports)
from file_manager import File_Manager
from volume_manager import Volume_Manager
from postgres_file_storage import File_Table


class CompositeFileLoader(FileManager, Volume_Manager, FileTable):
    """
    Composite class that combines file management, volume management, and database storage.
    
    This class uses multiple inheritance to combine functionality from:
    - FileManager: File system operations
    - Volume_Manager: Volume/path management
    - FileTable: Database storage operations
    """
    
    def __init__(self, base_table_name: str, volume_table_name: str, postgres_connector):
        """
        Initialize the CompositeFileLoader with database tables and connection.
        
        Args:
            base_table_name: Name of the file storage table
            volume_table_name: Name of the volume definition table
            postgres_connector: Active psycopg2 connection object
        """
        self.base_table_name = base_table_name
        self.volume_table_name = volume_table_name
        self.postgres_connector = postgres_connector
        
        # Initialize parent classes
        FileManager.__init__(self)
        Volume_Manager.__init__(self, postgres_connector, volume_table_name)
        FileTable.__init__(self, postgres_connector, base_table_name)

    def load_directory(self, update: bool, volume_name: str, extension_list: list, call_back_function=None):
        """
        Load all files from a directory into the database.
        
        Args:
            update (bool): Whether to update existing records
            volume_name (str): Name of the volume to load files into
            extension_list (list): List of file extensions to process (e.g., ['.txt', '.py'])
            call_back_function (callable, optional): Custom callback function to process file data
                                                   Signature: callback(update, volume_name, file_path, data)
                                                   If None, uses default store_record method
        """
        if call_back_function is None:
            call_back_function = self.store_record

        # Get the volume path from the database
        try:
            volume_path = self.get_volume_path(volume_name)
        except Exception as e:
            raise ValueError(f"Error getting volume path for '{volume_name}': {str(e)}")
        
        starting_directory = Path(volume_path)
        
        if not starting_directory.exists():
            raise ValueError(f"Volume directory does not exist: {starting_directory}")
        if not starting_directory.is_dir():
            raise ValueError(f"Volume path is not a directory: {starting_directory}")

        def internal_callback(file_path, data):
            """
            Internal callback that adapts the file_manager callback signature
            to our expected callback signature.
            """
            try:
                # file_path is already relative to starting_directory from FileManager.read_directory
                call_back_function(update, volume_name, file_path, data)
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
                raise
        
        # Use FileManager's read_directory method
        return self.read_directory(internal_callback, starting_directory, extension_list)

    def store_record(self, update: bool, volume_name: str, file_path: str, data: str):
        """
        Default callback function for storing file records.
        This method bridges the callback signature with the actual storage method.
        
        Args:
            update (bool): Whether to update existing records
            volume_name (str): Name of the volume
            file_path (str): Path of the file (relative to volume root)
            data (str): File content data
        """
        try:
            # Use FileTable's store_record method directly
            # The FileTable.store_record method handles the path parsing internally
            super().store_record(update, volume_name, file_path, data)
            
        except Exception as e:
            print(f"Error storing record for {file_path}: {e}")
            raise

    def export_volume_to_disk(self, volume_name: str, export_path: str = None, call_back_function=None):
        """
        Export files from database to disk using FileTable's built-in export functionality.
        
        Args:
            volume_name (str): Name of the volume to export
            export_path (str): Directory path where files should be exported (optional)
            call_back_function (callable, optional): Custom callback function to process file data before export
                                                   Signature: callback(content) -> modified_content
                                                   If None, files are exported as-is
        """
        try:
            # Use FileTable's export_files_to_disk method
            return super().export_files_to_disk(volume_name, export_path, call_back_function)
            
        except Exception as e:
            raise Exception(f"Error exporting volume '{volume_name}': {str(e)}")
    
    def get_volume_files(self, volume_name: str):
        """
        Get all file records for a specific volume from the database.
        
        Args:
            volume_name (str): Name of the volume
            
        Returns:
            list: List of file records from the database
        """
        # Use FileTable's retrieve_file method with a custom SQL query
        sql_query = f"""
        SELECT id, volume, file_path, file_name, file_extension, content, file_size, created_at, updated_at
        FROM {self.base_table_name}
        WHERE volume = '{volume_name}'
        ORDER BY file_path, file_name
        """
        
        try:
            return self.retrieve_file(sql_query)
        except Exception as e:
            raise Exception(f"Error retrieving files for volume '{volume_name}': {str(e)}")
    
    def get_volume_statistics(self, volume_name: str):
        """
        Get statistics about a volume.
        
        Args:
            volume_name (str): Name of the volume
            
        Returns:
            dict: Statistics including file count, total size, etc.
        """
        sql_query = f"""
        SELECT 
            COUNT(*) as file_count,
            SUM(file_size) as total_size,
            AVG(file_size) as avg_size,
            MIN(created_at) as oldest_file,
            MAX(updated_at) as newest_file
        FROM {self.base_table_name}
        WHERE volume = '{volume_name}'
        """
        
        try:
            results = self.retrieve_file(sql_query)
            if results:
                return results[0]
            else:
                return {
                    'file_count': 0,
                    'total_size': 0,
                    'avg_size': 0,
                    'oldest_file': None,
                    'newest_file': None
                }
        except Exception as e:
            raise Exception(f"Error getting statistics for volume '{volume_name}': {str(e)}")

    def clean_volume(self, volume_name: str):
        """
        Remove all file records for a specific volume from the database.
        
        Args:
            volume_name (str): Name of the volume to clean
            
        Returns:
            int: Number of records deleted
        """
        try:
            # Use FileTable's delete_by_volume method
            return super().delete_by_volume(volume_name)
        except Exception as e:
            raise Exception(f"Error cleaning volume '{volume_name}': {str(e)}")


def main():
    """Main function to test the file loader functionality."""
    print("=== FILE LOADER TEST ===")
    
    # Database connection parameters
    dbname = "knowledge_base"
    user = "gedgar"
    password = os.getenv("POSTGRES_PASSWORD")
    host = "localhost"
    port = "5432"

    if not password:
        print("Error: POSTGRES_PASSWORD environment variable not set")
        sys.exit(1)

    # Establish database connection
    try:
        conn = psycopg2.connect(
            dbname=dbname, 
            user=user, 
            password=password, 
            host=host, 
            port=port
        )
        print("✅ Database connection established")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)
    
    try:
        # Create composite file loader instance
        print("Creating CompositeFileLoader...")
        composite_loader = CompositeFileLoader(
            base_table_name="text_files", 
            volume_table_name="volume_definition_table", 
            postgres_connector=conn
        )
        print("✅ CompositeFileLoader created successfully")
        
        # Clean up existing volumes
        print("Cleaning up existing volumes...")
        deleted_volumes = composite_loader.delete_all_volumes()
        print(f"✅ Deleted {deleted_volumes} existing volumes")
        
        # Set up volume paths
        base_path = os.getcwd()
        kb_memory_path = os.path.join(base_path, "test_files/kb_go/kb_memory")
        postgres_path = os.path.join(base_path, "test_files/kb_go/postgres")
        
        # Check if test directories exist
        test_dirs_exist = Path(kb_memory_path).exists() and Path(postgres_path).exists()
        
        if test_dirs_exist:
            # Add volumes
            print("Adding volumes...")
            composite_loader.add_volume("kb_memory", kb_memory_path, "Knowledge base Go memory files")
            composite_loader.add_volume("postgres", postgres_path, "PostgreSQL Go files")
            print("✅ Volumes added successfully")
            
            # Load directories
            print("Loading kb_memory files...")
            kb_files = composite_loader.load_directory(
                update=False, 
                volume_name="kb_memory", 
                extension_list=[".go"]
            )
            print(f"✅ Loaded {len(kb_files)} kb_memory files")
            
            print("Loading postgres files...")
            pg_files = composite_loader.load_directory(
                update=False, 
                volume_name="postgres", 
                extension_list=[".go"]
            )
            print(f"✅ Loaded {len(pg_files)} postgres files")
            
            # Show statistics
            print("\nVolume Statistics:")
            for volume in ["kb_memory", "postgres"]:
                try:
                    stats = composite_loader.get_volume_statistics(volume)
                    print(f"  {volume}: {stats['file_count']} files, {stats['total_size']} bytes")
                except Exception as e:
                    print(f"  {volume}: Error getting stats - {e}")
            
            # Export files (optional)
            print("\nExporting files...")
            try:
                kb_export_path = os.path.join(base_path, "exported_files/kb_go/kb_memory")
                postgres_export_path = os.path.join(base_path, "exported_files/kb_go/postgres")
                
                kb_summary = composite_loader.export_volume_to_disk("kb_memory", kb_export_path)
                pg_summary = composite_loader.export_volume_to_disk("postgres", postgres_export_path)
                
                print(f"✅ KB Memory export: {kb_summary}")
                print(f"✅ Postgres export: {pg_summary}")
            except Exception as e:
                print(f"⚠️  Export failed: {e}")
            
            # Clean up database records
            print("\nCleaning up database records...")
            kb_deleted = composite_loader.clean_volume("kb_memory")
            pg_deleted = composite_loader.clean_volume("postgres")
            print(f"✅ Deleted {kb_deleted} kb_memory records and {pg_deleted} postgres records")
            
        else:
            print("⚠️  Test directories don't exist, running basic functionality test...")
            
            # Test basic volume operations
            test_volume_path = os.getcwd()  # Use current directory as test
            composite_loader.add_volume("test", test_volume_path, "Test volume")
            print("✅ Test volume added")
            
            volumes = composite_loader.get_all_volumes()
            print(f"✅ Found {len(volumes)} volumes in database")
            
            # Clean up
            composite_loader.delete_volume("test")
            print("✅ Test volume deleted")
        
        print("\n🎉 All tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        conn.close()
        print("✅ Database connection closed")


if __name__ == "__main__":
    main()
