import os
from pathlib import Path


class File_manager:
    def __init__(self ):
        """
        Initialize File_manager with starting directory and allowed extensions.
        
        Args:
            starting_directory: String path to the starting directory
            allowable_extension_list: List of allowed file extensions (e.g., ['.txt', '.py', '.md'])
        
        Raises:
            Exception: If starting_directory doesn't exist or isn't a directory
        """
        
        
    
    def _is_allowed_extension(self, file_path, allowable_extension_list):
        """
        Check if file has an allowed extension.
        
        Args:
            file_path: Path object
            
        Returns:
            bool: True if extension is allowed
        """
        return file_path.suffix.lower() in allowable_extension_list
    
    def read_directory(self, call_back_function, starting_directory, allowable_extension_list):
        """
        Recursively read directories and apply callback function to allowed files.
        
        Args:
            call_back_function: Function with signature callback(current_path, data)
                            where current_path is string (relative to starting_directory) and data is file content
        
        Returns:
            list: List of file paths that were processed (full paths)
            
        Raises:
            Exception: If file reading or callback execution fails
        """
        starting_directory = Path(starting_directory)
        if not starting_directory.exists():
            raise Exception(f"Starting directory does not exist: {starting_directory}")
        if not starting_directory.is_dir():
            raise Exception(f"Starting directory is not a directory: {starting_directory}")
        
        # Normalize extensions (ensure they start with '.')
        allowable_extensions = []
        for ext in allowable_extension_list:
            if not ext.startswith('.'):
                ext = '.' + ext
            allowable_extensions.append(ext.lower())
        
        processed_files = []
        
        for root, dirs, files in os.walk(starting_directory):
            for file in files:
                file_path = Path(root) / file
                
                if self._is_allowed_extension(file_path, allowable_extensions):
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            data = f.read()
                        
                        # Calculate relative path from starting directory
                        relative_path = file_path.relative_to(starting_directory)
                        
                        call_back_function(str(relative_path), data)
                        processed_files.append(str(file_path))  # Keep full path in return list
                        
                    except Exception as e:
                        raise Exception(f"Error processing file {file_path}: {str(e)}")
        
        return processed_files
    
    def write_file(self, path, data, new_file=False):
        """
        Write data to a file.
        
        Args:
            path: String path to the file
            data: String data to write
            new_file: Boolean - if False, file must exist; if True, file can exist or not
            
        Raises:
            Exception: If new_file=False and file doesn't exist, or if write operation fails
        """
        file_path = Path(path)
        
        if not new_file and not file_path.exists():
            raise Exception(f"File must exist before writing when new_file=False: {path}")
        
        try:
            # Create parent directories if they don't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(data)
                
        except Exception as e:
            raise Exception(f"Error writing file {path}: {str(e)}")
    
    def read_new_files(self, call_back_function, list_of_existing_files):
        """
        Read directory and apply callback only to files not in existing list.
        
        Args:
            call_back_function: Function with signature callback(current_path, data)
            list_of_existing_files: List of file paths that already exist
            
        Returns:
            list: List of all files processed (including those that matched existing files)
            
        Raises:
            Exception: If file reading or callback execution fails
        """
        processed_files = []
        existing_files_set = set(list_of_existing_files)
        
        for root, dirs, files in os.walk(self.starting_directory):
            for file in files:
                file_path = Path(root) / file
                file_path_str = str(file_path)
                
                if self._is_allowed_extension(file_path):
                    processed_files.append(file_path_str)
                    
                    # Only apply callback if file is not in existing list
                    if file_path_str not in existing_files_set:
                        try:
                            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                                data = f.read()
                            
                            call_back_function(file_path_str, data)
                            
                        except Exception as e:
                            raise Exception(f"Error processing new file {file_path}: {str(e)}")
        
        return processed_files

