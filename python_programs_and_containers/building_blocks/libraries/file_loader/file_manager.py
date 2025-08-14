import os
from pathlib import Path


class File_Manager:
    def __init__(self, starting_directory=None, allowable_extension_list=None):
        """
        Initialize FileManager with starting directory and allowed extensions.
        
        Args:
            starting_directory: String path to the starting directory (optional)
            allowable_extension_list: List of allowed file extensions (e.g., ['.txt', '.py', '.md'])
        
        Raises:
            Exception: If starting_directory doesn't exist or isn't a directory
        """
        self.starting_directory = None
        self.allowable_extensions = []
        
        if starting_directory:
            self.set_starting_directory(starting_directory)
        
        if allowable_extension_list:
            self.set_allowable_extensions(allowable_extension_list)
    
    def set_starting_directory(self, starting_directory):
        """Set and validate the starting directory."""
        starting_directory = Path(starting_directory)
        if not starting_directory.exists():
            raise Exception(f"Starting directory does not exist: {starting_directory}")
        if not starting_directory.is_dir():
            raise Exception(f"Starting directory is not a directory: {starting_directory}")
        
        self.starting_directory = starting_directory
    
    def set_allowable_extensions(self, allowable_extension_list):
        """Set and normalize the allowable extensions."""
        self.allowable_extensions = []
        for ext in allowable_extension_list:
            if not ext.startswith('.'):
                ext = '.' + ext
            self.allowable_extensions.append(ext.lower())
    
    def _is_allowed_extension(self, file_path, allowable_extension_list=None):
        """
        Check if file has an allowed extension.
        
        Args:
            file_path: Path object
            allowable_extension_list: Optional override for instance extensions
            
        Returns:
            bool: True if extension is allowed
        """
        extensions_to_check = allowable_extension_list or self.allowable_extensions
        return file_path.suffix.lower() in extensions_to_check
    
    def read_directory(self, call_back_function, starting_directory=None, allowable_extension_list=None):
        """
        Recursively read directories and apply callback function to allowed files.
        
        Args:
            call_back_function: Function with signature callback(current_path, data)
                            where current_path is string (relative to starting_directory) and data is file content
            starting_directory: Optional override for instance starting directory
            allowable_extension_list: Optional override for instance extensions
        
        Returns:
            list: List of file paths that were processed (full paths)
            
        Raises:
            Exception: If file reading or callback execution fails
        """
        # Use provided or instance starting directory
        start_dir = Path(starting_directory) if starting_directory else self.starting_directory
        if not start_dir:
            raise Exception("No starting directory specified")
        
        if not start_dir.exists():
            raise Exception(f"Starting directory does not exist: {start_dir}")
        if not start_dir.is_dir():
            raise Exception(f"Starting directory is not a directory: {start_dir}")
        
        # Use provided or instance extensions
        extensions = allowable_extension_list
        if extensions:
            # Normalize extensions (ensure they start with '.')
            normalized_extensions = []
            for ext in extensions:
                if not ext.startswith('.'):
                    ext = '.' + ext
                normalized_extensions.append(ext.lower())
            extensions = normalized_extensions
        else:
            extensions = self.allowable_extensions
        
        if not extensions:
            raise Exception("No allowable extensions specified")
        
        processed_files = []
        
        for root, dirs, files in os.walk(start_dir):
            for file in files:
                file_path = Path(root) / file
                
                if self._is_allowed_extension(file_path, extensions):
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            data = f.read()
                        
                        # Calculate relative path from starting directory
                        relative_path = file_path.relative_to(start_dir)
                        
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
    
    def read_new_files(self, call_back_function, list_of_existing_files, starting_directory=None, allowable_extension_list=None):
        """
        Read directory and apply callback only to files not in existing list.
        
        Args:
            call_back_function: Function with signature callback(current_path, data)
            list_of_existing_files: List of file paths that already exist
            starting_directory: Optional override for instance starting directory
            allowable_extension_list: Optional override for instance extensions
            
        Returns:
            list: List of all files processed (including those that matched existing files)
            
        Raises:
            Exception: If file reading or callback execution fails
        """
        # Use provided or instance starting directory
        start_dir = Path(starting_directory) if starting_directory else self.starting_directory
        if not start_dir:
            raise Exception("No starting directory specified")
        
        # Use provided or instance extensions
        extensions = allowable_extension_list or self.allowable_extensions
        if not extensions:
            raise Exception("No allowable extensions specified")
        
        processed_files = []
        existing_files_set = set(list_of_existing_files)
        
        for root, dirs, files in os.walk(start_dir):
            for file in files:
                file_path = Path(root) / file
                file_path_str = str(file_path)
                
                if self._is_allowed_extension(file_path, extensions):
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