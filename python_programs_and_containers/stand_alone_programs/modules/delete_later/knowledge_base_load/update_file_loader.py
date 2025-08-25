import json
import sys
import shutil
from pathlib import Path
from typing import Dict, List, Optional

def load_configuration_scripts(
    config_file: Path, 
    update_file: Path, 
    config_map: Dict[str, str], 
    update_dir: Path,
    force_update: bool = False
) -> tuple[bool, List[str]]:
    """
    Check update flag and load the configuration into the specified directory.
    Transfer files from config_map directory to update_dir.
    
    Args:
        config_file: Path to the configuration JSON file
        update_file: Path to the update JSON file
        config_map: Dictionary mapping configuration names to directory paths
        update_dir: Path to the directory where files should be transferred
    
    Returns:
        List of transferred file/directory names, or None if no transfer occurred
    
    Raises:
        FileNotFoundError: If required files or directories are not found
        KeyError: If required fields are missing from configuration
        TypeError: If config_map value is not a string
        NotADirectoryError: If expected directories are not directories
    """
    # Check if config file exists
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    
    # Load configuration file
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    print(f"Loaded configuration: {config}")
    
    # Check if update file exists
    if not update_file.exists():
        raise FileNotFoundError(f"Update file not found: {update_file}")
    
    # Load update file
    with open(update_file, 'r') as f:
        update = json.load(f)
    
    print(f"Update file: {update}")
    
    # Check required fields in config
    required_config_fields = ["site", "node_name", "master"]
    for field in required_config_fields:
        if field not in config:
            raise KeyError(f"Required field '{field}' not found in configuration")
    
    # Check if master flag is false
    if not config["master"]:
        print("Master flag is false. Exiting.")
        return False , [] # Return None instead of sys.exit(0)
    
    # Check required fields in update
    required_update_fields = ["update_flag", "config_dir"]
    for field in required_update_fields:
        if field not in update:
            raise KeyError(f"Required field '{field}' not found in update configuration")
    
    # Check if update_flag is false
    if not update["update_flag"] and not force_update:
        print("Update flag is false. Exiting.")
        return False , []# Return None instead of sys.exit(0)
    
    # Check if update_dir exists - raise exception if not
    if not update_dir.exists():
        raise FileNotFoundError(f"Update directory not found: {update_dir}")
    
    if not update_dir.is_dir():
        raise NotADirectoryError(f"Update path is not a directory: {update_dir}")
    
    # Check if config_dir is empty - configuration data is already loaded
    if update["config_dir"] == "":
        print("Configuration directory is empty. Configuration data is already loaded.")
        return True , []  # Return empty list to indicate no files transferred
    
    # Check if config_map contains the specified config_dir key
    if update["config_dir"] not in config_map:
        raise KeyError(f"Configuration directory '{update['config_dir']}' not found in config_map")
    
    # Check if config_map[update["config_dir"]] is a string
    input_directory_path = config_map[update["config_dir"]]
    if not isinstance(input_directory_path, str):
        raise TypeError(f"Expected string for input directory path, got {type(input_directory_path)}")
    
    # Convert to Path object and validate it's a valid directory
    input_directory = Path(input_directory_path)
    if not input_directory.exists():
        raise FileNotFoundError(f"Input directory not found: {input_directory}")
    
    if not input_directory.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_directory}")
    
    # Remove files in update_dir
    print(f"Removing files from update directory: {update_dir}")
    for item in update_dir.iterdir():
        try:
            if item.is_file():
                item.unlink()
                print(f"Removed file: {item}")
            elif item.is_dir():
                shutil.rmtree(item)
                print(f"Removed directory: {item}")
        except Exception as e:
            print(f"Warning: Could not remove {item}: {e}")
    
    # Copy files from input_directory to update_dir
    print(f"Copying files from {input_directory} to {update_dir}")
    files_transferred = []
    
    for item in input_directory.iterdir():
        try:
            if item.is_file():
                shutil.copy2(item, update_dir)
                files_transferred.append(item.name)
                print(f"Copied file: {item.name}")
            elif item.is_dir():
                dest_dir = update_dir / item.name
                shutil.copytree(item, dest_dir)
                files_transferred.append(f"{item.name}/")
                print(f"Copied directory: {item.name}")
        except Exception as e:
            print(f"Error copying {item}: {e}")
            # Optionally, you might want to raise here or continue
            # depending on your error handling requirements
    
    print(f"Configuration data is loaded. Files transferred: {files_transferred}")
    return True , files_transferred