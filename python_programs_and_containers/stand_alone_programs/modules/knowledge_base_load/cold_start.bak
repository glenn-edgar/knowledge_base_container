#!/usr/bin/env python3

import json
import subprocess
import time
import sys
import os
import argparse
from pathlib import Path
from update_file_loader import load_configuration_scripts
from run_kb_config_scripts import run_kb_config_scripts

def update_flag_to_false(update_file_path):
    """Update the update_flag to false in the update.json file"""
    try:
        # Read the current update.json file
        with open(update_file_path, 'r') as f:
            update_data = json.load(f)
        
        # Update the update_flag to false
        update_data['update_flag'] = False
        
        # Write the updated data back to the file
        with open(update_file_path, 'w') as f:
            json.dump(update_data, f, indent=2)
        
        print(f"Successfully updated update_flag to false in {update_file_path}")
        return True
    except Exception as e:
        print(f"Error updating update_flag: {e}")
        return False

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Load and run configuration scripts')
    parser.add_argument('--force-update', action='store_true', 
                        help='Force update by passing true to load_configuration_scripts')
    args = parser.parse_args()
    
    # Determine whether to force update based on command line parameter
    force_update = args.force_update
    print(f"Force update: {force_update}")
    
    # Determine the home directory
    home_dir = Path.home()
    print(f"Home directory: {home_dir}")
    
    # Read ~/mount_startup/node_definition.json
    config_file = home_dir / "mount_startup" / "node_definition.json"
    update_file = home_dir / "mount_startup" / "update.json"
    current_directory = os.getcwd()
    config_map = {
        "working": current_directory+"/knowledge_base_load/configs/mount_kb_definitions"
    }
    update_dir = home_dir / "mount_kb_definitions"
    result, files_transferred = load_configuration_scripts(config_file, update_file, config_map, update_dir, force_update)
    if result is False:
        print("not master or no update scheduled")
        sys.exit(0)

    if files_transferred is None:
        print("no files transferred")
    else:
        print(f"Configuration scripts loaded: {result}")
    
        # Verify file ~/mount_kb_definitions/start.py exists
        start_script = update_dir / "start.py"
        if not start_script.exists():
            raise FileNotFoundError(f"Start script not found: {start_script}")
        print(f"Verified start script exists: {start_script}")
        
       
    
    # Run kb config scripts
    run_kb_config_scripts(update_dir, 15)
    
    # If everything passed successfully, update the update_flag to false
    print("All operations completed successfully. Updating update_flag to false...")
    if update_flag_to_false(update_file):
        print("Update process completed and flag reset successfully")
    else:
        print("Warning: Could not reset update_flag, but all operations completed")
    
if __name__ == "__main__":
    main()