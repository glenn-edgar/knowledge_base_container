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


class ConfigurationScriptLoader:
    """Class to handle loading and running configuration scripts"""
    
    def __init__(self, force_update=False, home_dir=None):
        """
        Initialize the ConfigurationScriptLoader
        
        Args:
            force_update (bool): Whether to force update
            home_dir (Path): Home directory path (defaults to user home)
        """
        self.force_update = force_update
        self.home_dir = Path(home_dir) if home_dir else Path.home()
        self.current_directory = os.getcwd()
        
        # Define paths
        self.config_file = self.home_dir / "mount_startup" / "node_definition.json"
        self.update_file = self.home_dir / "mount_startup" / "update.json"
        self.update_dir = self.home_dir / "mount_kb_definitions"
        
        # Define config map
        self.config_map = {
            "working": f"{self.current_directory}/knowledge_base_load/configs/mount_kb_definitions"
        }
        
        print(f"Force update: {self.force_update}")
        print(f"Home directory: {self.home_dir}")
    
    def update_flag_to_false(self):
        """
        Update the update_flag to false in the update.json file
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read the current update.json file
            with open(self.update_file, 'r') as f:
                update_data = json.load(f)
            
            # Update the update_flag to false
            update_data['update_flag'] = False
            
            # Write the updated data back to the file
            with open(self.update_file, 'w') as f:
                json.dump(update_data, f, indent=2)
            
            print(f"Successfully updated update_flag to false in {self.update_file}")
            return True
        except Exception as e:
            print(f"Error updating update_flag: {e}")
            return False
    
    def load_scripts(self):
        """
        Load configuration scripts
        
        Returns:
            tuple: (result, files_transferred) from load_configuration_scripts
        """
        result, files_transferred = load_configuration_scripts(
            self.config_file, 
            self.update_file, 
            self.config_map, 
            self.update_dir, 
            self.force_update
        )
        
        if result is False:
            print("not master or no update scheduled")
            return result, files_transferred
        
        if files_transferred is None:
            print("no files transferred")
        else:
            print(f"Configuration scripts loaded: {result}")
            
            # Verify file ~/mount_kb_definitions/start.py exists
            start_script = self.update_dir / "start.py"
            if not start_script.exists():
                raise FileNotFoundError(f"Start script not found: {start_script}")
            print(f"Verified start script exists: {start_script}")
        
        return result, files_transferred
    
    def run_scripts(self, timeout=15):
        """
        Run kb config scripts
        
        Args:
            timeout (int): Timeout value for running scripts
        """
        run_kb_config_scripts(self.update_dir, timeout)
    
    def execute(self):
        """
        Main execution method
        
        Returns:
            int: Exit code (0 for success)
        """
        # Load configuration scripts
        result, files_transferred = self.load_scripts()
        
        if result is False:
            return 0
        
        # Run kb config scripts
        self.run_scripts(timeout=15)
        
        # If everything passed successfully, update the update_flag to false
        print("All operations completed successfully. Updating update_flag to false...")
        if self.update_flag_to_false():
            print("Update process completed and flag reset successfully")
        else:
            print("Warning: Could not reset update_flag, but all operations completed")
        
        return 0


