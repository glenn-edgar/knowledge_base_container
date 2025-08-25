#!/usr/bin/env python3

import json
import shutil
import subprocess
import time
import sys
import os
import argparse





class LoadNewKnowledgeBase:
    def __init__(self,config_directories:dict,config_dir:Path,exec_dir:Path,update_file_path:Path,force_update:bool=False):
        self.force_update = force_update
        self.update_file_path = update_file_path
        self.config_directories = config_directories
        self.config_dir = config_dir
        self.exec_dir = exec_dir
        if self.exec_dir.is_dir() is False:
            raise FileNotFoundError(f"Exec dir not found: {self.exec_dir}")
        if self.config_dir.is_dir() is False:
            raise FileNotFoundError(f"Config dir not found: {self.config_dir}")
        
        self.verify_update_file()
        self.abort = False
        if self.update_file_data["update_flag"] is False:
            if self.force_update is False:
               self.abort = True
        self.transfer_dir = self.config_dir / self.update_file_data["config_dir"]
        if self.transfer_dir.is_dir() is False:
            raise FileNotFoundError(f"Transfer dir not found: {self.transfer_dir}")
        
    def verify_update_file(self):
        if self.update_file_path.exists() is False:
            raise FileNotFoundError(f"Update file not found: {self.update_file_path}")
        with open(self.update_file_path, 'r') as f:
            self.update_file_data = json.load(f)
        if isinstance(self.update_file_data,dict) is False:
            raise TypeError(f"Update file data is not a dictionary: {self.update_file_data}")
        required_fields = ["update_flag","config_dir"]
        for field in required_fields:
            if field not in self.update_file_data:
                raise KeyError(f"Required field '{field}' not found in update file data: {self.update_file_data}")
        if isinstance(self.update_file_data["update_flag"],bool) is False:
            raise TypeError(f"Update flag is not a boolean: {self.update_file_data['update_flag']}")
        if isinstance(self.update_file_data["config_dir"],str) is False:
            raise TypeError(f"Config dir is not a string: {self.update_file_data['config_dir']}")
        if self.update_file_data["config_dir"] == "":
            return 
        if self.update_file_data["config_dir"] not in self.config_directories:
            raise KeyError(f"Config dir not found in config directories: {self.update_file_data['config_dir']}")
        
        
        
        
    def update_flag_to_false(self):
        """Update the update_flag to false in the update.json file"""
        try:
            # Read the current update.json file
            with open(self.update_file_path, 'r') as f:
                update_data = json.load(f)
            
            # Update the update_flag to false
            update_data['update_flag'] = False
            
            # Write the updated data back to the file
            with open(self.update_file_path, 'w') as f:
                json.dump(update_data, f, indent=2)
            
            print(f"Successfully updated update_flag to false in {self.update_file_path}")
            return True
        except Exception as e:
            print(f"Error updating update_flag: {e}")
            return False

    def load_configuration_scripts(self):
        #
        for item in self.exec_dir.iterdir():
            try:
                if item.is_file():
                    item.unlink()
                    print(f"Removed file: {item}")
                elif item.is_dir():
                    shutil.rmtree(item)
                    print(f"Removed directory: {item}")
            except Exception as e:
                print(f"Warning: Could not remove {item}: {e}")
    
        # Copy files from transfer_dir to exec_dir
        print(f"Copying files from {self.transfer_dir} to {self.exec_dir}")
        files_transferred = []
        
        for item in self.transfer_dir.iterdir():
            try:
                if item.is_file():
                    shutil.copy2(item, self.exec_dir)
                    files_transferred.append(item.name)
                    print(f"Copied file: {item.name}")
                elif item.is_dir():
                    dest_dir = self.exec_dir / item.name
                    shutil.copytree(item, dest_dir)
                    files_transferred.append(f"{item.name}/")
                    print(f"Copied directory: {item.name}")
            except Exception as e:
                print(f"Error copying {item}: {e}")
                # Optionally, you might want to raise here or continue
                # depending on your error handling requirements
        
        print(f"Configuration data is loaded. Files transferred: {files_transferred}")
        return True , files_transferred


    def load_knowledge_base(self):
        if self.abort is True:
            return False
        if self.update_file_data["config_dir"] != "":
            self.load_configuration_scripts()
        self.execute_kb_scripts()
        self.update_flag_to_false()
        return True

