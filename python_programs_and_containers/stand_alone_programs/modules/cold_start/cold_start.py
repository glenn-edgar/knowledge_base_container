#!/usr/bin/env python3

import json
import subprocess
import time
import sys
from pathlib import Path

def main():
    # Determine the home directory
    home_dir = Path.home()
    print(f"Home directory: {home_dir}")
    
    # Read ~/mount_startup/node_definition.json
    config_file = home_dir / "mount_startup" / "node_definition.json"
    
    try:
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        print(f"Loaded configuration: {config}")
        
        # Check required fields
        required_fields = ["site", "node_name", "master", "update_flag"]
        for field in required_fields:
            if field not in config:
                raise KeyError(f"Required field '{field}' not found in configuration")
        
        # Check if master flag is false
        if not config["master"]:
            print("Master flag is false. Exiting.")
            sys.exit(0)
        
        # Check if update_flag is false
        if not config["update_flag"]:
            print("Update flag is false. Exiting.")
            sys.exit(0)
        
        # If update_flag is true, start containers
        print("Update flag is true. Checking and starting containers...")
        containers = ["postgres-vector", "mosquitto-ram-ws_main"]
        containers_started = []
        
        for container in containers:
            # First check if container is already running
            check_result = subprocess.run(
                ["docker", "ps", "--filter", f"name={container}", "--filter", "status=running", "--format", "{{.Names}}"],
                capture_output=True,
                text=True
            )
            
            if container in check_result.stdout:
                print(f"Container {container} is already running")
            else:
                # Container is not running, try to start it
                try:
                    result = subprocess.run(
                        ["docker", "start", container],
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    print(f"Started container: {container}")
                    containers_started.append(container)
                except subprocess.CalledProcessError as e:
                    print(f"Failed to start container {container}: {e}")
                    raise
        
        # Only wait 10 seconds if at least one container was started
        if containers_started:
            print(f"Waiting 10 seconds for {len(containers_started)} container(s) to initialize...")
            time.sleep(10)
        else:
            print("All containers were already running. No wait needed.")
        
        # Verify that containers are running
        print("Verifying containers are running...")
        for container in containers:
            try:
                result = subprocess.run(
                    ["docker", "ps", "--filter", f"name={container}", "--filter", "status=running", "--format", "{{.Names}}"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                if container not in result.stdout:
                    raise RuntimeError(f"Container {container} is not running")
                print(f"Verified container {container} is running")
            except subprocess.CalledProcessError as e:
                print(f"Failed to verify container {container}: {e}")
                raise
        
        # Verify that directory ~/mount_kb_definitions exists
        kb_definitions_dir = home_dir / "mount_kb_definitions"
        if not kb_definitions_dir.exists():
            raise FileNotFoundError(f"Directory not found: {kb_definitions_dir}")
        print(f"Verified directory exists: {kb_definitions_dir}")
        
        # Verify file ~/mount_kb_definitions/start.py exists
        start_script = kb_definitions_dir / "start.py"
        if not start_script.exists():
            raise FileNotFoundError(f"Start script not found: {start_script}")
        print(f"Verified start script exists: {start_script}")
        
        # Run the script ~/mount_kb_definitions/start.py
        print("Running start script...")
        try:
            result = subprocess.run(
                [sys.executable, str(start_script)],
                cwd=str(kb_definitions_dir),
                check=True
            )
            print("Start script completed successfully")
        except subprocess.CalledProcessError as e:
            print(f"Start script failed with exit code {e.returncode}")
            print("Directory will NOT be moved due to start script failure")
            raise RuntimeError(f"Start script execution failed: {e}")
        
        # Only move directory if start script succeeded
        # Move ~/mount_kb_definitions/ to ~/mount_kb_definitions_completed
        completed_dir = home_dir / "mount_kb_definitions_completed"
        
        # Remove completed directory if it already exists
        if completed_dir.exists():
            import shutil
            shutil.rmtree(completed_dir)
            print(f"Removed existing directory: {completed_dir}")
        
        kb_definitions_dir.rename(completed_dir)
        print(f"Moved {kb_definitions_dir} to {completed_dir}")
        
        print("Process completed successfully!")
        
    except FileNotFoundError as e:
        print(f"File not found error: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        sys.exit(1)
    except RuntimeError as e:
        print(f"Runtime error: {e}")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Command execution error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()