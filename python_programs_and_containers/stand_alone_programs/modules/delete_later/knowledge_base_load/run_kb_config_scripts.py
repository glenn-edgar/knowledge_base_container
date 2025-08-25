

import subprocess
import sys
from pathlib import Path

def run_kb_config_scripts(kb_definitions: Path, timeout: float = 300):
    """
    Run KB configuration scripts with timeout protection.
    
    Args:
        kb_definitions: Path to the KB definitions directory
        timeout: Maximum time in seconds to wait for script completion (default: 300)
    
    Raises:
        FileNotFoundError: If the start script doesn't exist
        TimeoutError: If the script doesn't complete within the timeout period
        RuntimeError: If the script fails with a non-zero exit code
    """
    # Verify file ~/mount_kb_definitions/start.py exists
    print("kb definitions: ", kb_definitions)
    
    start_script = kb_definitions / "start.py"
    if not start_script.exists():
        raise FileNotFoundError(f"Start script not found: {start_script}")
    print(f"Verified start script exists: {start_script}")
    
    # Run the script ~/mount_kb_definitions/start.py
    print(f"Running start script with timeout of {timeout} seconds...")
    
    try:
        result = subprocess.run(
            [sys.executable, str(start_script)],
            cwd=str(kb_definitions),
            check=True,
            timeout=timeout,
            capture_output=True,
            text=True
        )
        print("Start script completed successfully")
        if result.stdout:
            print(f"Script output: {result.stdout}")
            
    except subprocess.TimeoutExpired as e:
        print(f"Start script timed out after {timeout} seconds")
        
        # Optionally kill the process if it's still running
        if e.stderr:
            print(f"Partial stderr: {e.stderr}")
        raise TimeoutError(f"Start script execution timed out after {timeout} seconds")
        
    except subprocess.CalledProcessError as e:
        print(f"Start script failed with exit code {e.returncode}")
        if e.stdout:
            print(f"Script stdout: {e.stdout}")
        if e.stderr:
            print(f"Script stderr: {e.stderr}")
    
        raise RuntimeError(f"Start script execution failed with exit code {e.returncode}: {e.stderr}")
        
    except Exception as e:
        print(f"Unexpected error during script execution: {e}")
        raise RuntimeError(f"Unexpected error during script execution: {e}")