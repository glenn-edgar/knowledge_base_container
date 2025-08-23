import subprocess
import sys
from pathlib import Path
from typing import Optional, Union


class KBConfigRunner:
    """
    A class to manage and execute KB configuration scripts with timeout protection.
    """
    
    DEFAULT_TIMEOUT = 300  # Default timeout in seconds
    DEFAULT_SCRIPT_NAME = "start.py"
    
    def __init__(self, kb_definitions: Union[str, Path], timeout: float = DEFAULT_TIMEOUT):
        """
        Initialize the KB Config Runner.
        
        Args:
            kb_definitions: Path to the KB definitions directory
            timeout: Maximum time in seconds to wait for script completion
        """
        self.kb_definitions = Path(kb_definitions)
        self.timeout = timeout
        self.script_name = self.DEFAULT_SCRIPT_NAME
        self._last_result = None
        
    @property
    def start_script(self) -> Path:
        """Get the full path to the start script."""
        return self.kb_definitions / self.script_name
    
    def set_timeout(self, timeout: float) -> None:
        """
        Update the timeout value.
        
        Args:
            timeout: New timeout value in seconds
        """
        if timeout <= 0:
            raise ValueError("Timeout must be a positive number")
        self.timeout = timeout
    
    def set_script_name(self, script_name: str) -> None:
        """
        Change the script name from the default 'start.py'.
        
        Args:
            script_name: Name of the script to run
        """
        self.script_name = script_name
    
    def verify_script_exists(self) -> bool:
        """
        Verify that the start script exists.
        
        Returns:
            True if the script exists, False otherwise
        """
        return self.start_script.exists()
    
    def run(self, capture_output: bool = True, check: bool = True) -> subprocess.CompletedProcess:
        """
        Run the KB configuration script with timeout protection.
        
        Args:
            capture_output: Whether to capture stdout and stderr
            check: Whether to raise an exception on non-zero exit code
        
        Returns:
            CompletedProcess instance with the result
        
        Raises:
            FileNotFoundError: If the start script doesn't exist
            TimeoutError: If the script doesn't complete within the timeout period
            RuntimeError: If the script fails with a non-zero exit code
        """
        # Verify script exists
        print(f"KB definitions: {self.kb_definitions}")
        
        if not self.verify_script_exists():
            raise FileNotFoundError(f"Start script not found: {self.start_script}")
        print(f"Verified start script exists: {self.start_script}")
        
        # Run the script
        print(f"Running start script with timeout of {self.timeout} seconds...")
        
        try:
            result = subprocess.run(
                [sys.executable, str(self.start_script)],
                cwd=str(self.kb_definitions),
                check=check,
                timeout=self.timeout,
                capture_output=capture_output,
                text=True
            )
            
            self._last_result = result
            print("Start script completed successfully")
            
            if result.stdout:
                print(f"Script output: {result.stdout}")
            
            return result
            
        except subprocess.TimeoutExpired as e:
            print(f"Start script timed out after {self.timeout} seconds")
            
            if e.stderr:
                print(f"Partial stderr: {e.stderr}")
            
            raise TimeoutError(
                f"Start script execution timed out after {self.timeout} seconds"
            )
            
        except subprocess.CalledProcessError as e:
            print(f"Start script failed with exit code {e.returncode}")
            
            if e.stdout:
                print(f"Script stdout: {e.stdout}")
            if e.stderr:
                print(f"Script stderr: {e.stderr}")
            
            raise RuntimeError(
                f"Start script execution failed with exit code {e.returncode}: {e.stderr}"
            )
            
        except Exception as e:
            print(f"Unexpected error during script execution: {e}")
            raise RuntimeError(f"Unexpected error during script execution: {e}")
    
    def run_with_custom_args(self, *args, **kwargs) -> subprocess.CompletedProcess:
        """
        Run the script with custom arguments.
        
        Args:
            *args: Positional arguments to pass to the script
            **kwargs: Keyword arguments for subprocess.run (excluding predefined ones)
        
        Returns:
            CompletedProcess instance with the result
        """
        if not self.verify_script_exists():
            raise FileNotFoundError(f"Start script not found: {self.start_script}")
        
        # Build command with custom arguments
        cmd = [sys.executable, str(self.start_script)] + list(args)
        
        # Remove kwargs that we handle internally
        run_kwargs = {
            'cwd': str(self.kb_definitions),
            'timeout': self.timeout,
            'text': True
        }
        run_kwargs.update(kwargs)
        
        try:
            result = subprocess.run(cmd, **run_kwargs)
            self._last_result = result
            return result
        except subprocess.TimeoutExpired as e:
            raise TimeoutError(
                f"Start script execution timed out after {self.timeout} seconds"
            )
        except Exception as e:
            raise RuntimeError(f"Unexpected error during script execution: {e}")
    
    def get_last_result(self) -> Optional[subprocess.CompletedProcess]:
        """
        Get the result of the last script execution.
        
        Returns:
            The last CompletedProcess result or None if no script has been run
        """
        return self._last_result
    
    def __repr__(self) -> str:
        """String representation of the KBConfigRunner instance."""
        return (
            f"KBConfigRunner(kb_definitions='{self.kb_definitions}', "
            f"timeout={self.timeout}, script='{self.script_name}')"
        )


# Convenience function that mimics the original interface
def run_kb_config_scripts(kb_definitions: Path, timeout: float = 300):
    """
    Run KB configuration scripts with timeout protection.
    
    This function provides backward compatibility with the original implementation.
    
    Args:
        kb_definitions: Path to the KB definitions directory
        timeout: Maximum time in seconds to wait for script completion (default: 300)
    
    Raises:
        FileNotFoundError: If the start script doesn't exist
        TimeoutError: If the script doesn't complete within the timeout period
        RuntimeError: If the script fails with a non-zero exit code
    """
    runner = KBConfigRunner(kb_definitions, timeout)
    runner.run()


# Example usage
if __name__ == "__main__":
    # Example 1: Basic usage
    runner = KBConfigRunner("/path/to/kb_definitions")
    try:
        result = runner.run()
        print(f"Script completed with return code: {result.returncode}")
    except (FileNotFoundError, TimeoutError, RuntimeError) as e:
        print(f"Error: {e}")
    
    # Example 2: Custom timeout
    runner.set_timeout(600)  # 10 minutes
    
    # Example 3: Custom script name
    runner.set_script_name("custom_start.py")
    
    # Example 4: Run with custom arguments
    try:
        result = runner.run_with_custom_args("--verbose", "--config", "production")
    except Exception as e:
        print(f"Error: {e}")
    
    # Example 5: Using the backward-compatible function
    try:
        run_kb_config_scripts(Path("/path/to/kb_definitions"), timeout=300)
    except Exception as e:
        print(f"Error: {e}")
