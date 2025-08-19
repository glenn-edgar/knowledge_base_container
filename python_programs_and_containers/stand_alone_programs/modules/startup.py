import json
import time
from pathlib import Path
from typing import List, Tuple

import docker
from docker.errors import APIError, DockerException, NotFound


def determine_initial_containers(config_file: Path) -> Tuple[bool, List[str]]:
    """
    Determine which containers to start based on configuration.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Tuple of (is_master: bool, container_list: List[str])
        
    Raises:
        FileNotFoundError: If configuration file doesn't exist
        KeyError: If required fields are missing from config
    """
    container_list = ["nats-ram"]
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    
    # Load configuration file
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    print(f"Loaded configuration: {config}")
    required_config_fields = ["site", "node_name", "master"]
    for field in required_config_fields:
        if field not in config:
            raise KeyError(f"Required field '{field}' not found in configuration")
    
    if config["master"]:
        container_list.append("postgres-vector")
        return True, container_list
    else:
        return False, container_list


def manage_containers_sdk(container_list: List[str], timeout: int = 30) -> None:
    """
    Manages Docker containers using Docker Python SDK.
    
    Args:
        container_list: List of container names to manage
        timeout: Maximum time in seconds to wait for containers to start (default: 30)
    
    Raises:
        ConnectionError: If unable to connect to Docker daemon
        RuntimeError: If containers fail to start or verification fails
        ValueError: If container is not found
    """
    
    try:
        client = docker.from_env()
    except DockerException as e:
        raise ConnectionError(f"Failed to connect to Docker daemon: {e}")
    
    def is_container_running(container_name: str) -> bool:
        """Check if a specific container is running."""
        try:
            container = client.containers.get(container_name)
            return container.status == "running"
        except NotFound:
            raise ValueError(f"Container '{container_name}' not found")
        except APIError as e:
            raise RuntimeError(f"Docker API error checking container {container_name}: {e}")
    
    def start_container(container_name: str) -> None:
        """Start a specific container."""
        try:
            container = client.containers.get(container_name)
            container.start()
        except NotFound:
            raise ValueError(f"Container '{container_name}' not found")
        except APIError as e:
            raise RuntimeError(f"Failed to start container {container_name}: {e}")
    
    def wait_for_container(container_name: str, timeout: int) -> None:
        """Wait for a container to be in running state within timeout period."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if is_container_running(container_name):
                return
            time.sleep(1)
        raise RuntimeError(f"Container '{container_name}' failed to start within {timeout} seconds")
    
    # Step 1: Check if all containers are already running
    all_running = True
    not_running = []
    
    for container in container_list:
        if is_container_running(container):
            print(f"Container '{container}' is already running")
        else:
            all_running = False
            not_running.append(container)
            print(f"Container '{container}' is not running")
    
    # If all containers are running, return early
    if all_running:
        print("All containers are already running")
        return
    
    # Step 2: Start containers that are not running
    for container in not_running:
        print(f"Starting container '{container}'...")
        start_container(container)
        wait_for_container(container, timeout)
        print(f"Container '{container}' started successfully")
    
    # Step 3: Wait 5 seconds
    print("Waiting 5 seconds before verification...")
    time.sleep(5)
    
    # Step 4: Verify all containers are still running
    verification_failed = []
    for container in container_list:
        if not is_container_running(container):
            verification_failed.append(container)
    
    if verification_failed:
        raise RuntimeError(f"Verification failed - containers not running: {', '.join(verification_failed)}")
    
    print("All containers verified as running")


def main() -> None:
    """Main entry point for the container management script."""
    try:
        home_dir = Path.home()
        config_file = home_dir / "mount_startup" / "node_definition.json"
        master_flag, container_list = determine_initial_containers(config_file)
        print(f"Master flag: {master_flag}")
        print(f"Containers to manage: {container_list}")
        manage_containers_sdk(container_list, timeout=15)
        print("Container initialization completed successfully")
    except FileNotFoundError as e:
        print(f"Configuration error: {e}")
        raise
    except KeyError as e:
        print(f"Invalid configuration: {e}")
        raise
    except (ConnectionError, ValueError, RuntimeError) as e:
        print(f"Container management error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()