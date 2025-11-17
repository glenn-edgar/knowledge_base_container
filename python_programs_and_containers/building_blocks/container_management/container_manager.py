#!/usr/bin/env python3

import docker
from typing import Optional, Dict, Any

class ContainerManager:
    def __init__(self, container_name: str):
        self.container_name = container_name
        
        # Initialize Docker client
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Docker client: {e}")
    
    @classmethod
    def container_exists(cls, container_name: str) -> bool:
        """
        Class method to check if a container exists (in any state).
        
        Args:
            container_name: Name of the container to check
            
        Returns:
            bool: True if container exists, False otherwise
        """
        try:
            client = docker.from_env()
            client.containers.get(container_name)
            return True
        except docker.errors.NotFound:
            return False
        except Exception as e:
            raise RuntimeError(f"Error checking if container exists: {e}")
    
    def start_container(self) -> docker.models.containers.Container:
        """Start the prebuilt container"""
        try:
            # Check if container already exists and is running
            container = self.docker_client.containers.get(self.container_name)
            if container.status == 'running':
                return container
            elif container.status in ['paused', 'exited']:
                # Start existing container
                container.start()
                return container
        except docker.errors.NotFound:
            # Container doesn't exist, this is an error since containers should be prebuilt
            raise RuntimeError(f"Container '{self.container_name}' not found. Container should be prebuilt.")
        except Exception as e:
            raise RuntimeError(f"Unexpected error starting container: {e}")
    
    def get_container_info(self) -> Optional[dict]:
        """Get information about the running container"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            return {
                'id': container.id[:12],
                'status': container.status,
                'name': container.name,
                'image': container.image.tags[0] if container.image.tags else 'unknown',
                'ports': container.attrs['NetworkSettings']['Ports'],
                'environment': container.attrs['Config']['Env'],
                'volumes': container.attrs['Mounts']
            }
        except docker.errors.NotFound:
            return None
        except Exception as e:
            raise RuntimeError(f"Error getting container info: {e}")
    
    def stop_container(self):
        """Stop the container (equivalent to docker stop)"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            if container.status == 'running':
                container.stop()
            elif container.status == 'paused':
                container.unpause()
                container.stop()
        except docker.errors.NotFound:
            raise RuntimeError(f"Container '{self.container_name}' not found")
        except Exception as e:
            raise RuntimeError(f"Error stopping container: {e}")
    
    def is_container_running(self) -> bool:
        """Check if the container is currently running"""
        info = self.get_container_info()
        return info is not None and info['status'] == 'running'
    
    def restart_container(self):
        """Restart the container"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            container.restart()
        except docker.errors.NotFound:
            raise RuntimeError(f"Container '{self.container_name}' not found")
        except Exception as e:
            raise RuntimeError(f"Error restarting container: {e}")
    
    def pause_container(self):
        """Pause the container"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            container.pause()
        except docker.errors.NotFound:
            raise RuntimeError(f"Container '{self.container_name}' not found")
        except Exception as e:
            raise RuntimeError(f"Error pausing container: {e}")
    
    def unpause_container(self):
        """Unpause the container"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            container.unpause()
        except docker.errors.NotFound:
            raise RuntimeError(f"Container '{self.container_name}' not found")
        except Exception as e:
            raise RuntimeError(f"Error unpausing container: {e}")
    
    def get_container_logs(self, tail: int = 100) -> str:
        """Get container logs"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            logs = container.logs(tail=tail, timestamps=True)
            return logs.decode('utf-8')
        except docker.errors.NotFound:
            raise RuntimeError(f"Container '{self.container_name}' not found")
        except Exception as e:
            raise RuntimeError(f"Error getting container logs: {e}")
    
    def execute_command(self, command: str) -> str:
        """Execute a command inside the running container"""
        try:
            container = self.docker_client.containers.get(self.container_name)
            if container.status != 'running':
                raise RuntimeError(f"Container '{self.container_name}' is not running")
            
            result = container.exec_run(command)
            return result.output.decode('utf-8')
        except docker.errors.NotFound:
            raise RuntimeError(f"Container '{self.container_name}' not found")
        except Exception as e:
            raise RuntimeError(f"Error executing command in container: {e}")


# Example usage:
if __name__ == "__main__":
    # Check if a container exists without creating an instance
    if ContainerManager.container_exists("my-postgres-container"):
        print("Container exists!")
        manager = ContainerManager("my-postgres-container")
        if manager.is_container_running():
            print("Container is running")
        else:
            print("Container exists but is not running")
    else:
        print("Container does not exist")