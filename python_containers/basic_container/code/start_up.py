import json
import time
import psycopg2
from psycopg2 import OperationalError, DatabaseError
import sys
import os
import signal
from contextlib import contextmanager

class Container_Start:
    def __init__(self, data_dir="/data", retry_interval=5, max_retries=None, connection_timeout=10):
        """
        Initialize the container startup process.
        
        Args:
            data_dir (str): Directory path containing configuration files (default: "/data")
            retry_interval (int): Time in seconds between connection retries (default: 5)
            max_retries (int): Maximum number of retries (None for infinite retries)
            connection_timeout (int): Timeout for each connection attempt in seconds (default: 10)
        """
        self.data_dir = data_dir
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        self.connection_timeout = connection_timeout
        self.connection = None
        
        # Ensure data directory exists
        if not os.path.exists(self.data_dir):
            self._post_error(f"Data directory does not exist: {self.data_dir}")
            return
        
        # Check success file
        if not self._check_success_file():
            self._post_error("Success file check failed")
            return
        
        # Load PostgreSQL configuration
        postgres_config = self._load_postgres_config()
        if not postgres_config:
            self._post_error("Failed to load PostgreSQL configuration")
            return
        
        # Keep trying to connect until successful
        self._connect_to_postgres(postgres_config)
    
    def _check_success_file(self):
        """Check if the success file exists and has the correct content."""
        success_file_path = os.path.join(self.data_dir, 'success.json')
        try:
            with open(success_file_path, 'r') as f:
                data = json.load(f)
                return data.get('success') == 'ok'
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            print(f"Error checking success file at {success_file_path}: {e}")
            return False
    
    def _load_postgres_config(self):
        """Load PostgreSQL connection configuration from JSON file."""
        config_file_path = os.path.join(self.data_dir, 'postgres_connector.json')
        try:
            with open(config_file_path, 'r') as f:
                config = json.load(f)
                
                # Validate required keys
                required_keys = ['dbname', 'user', 'host', 'port']
                missing_keys = [key for key in required_keys if key not in config]
                
                if missing_keys:
                    print(f"Missing required keys in postgres config: {missing_keys}")
                    print(f"Required keys: {required_keys}")
                    return None
                
                # Ensure port is a string (psycopg2 expects string)
                config['port'] = str(config['port'])
                
                return config
                
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading postgres config from {config_file_path}: {e}")
            return None
    
    @contextmanager
    def _timeout_handler(self, timeout_duration):
        """Context manager to handle connection timeouts."""
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Connection attempt timed out after {timeout_duration} seconds")
        
        # Set up the timeout
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_duration)
        
        try:
            yield
        finally:
            # Clean up
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    
    def _test_connection_with_timeout(self, config):
        """Test PostgreSQL connection with timeout."""
        connection = None
        try:
            with self._timeout_handler(self.connection_timeout):
                # Create connection with all parameters
                connection_params = {
                    'dbname': config['dbname'],
                    'user': config['user'],
                    'host': config['host'],
                    'port': config['port'],
                    'connect_timeout': self.connection_timeout  # psycopg2 built-in timeout
                }
                
                # Add password if provided in config
                if 'password' in config and config['password']:
                    connection_params['password'] = config['password']
                
                print(f"  Connecting to: {config['user']}@{config['host']}:{config['port']}/{config['dbname']}")
                connection = psycopg2.connect(**connection_params)
                
                # Test the connection with a simple query
                cursor = connection.cursor()
                cursor.execute('SELECT 1')
                result = cursor.fetchone()
                cursor.close()
                
                if result and result[0] == 1:
                    return connection
                else:
                    raise DatabaseError("Connection test query failed")
                    
        except TimeoutError as e:
            print(f"  Connection timed out: {e}")
            if connection:
                try:
                    connection.close()
                except:
                    pass
            raise
        except (OperationalError, DatabaseError) as e:
            print(f"  Database error: {e}")
            if connection:
                try:
                    connection.close()
                except:
                    pass
            raise
        except Exception as e:
            print(f"  Unexpected error: {e}")
            if connection:
                try:
                    connection.close()
                except:
                    pass
            raise
    
    def _connect_to_postgres(self, config):
        """Keep trying to connect to PostgreSQL until successful."""
        attempt = 0
        
        print("Starting PostgreSQL connection attempts...")
        
        while True:
            attempt += 1
            
            # Check if we've exceeded max retries
            if self.max_retries and attempt > self.max_retries:
                self._post_error(f"Failed to connect after {self.max_retries} attempts")
                return
            
            print(f"Attempt {attempt}:")
            
            try:
                # Try to connect with timeout
                self.connection = self._test_connection_with_timeout(config)
                print("✓ Successfully connected to PostgreSQL!")
                return  # Exit __init__ on successful connection
                
            except TimeoutError:
                print(f"✗ Connection timed out after {self.connection_timeout} seconds")
                
            except OperationalError as e:
                error_msg = str(e).strip()
                if "does not exist" in error_msg:
                    print(f"✗ Database '{config['dbname']}' does not exist")
                elif "could not connect to server" in error_msg:
                    print("✗ PostgreSQL server is not running or not accessible")
                elif "authentication failed" in error_msg:
                    print("✗ Authentication failed - check username/password")
                else:
                    print(f"✗ Connection failed: {error_msg}")
                    
            except Exception as e:
                print(f"✗ Unexpected error: {e}")
            
            # Wait before retrying
            if self.max_retries is None or attempt < self.max_retries:
                print(f"Retrying in {self.retry_interval} seconds...\n")
                time.sleep(self.retry_interval)
    
    def _post_error(self, error_message):
        """Post error and stop execution."""
        print(f"FATAL ERROR: {error_message}")
        sys.exit(1)
    
    def get_connection(self):
        """Get the current PostgreSQL connection."""
        return self.connection
    
    def close_connection(self):
        """Close the PostgreSQL connection if it exists."""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                print("PostgreSQL connection closed.")
            except:
                pass
    
    def __del__(self):
        """Cleanup when object is destroyed."""
        self.close_connection()
        
    def is_postgres_connection_alive(self) -> bool:
        """
        Check if a PostgreSQL connection is still alive.
        
        Args:
            connection: psycopg2 connection object
            
        Returns:
            bool: True if connection is alive, False otherwise
        """
        if self.connection is None:
            return False
            
        try:
            # Check if connection is closed
            if self.connection.closed != 0:
                return False
                
            # Try to execute a simple query
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
            return True
            
        except:
            return False
# Example usage:
if __name__ == "__main__":
    try:
        # With shorter timeout and retry settings for testing
        container = Container_Start( data_dir="/home/gedgar/mount_secrets/",
            connection_timeout=5,    # 5 second timeout per attempt
            retry_interval=1,        # 1 seconds between retries
            
        )
        
        print("Container started successfully!")
       
        # Your application logic here
        conn = container.get_connection()
        if conn:
            print("Database connection available for use")
        print("checking db connection")
        while container.is_postgres_connection_alive():
            print("checking connection")
            time.sleep(5)
            print("wait over")
       
        container.close_connection()
        
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    except Exception as e:
        print(f"Unexpected error: {e}")