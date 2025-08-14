#!/usr/bin/env python3

import psycopg2
import time

class PostgresConnectionWaiter:
    def __init__(self, 
                 host: str = 'localhost',
                 port: int = 5432,
                 database: str = 'vectordb',
                 user: str = 'postgres',
                 password: str = 'password'):
        
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    
    def wait_for_connection(self, max_attempts: int = 30, delay: int = 2) -> bool:
        """Wait for PostgreSQL to accept connections"""
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Attempt to connect to PostgreSQL
                connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    connect_timeout=5
                )
                
                # Test the connection with a simple query
                cursor = connection.cursor()
                cursor.execute('SELECT version();')
                version = cursor.fetchone()[0]
                
                cursor.close()
                connection.close()
                
                return True
                
            except psycopg2.OperationalError:
                if attempt >= max_attempts:
                    raise TimeoutError(f"PostgreSQL connection failed after {max_attempts} attempts")
                time.sleep(delay)
            except Exception as e:
                raise RuntimeError(f"Unexpected error during connection attempt {attempt}: {e}")
        
        return False
    
    def test_connection(self) -> dict:
        """Test connection and return database information"""
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=5
            )
            
            cursor = connection.cursor()
            
            # Get version
            cursor.execute('SELECT version();')
            version = cursor.fetchone()[0]
            
            # Get database name
            cursor.execute('SELECT current_database();')
            current_db = cursor.fetchone()[0]
            
            # Get current user
            cursor.execute('SELECT current_user;')
            current_user = cursor.fetchone()[0]
            
            cursor.close()
            connection.close()
            
            return {
                'connected': True,
                'version': version,
                'database': current_db,
                'user': current_user,
                'host': self.host,
                'port': self.port
            }
            
        except Exception as e:
            return {
                'connected': False,
                'error': str(e),
                'host': self.host,
                'port': self.port
            }
    
    def is_connected(self) -> bool:
        """Quick check if PostgreSQL is accepting connections"""
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=3
            )
            connection.close()
            return True
        except:
            return False


