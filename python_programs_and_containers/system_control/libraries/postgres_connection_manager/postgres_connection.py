#!/usr/bin/env python3

import psycopg2
import time

class Postgres_Connection_Manager:
    def __init__(self, postgres_object:dict):
        self.postgres_object = postgres_object
        
        self.host = postgres_object['host']
        self.port = postgres_object['port']
        self.database = postgres_object['dbname']
        self.user = postgres_object['user']
        self.password = postgres_object['password']
                 
  
    
    def wait_for_connection(self, max_attempts: int = 2, delay: int = .5) -> bool:
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
        
        raise Exception("Postgres connection failed")
    

    def is_connected(self) -> bool:
        """Quick check if PostgreSQL is accepting connections with ping test"""
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=3
            )
            
            # Perform a simple ping test by executing a lightweight query
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            
            # Verify we got the expected result
            return result is not None and result[0] == 1
            
        except:
            return False
