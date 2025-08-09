"""
Secrets Loader Module

A module for managing and loading encrypted secrets using AES encryption
and file storage capabilities.

This module combines AES encryption/decryption with file loading operations
to provide secure secrets management.

Dependencies:
    - aes: AES encryption/decryption functionality (AESCipher class)
    - file_loader: File management and storage capabilities (Composite_File_Loader class)
"""

import os
import sys
import base64
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor


# Import required modules from specific paths
try:
    parent_dir = Path(__file__).parent.parent
    sys.path.append(str(parent_dir))

    # Now you can import from sibling packages
    from aes.aes_encrption import AES_Cipher
    from file_loader.composite_file_loader import Composite_File_Loader  # or specific classes

    from aes.aes_encrption import AES_Cipher
    from file_loader import Composite_File_Loader
    
  
except ImportError as e:
        print(f"Error importing required modules: {e}")
        print("Make sure 'aes' module is available at '../aes/aes.py' and 'file_loader' module is available at '../fileloader/file_loader.py'")
        raise e

class SecretsLoader:
    """
    A class for loading and managing encrypted secrets using AES encryption
    and file storage capabilities.
    """
    
    def __init__(self, encryption_key, key_size=32, base_table: str = "text_files", 
                 volume_table_name: str = "secrets_volumes", input_directory: str = None, 
                 output_directory: str = None, postgres_connector=None,
                 volume_name: str = "secrets"):
                
        """
        Initialize the SecretsLoader with encryption and file loading capabilities.
        
        Args:
            encryption_key (str or bytes): The AES encryption key for securing secrets
            key_size (int): Key size in bytes (16, 24, or 32 for AES-128, 192, 256)
            base_table (str): Database table name for storing secrets
            volume_table_name (str): Database table name for volume definitions
            input_directory (str): Directory to load secrets from
            output_directory (str): Directory to export secrets to
            postgres_connector: PostgreSQL connection object
        """
        self.aes_cipher = AES_Cipher(key_size)
        self.input_directory = input_directory
        self.output_directory = output_directory
        
        # Handle encryption key
        if isinstance(encryption_key, str):
            if len(encryption_key) == key_size * 2:  # Hex string
                self.encryption_key = bytes.fromhex(encryption_key)
            else:
                # Pad or truncate string to proper key size
                encryption_key = encryption_key.ljust(key_size, '\0')[:key_size]
                self.encryption_key = encryption_key.encode('utf-8')
        else:
            self.encryption_key = encryption_key
        
        # Validate key length
        if len(self.encryption_key) != key_size:
            raise ValueError(f"Encryption key must be {key_size} bytes long")
            
        if postgres_connector:
            self.file_loader = Composite_File_Loader(
                base_table_name=base_table,
                volume_table_name=volume_table_name,
                postgres_connector=postgres_connector
            )
        else:
            raise Exception("PostgreSQL connection not available for database operations")
            
    def encrypt_secret(self, secret_data: str) -> str:
        """
        Encrypt secret data using AES encryption.
        
        Args:
            secret_data (str): The secret data to encrypt
            
        Returns:
            str: The encrypted secret data (base64 encoded)
        """
        return self.aes_cipher.encrypt(secret_data, self.encryption_key)
    
    def decrypt_secret(self, encrypted_data: str) -> str:
        """
        Decrypt secret data using AES encryption.
        
        Args:
            encrypted_data (str): The encrypted secret data (base64 encoded)
            
        Returns:
            str: The decrypted secret data
        """
        return self.aes_cipher.decrypt(encrypted_data, self.encryption_key)
    
    def _encrypt_callback(self, update: bool, volume_name: str, file_path: str, data: str):
        """
        Callback function for encrypting data during directory loading.
        
        Args:
            update (bool): Whether to update existing records
            volume_name (str): The volume name
            file_path (str): The file path
            data (str): The file data to encrypt
        """
        try:
            encrypted_data = self.encrypt_secret(data)
            # Use the file_loader's store_record method with encrypted data
            self.file_loader.store_record(update, volume_name, file_path, encrypted_data)
        except Exception as e:
                raise e
    
    def load_secrets_directory(self, volume_name: str = "secrets", update: bool = True,
                             extension_list: list = None) -> None:
        """
        Load all secrets from input directory into the database with encryption.
        
        Args:
            volume_name (str): Name for the volume in the database
            update (bool): Whether to update existing records
            extension_list (list): List of file extensions to process
        """
        if not self.file_loader:
            raise Exception("PostgreSQL connection not available for database operations")
        
        if extension_list is None:
            extension_list = [".json"]
        if not self.file_loader.check_volume(volume_name):
            self.file_loader.add_volume(volume_name, self.input_directory, "Secrets volume")
        try:
            # Add volume if it doesn't exist
            
            
            # Load directory with encryption callback
            self.file_loader.load_directory(
                update=update,
                volume_name=volume_name,
                extension_list=extension_list,
                call_back_function=self._encrypt_callback
            )
            
            
            
        except Exception as e:
            raise Exception(f"Error loading secrets directory {self.input_directory}: {e}")
    
    def _decrypt_and_export_callback(self, update: bool, volume_name: str, file_path: str, encrypted_data: str):
        
        """
        Callback function for decrypting data during export.
        
        Args:
            update (bool): Whether to update existing records
            volume_name (str): The volume name
            file_path (str): The file path
            encrypted_data (str): The encrypted data to decrypt
        """
        try:
            decrypted_data = self.decrypt_secret(encrypted_data)
            
            # Create output file path
            output_path = Path(self.output_directory) / Path(file_path).relative_to(Path(file_path).anchor)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write decrypted data to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(decrypted_data)
                
            
            
        except Exception as e:
            
            # Still try to export encrypted version
            try:
                output_path = Path(self.output_directory) / Path(file_path).relative_to(Path(file_path).anchor)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(encrypted_data)
                    
            
            except Exception as export_error:
                print(f"Failed to export file {file_path}: {export_error}")
                raise export_error
    
    def export_volume_to_directory(self, volume_name: str, decrypt: bool = True) -> None:
        """
        Export secrets from database volume to output directory.
        
        Args:
            volume_name (str): The volume name to export
            decrypt (bool): Whether to decrypt the secrets before saving
            
        Raises:
            Exception: If export fails or decryption fails
        """
        if not self.file_loader:
            raise Exception("PostgreSQL connection not available for database operations")
        
        if decrypt:
            def decrypt_callback(content):
                
                """
                Callback function to decrypt file content before writing to disk.
                
                Args:
                    content (str): Encrypted file content
                    
                Returns:
                    str: Decrypted file content
                    
                Raises:
                    Exception: If decryption fails
                """
                if not content:
                    return content
                
                
                try:
                    decrypted_content = self.aes_cipher.decrypt(content, self.encryption_key)
                    return decrypted_content
                except Exception as e:
                    raise Exception(f"Decryption failed: {e}")
            
            
            self.file_loader.export_files_to_disk(
                volume_name, 
                self.output_directory, 
                call_back_function=decrypt_callback
            )
            
        else:
            # Export without decryption
            self.file_loader.export_files_to_disk(volume_name, self.output_directory)
            
            
 
    
    @staticmethod 
    def generate_crypto_key(key_size: int = 32) -> str:
        """
        Generate a new cryptographic key in hex form.
        
        Args:
            key_size (int): Key size in bytes (16, 24, or 32 for AES-128, 192, 256)
            
        Returns:
            str: A new cryptographic key in hexadecimal format
        """
        cipher = AES_Cipher(key_size)
        key = cipher.generate_key()
        return cipher.key_to_hex(key)


# Example usage and testing
if __name__ == "__main__":
    dbname = "knowledge_base"
    user = "gedgar"
    password = os.getenv("POSTGRES_PASSWORD")
    host = "localhost"
    port = "5432"
    
  
    # Establish database connection
    conn = psycopg2.connect(
    dbname=dbname, 
    user=user, 
    password=password, 
    host=host, 
    port=port
    )
    
    test_key=os.getenv("SECRET_LOADER")
   # print(f"Test key: {test_key}")


    # Generate a new key for testing
    
    # Test basic functionality without database
    
    secrets_loader = SecretsLoader(
        encryption_key=test_key,
        input_directory="/home/gedgar/raw_secrets",
        output_directory="/home/gedgar/mount_secrets",
        postgres_connector=conn
    )
    
    secrets_loader.load_secrets_directory(volume_name="secrets", update=True, extension_list=[".json"])
    secrets_loader.export_volume_to_directory(volume_name="secrets", decrypt=True)
    
    exit()