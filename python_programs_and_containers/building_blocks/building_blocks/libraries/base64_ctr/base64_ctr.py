import base64
from typing import Union, Optional

class Base64Handler:
    """A class to handle Base64 encoding and decoding operations."""
    
    def __init__(self, url_safe: bool = False):
        """
        Initialize the Base64Handler.
        
        Args:
            url_safe (bool): If True, use URL-safe encoding (- and _ instead of + and /)
        """
        self.url_safe = url_safe
    
    def encode_string(self, text: str, encoding: str = 'utf-8') -> str:
        """
        Encode a string to Base64.
        
        Args:
            text (str): The string to encode
            encoding (str): Character encoding to use (default: utf-8)
            
        Returns:
            str: Base64 encoded string
        """
        text_bytes = text.encode(encoding)
        if self.url_safe:
            encoded_bytes = base64.urlsafe_b64encode(text_bytes)
        else:
            encoded_bytes = base64.b64encode(text_bytes)
        return encoded_bytes.decode('ascii')
    
    def decode_string(self, encoded_text: str, encoding: str = 'utf-8') -> str:
        """
        Decode a Base64 string.
        
        Args:
            encoded_text (str): The Base64 encoded string
            encoding (str): Character encoding for the output (default: utf-8)
            
        Returns:
            str: Decoded string
        """
        encoded_bytes = encoded_text.encode('ascii')
        if self.url_safe:
            decoded_bytes = base64.urlsafe_b64decode(encoded_bytes)
        else:
            decoded_bytes = base64.b64decode(encoded_bytes)
        return decoded_bytes.decode(encoding)
    
    def encode_bytes(self, data: bytes) -> str:
        """
        Encode bytes to Base64 string.
        
        Args:
            data (bytes): The bytes to encode
            
        Returns:
            str: Base64 encoded string
        """
        if self.url_safe:
            encoded_bytes = base64.urlsafe_b64encode(data)
        else:
            encoded_bytes = base64.b64encode(data)
        return encoded_bytes.decode('ascii')
    
    def decode_bytes(self, encoded_text: str) -> bytes:
        """
        Decode a Base64 string to bytes.
        
        Args:
            encoded_text (str): The Base64 encoded string
            
        Returns:
            bytes: Decoded bytes
        """
        encoded_bytes = encoded_text.encode('ascii')
        if self.url_safe:
            return base64.urlsafe_b64decode(encoded_bytes)
        else:
            return base64.b64decode(encoded_bytes)
    
    def encode_file(self, input_path: str, output_path: Optional[str] = None) -> str:
        """
        Encode a file to Base64.
        
        Args:
            input_path (str): Path to the input file
            output_path (str, optional): Path to save the encoded output
            
        Returns:
            str: Base64 encoded content
        """
        with open(input_path, 'rb') as file:
            file_content = file.read()
        
        encoded = self.encode_bytes(file_content)
        
        if output_path:
            with open(output_path, 'w') as output_file:
                output_file.write(encoded)
        
        return encoded
    
    def decode_file(self, input_path: str, output_path: str) -> None:
        """
        Decode a Base64 encoded file.
        
        Args:
            input_path (str): Path to the Base64 encoded file
            output_path (str): Path to save the decoded output
        """
        with open(input_path, 'r') as file:
            encoded_content = file.read()
        
        decoded = self.decode_bytes(encoded_content)
        
        with open(output_path, 'wb') as output_file:
            output_file.write(decoded)
    
    @staticmethod
    def is_valid_base64(text: str) -> bool:
        """
        Check if a string is valid Base64.
        
        Args:
            text (str): String to validate
            
        Returns:
            bool: True if valid Base64, False otherwise
        """
        try:
            # Try standard Base64
            base64.b64decode(text, validate=True)
            return True
        except Exception:
            try:
                # Try URL-safe Base64
                base64.urlsafe_b64decode(text)
                return True
            except Exception:
                return False


# Example usage
if __name__ == "__main__":
    # Create a standard Base64 handler
    handler = Base64Handler()
    
    # Example 1: Encode and decode a string
    original_text = "Hello, World! This is a test."
    encoded = handler.encode_string(original_text)
    decoded = handler.decode_string(encoded)
    
    print(f"Original: {original_text}")
    print(f"Encoded: {encoded}")
    print(f"Decoded: {decoded}")
    print()
    
    # Example 2: URL-safe encoding
    url_handler = Base64Handler(url_safe=True)
    url_encoded = url_handler.encode_string("test+data/with=special")
    print(f"URL-safe encoded: {url_encoded}")
    print(f"URL-safe decoded: {url_handler.decode_string(url_encoded)}")
    print()
    
    # Example 3: Working with bytes
    byte_data = b'\x00\x01\x02\x03\x04\x05'
    encoded_bytes = handler.encode_bytes(byte_data)
    decoded_bytes = handler.decode_bytes(encoded_bytes)
    print(f"Original bytes: {byte_data.hex()}")
    print(f"Encoded bytes: {encoded_bytes}")
    print(f"Decoded bytes: {decoded_bytes.hex()}")
    print()
    
    # Example 4: Validate Base64
    valid_b64 = "SGVsbG8gV29ybGQ="
    invalid_b64 = "This is not base64!"
    print(f"'{valid_b64}' is valid: {Base64Handler.is_valid_base64(valid_b64)}")
    print(f"'{invalid_b64}' is valid: {Base64Handler.is_valid_base64(invalid_b64)}")