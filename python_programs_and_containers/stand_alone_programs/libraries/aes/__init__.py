"""
AES Encryption Package

A secure AES encryption/decryption package using the cryptography library.

This package provides:
- AESCipher: Main class for AES encryption/decryption operations
- Support for AES-128, AES-192, and AES-256 encryption
- CBC mode with random IV generation
- PKCS7 padding
- Base64 encoding for encrypted data
- Comprehensive testing suite

Classes:
    AESCipher: Main AES encryption/decryption class

Functions:
    test_basic_encryption_decryption: Test basic encrypt/decrypt operations
    test_different_key_sizes: Test different AES key sizes
    test_empty_and_special_strings: Test edge cases and special characters
    test_error_handling: Test error handling and validation
    test_consistency: Test encryption consistency and randomness
    test_hex_conversion: Test key hex conversion utilities

Usage:
    from aes import AESCipher
    
    # Create cipher instance
    cipher = AESCipher(32)  # AES-256
    
    # Generate a key
    key = cipher.generate_key()
    
    # Encrypt data
    encrypted = cipher.encrypt("Hello World", key)
    
    # Decrypt data
    decrypted = cipher.decrypt(encrypted, key)

Dependencies:
    - cryptography: For AES encryption operations
    - secrets: For secure random key generation
    - base64: For encoding encrypted data
    - os: For random IV generation
"""

# Import the main AESCipher class
from .aes_encrption import AES_Cipher

# Import test functions for external use if needed
from .aes_encrption import (
    test_basic_encryption_decryption,
    test_different_key_sizes,
    test_empty_and_special_strings,
    test_error_handling,
    test_consistency,
    test_hex_conversion
)

# Package metadata
__version__ = "1.0.0"
__author__ = "Your Name"
__description__ = "Secure AES encryption/decryption package"

# Define what gets imported with "from aes import *"
__all__ = [
    "AESCipher",
    "test_basic_encryption_decryption",
    "test_different_key_sizes", 
    "test_empty_and_special_strings",
    "test_error_handling",
    "test_consistency",
    "test_hex_conversion"
]

# Package-level constants
SUPPORTED_KEY_SIZES = [16, 24, 32]  # AES-128, AES-192, AES-256
DEFAULT_KEY_SIZE = 32  # AES-256
AES_BLOCK_SIZE = 16

def create_cipher(key_size=DEFAULT_KEY_SIZE):
    """
    Convenience function to create an AESCipher instance.
    
    Args:
        key_size (int): Key size in bytes (16, 24, or 32)
        
    Returns:
        AESCipher: Configured AESCipher instance
        
    Raises:
        ValueError: If key_size is not supported
    """
    if key_size not in SUPPORTED_KEY_SIZES:
        raise ValueError(f"Unsupported key size: {key_size}. Must be one of {SUPPORTED_KEY_SIZES}")
    
    return AESCipher(key_size)

def generate_key(key_size=DEFAULT_KEY_SIZE):
    """
    Convenience function to generate a new AES key.
    
    Args:
        key_size (int): Key size in bytes (16, 24, or 32)
        
    Returns:
        bytes: Cryptographically secure random key
        
    Raises:
        ValueError: If key_size is not supported
    """
    cipher = create_cipher(key_size)
    return cipher.generate_key()

def run_all_tests():
    """
    Run all AES cipher tests.
    
    Returns:
        bool: True if all tests pass, False otherwise
    """
    print("Running AES Cipher Test Suite")
    print("=" * 50)
    
    test_functions = [
        test_basic_encryption_decryption,
        test_different_key_sizes,
        test_empty_and_special_strings,
        test_error_handling,
        test_consistency,
        test_hex_conversion
    ]
    
    test_results = []
    for test_func in test_functions:
        try:
            result = test_func()
            test_results.append(result)
        except Exception as e:
            print(f"Test {test_func.__name__} failed with exception: {e}")
            test_results.append(False)
    
    # Final results
    print("=" * 50)
    print("FINAL RESULTS:")
    passed = sum(test_results)
    total = len(test_results)
    
    for i, result in enumerate(test_results, 1):
        status = "PASS" if result else "FAIL"
        print(f"Test {i}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests PASSED!")
        return True
    else:
        print("❌ Some tests FAILED!")
        return False