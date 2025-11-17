import os
import secrets
import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

class AES_Cipher:
    def __init__(self, key_size=32):
        """
        Initialize AES cipher
        key_size: 16 for AES-128, 24 for AES-192, 32 for AES-256
        """
        self.key_size = key_size
        self.backend = default_backend()
    
    def generate_key(self):
        """Generate a cryptographically secure random AES key"""
        return secrets.token_bytes(self.key_size)
    
    def encrypt(self, plaintext, key):
        """
        Encrypt a string using AES in CBC mode
        Returns base64 encoded string containing IV + ciphertext
        """
        if len(key) != self.key_size:
            raise ValueError(f"Key must be {self.key_size} bytes long")
        
        # Convert string to bytes
        if isinstance(plaintext, str):
            plaintext = plaintext.encode('utf-8')
        
        # Generate random IV
        iv = os.urandom(16)  # AES block size is always 16 bytes
        
        # Pad the plaintext to be multiple of 16 bytes
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(plaintext)
        padded_data += padder.finalize()
        
        # Create cipher and encrypt
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=self.backend)
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()
        
        # Combine IV and ciphertext, then base64 encode
        encrypted_data = iv + ciphertext
        return base64.b64encode(encrypted_data).decode('utf-8')
    
    def decrypt(self, encrypted_data, key):
        """
        Decrypt a base64 encoded string containing IV + ciphertext
        Returns the original plaintext string
        """
        if len(key) != self.key_size:
            raise ValueError(f"Key must be {self.key_size} bytes long")
        
        try:
            # Decode from base64
            encrypted_bytes = base64.b64decode(encrypted_data.encode('utf-8'))
            
            # Extract IV and ciphertext
            iv = encrypted_bytes[:16]
            ciphertext = encrypted_bytes[16:]
            
            # Create cipher and decrypt
            cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=self.backend)
            decryptor = cipher.decryptor()
            padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()
            
            # Remove padding
            unpadder = padding.PKCS7(128).unpadder()
            plaintext = unpadder.update(padded_plaintext)
            plaintext += unpadder.finalize()
            
            return plaintext.decode('utf-8')
        
        except Exception as e:
            raise ValueError(f"Decryption failed: {str(e)}")
    
    def key_to_hex(self, key):
        """Convert key to hex string"""
        return key.hex()

def test_basic_encryption_decryption():
    """Test basic encryption and decryption functionality"""
    print("=== Test 1: Basic Encryption/Decryption ===")
    cipher = AES_Cipher(32)
    key = cipher.generate_key()
    
    test_message = "Hello, this is a test message!"
    print(f"Original message: {test_message}")
    
    encrypted = cipher.encrypt(test_message, key)
    print(f"Encrypted: {encrypted}")
    
    decrypted = cipher.decrypt(encrypted, key)
    print(f"Decrypted: {decrypted}")
    
    success = test_message == decrypted
    print(f"Test result: {'PASS' if success else 'FAIL'}")
    print()
    return success

def test_different_key_sizes():
    """Test AES with different key sizes"""
    print("=== Test 2: Different Key Sizes ===")
    key_sizes = [16, 24, 32]  # AES-128, AES-192, AES-256
    test_message = "Testing different key sizes"
    results = []
    
    for size in key_sizes:
        print(f"Testing AES-{size*8}...")
        cipher = AES_Cipher(size)
        key = cipher.generate_key()
        
        encrypted = cipher.encrypt(test_message, key)
        decrypted = cipher.decrypt(encrypted, key)
        
        success = test_message == decrypted
        results.append(success)
        print(f"  Key size {size} bytes: {'PASS' if success else 'FAIL'}")
    
    overall_success = all(results)
    print(f"Overall result: {'PASS' if overall_success else 'FAIL'}")
    print()
    return overall_success

def test_empty_and_special_strings():
    """Test encryption of empty strings and special characters"""
    print("=== Test 3: Empty and Special Strings ===")
    cipher = AES_Cipher(32)
    key = cipher.generate_key()
    
    test_cases = [
        "",  # Empty string
        " ",  # Single space
        "Hello\nWorld\t!",  # Newlines and tabs
        "Special chars: !@#$%^&*()_+-={}[]|\\:;\"'<>?,./",
        "Unicode: 你好世界 🌍 🚀 ñáéíóú",
        "A" * 1000,  # Long string
    ]
    
    results = []
    for i, test_string in enumerate(test_cases):
        try:
            encrypted = cipher.encrypt(test_string, key)
            decrypted = cipher.decrypt(encrypted, key)
            success = test_string == decrypted
            results.append(success)
            print(f"  Test case {i+1}: {'PASS' if success else 'FAIL'}")
        except Exception as e:
            print(f"  Test case {i+1}: FAIL - {str(e)}")
            results.append(False)
    
    overall_success = all(results)
    print(f"Overall result: {'PASS' if overall_success else 'FAIL'}")
    print()
    return overall_success

def test_error_handling():
    """Test error handling for invalid inputs"""
    print("=== Test 4: Error Handling ===")
    cipher = AES_Cipher(32)
    key = cipher.generate_key()
    wrong_key = cipher.generate_key()
    
    test_results = []
    
    # Test wrong key size
    try:
        cipher.encrypt("test", b"short_key")
        print("  Wrong key size: FAIL - Should have raised exception")
        test_results.append(False)
    except ValueError:
        print("  Wrong key size: PASS - Correctly raised ValueError")
        test_results.append(True)
    except Exception as e:
        print(f"  Wrong key size: FAIL - Wrong exception type: {type(e)}")
        test_results.append(False)
    
    # Test decryption with wrong key
    try:
        encrypted = cipher.encrypt("test message", key)
        cipher.decrypt(encrypted, wrong_key)
        print("  Wrong decryption key: FAIL - Should have failed")
        test_results.append(False)
    except ValueError:
        print("  Wrong decryption key: PASS - Correctly failed")
        test_results.append(True)
    except Exception as e:
        print(f"  Wrong decryption key: FAIL - Wrong exception type: {type(e)}")
        test_results.append(False)
    
    # Test invalid encrypted data
    try:
        cipher.decrypt("invalid_base64_data", key)
        print("  Invalid encrypted data: FAIL - Should have raised exception")
        test_results.append(False)
    except ValueError:
        print("  Invalid encrypted data: PASS - Correctly raised ValueError")
        test_results.append(True)
    except Exception as e:
        print(f"  Invalid encrypted data: FAIL - Wrong exception type: {type(e)}")
        test_results.append(False)
    
    overall_success = all(test_results)
    print(f"Overall result: {'PASS' if overall_success else 'FAIL'}")
    print()
    return overall_success

def test_consistency():
    """Test that multiple encryptions of the same text produce different ciphertexts"""
    print("=== Test 5: Encryption Consistency ===")
    cipher = AES_Cipher(32)
    key = cipher.generate_key()
    test_message = "Same message multiple times"
    
    # Encrypt the same message multiple times
    encryptions = []
    for i in range(5):
        encrypted = cipher.encrypt(test_message, key)
        encryptions.append(encrypted)
    
    # Check that all encryptions are different (due to random IV)
    all_different = len(set(encryptions)) == len(encryptions)
    print(f"Multiple encryptions are different: {'PASS' if all_different else 'FAIL'}")
    
    # Check that all decrypt to the same message
    decryptions = []
    for encrypted in encryptions:
        decrypted = cipher.decrypt(encrypted, key)
        decryptions.append(decrypted)
    
    all_same_decryption = all(d == test_message for d in decryptions)
    print(f"All decrypt to original message: {'PASS' if all_same_decryption else 'FAIL'}")
    
    overall_success = all_different and all_same_decryption
    print(f"Overall result: {'PASS' if overall_success else 'FAIL'}")
    print()
    return overall_success

def test_hex_conversion():
    """Test key to hex conversion functionality"""
    print("=== Test 6: Key Hex Conversion ===")
    cipher = AES_Cipher(32)
    key = cipher.generate_key()
    
    # Test hex conversion
    hex_key = cipher.key_to_hex(key)
    print(f"Key length: {len(key)} bytes")
    print(f"Hex length: {len(hex_key)} characters")
    print(f"Hex key: {hex_key}")
    
    # Verify hex conversion
    expected_hex_length = len(key) * 2  # Each byte = 2 hex chars
    hex_length_correct = len(hex_key) == expected_hex_length
    
    # Verify it's valid hex
    try:
        bytes.fromhex(hex_key)
        valid_hex = True
    except ValueError:
        valid_hex = False
    
    success = hex_length_correct and valid_hex
    print(f"Hex conversion: {'PASS' if success else 'FAIL'}")
    print()
    return success

if __name__ == "__main__":
    print("AES Cipher Test Suite")
    print("=" * 50)
    
    # Run all tests
    test_results = []
    
    test_results.append(test_basic_encryption_decryption())
    test_results.append(test_different_key_sizes())
    test_results.append(test_empty_and_special_strings())
    test_results.append(test_error_handling())
    test_results.append(test_consistency())
    test_results.append(test_hex_conversion())
    
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
    else:
        print("❌ Some tests FAILED!")
    
    print("=" * 50)

