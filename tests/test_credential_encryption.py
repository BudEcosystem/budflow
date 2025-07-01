"""Tests for credential encryption system."""

import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime, timezone

from budflow.credentials.encryption import (
    CredentialEncryption,
    EncryptionError,
    DecryptionError,
    generate_encryption_key,
    derive_key_from_password,
)


@pytest.fixture
def encryption_key():
    """Generate a test encryption key."""
    return generate_encryption_key()


@pytest.fixture
def credential_encryption(encryption_key):
    """Create CredentialEncryption instance."""
    return CredentialEncryption(encryption_key)


@pytest.fixture
def sample_credential_data():
    """Sample credential data to encrypt."""
    return {
        "api_key": "sk-1234567890abcdef",
        "api_secret": "super-secret-value",
        "endpoint": "https://api.example.com",
        "metadata": {
            "created": "2024-01-01T00:00:00Z",
            "environment": "production"
        }
    }


class TestEncryptionKey:
    """Test encryption key generation."""
    
    def test_generate_encryption_key(self):
        """Test generating encryption key."""
        key = generate_encryption_key()
        
        # Should be 32 bytes (256 bits) base64 encoded
        assert len(key) == 44  # Base64 encoded 32 bytes
        assert key.endswith("=")  # Base64 padding
        
        # Should be different each time
        key2 = generate_encryption_key()
        assert key != key2
    
    def test_derive_key_from_password(self):
        """Test deriving key from password."""
        password = "super-secure-password-123!"
        salt = b"test-salt-12345"
        
        key1 = derive_key_from_password(password, salt)
        key2 = derive_key_from_password(password, salt)
        
        # Same password and salt should produce same key
        assert key1 == key2
        
        # Different password should produce different key
        key3 = derive_key_from_password("different-password", salt)
        assert key1 != key3
        
        # Different salt should produce different key
        key4 = derive_key_from_password(password, b"different-salt")
        assert key1 != key4
    
    def test_derive_key_iterations(self):
        """Test key derivation with different iterations."""
        password = "test-password"
        salt = b"test-salt"
        
        key1 = derive_key_from_password(password, salt, iterations=100000)
        key2 = derive_key_from_password(password, salt, iterations=200000)
        
        # Different iterations should produce different keys
        assert key1 != key2


class TestCredentialEncryption:
    """Test credential encryption and decryption."""
    
    def test_encrypt_decrypt_string(self, credential_encryption):
        """Test encrypting and decrypting a string."""
        plaintext = "secret-api-key-12345"
        
        # Encrypt
        encrypted = credential_encryption.encrypt(plaintext)
        assert encrypted != plaintext
        assert isinstance(encrypted, str)
        
        # Decrypt
        decrypted = credential_encryption.decrypt(encrypted)
        assert decrypted == plaintext
    
    def test_encrypt_decrypt_dict(self, credential_encryption, sample_credential_data):
        """Test encrypting and decrypting a dictionary."""
        # Encrypt
        encrypted = credential_encryption.encrypt_dict(sample_credential_data)
        assert isinstance(encrypted, str)
        
        # Decrypt
        decrypted = credential_encryption.decrypt_dict(encrypted)
        assert decrypted == sample_credential_data
        assert decrypted["api_key"] == sample_credential_data["api_key"]
        assert decrypted["metadata"]["environment"] == "production"
    
    def test_encrypt_different_each_time(self, credential_encryption):
        """Test that encryption produces different output each time."""
        plaintext = "same-secret-value"
        
        encrypted1 = credential_encryption.encrypt(plaintext)
        encrypted2 = credential_encryption.encrypt(plaintext)
        
        # Should be different due to random nonce
        assert encrypted1 != encrypted2
        
        # But both should decrypt to same value
        assert credential_encryption.decrypt(encrypted1) == plaintext
        assert credential_encryption.decrypt(encrypted2) == plaintext
    
    def test_decrypt_with_wrong_key(self, sample_credential_data):
        """Test decryption with wrong key fails."""
        key1 = generate_encryption_key()
        key2 = generate_encryption_key()
        
        encryptor1 = CredentialEncryption(key1)
        encryptor2 = CredentialEncryption(key2)
        
        # Encrypt with key1
        encrypted = encryptor1.encrypt_dict(sample_credential_data)
        
        # Try to decrypt with key2
        with pytest.raises(DecryptionError):
            encryptor2.decrypt_dict(encrypted)
    
    def test_decrypt_corrupted_data(self, credential_encryption):
        """Test decryption of corrupted data fails."""
        encrypted = credential_encryption.encrypt("test-data")
        
        # Corrupt the encrypted data
        corrupted = encrypted[:-10] + "corrupted"
        
        with pytest.raises(DecryptionError):
            credential_encryption.decrypt(corrupted)
    
    def test_encrypt_empty_data(self, credential_encryption):
        """Test encrypting empty data."""
        # Empty string
        encrypted = credential_encryption.encrypt("")
        decrypted = credential_encryption.decrypt(encrypted)
        assert decrypted == ""
        
        # Empty dict
        encrypted = credential_encryption.encrypt_dict({})
        decrypted = credential_encryption.decrypt_dict(encrypted)
        assert decrypted == {}
    
    def test_encrypt_large_data(self, credential_encryption):
        """Test encrypting large data."""
        # Create large credential data
        large_data = {
            f"key_{i}": f"value_{i}" * 100
            for i in range(100)
        }
        
        encrypted = credential_encryption.encrypt_dict(large_data)
        decrypted = credential_encryption.decrypt_dict(encrypted)
        
        assert decrypted == large_data
    
    def test_encrypt_special_characters(self, credential_encryption):
        """Test encrypting data with special characters."""
        special_data = {
            "unicode": "Hello 世界 🌍",
            "symbols": "!@#$%^&*()_+-=[]{}|;:,.<>?",
            "newlines": "line1\nline2\rline3\r\nline4",
            "quotes": 'Single \' and double " quotes'
        }
        
        encrypted = credential_encryption.encrypt_dict(special_data)
        decrypted = credential_encryption.decrypt_dict(encrypted)
        
        assert decrypted == special_data
    
    def test_field_level_encryption(self, credential_encryption):
        """Test encrypting specific fields only."""
        data = {
            "username": "john_doe",
            "password": "secret123",
            "api_key": "sk-abcdef",
            "public_info": "not-secret"
        }
        
        # Encrypt only sensitive fields
        sensitive_fields = ["password", "api_key"]
        encrypted_data = {}
        
        for key, value in data.items():
            if key in sensitive_fields:
                encrypted_data[key] = credential_encryption.encrypt(value)
            else:
                encrypted_data[key] = value
        
        # Verify non-sensitive fields are unchanged
        assert encrypted_data["username"] == "john_doe"
        assert encrypted_data["public_info"] == "not-secret"
        
        # Verify sensitive fields are encrypted
        assert encrypted_data["password"] != "secret123"
        assert encrypted_data["api_key"] != "sk-abcdef"
        
        # Decrypt sensitive fields
        assert credential_encryption.decrypt(encrypted_data["password"]) == "secret123"
        assert credential_encryption.decrypt(encrypted_data["api_key"]) == "sk-abcdef"


class TestEncryptionIntegration:
    """Test encryption integration with credential system."""
    
    @pytest.mark.asyncio
    async def test_credential_storage_encryption(self, credential_encryption):
        """Test encrypting credentials before storage."""
        from budflow.models import Credential
        
        # Create credential with sensitive data
        credential = Credential(
            name="Test API",
            type="api_key",
            data={
                "api_key": "sk-1234567890",
                "api_secret": "super-secret"
            }
        )
        
        # Encrypt credential data
        original_data = credential.data.copy()
        credential.encrypted_data = credential_encryption.encrypt_dict(credential.data)
        credential.data = {}  # Clear plaintext
        
        # Verify encrypted storage
        assert credential.encrypted_data != json.dumps(original_data)
        assert credential.data == {}
        
        # Decrypt for use
        decrypted = credential_encryption.decrypt_dict(credential.encrypted_data)
        assert decrypted == original_data
    
    def test_encryption_key_rotation(self, sample_credential_data):
        """Test rotating encryption keys."""
        # Original key and encryption
        old_key = generate_encryption_key()
        old_encryptor = CredentialEncryption(old_key)
        encrypted = old_encryptor.encrypt_dict(sample_credential_data)
        
        # New key for rotation
        new_key = generate_encryption_key()
        new_encryptor = CredentialEncryption(new_key)
        
        # Decrypt with old key and re-encrypt with new key
        decrypted = old_encryptor.decrypt_dict(encrypted)
        re_encrypted = new_encryptor.encrypt_dict(decrypted)
        
        # Verify new encryption works
        final_decrypted = new_encryptor.decrypt_dict(re_encrypted)
        assert final_decrypted == sample_credential_data
        
        # Verify old key no longer works
        with pytest.raises(DecryptionError):
            old_encryptor.decrypt_dict(re_encrypted)
    
    def test_encryption_performance(self, credential_encryption, sample_credential_data):
        """Test encryption performance."""
        import time
        
        # Measure encryption time
        start = time.time()
        for _ in range(100):
            encrypted = credential_encryption.encrypt_dict(sample_credential_data)
        encryption_time = time.time() - start
        
        # Measure decryption time
        encrypted = credential_encryption.encrypt_dict(sample_credential_data)
        start = time.time()
        for _ in range(100):
            decrypted = credential_encryption.decrypt_dict(encrypted)
        decryption_time = time.time() - start
        
        # Should be reasonably fast (less than 1ms per operation)
        assert encryption_time < 0.1  # 100 operations in 100ms
        assert decryption_time < 0.1
    
    def test_concurrent_encryption(self, credential_encryption, sample_credential_data):
        """Test concurrent encryption operations."""
        import asyncio
        
        async def encrypt_async(data):
            # Simulate async encryption
            await asyncio.sleep(0.001)
            return credential_encryption.encrypt_dict(data)
        
        async def test_concurrent():
            # Encrypt same data concurrently
            tasks = [
                encrypt_async(sample_credential_data)
                for _ in range(10)
            ]
            results = await asyncio.gather(*tasks)
            
            # All should encrypt successfully
            assert len(results) == 10
            
            # All should be different (due to random nonce)
            assert len(set(results)) == 10
            
            # All should decrypt to same data
            for encrypted in results:
                decrypted = credential_encryption.decrypt_dict(encrypted)
                assert decrypted == sample_credential_data
        
        asyncio.run(test_concurrent())