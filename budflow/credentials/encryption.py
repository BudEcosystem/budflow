"""Credential encryption system for BudFlow.

This module provides AES-256-GCM encryption for credentials,
ensuring sensitive data is encrypted at rest.
"""

import base64
import json
import os
from typing import Any, Dict, Union, Optional

import structlog
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

from budflow.config import settings

logger = structlog.get_logger()


class EncryptionError(Exception):
    """Base exception for encryption errors."""
    pass


class DecryptionError(Exception):
    """Exception raised when decryption fails."""
    pass


def generate_encryption_key() -> str:
    """Generate a new encryption key.
    
    Returns:
        Base64 encoded 256-bit key
    """
    key = os.urandom(32)  # 256 bits
    return base64.b64encode(key).decode('utf-8')


def derive_key_from_password(
    password: str,
    salt: bytes,
    iterations: int = 100000
) -> bytes:
    """Derive encryption key from password using PBKDF2.
    
    Args:
        password: Password to derive key from
        salt: Salt for key derivation
        iterations: Number of iterations for PBKDF2
    
    Returns:
        32-byte encryption key
    """
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=iterations,
        backend=default_backend()
    )
    return kdf.derive(password.encode('utf-8'))


class CredentialEncryption:
    """Handles credential encryption and decryption using AES-256-GCM."""
    
    def __init__(self, encryption_key: Optional[str] = None):
        """Initialize credential encryption.
        
        Args:
            encryption_key: Base64 encoded encryption key
        """
        if encryption_key:
            self.key = base64.b64decode(encryption_key)
        else:
            # Use key from settings or generate new one
            if settings.encryption_key:
                self.key = base64.b64decode(settings.encryption_key)
            else:
                logger.warning("No encryption key provided, generating temporary key")
                self.key = os.urandom(32)
        
        if len(self.key) != 32:
            raise EncryptionError("Encryption key must be 256 bits (32 bytes)")
    
    def encrypt(self, plaintext: str) -> str:
        """Encrypt a string using AES-256-GCM.
        
        Args:
            plaintext: String to encrypt
        
        Returns:
            Base64 encoded encrypted data with nonce prepended
        """
        try:
            # Generate random nonce (96 bits for GCM)
            nonce = os.urandom(12)
            
            # Create cipher
            cipher = Cipher(
                algorithms.AES(self.key),
                modes.GCM(nonce),
                backend=default_backend()
            )
            encryptor = cipher.encryptor()
            
            # Encrypt data
            ciphertext = encryptor.update(plaintext.encode('utf-8')) + encryptor.finalize()
            
            # Combine nonce + ciphertext + tag
            encrypted_data = nonce + ciphertext + encryptor.tag
            
            # Return base64 encoded
            return base64.b64encode(encrypted_data).decode('utf-8')
            
        except Exception as e:
            logger.error(f"Encryption failed: {str(e)}")
            raise EncryptionError(f"Failed to encrypt data: {str(e)}")
    
    def decrypt(self, encrypted: str) -> str:
        """Decrypt a string encrypted with AES-256-GCM.
        
        Args:
            encrypted: Base64 encoded encrypted data
        
        Returns:
            Decrypted string
        """
        try:
            # Decode from base64
            encrypted_data = base64.b64decode(encrypted)
            
            # Extract components
            nonce = encrypted_data[:12]
            tag = encrypted_data[-16:]
            ciphertext = encrypted_data[12:-16]
            
            # Create cipher
            cipher = Cipher(
                algorithms.AES(self.key),
                modes.GCM(nonce, tag),
                backend=default_backend()
            )
            decryptor = cipher.decryptor()
            
            # Decrypt data
            plaintext = decryptor.update(ciphertext) + decryptor.finalize()
            
            return plaintext.decode('utf-8')
            
        except Exception as e:
            logger.error(f"Decryption failed: {str(e)}")
            raise DecryptionError(f"Failed to decrypt data: {str(e)}")
    
    def encrypt_dict(self, data: Dict[str, Any]) -> str:
        """Encrypt a dictionary.
        
        Args:
            data: Dictionary to encrypt
        
        Returns:
            Base64 encoded encrypted JSON
        """
        json_str = json.dumps(data, separators=(',', ':'))
        return self.encrypt(json_str)
    
    def decrypt_dict(self, encrypted: str) -> Dict[str, Any]:
        """Decrypt a dictionary.
        
        Args:
            encrypted: Base64 encoded encrypted JSON
        
        Returns:
            Decrypted dictionary
        """
        json_str = self.decrypt(encrypted)
        return json.loads(json_str)
    
    def encrypt_field(self, value: Any) -> str:
        """Encrypt a single field value.
        
        Args:
            value: Value to encrypt (will be JSON encoded)
        
        Returns:
            Encrypted value
        """
        json_str = json.dumps(value)
        return self.encrypt(json_str)
    
    def decrypt_field(self, encrypted: str) -> Any:
        """Decrypt a single field value.
        
        Args:
            encrypted: Encrypted value
        
        Returns:
            Decrypted value
        """
        json_str = self.decrypt(encrypted)
        return json.loads(json_str)


class CredentialEncryptor:
    """High-level credential encryptor with field-level encryption."""
    
    def __init__(self, encryption: Optional[CredentialEncryption] = None):
        """Initialize credential encryptor.
        
        Args:
            encryption: CredentialEncryption instance
        """
        self.encryption = encryption or CredentialEncryption()
        
        # Fields that should always be encrypted
        self.sensitive_fields = {
            "password", "api_key", "api_secret", "secret", "token",
            "private_key", "client_secret", "refresh_token", "access_token"
        }
    
    def encrypt_credential_data(
        self,
        data: Dict[str, Any],
        encrypt_all: bool = False
    ) -> Dict[str, Any]:
        """Encrypt credential data.
        
        Args:
            data: Credential data dictionary
            encrypt_all: Whether to encrypt all fields or just sensitive ones
        
        Returns:
            Dictionary with encrypted fields
        """
        encrypted_data = {}
        
        for key, value in data.items():
            should_encrypt = encrypt_all or any(
                sensitive in key.lower() for sensitive in self.sensitive_fields
            )
            
            if should_encrypt and value is not None:
                encrypted_data[key] = {
                    "_encrypted": True,
                    "value": self.encryption.encrypt_field(value)
                }
            else:
                encrypted_data[key] = value
        
        return encrypted_data
    
    def decrypt_credential_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt credential data.
        
        Args:
            data: Credential data with encrypted fields
        
        Returns:
            Dictionary with decrypted fields
        """
        decrypted_data = {}
        
        for key, value in data.items():
            if isinstance(value, dict) and value.get("_encrypted"):
                try:
                    decrypted_data[key] = self.encryption.decrypt_field(value["value"])
                except DecryptionError:
                    logger.error(f"Failed to decrypt field: {key}")
                    decrypted_data[key] = None
            else:
                decrypted_data[key] = value
        
        return decrypted_data
    
    def rotate_encryption_key(
        self,
        old_key: str,
        new_key: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Rotate encryption key for credential data.
        
        Args:
            old_key: Old encryption key
            new_key: New encryption key
            data: Encrypted credential data
        
        Returns:
            Re-encrypted data with new key
        """
        # Create encryptors with old and new keys
        old_encryption = CredentialEncryption(old_key)
        new_encryption = CredentialEncryption(new_key)
        
        # Decrypt with old key
        old_encryptor = CredentialEncryptor(old_encryption)
        decrypted = old_encryptor.decrypt_credential_data(data)
        
        # Re-encrypt with new key
        new_encryptor = CredentialEncryptor(new_encryption)
        return new_encryptor.encrypt_credential_data(decrypted, encrypt_all=True)


# Global credential encryptor instance
_credential_encryptor = None


def get_credential_encryptor() -> CredentialEncryptor:
    """Get global credential encryptor instance."""
    global _credential_encryptor
    if _credential_encryptor is None:
        _credential_encryptor = CredentialEncryptor()
    return _credential_encryptor


def encrypt_credential(data: Dict[str, Any]) -> Dict[str, Any]:
    """Encrypt credential data using global encryptor.
    
    Args:
        data: Credential data to encrypt
    
    Returns:
        Encrypted credential data
    """
    encryptor = get_credential_encryptor()
    return encryptor.encrypt_credential_data(data)


def decrypt_credential(data: Dict[str, Any]) -> Dict[str, Any]:
    """Decrypt credential data using global encryptor.
    
    Args:
        data: Encrypted credential data
    
    Returns:
        Decrypted credential data
    """
    encryptor = get_credential_encryptor()
    return encryptor.decrypt_credential_data(data)


def is_encrypted_field(value: Any) -> bool:
    """Check if a field value is encrypted.
    
    Args:
        value: Field value to check
    
    Returns:
        True if field is encrypted
    """
    return (
        isinstance(value, dict) and
        value.get("_encrypted") is True and
        "value" in value
    )