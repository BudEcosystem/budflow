"""Credential data models."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from sqlalchemy import (
    Boolean, Column, DateTime, Enum as SQLEnum, ForeignKey, Integer,
    JSON, String, Text, UniqueConstraint, Index
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from budflow.database import Base


class CredentialType(str, Enum):
    """Credential type enumeration."""
    API_KEY = "apiKey"
    BASIC_AUTH = "basicAuth"
    BEARER_TOKEN = "bearerToken"
    OAUTH1 = "oauth1"
    OAUTH2 = "oauth2"
    SSH = "ssh"
    DATABASE = "database"
    CUSTOM = "custom"


class Credential(Base):
    """Credential model for storing encrypted authentication data."""
    
    __tablename__ = "credentials"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    
    # User relationship
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    user: Mapped["User"] = relationship("User", back_populates="credentials")
    
    # Encrypted credential data
    data: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)  # Encrypted
    
    # Additional settings
    settings: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    
    # OAuth specific fields
    oauth_token_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)  # Encrypted
    
    # Metadata
    description: Mapped[Optional[str]] = mapped_column(Text)
    no_data_expression: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # Usage tracking
    usage_count: Mapped[int] = mapped_column(Integer, default=0)
    
    # Relationships
    shared_with: Mapped[List["SharedCredential"]] = relationship(
        "SharedCredential", back_populates="credential", cascade="all, delete-orphan"
    )
    node_credentials: Mapped[List["NodeCredential"]] = relationship(
        "NodeCredential", back_populates="credential", cascade="all, delete-orphan"
    )
    
    # Indexes
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_credentials_user_name"),
        Index("ix_credentials_user_id", "user_id"),
        Index("ix_credentials_type", "type"),
        Index("ix_credentials_updated_at", "updated_at"),
    )
    
    @hybrid_property
    def is_expired(self) -> bool:
        """Check if credential is expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) > self.expires_at
    
    @hybrid_property
    def is_oauth(self) -> bool:
        """Check if credential is OAuth type."""
        return self.type in (CredentialType.OAUTH1, CredentialType.OAUTH2)
    
    def to_dict(self, include_data: bool = False) -> Dict[str, Any]:
        """Convert credential to dictionary."""
        result = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "user_id": self.user_id,
            "description": self.description,
            "settings": self.settings,
            "no_data_expression": self.no_data_expression,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "usage_count": self.usage_count,
            "is_expired": self.is_expired,
            "is_oauth": self.is_oauth,
        }
        
        # Only include encrypted data if explicitly requested (for internal use)
        if include_data:
            result["data"] = self.data
            if self.oauth_token_data:
                result["oauth_token_data"] = self.oauth_token_data
        
        return result
    
    def __repr__(self) -> str:
        return f"<Credential(id={self.id}, name='{self.name}', type='{self.type}')>"


class SharedCredential(Base):
    """Model for credential sharing permissions."""
    
    __tablename__ = "shared_credentials"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # Credential relationship
    credential_id: Mapped[int] = mapped_column(Integer, ForeignKey("credentials.id"), nullable=False)
    credential: Mapped["Credential"] = relationship("Credential")
    
    # User relationship
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    user: Mapped["User"] = relationship("User")
    
    # Permission level (use, edit)
    permission: Mapped[str] = mapped_column(String(20), default="use", nullable=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint("credential_id", "user_id", name="uq_shared_credentials_credential_user"),
        Index("ix_shared_credentials_credential_id", "credential_id"),
        Index("ix_shared_credentials_user_id", "user_id"),
    )
    
    def __repr__(self) -> str:
        return f"<SharedCredential(credential_id={self.credential_id}, user_id={self.user_id}, permission='{self.permission}')>"


class NodeCredential(Base):
    """Model for tracking credential usage in workflow nodes."""
    
    __tablename__ = "node_credentials"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # Node relationship
    node_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflow_nodes.id"), nullable=False)
    node: Mapped["WorkflowNode"] = relationship("WorkflowNode")
    
    # Credential relationship
    credential_id: Mapped[int] = mapped_column(Integer, ForeignKey("credentials.id"), nullable=False)
    credential: Mapped["Credential"] = relationship("Credential")
    
    # Credential key in node parameters
    parameter_key: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint("node_id", "parameter_key", name="uq_node_credentials_node_param"),
        Index("ix_node_credentials_node_id", "node_id"),
        Index("ix_node_credentials_credential_id", "credential_id"),
    )
    
    def __repr__(self) -> str:
        return f"<NodeCredential(node_id={self.node_id}, credential_id={self.credential_id})>"


class CredentialTypeDefinition(Base):
    """Model for storing credential type definitions."""
    
    __tablename__ = "credential_types"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # Icon and documentation
    icon: Mapped[Optional[str]] = mapped_column(String(50))
    documentation_url: Mapped[Optional[str]] = mapped_column(String(500))
    
    # Properties schema (JSON Schema format)
    properties: Mapped[List[Dict[str, Any]]] = mapped_column(JSON, nullable=False)
    
    # OAuth specific configuration
    oauth_config: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
    # Test function name (if applicable)
    test_function: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Generic credential type (for custom integrations)
    generic: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert credential type to dictionary."""
        return {
            "name": self.name,
            "displayName": self.display_name,
            "icon": self.icon,
            "documentationUrl": self.documentation_url,
            "properties": self.properties,
            "oauthConfig": self.oauth_config,
            "testFunction": self.test_function,
            "generic": self.generic,
        }
    
    def __repr__(self) -> str:
        return f"<CredentialTypeDefinition(name='{self.name}', display_name='{self.display_name}')>"