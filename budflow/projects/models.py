"""Project and folder models."""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from budflow.database import Base


class Project(Base):
    """Project model for organizing workflows."""
    
    __tablename__ = "projects"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    type: Mapped[str] = mapped_column(String(50), nullable=False, default="team")
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    folders: Mapped[List["Folder"]] = relationship("Folder", back_populates="project")
    workflows: Mapped[List["Workflow"]] = relationship("Workflow", back_populates="project")
    
    def __repr__(self) -> str:
        return f"<Project(id={self.id}, name='{self.name}', type='{self.type}')>"


class Folder(Base):
    """Folder model for organizing workflows within projects."""
    
    __tablename__ = "folders"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # Relationships
    parent_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("folders.id"))
    project_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("projects.id"))
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    parent: Mapped[Optional["Folder"]] = relationship("Folder", remote_side=[id], back_populates="children")
    children: Mapped[List["Folder"]] = relationship("Folder", back_populates="parent")
    project: Mapped[Optional["Project"]] = relationship("Project", back_populates="folders")
    workflows: Mapped[List["Workflow"]] = relationship("Workflow", back_populates="folder")
    
    def __repr__(self) -> str:
        return f"<Folder(id={self.id}, name='{self.name}', project_id={self.project_id})>"