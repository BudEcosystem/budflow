"""File action node for file system operations."""

import asyncio
import os
import shutil
import base64
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import aiofiles
import structlog

from budflow.nodes.base import ActionNode, NodeExecutionContext


logger = structlog.get_logger()


class FileNode(ActionNode):
    """Node for file system operations."""
    
    # Node metadata
    name = "File Operation"
    display_name = "File Operation"
    description = "Perform file system operations"
    category = "Files"
    version = "1.0.0"
    tags = ["file", "filesystem", "io"]
    
    def __init__(self, node_data):
        """Initialize File node."""
        super().__init__(node_data)
    
    @classmethod
    def get_definition(cls):
        """Get node definition."""
        from budflow.nodes.base import NodeDefinition, NodeParameter, ParameterType, NodeType, NodeCategory
        
        return NodeDefinition(
            type=NodeType.ACTION,
            name="File Operation",
            description="Perform file system operations",
            category=NodeCategory.UTILITY,
            parameters=[
                NodeParameter(
                    name="operation",
                    display_name="Operation",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="read",
                    options=["read", "write", "append", "delete", "copy", "move", "list", "exists", "stat"]
                ),
                NodeParameter(
                    name="path",
                    display_name="File Path",
                    type=ParameterType.STRING,
                    required=True,
                    description="Path to the file or directory"
                ),
                NodeParameter(
                    name="content",
                    display_name="Content",
                    type=ParameterType.STRING,
                    required=False,
                    multiline=True,
                    description="Content to write (for write/append operations)"
                )
            ]
        )
    
    async def execute(self, context: NodeExecutionContext) -> Dict[str, Any]:
        """Execute file operation."""
        try:
            operation = self.parameters.get("operation", "read")
            file_path = self.parameters.get("path")
            
            if not file_path:
                return {
                    "success": False,
                    "error": "File path is required"
                }
            
            # Security check - prevent access to sensitive paths
            if not self._is_path_safe(file_path):
                return {
                    "success": False,
                    "error": "Access to this path is not allowed"
                }
            
            # Execute operation
            if operation == "read":
                result = await self._read_file(file_path)
            elif operation == "write":
                result = await self._write_file(file_path)
            elif operation == "append":
                result = await self._append_file(file_path)
            elif operation == "delete":
                result = await self._delete_file(file_path)
            elif operation == "copy":
                result = await self._copy_file(file_path)
            elif operation == "move":
                result = await self._move_file(file_path)
            elif operation == "list":
                result = await self._list_directory(file_path)
            elif operation == "exists":
                result = await self._check_exists(file_path)
            elif operation == "stat":
                result = await self._get_file_stats(file_path)
            else:
                return {
                    "success": False,
                    "error": f"Unsupported operation: {operation}"
                }
            
            return result
            
        except Exception as e:
            logger.error(
                "File operation failed",
                error=str(e),
                operation=self.parameters.get("operation"),
                path=self.parameters.get("path")
            )
            return {
                "success": False,
                "error": f"File operation failed: {str(e)}"
            }
    
    def _is_path_safe(self, path: str) -> bool:
        """Check if path is safe to access."""
        # Convert to absolute path
        abs_path = os.path.abspath(path)
        
        # Block access to sensitive system directories
        blocked_paths = [
            "/etc",
            "/sys",
            "/proc",
            "/dev",
            "/root",
            "/home"
        ]
        
        for blocked in blocked_paths:
            if abs_path.startswith(blocked):
                return False
        
        return True
    
    async def _read_file(self, file_path: str) -> Dict[str, Any]:
        """Read file content."""
        encoding = self.parameters.get("encoding", "utf-8")
        binary_mode = self.parameters.get("binary", False)
        
        try:
            if binary_mode:
                async with aiofiles.open(file_path, "rb") as file:
                    content = await file.read()
                    # Return base64 encoded binary data
                    content_b64 = base64.b64encode(content).decode("utf-8")
                    return {
                        "success": True,
                        "content": content_b64,
                        "binary": True,
                        "size": len(content)
                    }
            else:
                async with aiofiles.open(file_path, "r", encoding=encoding) as file:
                    content = await file.read()
                    return {
                        "success": True,
                        "content": content,
                        "encoding": encoding,
                        "size": len(content.encode(encoding))
                    }
                    
        except FileNotFoundError:
            return {
                "success": False,
                "error": f"File not found: {file_path}"
            }
        except UnicodeDecodeError:
            return {
                "success": False,
                "error": f"Unable to decode file with encoding: {encoding}"
            }
    
    async def _write_file(self, file_path: str) -> Dict[str, Any]:
        """Write content to file."""
        content = self.parameters.get("content", "")
        encoding = self.parameters.get("encoding", "utf-8")
        binary_mode = self.parameters.get("binary", False)
        create_dirs = self.parameters.get("createDirectories", True)
        
        # Create parent directories if needed
        if create_dirs:
            parent_dir = os.path.dirname(file_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)
        
        try:
            if binary_mode:
                # Decode base64 content for binary mode
                if isinstance(content, str):
                    binary_content = base64.b64decode(content)
                else:
                    binary_content = content
                
                async with aiofiles.open(file_path, "wb") as file:
                    await file.write(binary_content)
                    
                return {
                    "success": True,
                    "bytesWritten": len(binary_content),
                    "binary": True
                }
            else:
                async with aiofiles.open(file_path, "w", encoding=encoding) as file:
                    await file.write(content)
                    
                return {
                    "success": True,
                    "bytesWritten": len(content.encode(encoding)),
                    "encoding": encoding
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to write file: {str(e)}"
            }
    
    async def _append_file(self, file_path: str) -> Dict[str, Any]:
        """Append content to file."""
        content = self.parameters.get("content", "")
        encoding = self.parameters.get("encoding", "utf-8")
        
        try:
            async with aiofiles.open(file_path, "a", encoding=encoding) as file:
                await file.write(content)
                
            return {
                "success": True,
                "bytesAppended": len(content.encode(encoding)),
                "encoding": encoding
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to append to file: {str(e)}"
            }
    
    async def _delete_file(self, file_path: str) -> Dict[str, Any]:
        """Delete file or directory."""
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                return {
                    "success": True,
                    "deleted": True,
                    "type": "file"
                }
            elif os.path.isdir(file_path):
                recursive = self.parameters.get("recursive", False)
                if recursive:
                    shutil.rmtree(file_path)
                else:
                    os.rmdir(file_path)
                return {
                    "success": True,
                    "deleted": True,
                    "type": "directory"
                }
            else:
                return {
                    "success": False,
                    "error": f"Path does not exist: {file_path}"
                }
                
        except OSError as e:
            return {
                "success": False,
                "error": f"Failed to delete: {str(e)}"
            }
    
    async def _copy_file(self, source_path: str) -> Dict[str, Any]:
        """Copy file or directory."""
        destination = self.parameters.get("destination")
        
        if not destination:
            return {
                "success": False,
                "error": "Destination path is required for copy operation"
            }
        
        try:
            if os.path.isfile(source_path):
                shutil.copy2(source_path, destination)
                return {
                    "success": True,
                    "copied": True,
                    "type": "file",
                    "destination": destination
                }
            elif os.path.isdir(source_path):
                shutil.copytree(source_path, destination)
                return {
                    "success": True,
                    "copied": True,
                    "type": "directory",
                    "destination": destination
                }
            else:
                return {
                    "success": False,
                    "error": f"Source path does not exist: {source_path}"
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to copy: {str(e)}"
            }
    
    async def _move_file(self, source_path: str) -> Dict[str, Any]:
        """Move/rename file or directory."""
        destination = self.parameters.get("destination")
        
        if not destination:
            return {
                "success": False,
                "error": "Destination path is required for move operation"
            }
        
        try:
            shutil.move(source_path, destination)
            return {
                "success": True,
                "moved": True,
                "destination": destination
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to move: {str(e)}"
            }
    
    async def _list_directory(self, dir_path: str) -> Dict[str, Any]:
        """List directory contents."""
        recursive = self.parameters.get("recursive", False)
        
        try:
            if not os.path.isdir(dir_path):
                return {
                    "success": False,
                    "error": f"Path is not a directory: {dir_path}"
                }
            
            files = []
            directories = []
            
            if recursive:
                for root, dirs, filenames in os.walk(dir_path):
                    for filename in filenames:
                        file_path = os.path.join(root, filename)
                        files.append({
                            "name": filename,
                            "path": file_path,
                            "size": os.path.getsize(file_path)
                        })
                    for dirname in dirs:
                        dir_full_path = os.path.join(root, dirname)
                        directories.append({
                            "name": dirname,
                            "path": dir_full_path
                        })
            else:
                for item in os.listdir(dir_path):
                    item_path = os.path.join(dir_path, item)
                    if os.path.isfile(item_path):
                        files.append({
                            "name": item,
                            "path": item_path,
                            "size": os.path.getsize(item_path)
                        })
                    elif os.path.isdir(item_path):
                        directories.append({
                            "name": item,
                            "path": item_path
                        })
            
            return {
                "success": True,
                "files": files,
                "directories": directories,
                "totalItems": len(files) + len(directories)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to list directory: {str(e)}"
            }
    
    async def _check_exists(self, file_path: str) -> Dict[str, Any]:
        """Check if file or directory exists."""
        exists = os.path.exists(file_path)
        
        result = {
            "success": True,
            "exists": exists
        }
        
        if exists:
            result.update({
                "isFile": os.path.isfile(file_path),
                "isDirectory": os.path.isdir(file_path),
                "isSymlink": os.path.islink(file_path)
            })
        
        return result
    
    async def _get_file_stats(self, file_path: str) -> Dict[str, Any]:
        """Get file statistics."""
        try:
            stat = os.stat(file_path)
            
            return {
                "success": True,
                "size": stat.st_size,
                "mode": stat.st_mode,
                "uid": stat.st_uid,
                "gid": stat.st_gid,
                "accessTime": stat.st_atime,
                "modifyTime": stat.st_mtime,
                "createTime": stat.st_ctime,
                "isFile": os.path.isfile(file_path),
                "isDirectory": os.path.isdir(file_path)
            }
            
        except FileNotFoundError:
            return {
                "success": False,
                "error": f"File not found: {file_path}"
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to get file stats: {str(e)}"
            }
    
    def validate_parameters(self) -> Dict[str, Any]:
        """Validate node parameters."""
        errors = []
        
        # Check required parameters
        if not self.parameters.get("path"):
            errors.append("File path is required")
        
        operation = self.parameters.get("operation", "read")
        valid_operations = [
            "read", "write", "append", "delete", "copy", "move",
            "list", "exists", "stat"
        ]
        if operation not in valid_operations:
            errors.append(f"Invalid operation: {operation}")
        
        # Validate operation-specific parameters
        if operation == "write" and not self.parameters.get("content"):
            errors.append("Content is required for write operation")
        
        if operation in ["copy", "move"] and not self.parameters.get("destination"):
            errors.append(f"Destination is required for {operation} operation")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }