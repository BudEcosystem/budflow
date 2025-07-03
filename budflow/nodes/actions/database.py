"""Database action nodes for various database operations."""

import asyncio
import json
from typing import Any, Dict, List, Optional, Union
from abc import abstractmethod

import structlog
import asyncpg
import aiomysql
from motor.motor_asyncio import AsyncIOMotorClient

from budflow.nodes.base import ActionNode, NodeExecutionContext


logger = structlog.get_logger()


class DatabaseNode(ActionNode):
    """Base class for database nodes."""
    
    # Node metadata
    name = "Database Query"
    display_name = "Database Query"
    description = "Execute database queries"
    category = "Data"
    version = "1.0.0"
    tags = ["database", "query", "sql"]
    
    def __init__(self, node_data):
        """Initialize Database node."""
        super().__init__(node_data)
    
    @classmethod
    def get_definition(cls):
        """Get node definition."""
        from budflow.nodes.base import NodeDefinition, NodeParameter, ParameterType, NodeType, NodeCategory
        
        return NodeDefinition(
            type=NodeType.ACTION,
            name="Database Query",
            description="Execute database queries",
            category=NodeCategory.DATABASE,
            parameters=[
                NodeParameter(
                    name="operation",
                    display_name="Operation",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="select",
                    options=["select", "insert", "update", "delete"]
                ),
                NodeParameter(
                    name="query",
                    display_name="Query",
                    type=ParameterType.STRING,
                    required=True,
                    multiline=True,
                    description="SQL query to execute"
                )
            ]
        )
    
    @abstractmethod
    async def _connect(self, credentials: Dict[str, Any]):
        """Create database connection."""
        pass
    
    @abstractmethod
    async def _execute_query(self, connection, query: str, parameters: List[Any] = None):
        """Execute database query."""
        pass
    
    @abstractmethod
    async def _close_connection(self, connection):
        """Close database connection."""
        pass
    
    async def execute(self, context: NodeExecutionContext) -> Dict[str, Any]:
        """Execute database operation."""
        try:
            operation = self.parameters.get("operation", "select")
            
            # Validate credentials
            credentials_validation = self._validate_credentials(context.credentials)
            if not credentials_validation["valid"]:
                return {
                    "success": False,
                    "error": credentials_validation["error"]
                }
            
            # Connect to database
            connection = await self._connect(context.credentials)
            
            try:
                # Execute operation
                if operation in ["select", "insert", "update", "delete"]:
                    result = await self._execute_sql_operation(connection)
                else:
                    return {
                        "success": False,
                        "error": f"Unsupported operation: {operation}"
                    }
                
                return result
                
            finally:
                await self._close_connection(connection)
                
        except Exception as e:
            logger.error(
                "Database operation failed",
                error=str(e),
                operation=self.parameters.get("operation"),
                database_type=self.node_data.type
            )
            return {
                "success": False,
                "error": f"Database operation failed: {str(e)}"
            }
    
    async def _execute_sql_operation(self, connection) -> Dict[str, Any]:
        """Execute SQL operation."""
        query = self.parameters.get("query", "")
        parameters = self.parameters.get("parameters", [])
        
        if not query:
            return {
                "success": False,
                "error": "SQL query is required"
            }
        
        # Execute query
        results = await self._execute_query(connection, query, parameters)
        
        # Format results
        if isinstance(results, list):
            # SELECT query results
            return {
                "success": True,
                "data": results,
                "rowCount": len(results)
            }
        else:
            # INSERT/UPDATE/DELETE results
            return {
                "success": True,
                "data": results if results else [],
                "rowCount": 1 if results else 0
            }
    
    def _validate_credentials(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Validate database credentials."""
        required_fields = ["host", "port", "database", "username", "password"]
        
        for field in required_fields:
            if not credentials.get(field):
                return {
                    "valid": False,
                    "error": f"Missing database credential: {field}"
                }
        
        return {"valid": True}


class PostgreSQLNode(DatabaseNode):
    """PostgreSQL database node."""
    
    name = "PostgreSQL Query"
    display_name = "PostgreSQL Query"
    description = "Execute PostgreSQL queries"
    
    @classmethod
    def get_definition(cls):
        definition = super().get_definition()
        definition.type = "postgresql.query"
        definition.name = "PostgreSQL Query"
        return definition
    
    async def _connect(self, credentials: Dict[str, Any]):
        """Create PostgreSQL connection."""
        connection = await asyncpg.connect(
            host=credentials["host"],
            port=int(credentials["port"]),
            database=credentials["database"],
            user=credentials["username"],
            password=credentials["password"]
        )
        return connection
    
    async def _execute_query(self, connection, query: str, parameters: List[Any] = None):
        """Execute PostgreSQL query."""
        if parameters:
            results = await connection.fetch(query, *parameters)
        else:
            results = await connection.fetch(query)
        
        # Convert Records to dicts
        return [dict(record) for record in results]
    
    async def _close_connection(self, connection):
        """Close PostgreSQL connection."""
        await connection.close()


class MySQLNode(DatabaseNode):
    """MySQL database node."""
    
    name = "MySQL Query"
    display_name = "MySQL Query"
    description = "Execute MySQL queries"
    
    @classmethod
    def get_definition(cls):
        definition = super().get_definition()
        definition.type = "mysql.query"
        definition.name = "MySQL Query"
        return definition
    
    async def _connect(self, credentials: Dict[str, Any]):
        """Create MySQL connection."""
        connection = await aiomysql.connect(
            host=credentials["host"],
            port=int(credentials["port"]),
            db=credentials["database"],
            user=credentials["username"],
            password=credentials["password"],
            autocommit=True
        )
        return connection
    
    async def _execute_query(self, connection, query: str, parameters: List[Any] = None):
        """Execute MySQL query."""
        async with connection.cursor(aiomysql.DictCursor) as cursor:
            if parameters:
                await cursor.execute(query, parameters)
            else:
                await cursor.execute(query)
            
            # Fetch results for SELECT queries
            if query.strip().upper().startswith("SELECT"):
                results = await cursor.fetchall()
                return list(results)
            else:
                # For INSERT/UPDATE/DELETE, return affected rows
                return {"affected_rows": cursor.rowcount}
    
    async def _close_connection(self, connection):
        """Close MySQL connection."""
        connection.close()


class MongoDBNode(ActionNode):
    """MongoDB database node."""
    
    name = "MongoDB Query"
    display_name = "MongoDB Query"
    description = "Execute MongoDB operations"
    category = "Data"
    version = "1.0.0"
    tags = ["mongodb", "nosql", "database"]
    
    def __init__(self, node_data):
        """Initialize MongoDB node."""
        super().__init__(node_data)
    
    @classmethod
    def get_definition(cls):
        """Get node definition."""
        from budflow.nodes.base import NodeDefinition, NodeParameter, ParameterType, NodeType, NodeCategory
        
        return NodeDefinition(
            type=NodeType.ACTION,
            name="MongoDB Query",
            description="Execute MongoDB operations",
            category=NodeCategory.DATABASE,
            parameters=[
                NodeParameter(
                    name="operation",
                    display_name="Operation",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="find",
                    options=["find", "findOne", "insert", "update", "delete"]
                ),
                NodeParameter(
                    name="collection",
                    display_name="Collection",
                    type=ParameterType.STRING,
                    required=True,
                    description="MongoDB collection name"
                ),
                NodeParameter(
                    name="query",
                    display_name="Query",
                    type=ParameterType.JSON,
                    required=False,
                    description="MongoDB query document"
                )
            ]
        )
    
    async def execute(self, context: NodeExecutionContext) -> Dict[str, Any]:
        """Execute MongoDB operation."""
        try:
            operation = self.parameters.get("operation", "find")
            collection_name = self.parameters.get("collection")
            
            if not collection_name:
                return {
                    "success": False,
                    "error": "Collection name is required"
                }
            
            # Validate credentials
            credentials_validation = self._validate_credentials(context.credentials)
            if not credentials_validation["valid"]:
                return {
                    "success": False,
                    "error": credentials_validation["error"]
                }
            
            # Connect to MongoDB
            client = await self._connect(context.credentials)
            
            try:
                database = client[context.credentials["database"]]
                collection = database[collection_name]
                
                # Execute operation
                if operation == "find":
                    result = await self._execute_find(collection)
                elif operation == "findOne":
                    result = await self._execute_find_one(collection)
                elif operation == "insert":
                    result = await self._execute_insert(collection)
                elif operation == "update":
                    result = await self._execute_update(collection)
                elif operation == "delete":
                    result = await self._execute_delete(collection)
                else:
                    return {
                        "success": False,
                        "error": f"Unsupported operation: {operation}"
                    }
                
                return result
                
            finally:
                client.close()
                
        except Exception as e:
            logger.error(
                "MongoDB operation failed",
                error=str(e),
                operation=self.parameters.get("operation"),
                collection=self.parameters.get("collection")
            )
            return {
                "success": False,
                "error": f"MongoDB operation failed: {str(e)}"
            }
    
    async def _connect(self, credentials: Dict[str, Any]):
        """Create MongoDB connection."""
        host = credentials["host"]
        port = int(credentials["port"])
        username = credentials.get("username")
        password = credentials.get("password")
        
        if username and password:
            connection_string = f"mongodb://{username}:{password}@{host}:{port}"
        else:
            connection_string = f"mongodb://{host}:{port}"
        
        client = AsyncIOMotorClient(connection_string)
        return client
    
    async def _execute_find(self, collection) -> Dict[str, Any]:
        """Execute MongoDB find operation."""
        query = self.parameters.get("query", {})
        limit = self.parameters.get("limit", 0)
        sort = self.parameters.get("sort", {})
        
        cursor = collection.find(query)
        
        if sort:
            cursor = cursor.sort(list(sort.items()))
        
        if limit > 0:
            cursor = cursor.limit(limit)
        
        results = await cursor.to_list(length=None)
        
        # Convert ObjectIds to strings
        for result in results:
            if "_id" in result:
                result["_id"] = str(result["_id"])
        
        return {
            "success": True,
            "data": results,
            "count": len(results)
        }
    
    async def _execute_find_one(self, collection) -> Dict[str, Any]:
        """Execute MongoDB findOne operation."""
        query = self.parameters.get("query", {})
        
        result = await collection.find_one(query)
        
        if result and "_id" in result:
            result["_id"] = str(result["_id"])
        
        return {
            "success": True,
            "data": result,
            "found": result is not None
        }
    
    async def _execute_insert(self, collection) -> Dict[str, Any]:
        """Execute MongoDB insert operation."""
        document = self.parameters.get("document", {})
        
        if not document:
            return {
                "success": False,
                "error": "Document data is required for insert"
            }
        
        result = await collection.insert_one(document)
        
        return {
            "success": True,
            "insertedId": str(result.inserted_id),
            "acknowledged": result.acknowledged
        }
    
    async def _execute_update(self, collection) -> Dict[str, Any]:
        """Execute MongoDB update operation."""
        query = self.parameters.get("query", {})
        update = self.parameters.get("update", {})
        upsert = self.parameters.get("upsert", False)
        
        if not update:
            return {
                "success": False,
                "error": "Update data is required"
            }
        
        result = await collection.update_many(query, update, upsert=upsert)
        
        return {
            "success": True,
            "matchedCount": result.matched_count,
            "modifiedCount": result.modified_count,
            "upsertedId": str(result.upserted_id) if result.upserted_id else None
        }
    
    async def _execute_delete(self, collection) -> Dict[str, Any]:
        """Execute MongoDB delete operation."""
        query = self.parameters.get("query", {})
        
        if not query:
            return {
                "success": False,
                "error": "Query is required for delete operation"
            }
        
        result = await collection.delete_many(query)
        
        return {
            "success": True,
            "deletedCount": result.deleted_count
        }
    
    def _validate_credentials(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Validate MongoDB credentials."""
        required_fields = ["host", "port", "database"]
        
        for field in required_fields:
            if not credentials.get(field):
                return {
                    "valid": False,
                    "error": f"Missing MongoDB credential: {field}"
                }
        
        return {"valid": True}
    
    def validate_parameters(self) -> Dict[str, Any]:
        """Validate node parameters."""
        errors = []
        
        # Check required parameters
        if not self.parameters.get("collection"):
            errors.append("Collection name is required")
        
        operation = self.parameters.get("operation", "find")
        valid_operations = ["find", "findOne", "insert", "update", "delete"]
        if operation not in valid_operations:
            errors.append(f"Invalid operation: {operation}")
        
        # Validate operation-specific parameters
        if operation == "insert" and not self.parameters.get("document"):
            errors.append("Document is required for insert operation")
        
        if operation == "update" and not self.parameters.get("update"):
            errors.append("Update data is required for update operation")
        
        if operation == "delete" and not self.parameters.get("query"):
            errors.append("Query is required for delete operation")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }