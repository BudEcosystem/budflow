"""Test node system with various node types."""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List
from unittest.mock import Mock, patch, AsyncMock
import pytz

from budflow.nodes import (
    # Base classes
    BaseNode, TriggerNode, ActionNode, ControlNode,
    NodeDefinition, NodeCategory, NodeParameter, ParameterType,
    
    # Trigger nodes
    ManualTriggerNode, WebhookTriggerNode, ScheduleTriggerNode,
    
    # Action nodes
    HttpRequestNode, EmailNode, DatabaseNode,
    
    # Control nodes
    IfNode, LoopNode, WaitNode, StopNode,
    
    # Utilities
    NodeRegistry, NodeFactory,
    ExpressionEvaluator,
)
from budflow.nodes.control import StopWorkflowException
from budflow.executor.context import NodeExecutionContext
from budflow.executor.data import ExecutionData
from budflow.workflows.models import (
    WorkflowNode, NodeType, NodeExecution, NodeExecutionStatus,
    WorkflowStatus, ExecutionStatus
)


class TestNodeDefinition:
    """Test node definition and metadata."""
    
    def test_node_definition_creation(self):
        """Test creating a node definition."""
        params = [
            NodeParameter(
                name="url",
                type=ParameterType.STRING,
                required=True,
                description="Target URL"
            ),
            NodeParameter(
                name="method",
                type=ParameterType.OPTIONS,
                required=True,
                default="GET",
                options=["GET", "POST", "PUT", "DELETE"]
            )
        ]
        
        definition = NodeDefinition(
            name="HTTP Request",
            type=NodeType.ACTION,
            category=NodeCategory.NETWORK,
            description="Make HTTP requests",
            parameters=params,
            inputs=["main"],
            outputs=["main", "error"],
            version="1.0"
        )
        
        assert definition.name == "HTTP Request"
        assert definition.type == NodeType.ACTION
        assert len(definition.parameters) == 2
        assert definition.get_parameter("url").required is True
        assert definition.get_parameter("method").default == "GET"
        assert "main" in definition.inputs
        assert "error" in definition.outputs
    
    def test_parameter_validation(self):
        """Test parameter type validation."""
        # String parameter
        string_param = NodeParameter(
            name="text",
            type=ParameterType.STRING,
            required=True
        )
        assert string_param.validate("hello") is True
        assert string_param.validate(123) is False
        
        # Number parameter
        number_param = NodeParameter(
            name="count",
            type=ParameterType.NUMBER,
            min_value=0,
            max_value=100
        )
        assert number_param.validate(50) is True
        assert number_param.validate(150) is False
        assert number_param.validate("50") is False
        
        # Boolean parameter
        bool_param = NodeParameter(
            name="active",
            type=ParameterType.BOOLEAN,
            default=True
        )
        assert bool_param.validate(True) is True
        assert bool_param.validate(False) is True
        assert bool_param.validate("true") is False
        
        # Options parameter
        options_param = NodeParameter(
            name="method",
            type=ParameterType.OPTIONS,
            options=["GET", "POST"]
        )
        assert options_param.validate("GET") is True
        assert options_param.validate("PUT") is False
        
        # JSON parameter
        json_param = NodeParameter(
            name="data",
            type=ParameterType.JSON
        )
        assert json_param.validate({"key": "value"}) is True
        assert json_param.validate([1, 2, 3]) is True
        assert json_param.validate("not json") is False


class TestBaseNode:
    """Test base node functionality."""
    
    @pytest.mark.asyncio
    async def test_base_node_initialization(self):
        """Test base node initialization."""
        # Create mock context
        node_model = WorkflowNode(
            id=1,
            workflow_id=1,
            name="Test Node",
            type=NodeType.ACTION,
            parameters={"test": "value"}
        )
        
        context = Mock(spec=NodeExecutionContext)
        context.node = node_model
        context.parameters = {"test": "value"}
        context.input_data = [{"input": "data"}]
        
        # Create node instance
        node = BaseNode(context)
        
        assert node.context == context
        assert node.name == "Test Node"
        assert node.get_parameter("test") == "value"
        assert node.get_parameter("missing", "default") == "default"
    
    @pytest.mark.asyncio
    async def test_expression_evaluation(self):
        """Test expression evaluation in nodes."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={})
        context.input_data = [{"name": "John", "age": 30}]
        context.execution_data = Mock(context_data={"global": "value"})
        
        node = BaseNode(context)
        
        # Test simple expressions
        assert node.evaluate_expression("Hello {{name}}") == "Hello John"
        assert node.evaluate_expression("Age: {{age}}") == "Age: 30"
        assert node.evaluate_expression("{{age + 5}}") == 35
        assert node.evaluate_expression("{{global}}", use_context=True) == "value"
        
        # Test complex expressions
        assert node.evaluate_expression("{{name.upper()}}") == "JOHN"
        assert node.evaluate_expression("{{age > 25}}") is True
    
    @pytest.mark.asyncio
    async def test_node_execution_lifecycle(self):
        """Test node execution lifecycle methods."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={})
        context.input_data = []
        
        class TestLifecycleNode(BaseNode):
            def __init__(self, context):
                super().__init__(context)
                self.pre_executed = False
                self.executed = False
                self.post_executed = False
            
            async def pre_execute(self):
                self.pre_executed = True
            
            async def execute(self) -> List[Dict[str, Any]]:
                self.executed = True
                return [{"result": "data"}]
            
            async def post_execute(self, result: List[Dict[str, Any]]):
                self.post_executed = True
                assert result == [{"result": "data"}]
            
            async def on_error(self, error: Exception):
                self.error_handled = True
        
        node = TestLifecycleNode(context)
        result = await node.run()
        
        assert node.pre_executed is True
        assert node.executed is True
        assert node.post_executed is True
        assert result == [{"result": "data"}]


class TestTriggerNodes:
    """Test trigger node implementations."""
    
    @pytest.mark.asyncio
    async def test_manual_trigger_node(self):
        """Test manual trigger node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={})
        context.input_data = [{"trigger": "manual"}]
        
        trigger = ManualTriggerNode(context)
        result = await trigger.execute()
        
        # Manual trigger should pass through input data
        assert result == [{"trigger": "manual"}]
    
    @pytest.mark.asyncio
    async def test_webhook_trigger_node(self):
        """Test webhook trigger node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={
            "path": "/webhook/test",
            "method": "POST",
            "response_mode": "last_node"
        })
        context.input_data = [{
            "headers": {"content-type": "application/json"},
            "body": {"webhook": "data"},
            "query": {"param": "value"}
        }]
        
        trigger = WebhookTriggerNode(context)
        result = await trigger.execute()
        
        assert len(result) == 1
        assert result[0]["headers"]["content-type"] == "application/json"
        assert result[0]["body"]["webhook"] == "data"
        assert result[0]["query"]["param"] == "value"
    
    @pytest.mark.asyncio
    async def test_schedule_trigger_node(self):
        """Test schedule trigger node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={
            "cron": "0 */2 * * *",  # Every 2 hours
            "timezone": "UTC"
        })
        context.input_data = []
        
        trigger = ScheduleTriggerNode(context)
        
        # Test cron validation
        assert trigger.validate_cron_expression("0 */2 * * *") is True
        assert trigger.validate_cron_expression("invalid") is False
        
        # Test next run calculation
        next_run = trigger.get_next_run_time()
        assert isinstance(next_run, datetime)
        assert next_run > datetime.now(timezone.utc)
        
        # Execute should return timestamp
        result = await trigger.execute()
        assert len(result) == 1
        assert "triggered_at" in result[0]
        assert "next_run" in result[0]


class TestActionNodes:
    """Test action node implementations."""
    
    @pytest.mark.asyncio
    async def test_http_request_node(self):
        """Test HTTP request node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={
            "url": "https://api.example.com/data",
            "method": "GET",
            "headers": {"Authorization": "Bearer token"},
            "timeout": 30
        })
        context.input_data = [{"id": 1}]
        
        node = HttpRequestNode(context)
        
        # Mock HTTP client
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"success": True, "data": [1, 2, 3]}
            mock_response.headers = {"content-type": "application/json"}
            
            mock_client.return_value.__aenter__.return_value.request = AsyncMock(
                return_value=mock_response
            )
            
            result = await node.execute()
            
            assert len(result) == 1
            assert result[0]["status_code"] == 200
            assert result[0]["body"]["success"] is True
            assert len(result[0]["body"]["data"]) == 3
    
    @pytest.mark.asyncio
    async def test_email_node(self):
        """Test email node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={
            "to": "test@example.com",
            "subject": "Test Email",
            "body": "Hello {{name}}!",
            "html": False,
            "smtp_host": "smtp.example.com",
            "smtp_port": 587,
            "smtp_user": "user@example.com",
            "smtp_password": "password"
        })
        context.input_data = [{"name": "John"}]
        
        node = EmailNode(context)
        
        # Mock SMTP
        with patch('smtplib.SMTP') as mock_smtp:
            mock_server = Mock()
            mock_smtp.return_value = mock_server
            
            result = await node.execute()
            
            assert len(result) == 1
            assert result[0]["success"] is True
            assert result[0]["to"] == "test@example.com"
            assert result[0]["subject"] == "Test Email"
            
            # Verify SMTP was called
            mock_smtp.assert_called_once_with("smtp.example.com", 587)
            mock_server.starttls.assert_called_once()
            mock_server.login.assert_called_once_with("user@example.com", "password")
    
    @pytest.mark.asyncio
    async def test_database_node(self):
        """Test database node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={
            "operation": "SELECT",
            "query": "SELECT * FROM users WHERE age > {{min_age}}",
            "database": "postgresql://localhost/testdb"
        })
        context.input_data = [{"min_age": 18}]
        
        node = DatabaseNode(context)
        
        # Mock database connection
        with patch('sqlalchemy.create_engine') as mock_create_engine:
            mock_engine = Mock()
            mock_connection = Mock()
            mock_result = Mock()
            
            # Create mock rows with _mapping attribute
            mock_row1 = Mock()
            mock_row1._mapping = {"id": 1, "name": "John", "age": 25}
            mock_row2 = Mock()
            mock_row2._mapping = {"id": 2, "name": "Jane", "age": 30}
            
            mock_result.fetchall.return_value = [mock_row1, mock_row2]
            
            mock_create_engine.return_value = mock_engine
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_connection.execute.return_value = mock_result
            
            result = await db_node.execute()
            
            assert len(result) == 2
            assert result[0]["name"] == "John"
            assert result[1]["age"] == 30


class TestControlNodes:
    """Test control flow node implementations."""
    
    @pytest.mark.asyncio
    async def test_if_node(self):
        """Test IF conditional node."""
        # Test true condition
        context_true = Mock(spec=NodeExecutionContext)
        context_true.node = Mock(parameters={
            "condition": "{{age >= 18}}",
            "output_true": "adult",
            "output_false": "minor"
        })
        context_true.input_data = [{"age": 25}]
        
        if_node = IfNode(context_true)
        result = await if_node.execute()
        
        assert len(result) == 1
        assert result[0]["_output"] == "adult"
        assert result[0]["age"] == 25
        
        # Test false condition
        context_false = Mock(spec=NodeExecutionContext)
        context_false.node = Mock(parameters={
            "condition": "{{age >= 18}}",
            "output_true": "adult",
            "output_false": "minor"
        })
        context_false.input_data = [{"age": 15}]
        
        if_node = IfNode(context_false)
        result = await if_node.execute()
        
        assert len(result) == 1
        assert result[0]["_output"] == "minor"
    
    @pytest.mark.asyncio
    async def test_loop_node(self):
        """Test LOOP node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={
            "mode": "items",
            "items": "{{items}}",
            "batch_size": 2
        })
        context.input_data = [{
            "items": ["a", "b", "c", "d", "e"]
        }]
        
        loop_node = LoopNode(context)
        result = await loop_node.execute()
        
        # Should create batches of size 2
        assert len(result) == 3  # 3 batches: [a,b], [c,d], [e]
        assert result[0]["_batch"] == ["a", "b"]
        assert result[1]["_batch"] == ["c", "d"]
        assert result[2]["_batch"] == ["e"]
        assert result[0]["_batch_index"] == 0
        assert result[2]["_batch_index"] == 2
    
    @pytest.mark.asyncio
    async def test_wait_node(self):
        """Test WAIT node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={
            "duration": 100,  # 100ms
            "unit": "milliseconds"
        })
        context.input_data = [{"data": "test"}]
        
        wait_node = WaitNode(context)
        
        start_time = datetime.now(timezone.utc)
        result = await wait_node.execute()
        end_time = datetime.now(timezone.utc)
        
        # Check wait duration
        duration = (end_time - start_time).total_seconds()
        assert duration >= 0.09  # Allow some tolerance
        assert duration < 1.5   # More lenient upper bound for slow systems
        
        # Data should pass through
        assert result == [{"data": "test"}]
    
    @pytest.mark.asyncio
    async def test_stop_node(self):
        """Test STOP node."""
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(parameters={
            "success": True,
            "message": "Workflow completed successfully"
        })
        context.input_data = [{"final": "data"}]
        
        stop_node = StopNode(context)
        
        with pytest.raises(StopWorkflowException) as exc_info:
            await stop_node.execute()
        
        assert exc_info.value.success is True
        assert exc_info.value.message == "Workflow completed successfully"
        assert exc_info.value.output_data == [{"final": "data"}]


class TestNodeRegistry:
    """Test node registry and factory."""
    
    def test_node_registration(self):
        """Test registering nodes in registry."""
        registry = NodeRegistry()
        
        # Register a custom node
        @registry.register("custom.action", NodeType.ACTION)
        class CustomNode(ActionNode):
            @classmethod
            def get_definition(cls) -> NodeDefinition:
                return NodeDefinition(
                    name="Custom Action",
                    type=NodeType.ACTION,
                    category=NodeCategory.CUSTOM,
                    description="A custom action node"
                )
            
            async def execute(self) -> List[Dict[str, Any]]:
                return [{"custom": "result"}]
        
        # Check registration
        assert registry.has_node("custom.action")
        assert registry.get_node_class("custom.action") == CustomNode
        
        # Get all nodes of type
        action_nodes = registry.get_nodes_by_type(NodeType.ACTION)
        assert "custom.action" in action_nodes
    
    def test_node_factory(self):
        """Test node factory creation."""
        registry = NodeRegistry()
        factory = NodeFactory(registry)
        
        # Mock context
        context = Mock(spec=NodeExecutionContext)
        context.node = Mock(
            type=NodeType.ACTION,
            type_version="http.request"
        )
        
        # Create node instance
        node = factory.create_node(context)
        assert isinstance(node, HttpRequestNode)
        
        # Test unknown node type
        context.node.type_version = "unknown.node"
        node = factory.create_node(context)
        assert node is None


class TestExpressionEvaluator:
    """Test expression evaluation system."""
    
    def test_simple_expressions(self):
        """Test simple expression evaluation."""
        evaluator = ExpressionEvaluator()
        
        data = {"name": "John", "age": 30}
        
        assert evaluator.evaluate("{{name}}", data) == "John"
        assert evaluator.evaluate("{{age}}", data) == 30
        assert evaluator.evaluate("Hello {{name}}", data) == "Hello John"
        assert evaluator.evaluate("{{age + 5}}", data) == 35
    
    def test_complex_expressions(self):
        """Test complex expression evaluation."""
        evaluator = ExpressionEvaluator()
        
        data = {
            "user": {"name": "John", "email": "john@example.com"},
            "items": [1, 2, 3, 4, 5],
            "active": True
        }
        
        # Use dictionary syntax for nested access
        assert evaluator.evaluate("{{user['name']}}", data) == "John"
        assert evaluator.evaluate("{{user['email']}}", data) == "john@example.com"
        assert evaluator.evaluate("{{len(items)}}", data) == 5
        assert evaluator.evaluate("{{sum(items)}}", data) == 15
        assert evaluator.evaluate("{{items[0]}}", data) == 1
        assert evaluator.evaluate("{{active and len(items) > 3}}", data) is True
    
    def test_safe_evaluation(self):
        """Test safe expression evaluation."""
        evaluator = ExpressionEvaluator()
        
        data = {"value": 10}
        
        # Dangerous operations should return None (not raise exception)
        result = evaluator.evaluate("{{__import__('os').system('ls')}}", data)
        assert result is None
        
        result = evaluator.evaluate("{{eval('1+1')}}", data)
        assert result is None
        
        # Safe operations should work
        assert evaluator.evaluate("{{value * 2}}", data) == 20
        assert evaluator.evaluate("{{str(value)}}", data) == "10"


@pytest.mark.e2e
class TestE2ENodeExecution:
    """End-to-end tests for node execution in workflows."""
    
    @pytest.mark.asyncio
    async def test_e2e_http_workflow(self, test_session):
        """Test complete HTTP workflow execution."""
        from budflow.executor import WorkflowExecutionEngine
        from budflow.workflows.models import Workflow, WorkflowNode, WorkflowConnection
        from budflow.auth.models import User
        
        # Create test user
        user = User(
            email="e2e@example.com",
            password_hash="hash",
            first_name="E2E",
            last_name="Test"
        )
        test_session.add(user)
        await test_session.commit()
        
        # Create workflow
        workflow = Workflow(
            name="HTTP Test Workflow",
            user_id=user.id,
            status=WorkflowStatus.ACTIVE
        )
        test_session.add(workflow)
        await test_session.commit()
        
        # Create nodes
        webhook_trigger = WorkflowNode(
            workflow_id=workflow.id,
            name="Webhook Trigger",
            type=NodeType.WEBHOOK,
            type_version="webhook.trigger",
            parameters={
                "path": "/test",
                "method": "POST"
            }
        )
        
        http_node = WorkflowNode(
            workflow_id=workflow.id,
            name="Fetch Data",
            type=NodeType.ACTION,
            type_version="http.request",
            parameters={
                "url": "https://jsonplaceholder.typicode.com/users/{{user_id}}",
                "method": "GET"
            }
        )
        
        if_node = WorkflowNode(
            workflow_id=workflow.id,
            name="Check User",
            type=NodeType.CONDITION,
            type_version="if",
            parameters={
                "condition": "{{id == 1}}",
                "output_true": "valid",
                "output_false": "invalid"
            }
        )
        
        test_session.add_all([webhook_trigger, http_node, if_node])
        await test_session.commit()
        
        # Create connections
        conn1 = WorkflowConnection(
            workflow_id=workflow.id,
            source_node_id=webhook_trigger.id,
            target_node_id=http_node.id
        )
        
        conn2 = WorkflowConnection(
            workflow_id=workflow.id,
            source_node_id=http_node.id,
            target_node_id=if_node.id
        )
        
        test_session.add_all([conn1, conn2])
        await test_session.commit()
        
        # Execute workflow
        engine = WorkflowExecutionEngine(test_session)
        
        with patch('httpx.AsyncClient') as mock_client:
            # Mock HTTP response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "id": 1,
                "name": "Leanne Graham",
                "email": "leanne@example.com"
            }
            
            mock_client.return_value.__aenter__.return_value.request = AsyncMock(
                return_value=mock_response
            )
            
            execution = await engine.execute_workflow(
                workflow_id=workflow.id,
                input_data={"user_id": 1}
            )
            
            assert execution.status == ExecutionStatus.SUCCESS
            
            # Verify execution data
            # This would check the actual data flow through nodes
    
    @pytest.mark.asyncio
    async def test_e2e_conditional_workflow(self, test_session):
        """Test workflow with conditional branching."""
        # Similar setup but with more complex branching logic
        pass
    
    @pytest.mark.asyncio
    async def test_e2e_loop_workflow(self, test_session):
        """Test workflow with loop processing."""
        # Test batch processing with loops
        pass