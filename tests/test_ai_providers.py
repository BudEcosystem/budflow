"""Test AI providers and basic AI functionality."""

import pytest
from unittest.mock import AsyncMock, Mock, patch

from budflow.ai.providers import (
    OpenAIProvider,
    AnthropicProvider,
    AIMessage,
    AIResponse,
    AIFunction,
    AIProviderFactory,
    AIProviderType,
)


@pytest.fixture
def openai_provider():
    """Create OpenAI provider for testing."""
    return OpenAIProvider(api_key="test-key")


@pytest.fixture
def anthropic_provider():
    """Create Anthropic provider for testing."""
    return AnthropicProvider(api_key="test-key")


@pytest.fixture
def ai_messages():
    """Create test AI messages."""
    return [
        AIMessage(role="system", content="You are a helpful assistant."),
        AIMessage(role="user", content="Hello, how are you?"),
    ]


@pytest.fixture
def ai_function():
    """Create test AI function."""
    return AIFunction(
        name="get_weather",
        description="Get weather information for a location",
        parameters={
            "location": {"type": "string", "description": "City name"},
            "units": {"type": "string", "enum": ["celsius", "fahrenheit"]},
        },
        required=["location"]
    )


@pytest.mark.unit
class TestAIMessage:
    """Test AI message model."""
    
    def test_message_creation(self):
        """Test creating AI message."""
        message = AIMessage(role="user", content="Hello")
        
        assert message.role == "user"
        assert message.content == "Hello"
        assert message.name is None
        assert message.function_call is None
        assert message.tool_calls is None
    
    def test_message_with_function_call(self):
        """Test message with function call."""
        function_call = {"name": "get_weather", "arguments": '{"location": "NYC"}'}
        message = AIMessage(
            role="assistant",
            content="",
            function_call=function_call
        )
        
        assert message.function_call == function_call


@pytest.mark.unit
class TestAIFunction:
    """Test AI function model."""
    
    def test_function_creation(self, ai_function):
        """Test creating AI function."""
        assert ai_function.name == "get_weather"
        assert ai_function.description == "Get weather information for a location"
        assert "location" in ai_function.parameters
        assert "location" in ai_function.required
    
    def test_openai_format_conversion(self, ai_function):
        """Test converting to OpenAI format."""
        openai_format = ai_function.to_openai_format()
        
        expected = {
            "name": "get_weather",
            "description": "Get weather information for a location",
            "parameters": {
                "type": "object",
                "properties": ai_function.parameters,
                "required": ["location"],
            }
        }
        
        assert openai_format == expected


@pytest.mark.unit
class TestOpenAIProvider:
    """Test OpenAI provider."""
    
    def test_provider_initialization(self, openai_provider):
        """Test provider initialization."""
        assert openai_provider.api_key == "test-key"
        assert openai_provider.client is not None
        assert len(openai_provider.models) > 0
        assert "gpt-3.5-turbo" in openai_provider.models
    
    def test_get_models(self, openai_provider):
        """Test getting available models."""
        models = openai_provider.get_models()
        
        assert len(models) > 0
        assert any(model.name == "gpt-3.5-turbo" for model in models)
        assert any(model.supports_functions for model in models)
    
    def test_token_counting(self, openai_provider):
        """Test token counting."""
        text = "Hello, world!"
        token_count = openai_provider.count_tokens(text)
        
        assert isinstance(token_count, (int, float))
        assert token_count > 0
    
    @pytest.mark.asyncio
    async def test_chat_completion_mock(self, openai_provider, ai_messages):
        """Test chat completion with mocked response."""
        # Mock the OpenAI client
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Hello! I'm doing well, thank you."
        mock_response.choices[0].message.tool_calls = None
        mock_response.choices[0].finish_reason = "stop"
        mock_response.usage.prompt_tokens = 10
        mock_response.usage.completion_tokens = 15
        mock_response.usage.total_tokens = 25
        mock_response.model = "gpt-3.5-turbo"
        mock_response.id = "test-response-id"
        
        openai_provider.client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        response = await openai_provider.chat_completion(
            messages=ai_messages,
            model="gpt-3.5-turbo"
        )
        
        assert isinstance(response, AIResponse)
        assert response.content == "Hello! I'm doing well, thank you."
        assert response.usage["total_tokens"] == 25
        assert response.model == "gpt-3.5-turbo"
    
    @pytest.mark.asyncio
    async def test_embeddings_mock(self, openai_provider):
        """Test embeddings with mocked response."""
        # Mock the OpenAI client
        mock_response = Mock()
        mock_response.data = [
            Mock(embedding=[0.1, 0.2, 0.3]),
            Mock(embedding=[0.4, 0.5, 0.6]),
        ]
        
        openai_provider.client.embeddings.create = AsyncMock(return_value=mock_response)
        
        texts = ["Hello", "World"]
        embeddings = await openai_provider.embeddings(texts)
        
        assert len(embeddings) == 2
        assert embeddings[0] == [0.1, 0.2, 0.3]
        assert embeddings[1] == [0.4, 0.5, 0.6]


@pytest.mark.unit
class TestAnthropicProvider:
    """Test Anthropic provider."""
    
    def test_provider_initialization(self, anthropic_provider):
        """Test provider initialization."""
        assert anthropic_provider.api_key == "test-key"
        assert len(anthropic_provider.models) > 0
        assert "claude-3-sonnet" in anthropic_provider.models
    
    def test_get_models(self, anthropic_provider):
        """Test getting available models."""
        models = anthropic_provider.get_models()
        
        assert len(models) > 0
        assert any(model.name.startswith("claude-3") for model in models)
        assert any(model.supports_vision for model in models)
    
    def test_token_counting(self, anthropic_provider):
        """Test token counting (estimation)."""
        text = "Hello, world!"
        token_count = anthropic_provider.count_tokens(text)
        
        assert isinstance(token_count, (int, float))
        assert token_count > 0
    
    @pytest.mark.asyncio
    async def test_embeddings_not_supported(self, anthropic_provider):
        """Test that embeddings raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            await anthropic_provider.embeddings(["test"])


@pytest.mark.unit
class TestAIProviderFactory:
    """Test AI provider factory."""
    
    def test_create_openai_provider(self):
        """Test creating OpenAI provider."""
        provider = AIProviderFactory.create_provider(
            AIProviderType.OPENAI,
            api_key="test-key"
        )
        
        assert isinstance(provider, OpenAIProvider)
        assert provider.api_key == "test-key"
    
    def test_create_anthropic_provider(self):
        """Test creating Anthropic provider."""
        provider = AIProviderFactory.create_provider(
            AIProviderType.ANTHROPIC,
            api_key="test-key"
        )
        
        assert isinstance(provider, AnthropicProvider)
        assert provider.api_key == "test-key"
    
    def test_unsupported_provider_type(self):
        """Test creating unsupported provider type."""
        with pytest.raises(ValueError, match="Unsupported provider type"):
            AIProviderFactory.create_provider(
                "unsupported_type",
                api_key="test-key"
            )
    
    def test_get_available_providers(self):
        """Test getting available providers."""
        providers = AIProviderFactory.get_available_providers()
        
        assert AIProviderType.OPENAI in providers
        assert AIProviderType.LOCAL in providers


@pytest.mark.integration
class TestProviderIntegration:
    """Integration tests for AI providers."""
    
    @pytest.mark.asyncio
    async def test_openai_chat_completion_integration(self):
        """Integration test for OpenAI chat completion."""
        # Skip if no API key
        pytest.skip("Requires OpenAI API key")
        
        provider = OpenAIProvider(api_key="your-api-key")
        messages = [AIMessage(role="user", content="Say hello")]
        
        response = await provider.chat_completion(messages, model="gpt-3.5-turbo")
        
        assert isinstance(response, AIResponse)
        assert len(response.content) > 0
        assert response.usage["total_tokens"] > 0
    
    @pytest.mark.asyncio
    async def test_openai_embeddings_integration(self):
        """Integration test for OpenAI embeddings."""
        # Skip if no API key
        pytest.skip("Requires OpenAI API key")
        
        provider = OpenAIProvider(api_key="your-api-key")
        texts = ["Hello", "World"]
        
        embeddings = await provider.embeddings(texts)
        
        assert len(embeddings) == 2
        assert len(embeddings[0]) == 1536  # Ada-002 dimensions
        assert len(embeddings[1]) == 1536