"""AI provider implementations for BudFlow.

This module provides unified interfaces to various AI providers including
OpenAI, Anthropic, and local models.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, AsyncIterator, Union
from uuid import uuid4

import openai
import tiktoken
from pydantic import BaseModel, Field, ConfigDict

logger = logging.getLogger(__name__)


class AIProviderType(str, Enum):
    """AI provider types."""
    
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    LOCAL = "local"
    AZURE_OPENAI = "azure_openai"


class AIModel(BaseModel):
    """AI model configuration."""
    
    name: str
    provider: AIProviderType
    max_tokens: int = 4096
    context_window: int = 8192
    supports_functions: bool = False
    supports_vision: bool = False
    supports_streaming: bool = True
    cost_per_token: float = 0.0
    
    model_config = ConfigDict(from_attributes=True)


class AIMessage(BaseModel):
    """AI message structure."""
    
    role: str  # "system", "user", "assistant", "function"
    content: str
    name: Optional[str] = None
    function_call: Optional[Dict[str, Any]] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None
    
    model_config = ConfigDict(from_attributes=True)


class AIResponse(BaseModel):
    """AI response structure."""
    
    content: str
    usage: Dict[str, int] = Field(default_factory=dict)
    model: str
    finish_reason: Optional[str] = None
    function_calls: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    model_config = ConfigDict(from_attributes=True)


class AIFunction(BaseModel):
    """AI function definition."""
    
    name: str
    description: str
    parameters: Dict[str, Any]
    required: List[str] = Field(default_factory=list)
    
    def to_openai_format(self) -> Dict[str, Any]:
        """Convert to OpenAI function format."""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": {
                "type": "object",
                "properties": self.parameters,
                "required": self.required,
            }
        }
    
    model_config = ConfigDict(from_attributes=True)


class AIProvider(ABC):
    """Abstract base class for AI providers."""
    
    def __init__(self, api_key: str, **kwargs):
        self.api_key = api_key
        self.config = kwargs
    
    @abstractmethod
    async def chat_completion(
        self,
        messages: List[AIMessage],
        model: str = None,
        temperature: float = 0.7,
        max_tokens: int = None,
        functions: Optional[List[AIFunction]] = None,
        stream: bool = False,
        **kwargs
    ) -> Union[AIResponse, AsyncIterator[str]]:
        """Generate chat completion."""
        pass
    
    @abstractmethod
    async def text_completion(
        self,
        prompt: str,
        model: str = None,
        temperature: float = 0.7,
        max_tokens: int = None,
        **kwargs
    ) -> AIResponse:
        """Generate text completion."""
        pass
    
    @abstractmethod
    async def embeddings(
        self,
        texts: List[str],
        model: str = None,
        **kwargs
    ) -> List[List[float]]:
        """Generate embeddings."""
        pass
    
    @abstractmethod
    def get_models(self) -> List[AIModel]:
        """Get available models."""
        pass
    
    @abstractmethod
    def count_tokens(self, text: str, model: str = None) -> int:
        """Count tokens in text."""
        pass


class OpenAIProvider(AIProvider):
    """OpenAI provider implementation."""
    
    def __init__(self, api_key: str, base_url: Optional[str] = None, **kwargs):
        super().__init__(api_key, **kwargs)
        self.client = openai.AsyncOpenAI(
            api_key=api_key,
            base_url=base_url,
        )
        
        # Model configurations
        self.models = {
            "gpt-4": AIModel(
                name="gpt-4",
                provider=AIProviderType.OPENAI,
                max_tokens=8192,
                context_window=8192,
                supports_functions=True,
                supports_vision=False,
                cost_per_token=0.00003,
            ),
            "gpt-4-turbo": AIModel(
                name="gpt-4-turbo",
                provider=AIProviderType.OPENAI,
                max_tokens=4096,
                context_window=128000,
                supports_functions=True,
                supports_vision=True,
                cost_per_token=0.00001,
            ),
            "gpt-3.5-turbo": AIModel(
                name="gpt-3.5-turbo",
                provider=AIProviderType.OPENAI,
                max_tokens=4096,
                context_window=16385,
                supports_functions=True,
                supports_vision=False,
                cost_per_token=0.0000015,
            ),
        }
    
    async def chat_completion(
        self,
        messages: List[AIMessage],
        model: str = "gpt-3.5-turbo",
        temperature: float = 0.7,
        max_tokens: int = None,
        functions: Optional[List[AIFunction]] = None,
        stream: bool = False,
        **kwargs
    ) -> Union[AIResponse, AsyncIterator[str]]:
        """Generate chat completion using OpenAI."""
        try:
            # Convert messages to OpenAI format
            openai_messages = []
            for msg in messages:
                openai_msg = {
                    "role": msg.role,
                    "content": msg.content,
                }
                if msg.name:
                    openai_msg["name"] = msg.name
                if msg.function_call:
                    openai_msg["function_call"] = msg.function_call
                if msg.tool_calls:
                    openai_msg["tool_calls"] = msg.tool_calls
                openai_messages.append(openai_msg)
            
            # Prepare request parameters
            params = {
                "model": model,
                "messages": openai_messages,
                "temperature": temperature,
                "stream": stream,
            }
            
            if max_tokens:
                params["max_tokens"] = max_tokens
            
            # Add functions if provided
            if functions:
                params["tools"] = [
                    {"type": "function", "function": func.to_openai_format()}
                    for func in functions
                ]
            
            # Update with any additional parameters
            params.update(kwargs)
            
            if stream:
                return self._stream_chat_completion(params)
            else:
                response = await self.client.chat.completions.create(**params)
                
                # Extract function calls if any
                function_calls = []
                if response.choices[0].message.tool_calls:
                    function_calls = [
                        {
                            "name": call.function.name,
                            "arguments": call.function.arguments,
                            "id": call.id,
                        }
                        for call in response.choices[0].message.tool_calls
                    ]
                
                return AIResponse(
                    content=response.choices[0].message.content or "",
                    usage={
                        "prompt_tokens": response.usage.prompt_tokens,
                        "completion_tokens": response.usage.completion_tokens,
                        "total_tokens": response.usage.total_tokens,
                    } if response.usage else {},
                    model=response.model,
                    finish_reason=response.choices[0].finish_reason,
                    function_calls=function_calls,
                    metadata={"response_id": response.id},
                )
                
        except Exception as e:
            logger.error(f"OpenAI chat completion error: {e}")
            raise
    
    async def _stream_chat_completion(self, params: Dict[str, Any]) -> AsyncIterator[str]:
        """Stream chat completion."""
        try:
            async for chunk in await self.client.chat.completions.create(**params):
                if chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content
        except Exception as e:
            logger.error(f"OpenAI streaming error: {e}")
            raise
    
    async def text_completion(
        self,
        prompt: str,
        model: str = "gpt-3.5-turbo-instruct",
        temperature: float = 0.7,
        max_tokens: int = None,
        **kwargs
    ) -> AIResponse:
        """Generate text completion using OpenAI."""
        try:
            params = {
                "model": model,
                "prompt": prompt,
                "temperature": temperature,
            }
            
            if max_tokens:
                params["max_tokens"] = max_tokens
            
            params.update(kwargs)
            
            response = await self.client.completions.create(**params)
            
            return AIResponse(
                content=response.choices[0].text,
                usage={
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                } if response.usage else {},
                model=response.model,
                finish_reason=response.choices[0].finish_reason,
                metadata={"response_id": response.id},
            )
            
        except Exception as e:
            logger.error(f"OpenAI text completion error: {e}")
            raise
    
    async def embeddings(
        self,
        texts: List[str],
        model: str = "text-embedding-ada-002",
        **kwargs
    ) -> List[List[float]]:
        """Generate embeddings using OpenAI."""
        try:
            response = await self.client.embeddings.create(
                model=model,
                input=texts,
                **kwargs
            )
            
            return [data.embedding for data in response.data]
            
        except Exception as e:
            logger.error(f"OpenAI embeddings error: {e}")
            raise
    
    def get_models(self) -> List[AIModel]:
        """Get available OpenAI models."""
        return list(self.models.values())
    
    def count_tokens(self, text: str, model: str = "gpt-3.5-turbo") -> int:
        """Count tokens using tiktoken."""
        try:
            # Get encoding for model
            if model.startswith("gpt-4"):
                encoding = tiktoken.get_encoding("cl100k_base")
            elif model.startswith("gpt-3.5"):
                encoding = tiktoken.get_encoding("cl100k_base")
            else:
                encoding = tiktoken.get_encoding("cl100k_base")
            
            return len(encoding.encode(text))
            
        except Exception as e:
            logger.warning(f"Token counting error: {e}")
            # Fallback: rough estimation
            return len(text.split()) * 1.3


class AnthropicProvider(AIProvider):
    """Anthropic provider implementation."""
    
    def __init__(self, api_key: str, **kwargs):
        super().__init__(api_key, **kwargs)
        
        # Try to import Anthropic client
        try:
            import anthropic
            self.client = anthropic.AsyncAnthropic(api_key=api_key)
        except ImportError:
            logger.warning("Anthropic client not available. Install with: pip install anthropic")
            self.client = None
        
        # Model configurations
        self.models = {
            "claude-3-opus": AIModel(
                name="claude-3-opus-20240229",
                provider=AIProviderType.ANTHROPIC,
                max_tokens=4096,
                context_window=200000,
                supports_functions=True,
                supports_vision=True,
                cost_per_token=0.000015,
            ),
            "claude-3-sonnet": AIModel(
                name="claude-3-sonnet-20240229",
                provider=AIProviderType.ANTHROPIC,
                max_tokens=4096,
                context_window=200000,
                supports_functions=True,
                supports_vision=True,
                cost_per_token=0.000003,
            ),
            "claude-3-haiku": AIModel(
                name="claude-3-haiku-20240307",
                provider=AIProviderType.ANTHROPIC,
                max_tokens=4096,
                context_window=200000,
                supports_functions=True,
                supports_vision=True,
                cost_per_token=0.00000025,
            ),
        }
    
    async def chat_completion(
        self,
        messages: List[AIMessage],
        model: str = "claude-3-sonnet-20240229",
        temperature: float = 0.7,
        max_tokens: int = 1024,
        functions: Optional[List[AIFunction]] = None,
        stream: bool = False,
        **kwargs
    ) -> Union[AIResponse, AsyncIterator[str]]:
        """Generate chat completion using Anthropic."""
        if not self.client:
            raise RuntimeError("Anthropic client not available")
        
        try:
            # Convert messages to Anthropic format
            system_message = ""
            user_messages = []
            
            for msg in messages:
                if msg.role == "system":
                    system_message += msg.content + "\n"
                elif msg.role in ["user", "assistant"]:
                    user_messages.append({
                        "role": msg.role,
                        "content": msg.content,
                    })
            
            # Prepare request parameters
            params = {
                "model": model,
                "messages": user_messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "stream": stream,
            }
            
            if system_message.strip():
                params["system"] = system_message.strip()
            
            # Add tools if provided
            if functions:
                params["tools"] = [
                    {
                        "name": func.name,
                        "description": func.description,
                        "input_schema": {
                            "type": "object",
                            "properties": func.parameters,
                            "required": func.required,
                        }
                    }
                    for func in functions
                ]
            
            params.update(kwargs)
            
            if stream:
                return self._stream_chat_completion(params)
            else:
                response = await self.client.messages.create(**params)
                
                # Extract function calls if any
                function_calls = []
                if hasattr(response, 'content') and response.content:
                    for content_block in response.content:
                        if hasattr(content_block, 'type') and content_block.type == 'tool_use':
                            function_calls.append({
                                "name": content_block.name,
                                "arguments": content_block.input,
                                "id": content_block.id,
                            })
                
                # Get text content
                text_content = ""
                if hasattr(response, 'content') and response.content:
                    for content_block in response.content:
                        if hasattr(content_block, 'type') and content_block.type == 'text':
                            text_content += content_block.text
                
                return AIResponse(
                    content=text_content,
                    usage={
                        "prompt_tokens": response.usage.input_tokens,
                        "completion_tokens": response.usage.output_tokens,
                        "total_tokens": response.usage.input_tokens + response.usage.output_tokens,
                    } if hasattr(response, 'usage') else {},
                    model=response.model,
                    finish_reason=response.stop_reason,
                    function_calls=function_calls,
                    metadata={"response_id": response.id},
                )
                
        except Exception as e:
            logger.error(f"Anthropic chat completion error: {e}")
            raise
    
    async def _stream_chat_completion(self, params: Dict[str, Any]) -> AsyncIterator[str]:
        """Stream chat completion."""
        try:
            async with self.client.messages.stream(**params) as stream:
                async for chunk in stream:
                    if hasattr(chunk, 'delta') and hasattr(chunk.delta, 'text'):
                        yield chunk.delta.text
        except Exception as e:
            logger.error(f"Anthropic streaming error: {e}")
            raise
    
    async def text_completion(
        self,
        prompt: str,
        model: str = "claude-3-sonnet-20240229",
        temperature: float = 0.7,
        max_tokens: int = 1024,
        **kwargs
    ) -> AIResponse:
        """Generate text completion using Anthropic."""
        # Convert to chat format
        messages = [AIMessage(role="user", content=prompt)]
        return await self.chat_completion(
            messages=messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )
    
    async def embeddings(
        self,
        texts: List[str],
        model: str = None,
        **kwargs
    ) -> List[List[float]]:
        """Anthropic doesn't provide embeddings - use OpenAI fallback."""
        raise NotImplementedError("Anthropic doesn't provide embeddings API")
    
    def get_models(self) -> List[AIModel]:
        """Get available Anthropic models."""
        return list(self.models.values())
    
    def count_tokens(self, text: str, model: str = None) -> int:
        """Count tokens (rough estimation for Anthropic)."""
        # Anthropic uses similar tokenization to OpenAI
        # Rough estimation: ~4 characters per token
        return len(text) // 4


class LocalProvider(AIProvider):
    """Local model provider implementation."""
    
    def __init__(self, base_url: str, model_name: str = "local-model", **kwargs):
        super().__init__("", **kwargs)
        self.base_url = base_url
        self.model_name = model_name
        
        # Model configuration
        self.models = {
            model_name: AIModel(
                name=model_name,
                provider=AIProviderType.LOCAL,
                max_tokens=2048,
                context_window=4096,
                supports_functions=False,
                supports_vision=False,
                cost_per_token=0.0,
            )
        }
    
    async def chat_completion(
        self,
        messages: List[AIMessage],
        model: str = None,
        temperature: float = 0.7,
        max_tokens: int = None,
        functions: Optional[List[AIFunction]] = None,
        stream: bool = False,
        **kwargs
    ) -> Union[AIResponse, AsyncIterator[str]]:
        """Generate chat completion using local model."""
        # Implementation would depend on the local model API
        # This is a placeholder for Ollama, LM Studio, etc.
        raise NotImplementedError("Local provider implementation depends on specific API")
    
    async def text_completion(
        self,
        prompt: str,
        model: str = None,
        temperature: float = 0.7,
        max_tokens: int = None,
        **kwargs
    ) -> AIResponse:
        """Generate text completion using local model."""
        raise NotImplementedError("Local provider implementation depends on specific API")
    
    async def embeddings(
        self,
        texts: List[str],
        model: str = None,
        **kwargs
    ) -> List[List[float]]:
        """Generate embeddings using local model."""
        raise NotImplementedError("Local provider implementation depends on specific API")
    
    def get_models(self) -> List[AIModel]:
        """Get available local models."""
        return list(self.models.values())
    
    def count_tokens(self, text: str, model: str = None) -> int:
        """Count tokens (rough estimation)."""
        return len(text.split())


class AIProviderFactory:
    """Factory for creating AI providers."""
    
    @staticmethod
    def create_provider(
        provider_type: AIProviderType,
        api_key: str = None,
        **kwargs
    ) -> AIProvider:
        """Create AI provider instance."""
        if provider_type == AIProviderType.OPENAI:
            return OpenAIProvider(api_key=api_key, **kwargs)
        elif provider_type == AIProviderType.ANTHROPIC:
            return AnthropicProvider(api_key=api_key, **kwargs)
        elif provider_type == AIProviderType.LOCAL:
            return LocalProvider(**kwargs)
        elif provider_type == AIProviderType.AZURE_OPENAI:
            # Azure OpenAI uses the same client as OpenAI
            return OpenAIProvider(api_key=api_key, **kwargs)
        else:
            raise ValueError(f"Unsupported provider type: {provider_type}")
    
    @staticmethod
    def get_available_providers() -> List[AIProviderType]:
        """Get list of available providers."""
        providers = [AIProviderType.OPENAI, AIProviderType.LOCAL]
        
        # Check if Anthropic is available
        try:
            import anthropic
            providers.append(AIProviderType.ANTHROPIC)
        except ImportError:
            pass
        
        return providers


# Global provider registry
_providers: Dict[str, AIProvider] = {}


def register_provider(name: str, provider: AIProvider) -> None:
    """Register an AI provider."""
    _providers[name] = provider


def get_provider(name: str) -> Optional[AIProvider]:
    """Get registered AI provider."""
    return _providers.get(name)


def list_providers() -> List[str]:
    """List registered provider names."""
    return list(_providers.keys())


def remove_provider(name: str) -> bool:
    """Remove registered provider."""
    if name in _providers:
        del _providers[name]
        return True
    return False