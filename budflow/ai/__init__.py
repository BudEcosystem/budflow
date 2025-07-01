"""AI integration package for BudFlow.

This package provides comprehensive AI capabilities including:
- Multiple AI provider support (OpenAI, Anthropic, Local models)
- LangChain integration
- Vector embeddings and search
- AI-powered workflow automation
- Intelligent data processing
- Natural language workflow generation
"""

from .providers import AIProvider, OpenAIProvider, AnthropicProvider
from .embeddings import EmbeddingService, VectorStore
from .chains import ChainBuilder, WorkflowChain
from .agents import AIAgent, WorkflowAgent
from .tools import AITool, get_builtin_tools
from .prompts import PromptTemplate, PromptManager
from .memory import ConversationMemory, WorkflowMemory

__all__ = [
    # Providers
    "AIProvider",
    "OpenAIProvider", 
    "AnthropicProvider",
    
    # Embeddings
    "EmbeddingService",
    "VectorStore",
    
    # Chains
    "ChainBuilder",
    "WorkflowChain",
    
    # Agents
    "AIAgent",
    "WorkflowAgent",
    
    # Tools
    "AITool",
    "get_builtin_tools",
    
    # Prompts
    "PromptTemplate",
    "PromptManager",
    
    # Memory
    "ConversationMemory",
    "WorkflowMemory",
]