"""AI chains for building complex AI workflows in BudFlow.

This module provides building blocks for creating sophisticated AI workflows
using chains, agents, and tools.
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union
from uuid import uuid4

from pydantic import BaseModel, Field, ConfigDict

from .providers import AIProvider, AIMessage, AIResponse, AIFunction
from .embeddings import EmbeddingService, SearchResult

logger = logging.getLogger(__name__)


class ChainType(str, Enum):
    """Types of AI chains."""
    
    SIMPLE = "simple"
    SEQUENTIAL = "sequential"
    CONDITIONAL = "conditional"
    PARALLEL = "parallel"
    MAP_REDUCE = "map_reduce"
    RETRIEVAL_QA = "retrieval_qa"
    CONVERSATION = "conversation"


class ChainStep(BaseModel):
    """A single step in an AI chain."""
    
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: Optional[str] = None
    prompt_template: str
    model: Optional[str] = None
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    functions: List[AIFunction] = Field(default_factory=list)
    condition: Optional[str] = None  # For conditional steps
    variables: Dict[str, Any] = Field(default_factory=dict)
    
    model_config = ConfigDict(from_attributes=True)


class ChainResult(BaseModel):
    """Result from executing an AI chain."""
    
    chain_id: str
    step_results: Dict[str, Any] = Field(default_factory=dict)
    final_output: Any
    total_tokens: int = 0
    execution_time: float = 0.0
    success: bool = True
    error: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)


class BaseChain(ABC):
    """Abstract base class for AI chains."""
    
    def __init__(
        self,
        provider: AIProvider,
        chain_id: Optional[str] = None,
        name: Optional[str] = None
    ):
        self.provider = provider
        self.chain_id = chain_id or str(uuid4())
        self.name = name or f"Chain-{self.chain_id[:8]}"
        self.steps: List[ChainStep] = []
        self.variables: Dict[str, Any] = {}
    
    @abstractmethod
    async def execute(self, inputs: Dict[str, Any]) -> ChainResult:
        """Execute the chain."""
        pass
    
    def add_step(self, step: ChainStep) -> None:
        """Add a step to the chain."""
        self.steps.append(step)
    
    def set_variables(self, variables: Dict[str, Any]) -> None:
        """Set chain variables."""
        self.variables.update(variables)
    
    def _format_prompt(self, template: str, variables: Dict[str, Any]) -> str:
        """Format prompt template with variables."""
        try:
            return template.format(**variables)
        except KeyError as e:
            raise ValueError(f"Missing variable in prompt template: {e}")


class SimpleChain(BaseChain):
    """Simple single-step AI chain."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ChainResult:
        """Execute simple chain."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            if not self.steps:
                raise ValueError("Chain has no steps")
            
            step = self.steps[0]
            
            # Merge inputs with chain and step variables
            all_variables = {**self.variables, **step.variables, **inputs}
            
            # Format prompt
            prompt = self._format_prompt(step.prompt_template, all_variables)
            
            # Create messages
            messages = [AIMessage(role="user", content=prompt)]
            
            # Execute AI completion
            response = await self.provider.chat_completion(
                messages=messages,
                model=step.model,
                temperature=step.temperature,
                max_tokens=step.max_tokens,
                functions=step.functions if step.functions else None
            )
            
            # Calculate execution time
            execution_time = asyncio.get_event_loop().time() - start_time
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results={step.id: response.content},
                final_output=response.content,
                total_tokens=response.usage.get("total_tokens", 0),
                execution_time=execution_time,
                success=True
            )
            
        except Exception as e:
            execution_time = asyncio.get_event_loop().time() - start_time
            logger.error(f"Simple chain execution failed: {e}")
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results={},
                final_output=None,
                execution_time=execution_time,
                success=False,
                error=str(e)
            )


class SequentialChain(BaseChain):
    """Sequential multi-step AI chain."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ChainResult:
        """Execute sequential chain."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            step_results = {}
            total_tokens = 0
            current_variables = {**self.variables, **inputs}
            
            for step in self.steps:
                # Merge current variables with step variables
                step_variables = {**current_variables, **step.variables}
                
                # Format prompt
                prompt = self._format_prompt(step.prompt_template, step_variables)
                
                # Create messages
                messages = [AIMessage(role="user", content=prompt)]
                
                # Execute AI completion
                response = await self.provider.chat_completion(
                    messages=messages,
                    model=step.model,
                    temperature=step.temperature,
                    max_tokens=step.max_tokens,
                    functions=step.functions if step.functions else None
                )
                
                # Store step result
                step_results[step.id] = response.content
                total_tokens += response.usage.get("total_tokens", 0)
                
                # Update variables for next step
                current_variables[f"step_{step.name}_output"] = response.content
                current_variables["previous_output"] = response.content
            
            # Calculate execution time
            execution_time = asyncio.get_event_loop().time() - start_time
            
            # Final output is the last step's output
            final_output = list(step_results.values())[-1] if step_results else None
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results=step_results,
                final_output=final_output,
                total_tokens=total_tokens,
                execution_time=execution_time,
                success=True
            )
            
        except Exception as e:
            execution_time = asyncio.get_event_loop().time() - start_time
            logger.error(f"Sequential chain execution failed: {e}")
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results=step_results,
                final_output=None,
                execution_time=execution_time,
                success=False,
                error=str(e)
            )


class ConditionalChain(BaseChain):
    """Conditional AI chain with branching logic."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ChainResult:
        """Execute conditional chain."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            step_results = {}
            total_tokens = 0
            current_variables = {**self.variables, **inputs}
            
            for step in self.steps:
                # Check condition if present
                if step.condition:
                    condition_result = self._evaluate_condition(step.condition, current_variables)
                    if not condition_result:
                        logger.debug(f"Skipping step {step.name} due to condition")
                        continue
                
                # Merge current variables with step variables
                step_variables = {**current_variables, **step.variables}
                
                # Format prompt
                prompt = self._format_prompt(step.prompt_template, step_variables)
                
                # Create messages
                messages = [AIMessage(role="user", content=prompt)]
                
                # Execute AI completion
                response = await self.provider.chat_completion(
                    messages=messages,
                    model=step.model,
                    temperature=step.temperature,
                    max_tokens=step.max_tokens,
                    functions=step.functions if step.functions else None
                )
                
                # Store step result
                step_results[step.id] = response.content
                total_tokens += response.usage.get("total_tokens", 0)
                
                # Update variables for next step
                current_variables[f"step_{step.name}_output"] = response.content
                current_variables["previous_output"] = response.content
            
            # Calculate execution time
            execution_time = asyncio.get_event_loop().time() - start_time
            
            # Final output is the last executed step's output
            final_output = list(step_results.values())[-1] if step_results else None
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results=step_results,
                final_output=final_output,
                total_tokens=total_tokens,
                execution_time=execution_time,
                success=True
            )
            
        except Exception as e:
            execution_time = asyncio.get_event_loop().time() - start_time
            logger.error(f"Conditional chain execution failed: {e}")
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results=step_results,
                final_output=None,
                execution_time=execution_time,
                success=False,
                error=str(e)
            )
    
    def _evaluate_condition(self, condition: str, variables: Dict[str, Any]) -> bool:
        """Evaluate a condition string."""
        try:
            # Simple condition evaluation
            # In production, use a safe expression evaluator
            return eval(condition, {"__builtins__": {}}, variables)
        except Exception as e:
            logger.warning(f"Failed to evaluate condition '{condition}': {e}")
            return False


class ParallelChain(BaseChain):
    """Parallel AI chain that executes steps concurrently."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ChainResult:
        """Execute parallel chain."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Merge inputs with chain variables
            all_variables = {**self.variables, **inputs}
            
            # Create tasks for all steps
            tasks = []
            for step in self.steps:
                task = self._execute_step(step, all_variables)
                tasks.append(task)
            
            # Execute all steps in parallel
            step_responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            step_results = {}
            total_tokens = 0
            
            for step, response in zip(self.steps, step_responses):
                if isinstance(response, Exception):
                    logger.error(f"Step {step.name} failed: {response}")
                    step_results[step.id] = f"Error: {response}"
                else:
                    step_results[step.id] = response.content
                    total_tokens += response.usage.get("total_tokens", 0)
            
            # Calculate execution time
            execution_time = asyncio.get_event_loop().time() - start_time
            
            # Combine all outputs
            final_output = step_results
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results=step_results,
                final_output=final_output,
                total_tokens=total_tokens,
                execution_time=execution_time,
                success=True
            )
            
        except Exception as e:
            execution_time = asyncio.get_event_loop().time() - start_time
            logger.error(f"Parallel chain execution failed: {e}")
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results={},
                final_output=None,
                execution_time=execution_time,
                success=False,
                error=str(e)
            )
    
    async def _execute_step(self, step: ChainStep, variables: Dict[str, Any]) -> AIResponse:
        """Execute a single step."""
        # Merge variables with step variables
        step_variables = {**variables, **step.variables}
        
        # Format prompt
        prompt = self._format_prompt(step.prompt_template, step_variables)
        
        # Create messages
        messages = [AIMessage(role="user", content=prompt)]
        
        # Execute AI completion
        return await self.provider.chat_completion(
            messages=messages,
            model=step.model,
            temperature=step.temperature,
            max_tokens=step.max_tokens,
            functions=step.functions if step.functions else None
        )


class RetrievalQAChain(BaseChain):
    """Retrieval-augmented generation chain."""
    
    def __init__(
        self,
        provider: AIProvider,
        embedding_service: EmbeddingService,
        chain_id: Optional[str] = None,
        name: Optional[str] = None,
        k_documents: int = 5
    ):
        super().__init__(provider, chain_id, name)
        self.embedding_service = embedding_service
        self.k_documents = k_documents
    
    async def execute(self, inputs: Dict[str, Any]) -> ChainResult:
        """Execute retrieval QA chain."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            query = inputs.get("query")
            if not query:
                raise ValueError("Query is required for retrieval QA")
            
            # Retrieve relevant documents
            search_results = await self.embedding_service.search(
                query=query,
                k=self.k_documents
            )
            
            # Prepare context from retrieved documents
            context_parts = []
            for result in search_results:
                context_parts.append(f"Document {result.rank}: {result.document.content}")
            
            context = "\n\n".join(context_parts)
            
            # Merge inputs with retrieved context
            all_variables = {
                **self.variables,
                **inputs,
                "context": context,
                "retrieved_documents": len(search_results)
            }
            
            # Use first step as QA template
            if not self.steps:
                # Default QA prompt
                prompt_template = """Based on the following context, answer the question as accurately as possible.

Context:
{context}

Question: {query}

Answer:"""
            else:
                prompt_template = self.steps[0].prompt_template
            
            # Format prompt
            prompt = self._format_prompt(prompt_template, all_variables)
            
            # Create messages
            messages = [AIMessage(role="user", content=prompt)]
            
            # Execute AI completion
            step = self.steps[0] if self.steps else ChainStep(
                name="qa",
                prompt_template=prompt_template
            )
            
            response = await self.provider.chat_completion(
                messages=messages,
                model=step.model,
                temperature=step.temperature,
                max_tokens=step.max_tokens
            )
            
            # Calculate execution time
            execution_time = asyncio.get_event_loop().time() - start_time
            
            # Include retrieval metadata
            metadata = {
                "retrieved_documents": len(search_results),
                "search_scores": [r.score for r in search_results],
                "context_length": len(context)
            }
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results={
                    "retrieval": search_results,
                    "qa": response.content
                },
                final_output={
                    "answer": response.content,
                    "sources": search_results,
                    "metadata": metadata
                },
                total_tokens=response.usage.get("total_tokens", 0),
                execution_time=execution_time,
                success=True
            )
            
        except Exception as e:
            execution_time = asyncio.get_event_loop().time() - start_time
            logger.error(f"Retrieval QA chain execution failed: {e}")
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results={},
                final_output=None,
                execution_time=execution_time,
                success=False,
                error=str(e)
            )


class ConversationChain(BaseChain):
    """Conversation chain with memory."""
    
    def __init__(
        self,
        provider: AIProvider,
        chain_id: Optional[str] = None,
        name: Optional[str] = None,
        max_history: int = 10
    ):
        super().__init__(provider, chain_id, name)
        self.conversation_history: List[AIMessage] = []
        self.max_history = max_history
    
    async def execute(self, inputs: Dict[str, Any]) -> ChainResult:
        """Execute conversation chain."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            user_message = inputs.get("message")
            if not user_message:
                raise ValueError("Message is required for conversation")
            
            # Add system message if provided
            system_message = inputs.get("system_message")
            if system_message and not self.conversation_history:
                self.conversation_history.append(
                    AIMessage(role="system", content=system_message)
                )
            
            # Add user message to history
            self.conversation_history.append(
                AIMessage(role="user", content=user_message)
            )
            
            # Trim history if needed
            if len(self.conversation_history) > self.max_history:
                # Keep system message if present
                system_msgs = [msg for msg in self.conversation_history if msg.role == "system"]
                other_msgs = [msg for msg in self.conversation_history if msg.role != "system"]
                
                # Keep most recent messages
                recent_msgs = other_msgs[-(self.max_history - len(system_msgs)):]
                self.conversation_history = system_msgs + recent_msgs
            
            # Use first step for conversation template
            step = self.steps[0] if self.steps else ChainStep(
                name="conversation",
                prompt_template="{message}"
            )
            
            # Execute AI completion with conversation history
            response = await self.provider.chat_completion(
                messages=self.conversation_history,
                model=step.model,
                temperature=step.temperature,
                max_tokens=step.max_tokens
            )
            
            # Add assistant response to history
            self.conversation_history.append(
                AIMessage(role="assistant", content=response.content)
            )
            
            # Calculate execution time
            execution_time = asyncio.get_event_loop().time() - start_time
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results={"conversation": response.content},
                final_output=response.content,
                total_tokens=response.usage.get("total_tokens", 0),
                execution_time=execution_time,
                success=True
            )
            
        except Exception as e:
            execution_time = asyncio.get_event_loop().time() - start_time
            logger.error(f"Conversation chain execution failed: {e}")
            
            return ChainResult(
                chain_id=self.chain_id,
                step_results={},
                final_output=None,
                execution_time=execution_time,
                success=False,
                error=str(e)
            )
    
    def clear_history(self) -> None:
        """Clear conversation history."""
        self.conversation_history.clear()
    
    def get_history(self) -> List[AIMessage]:
        """Get conversation history."""
        return self.conversation_history.copy()


class ChainBuilder:
    """Builder for creating AI chains."""
    
    def __init__(self, provider: AIProvider):
        self.provider = provider
        self.embedding_service: Optional[EmbeddingService] = None
    
    def set_embedding_service(self, embedding_service: EmbeddingService) -> "ChainBuilder":
        """Set embedding service for retrieval chains."""
        self.embedding_service = embedding_service
        return self
    
    def simple_chain(
        self,
        prompt_template: str,
        name: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.7
    ) -> SimpleChain:
        """Create a simple chain."""
        chain = SimpleChain(self.provider, name=name)
        step = ChainStep(
            name="main",
            prompt_template=prompt_template,
            model=model,
            temperature=temperature
        )
        chain.add_step(step)
        return chain
    
    def sequential_chain(self, name: Optional[str] = None) -> SequentialChain:
        """Create a sequential chain."""
        return SequentialChain(self.provider, name=name)
    
    def conditional_chain(self, name: Optional[str] = None) -> ConditionalChain:
        """Create a conditional chain."""
        return ConditionalChain(self.provider, name=name)
    
    def parallel_chain(self, name: Optional[str] = None) -> ParallelChain:
        """Create a parallel chain."""
        return ParallelChain(self.provider, name=name)
    
    def retrieval_qa_chain(
        self,
        name: Optional[str] = None,
        k_documents: int = 5
    ) -> RetrievalQAChain:
        """Create a retrieval QA chain."""
        if not self.embedding_service:
            raise ValueError("Embedding service is required for retrieval QA")
        
        return RetrievalQAChain(
            self.provider,
            self.embedding_service,
            name=name,
            k_documents=k_documents
        )
    
    def conversation_chain(
        self,
        name: Optional[str] = None,
        max_history: int = 10
    ) -> ConversationChain:
        """Create a conversation chain."""
        return ConversationChain(
            self.provider,
            name=name,
            max_history=max_history
        )


class WorkflowChain:
    """Special chain type for BudFlow workflow integration."""
    
    def __init__(self, provider: AIProvider):
        self.provider = provider
        self.workflow_id: Optional[str] = None
        self.execution_id: Optional[str] = None
    
    async def generate_workflow_from_description(
        self,
        description: str,
        available_nodes: List[str]
    ) -> Dict[str, Any]:
        """Generate workflow configuration from natural language description."""
        prompt = f"""
        Generate a workflow configuration for the following description:
        
        Description: {description}
        
        Available nodes: {', '.join(available_nodes)}
        
        Return a JSON workflow configuration with nodes and connections.
        The workflow should be practical and executable.
        
        Workflow JSON:
        """
        
        messages = [AIMessage(role="user", content=prompt)]
        
        response = await self.provider.chat_completion(
            messages=messages,
            temperature=0.3,  # Lower temperature for more consistent output
            max_tokens=2000
        )
        
        try:
            # Parse the JSON response
            workflow_config = json.loads(response.content)
            return workflow_config
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse workflow JSON: {e}")
            raise ValueError(f"Invalid workflow JSON generated: {e}")
    
    async def optimize_workflow(
        self,
        workflow_config: Dict[str, Any],
        performance_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Optimize workflow configuration based on performance data."""
        prompt = f"""
        Optimize the following workflow configuration:
        
        Current Workflow:
        {json.dumps(workflow_config, indent=2)}
        
        Performance Data:
        {json.dumps(performance_data or {}, indent=2)}
        
        Suggest optimizations for:
        1. Performance improvements
        2. Error reduction
        3. Resource efficiency
        4. Better node organization
        
        Return the optimized workflow JSON:
        """
        
        messages = [AIMessage(role="user", content=prompt)]
        
        response = await self.provider.chat_completion(
            messages=messages,
            temperature=0.3,
            max_tokens=3000
        )
        
        try:
            optimized_config = json.loads(response.content)
            return optimized_config
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse optimized workflow JSON: {e}")
            raise ValueError(f"Invalid optimized workflow JSON: {e}")
    
    async def explain_workflow(self, workflow_config: Dict[str, Any]) -> str:
        """Generate human-readable explanation of workflow."""
        prompt = f"""
        Explain the following workflow in simple, clear language:
        
        Workflow Configuration:
        {json.dumps(workflow_config, indent=2)}
        
        Provide a step-by-step explanation of what this workflow does,
        including the purpose of each node and how data flows through the workflow.
        
        Explanation:
        """
        
        messages = [AIMessage(role="user", content=prompt)]
        
        response = await self.provider.chat_completion(
            messages=messages,
            temperature=0.5,
            max_tokens=1500
        )
        
        return response.content
    
    async def suggest_improvements(
        self,
        workflow_config: Dict[str, Any],
        user_feedback: Optional[str] = None
    ) -> List[str]:
        """Suggest improvements for workflow."""
        prompt = f"""
        Analyze the following workflow and suggest improvements:
        
        Workflow:
        {json.dumps(workflow_config, indent=2)}
        
        User Feedback: {user_feedback or 'None'}
        
        Provide specific, actionable suggestions for:
        1. Adding useful features
        2. Improving error handling
        3. Enhancing performance
        4. Better user experience
        5. Security improvements
        
        Return suggestions as a JSON array of strings:
        """
        
        messages = [AIMessage(role="user", content=prompt)]
        
        response = await self.provider.chat_completion(
            messages=messages,
            temperature=0.7,
            max_tokens=2000
        )
        
        try:
            suggestions = json.loads(response.content)
            return suggestions if isinstance(suggestions, list) else [response.content]
        except json.JSONDecodeError:
            # Fallback to plain text
            return [response.content]