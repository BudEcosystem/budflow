"""Vector embeddings and search for BudFlow AI.

This module provides vector embedding generation, storage, and similarity search
capabilities for AI-powered features.
"""

import asyncio
import json
import logging
import numpy as np
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

import numpy as np
from pydantic import BaseModel, Field, ConfigDict

from .providers import AIProvider, OpenAIProvider

logger = logging.getLogger(__name__)


class EmbeddingConfig(BaseModel):
    """Configuration for embedding service."""
    
    provider: str = "openai"
    model: str = "text-embedding-ada-002"
    dimensions: int = 1536
    batch_size: int = 100
    cache_embeddings: bool = True
    similarity_threshold: float = 0.8
    
    model_config = ConfigDict(from_attributes=True)


class Document(BaseModel):
    """Document with embeddings."""
    
    id: str = Field(default_factory=lambda: str(uuid4()))
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    embedding: Optional[List[float]] = None
    
    model_config = ConfigDict(from_attributes=True)


class SearchResult(BaseModel):
    """Search result with similarity score."""
    
    document: Document
    score: float
    rank: int
    
    model_config = ConfigDict(from_attributes=True)


class VectorStore(ABC):
    """Abstract vector store interface."""
    
    @abstractmethod
    async def add_documents(self, documents: List[Document]) -> None:
        """Add documents to the store."""
        pass
    
    @abstractmethod
    async def search(
        self, 
        query_embedding: List[float], 
        k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None
    ) -> List[SearchResult]:
        """Search for similar documents."""
        pass
    
    @abstractmethod
    async def delete_document(self, document_id: str) -> bool:
        """Delete a document."""
        pass
    
    @abstractmethod
    async def update_document(self, document: Document) -> bool:
        """Update a document."""
        pass
    
    @abstractmethod
    async def get_document(self, document_id: str) -> Optional[Document]:
        """Get a document by ID."""
        pass


class InMemoryVectorStore(VectorStore):
    """In-memory vector store implementation."""
    
    def __init__(self):
        self.documents: Dict[str, Document] = {}
        self.embeddings: np.ndarray = None
        self.document_ids: List[str] = []
    
    async def add_documents(self, documents: List[Document]) -> None:
        """Add documents to the store."""
        for doc in documents:
            if not doc.embedding:
                raise ValueError(f"Document {doc.id} has no embedding")
            
            self.documents[doc.id] = doc
            
            if doc.id not in self.document_ids:
                self.document_ids.append(doc.id)
        
        # Rebuild embeddings matrix
        self._rebuild_embeddings_matrix()
    
    async def search(
        self, 
        query_embedding: List[float], 
        k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None
    ) -> List[SearchResult]:
        """Search for similar documents."""
        if self.embeddings is None or len(self.document_ids) == 0:
            return []
        
        # Convert query to numpy array
        query_vector = np.array(query_embedding)
        
        # Calculate cosine similarities
        similarities = self._cosine_similarity(query_vector, self.embeddings)
        
        # Get top k results
        top_indices = np.argsort(similarities)[::-1][:k]
        
        results = []
        for rank, idx in enumerate(top_indices):
            doc_id = self.document_ids[idx]
            document = self.documents[doc_id]
            
            # Apply metadata filter if provided
            if filter_metadata:
                if not self._matches_filter(document.metadata, filter_metadata):
                    continue
            
            results.append(SearchResult(
                document=document,
                score=float(similarities[idx]),
                rank=rank + 1
            ))
        
        return results
    
    async def delete_document(self, document_id: str) -> bool:
        """Delete a document."""
        if document_id in self.documents:
            del self.documents[document_id]
            self.document_ids.remove(document_id)
            self._rebuild_embeddings_matrix()
            return True
        return False
    
    async def update_document(self, document: Document) -> bool:
        """Update a document."""
        if document.id in self.documents:
            self.documents[document.id] = document
            self._rebuild_embeddings_matrix()
            return True
        return False
    
    async def get_document(self, document_id: str) -> Optional[Document]:
        """Get a document by ID."""
        return self.documents.get(document_id)
    
    def _rebuild_embeddings_matrix(self) -> None:
        """Rebuild the embeddings matrix."""
        if not self.document_ids:
            self.embeddings = None
            return
        
        embeddings_list = []
        for doc_id in self.document_ids:
            doc = self.documents[doc_id]
            if doc.embedding:
                embeddings_list.append(doc.embedding)
        
        if embeddings_list:
            self.embeddings = np.array(embeddings_list)
        else:
            self.embeddings = None
    
    def _cosine_similarity(self, query: np.ndarray, embeddings: np.ndarray) -> np.ndarray:
        """Calculate cosine similarity between query and embeddings."""
        # Normalize vectors
        query_norm = query / np.linalg.norm(query)
        embeddings_norm = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        
        # Calculate dot product
        similarities = np.dot(embeddings_norm, query_norm)
        return similarities
    
    def _matches_filter(self, metadata: Dict[str, Any], filter_dict: Dict[str, Any]) -> bool:
        """Check if metadata matches filter criteria."""
        for key, value in filter_dict.items():
            if key not in metadata or metadata[key] != value:
                return False
        return True


class RedisVectorStore(VectorStore):
    """Redis-based vector store implementation."""
    
    def __init__(self, redis_client, index_name: str = "budflow_vectors"):
        self.redis = redis_client
        self.index_name = index_name
    
    async def add_documents(self, documents: List[Document]) -> None:
        """Add documents to Redis."""
        try:
            # Check if RediSearch is available
            pipeline = self.redis.pipeline()
            
            for doc in documents:
                if not doc.embedding:
                    raise ValueError(f"Document {doc.id} has no embedding")
                
                # Store document data
                doc_key = f"{self.index_name}:doc:{doc.id}"
                doc_data = {
                    "content": doc.content,
                    "metadata": json.dumps(doc.metadata),
                    "embedding": json.dumps(doc.embedding),
                }
                
                pipeline.hset(doc_key, mapping=doc_data)
            
            await pipeline.execute()
            
        except Exception as e:
            logger.error(f"Failed to add documents to Redis: {e}")
            raise
    
    async def search(
        self, 
        query_embedding: List[float], 
        k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None
    ) -> List[SearchResult]:
        """Search for similar documents in Redis."""
        try:
            # Get all document keys
            pattern = f"{self.index_name}:doc:*"
            keys = await self.redis.keys(pattern)
            
            if not keys:
                return []
            
            # Get all documents
            pipeline = self.redis.pipeline()
            for key in keys:
                pipeline.hgetall(key)
            
            results = await pipeline.execute()
            
            # Calculate similarities
            documents_with_scores = []
            query_vector = np.array(query_embedding)
            
            for key, doc_data in zip(keys, results):
                if not doc_data:
                    continue
                
                try:
                    doc_id = key.decode().split(":")[-1]
                    content = doc_data[b"content"].decode()
                    metadata = json.loads(doc_data[b"metadata"].decode())
                    embedding = json.loads(doc_data[b"embedding"].decode())
                    
                    # Apply metadata filter
                    if filter_metadata and not self._matches_filter(metadata, filter_metadata):
                        continue
                    
                    # Calculate similarity
                    doc_vector = np.array(embedding)
                    similarity = self._cosine_similarity(query_vector, doc_vector)
                    
                    document = Document(
                        id=doc_id,
                        content=content,
                        metadata=metadata,
                        embedding=embedding
                    )
                    
                    documents_with_scores.append((document, similarity))
                    
                except Exception as e:
                    logger.warning(f"Failed to process document {key}: {e}")
                    continue
            
            # Sort by similarity and take top k
            documents_with_scores.sort(key=lambda x: x[1], reverse=True)
            documents_with_scores = documents_with_scores[:k]
            
            # Create search results
            search_results = []
            for rank, (document, score) in enumerate(documents_with_scores):
                search_results.append(SearchResult(
                    document=document,
                    score=float(score),
                    rank=rank + 1
                ))
            
            return search_results
            
        except Exception as e:
            logger.error(f"Failed to search in Redis: {e}")
            raise
    
    async def delete_document(self, document_id: str) -> bool:
        """Delete a document from Redis."""
        try:
            doc_key = f"{self.index_name}:doc:{document_id}"
            result = await self.redis.delete(doc_key)
            return result > 0
        except Exception as e:
            logger.error(f"Failed to delete document: {e}")
            return False
    
    async def update_document(self, document: Document) -> bool:
        """Update a document in Redis."""
        try:
            if not document.embedding:
                raise ValueError(f"Document {document.id} has no embedding")
            
            doc_key = f"{self.index_name}:doc:{document.id}"
            doc_data = {
                "content": document.content,
                "metadata": json.dumps(document.metadata),
                "embedding": json.dumps(document.embedding),
            }
            
            await self.redis.hset(doc_key, mapping=doc_data)
            return True
            
        except Exception as e:
            logger.error(f"Failed to update document: {e}")
            return False
    
    async def get_document(self, document_id: str) -> Optional[Document]:
        """Get a document by ID from Redis."""
        try:
            doc_key = f"{self.index_name}:doc:{document_id}"
            doc_data = await self.redis.hgetall(doc_key)
            
            if not doc_data:
                return None
            
            return Document(
                id=document_id,
                content=doc_data[b"content"].decode(),
                metadata=json.loads(doc_data[b"metadata"].decode()),
                embedding=json.loads(doc_data[b"embedding"].decode())
            )
            
        except Exception as e:
            logger.error(f"Failed to get document: {e}")
            return None
    
    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors."""
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        return dot_product / (norm1 * norm2)
    
    def _matches_filter(self, metadata: Dict[str, Any], filter_dict: Dict[str, Any]) -> bool:
        """Check if metadata matches filter criteria."""
        for key, value in filter_dict.items():
            if key not in metadata or metadata[key] != value:
                return False
        return True


class EmbeddingService:
    """Service for generating and managing embeddings."""
    
    def __init__(
        self, 
        ai_provider: AIProvider,
        vector_store: Optional[VectorStore] = None,
        config: Optional[EmbeddingConfig] = None
    ):
        self.provider = ai_provider
        self.vector_store = vector_store or InMemoryVectorStore()
        self.config = config or EmbeddingConfig()
        self._embedding_cache: Dict[str, List[float]] = {}
    
    async def embed_text(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        # Check cache first
        if self.config.cache_embeddings and text in self._embedding_cache:
            return self._embedding_cache[text]
        
        try:
            embeddings = await self.provider.embeddings(
                texts=[text],
                model=self.config.model
            )
            
            embedding = embeddings[0]
            
            # Cache the embedding
            if self.config.cache_embeddings:
                self._embedding_cache[text] = embedding
            
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise
    
    async def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts."""
        # Check cache for existing embeddings
        cached_embeddings = {}
        uncached_texts = []
        
        if self.config.cache_embeddings:
            for text in texts:
                if text in self._embedding_cache:
                    cached_embeddings[text] = self._embedding_cache[text]
                else:
                    uncached_texts.append(text)
        else:
            uncached_texts = texts
        
        # Generate embeddings for uncached texts
        new_embeddings = {}
        if uncached_texts:
            try:
                # Process in batches
                for i in range(0, len(uncached_texts), self.config.batch_size):
                    batch = uncached_texts[i:i + self.config.batch_size]
                    batch_embeddings = await self.provider.embeddings(
                        texts=batch,
                        model=self.config.model
                    )
                    
                    for text, embedding in zip(batch, batch_embeddings):
                        new_embeddings[text] = embedding
                        
                        # Cache the embedding
                        if self.config.cache_embeddings:
                            self._embedding_cache[text] = embedding
                            
            except Exception as e:
                logger.error(f"Failed to generate embeddings: {e}")
                raise
        
        # Combine cached and new embeddings in original order
        all_embeddings = {**cached_embeddings, **new_embeddings}
        return [all_embeddings[text] for text in texts]
    
    async def add_documents(
        self, 
        texts: List[str], 
        metadatas: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[str]] = None
    ) -> List[Document]:
        """Add documents with embeddings to the vector store."""
        if metadatas is None:
            metadatas = [{}] * len(texts)
        
        if ids is None:
            ids = [str(uuid4()) for _ in range(len(texts))]
        
        if len(texts) != len(metadatas) or len(texts) != len(ids):
            raise ValueError("texts, metadatas, and ids must have the same length")
        
        # Generate embeddings
        embeddings = await self.embed_texts(texts)
        
        # Create documents
        documents = []
        for text, metadata, doc_id, embedding in zip(texts, metadatas, ids, embeddings):
            document = Document(
                id=doc_id,
                content=text,
                metadata=metadata,
                embedding=embedding
            )
            documents.append(document)
        
        # Add to vector store
        await self.vector_store.add_documents(documents)
        
        return documents
    
    async def search(
        self, 
        query: str, 
        k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None,
        similarity_threshold: Optional[float] = None
    ) -> List[SearchResult]:
        """Search for similar documents."""
        # Generate query embedding
        query_embedding = await self.embed_text(query)
        
        # Search in vector store
        results = await self.vector_store.search(
            query_embedding=query_embedding,
            k=k,
            filter_metadata=filter_metadata
        )
        
        # Apply similarity threshold filter
        threshold = similarity_threshold or self.config.similarity_threshold
        filtered_results = [
            result for result in results 
            if result.score >= threshold
        ]
        
        return filtered_results
    
    async def search_by_embedding(
        self, 
        query_embedding: List[float], 
        k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None,
        similarity_threshold: Optional[float] = None
    ) -> List[SearchResult]:
        """Search using a pre-computed embedding."""
        # Search in vector store
        results = await self.vector_store.search(
            query_embedding=query_embedding,
            k=k,
            filter_metadata=filter_metadata
        )
        
        # Apply similarity threshold filter
        threshold = similarity_threshold or self.config.similarity_threshold
        filtered_results = [
            result for result in results 
            if result.score >= threshold
        ]
        
        return filtered_results
    
    async def delete_document(self, document_id: str) -> bool:
        """Delete a document from the vector store."""
        return await self.vector_store.delete_document(document_id)
    
    async def update_document(
        self, 
        document_id: str, 
        text: str, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update a document in the vector store."""
        # Generate new embedding
        embedding = await self.embed_text(text)
        
        # Create updated document
        document = Document(
            id=document_id,
            content=text,
            metadata=metadata or {},
            embedding=embedding
        )
        
        return await self.vector_store.update_document(document)
    
    async def get_document(self, document_id: str) -> Optional[Document]:
        """Get a document by ID."""
        return await self.vector_store.get_document(document_id)
    
    def clear_cache(self) -> None:
        """Clear the embedding cache."""
        self._embedding_cache.clear()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get embedding cache statistics."""
        return {
            "cache_size": len(self._embedding_cache),
            "cache_enabled": self.config.cache_embeddings,
        }


# Global embedding service instance
_embedding_service: Optional[EmbeddingService] = None


def get_embedding_service() -> Optional[EmbeddingService]:
    """Get global embedding service instance."""
    return _embedding_service


def set_embedding_service(service: EmbeddingService) -> None:
    """Set global embedding service instance."""
    global _embedding_service
    _embedding_service = service


async def create_default_embedding_service(
    openai_api_key: str,
    vector_store: Optional[VectorStore] = None
) -> EmbeddingService:
    """Create default embedding service with OpenAI provider."""
    provider = OpenAIProvider(api_key=openai_api_key)
    store = vector_store or InMemoryVectorStore()
    
    service = EmbeddingService(
        ai_provider=provider,
        vector_store=store
    )
    
    set_embedding_service(service)
    return service