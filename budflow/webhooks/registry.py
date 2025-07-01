"""Webhook registry for managing webhook registrations."""

import logging
from typing import Dict, List, Optional, Tuple
from threading import RLock

from .models import WebhookMethod, WebhookPath, WebhookRegistration
from .exceptions import DuplicateWebhookError, WebhookNotFoundError

logger = logging.getLogger(__name__)


class WebhookRegistry:
    """In-memory registry for webhook registrations."""
    
    def __init__(self):
        # Static webhooks: (path, method) -> registration
        self._static_webhooks: Dict[Tuple[str, WebhookMethod], WebhookRegistration] = {}
        
        # Dynamic webhooks: list of registrations with patterns
        self._dynamic_webhooks: List[Tuple[WebhookPath, WebhookRegistration]] = []
        
        # Index by workflow ID for fast lookup
        self._workflow_index: Dict[str, List[WebhookRegistration]] = {}
        
        # Index by webhook ID
        self._id_index: Dict[str, WebhookRegistration] = {}
        
        # Thread safety
        self._lock = RLock()
    
    def register(self, registration: WebhookRegistration) -> None:
        """Register a webhook."""
        with self._lock:
            # Check for duplicates
            existing = self._find_existing(registration.path, registration.method)
            if existing and existing.id != registration.id:
                raise DuplicateWebhookError(
                    path=registration.path,
                    method=registration.method.value,
                    existing_workflow_id=existing.workflow_id,
                    new_workflow_id=registration.workflow_id,
                )
            
            # Parse path
            webhook_path = WebhookPath(registration.path)
            
            # Register based on type
            if webhook_path.is_dynamic:
                # Remove any existing registration with same ID
                self._dynamic_webhooks = [
                    (path, reg) for path, reg in self._dynamic_webhooks
                    if reg.id != registration.id
                ]
                self._dynamic_webhooks.append((webhook_path, registration))
            else:
                key = (webhook_path.path, registration.method)
                self._static_webhooks[key] = registration
            
            # Update indexes
            self._id_index[registration.id] = registration
            
            # Update workflow index
            if registration.workflow_id not in self._workflow_index:
                self._workflow_index[registration.workflow_id] = []
            
            # Remove old entry if exists
            self._workflow_index[registration.workflow_id] = [
                r for r in self._workflow_index[registration.workflow_id]
                if r.id != registration.id
            ]
            self._workflow_index[registration.workflow_id].append(registration)
            
            logger.info(
                f"Registered webhook {registration.method} {registration.path} "
                f"for workflow {registration.workflow_id}"
            )
    
    def unregister(self, webhook_id: str) -> Optional[WebhookRegistration]:
        """Unregister a webhook by ID."""
        with self._lock:
            registration = self._id_index.get(webhook_id)
            if not registration:
                return None
            
            # Remove from appropriate storage
            webhook_path = WebhookPath(registration.path)
            
            if webhook_path.is_dynamic:
                self._dynamic_webhooks = [
                    (path, reg) for path, reg in self._dynamic_webhooks
                    if reg.id != webhook_id
                ]
            else:
                key = (webhook_path.path, registration.method)
                self._static_webhooks.pop(key, None)
            
            # Remove from indexes
            self._id_index.pop(webhook_id, None)
            
            # Remove from workflow index
            if registration.workflow_id in self._workflow_index:
                self._workflow_index[registration.workflow_id] = [
                    r for r in self._workflow_index[registration.workflow_id]
                    if r.id != webhook_id
                ]
                
                # Clean up empty list
                if not self._workflow_index[registration.workflow_id]:
                    del self._workflow_index[registration.workflow_id]
            
            logger.info(
                f"Unregistered webhook {registration.method} {registration.path} "
                f"(ID: {webhook_id})"
            )
            
            return registration
    
    def find(self, path: str, method: WebhookMethod) -> Optional[WebhookRegistration]:
        """Find a webhook registration by path and method."""
        with self._lock:
            # Normalize path
            normalized_path = WebhookPath(path).path
            
            # Check static webhooks first (faster)
            key = (normalized_path, method)
            if key in self._static_webhooks:
                registration = self._static_webhooks[key]
                # Return a copy to avoid mutations
                return registration.model_copy()
            
            # Check dynamic webhooks
            for webhook_path, registration in self._dynamic_webhooks:
                if registration.method != method:
                    continue
                
                params = webhook_path.match(path)
                if params is not None:
                    # Return copy with path params
                    reg_copy = registration.model_copy()
                    reg_copy.path_params = params
                    return reg_copy
            
            return None
    
    def get_by_id(self, webhook_id: str) -> Optional[WebhookRegistration]:
        """Get a webhook registration by ID."""
        with self._lock:
            registration = self._id_index.get(webhook_id)
            return registration.model_copy() if registration else None
    
    def list_by_workflow(self, workflow_id: str) -> List[WebhookRegistration]:
        """List all webhooks for a workflow."""
        with self._lock:
            registrations = self._workflow_index.get(workflow_id, [])
            return [r.model_copy() for r in registrations]
    
    def list_all(self) -> List[WebhookRegistration]:
        """List all registered webhooks."""
        with self._lock:
            return [r.model_copy() for r in self._id_index.values()]
    
    def update(self, webhook_id: str, **updates) -> Optional[WebhookRegistration]:
        """Update a webhook registration."""
        with self._lock:
            registration = self._id_index.get(webhook_id)
            if not registration:
                return None
            
            # Create updated registration
            updated_data = registration.model_dump()
            updated_data.update(updates)
            updated_registration = WebhookRegistration.model_validate(updated_data)
            
            # Re-register (handles all index updates)
            self.unregister(webhook_id)
            self.register(updated_registration)
            
            return updated_registration
    
    def deactivate_by_workflow(self, workflow_id: str) -> int:
        """Deactivate all webhooks for a workflow."""
        with self._lock:
            count = 0
            for registration in self._workflow_index.get(workflow_id, []):
                if registration.is_active:
                    registration.is_active = False
                    count += 1
            
            logger.info(f"Deactivated {count} webhooks for workflow {workflow_id}")
            return count
    
    def activate_by_workflow(self, workflow_id: str) -> int:
        """Activate all webhooks for a workflow."""
        with self._lock:
            count = 0
            for registration in self._workflow_index.get(workflow_id, []):
                if not registration.is_active:
                    registration.is_active = True
                    count += 1
            
            logger.info(f"Activated {count} webhooks for workflow {workflow_id}")
            return count
    
    def _find_existing(
        self, path: str, method: WebhookMethod
    ) -> Optional[WebhookRegistration]:
        """Find existing webhook registration (internal method)."""
        webhook_path = WebhookPath(path)
        
        if not webhook_path.is_dynamic:
            key = (webhook_path.path, method)
            return self._static_webhooks.get(key)
        
        # For dynamic paths, check if exact same path exists
        for _, registration in self._dynamic_webhooks:
            if registration.path == path and registration.method == method:
                return registration
        
        return None
    
    def clear(self) -> None:
        """Clear all registrations (mainly for testing)."""
        with self._lock:
            self._static_webhooks.clear()
            self._dynamic_webhooks.clear()
            self._workflow_index.clear()
            self._id_index.clear()
            logger.info("Cleared all webhook registrations")