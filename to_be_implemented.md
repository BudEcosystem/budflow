# BudFlow Python - Features To Be Implemented

This document provides a comprehensive list of features, components, and improvements that need to be implemented in BudFlow Python to achieve full n8n feature parity and beyond.

## Important: Python-First Implementation

BudFlow Python is a **pure Python implementation** that does not depend on Node.js, TypeScript, or any JavaScript runtime. All features listed in this document must be implemented using Python libraries and frameworks. Where n8n uses Node.js-specific technologies (like Bull for queues), we use Python equivalents (like Celery). This ensures:

- **Native Python performance** without cross-runtime overhead
- **Simplified deployment** with standard Python tooling
- **Better integration** with Python data science and AI ecosystems
- **Consistent developer experience** for Python teams

## Table of Contents

1. [Critical Infrastructure](#critical-infrastructure)
2. [Queue System and Worker Management](#queue-system-and-worker-management)
3. [Task Runner and Code Isolation](#task-runner-and-code-isolation)
4. [Webhook System](#webhook-system)
5. [Binary Data Handling](#binary-data-handling)
6. [External Secrets Management](#external-secrets-management)
7. [Multi-Main Setup for High Availability](#multi-main-setup-for-high-availability)
8. [Advanced Execution Features](#advanced-execution-features)
9. [Node System Enhancements](#node-system-enhancements)
10. [Data Processing Features](#data-processing-features)
11. [Security Enhancements](#security-enhancements)
12. [Performance and Monitoring](#performance-and-monitoring)
13. [Integration Features](#integration-features)
14. [AI Features](#ai-features)
15. [Code TODOs and FIXMEs](#code-todos-and-fixmes)

## Critical Infrastructure

### 1. Queue System with Python/Redis ⚠️ **Priority: Critical**

**Current State**: Basic Redis connection exists, no queue implementation

**Required Implementation**:
- [ ] Python queue integration using Celery/RQ/Dramatiq for job management
- [ ] Job priority system (0-1000 scale like n8n)
- [ ] Delayed job scheduling
- [ ] Recurring job support with APScheduler/Celery Beat
- [ ] Job progress tracking and reporting
- [ ] Worker process management with multiprocessing/asyncio
- [ ] Queue recovery mechanisms
- [ ] Dead letter queue (DLQ) handling
- [ ] Batch job processing
- [ ] Queue health monitoring
- [ ] Graceful shutdown handling

**Technology Choices**:
- Celery with Redis backend for distributed task queue
- APScheduler for cron-like scheduling
- Flower for queue monitoring dashboard

**Implementation Path**: `budflow/queue/`

### 2. Task Runner for Isolated Code Execution ⚠️ **Priority: Critical**

**Current State**: No isolated execution environment

**Required Implementation**:
- [ ] Process isolation using Python multiprocessing/subprocess
- [ ] Resource limits enforcement:
  - [ ] CPU throttling with resource.setrlimit()
  - [ ] Memory limits using memory_profiler/tracemalloc
  - [ ] Execution time limits with signal/asyncio timeouts
  - [ ] Payload size restrictions (1GB default)
- [ ] Secure sandbox environment using:
  - [ ] RestrictedPython for safe code evaluation
  - [ ] Docker containers for complete isolation
  - [ ] PyPy sandbox mode for restricted execution
- [ ] Inter-process communication via:
  - [ ] multiprocessing.Queue/Pipe
  - [ ] ZeroMQ for high-performance IPC
  - [ ] Redis pub/sub for distributed communication
- [ ] Heartbeat monitoring with asyncio/threading
- [ ] Automatic restart using supervisor/systemd
- [ ] Environment variable control with os.environ filtering
- [ ] File system sandboxing using:
  - [ ] chroot/namespace isolation
  - [ ] Docker volume mounts
  - [ ] Temporary directories with cleanup

**Technology Choices**:
- Docker SDK for Python for container management
- RestrictedPython for code sandboxing
- psutil for resource monitoring

**Implementation Path**: `budflow/executor/task_runner.py`

### 3. Webhook System 🔴 **Priority: High**

**Current State**: Webhook nodes exist but no webhook service

**Required Implementation**:
- [ ] WebhookService for registration/deregistration
- [ ] Static webhook handling (fixed paths)
- [ ] Dynamic webhook handling with path parameters
- [ ] UUID extraction from dynamic paths
- [ ] Webhook caching layer with Redis
- [ ] Test webhook functionality
- [ ] Binary data reception
- [ ] Custom response formatting
- [ ] Webhook authentication support
- [ ] Rate limiting per webhook
- [ ] Webhook event replay capability

**Implementation Path**: `budflow/webhooks/`

**Code TODOs**:
- `workflows/service.py:667`: Register triggers with webhook services
- `workflows/service.py:679`: Unregister triggers from webhook services

### 4. Binary Data Handling 🔴 **Priority: High**

**Current State**: No binary data management

**Required Implementation**:
- [ ] BinaryDataService abstraction layer
- [ ] File system storage backend
- [ ] S3-compatible storage backend
- [ ] Stream processing for large files
- [ ] Automatic cleanup with TTL
- [ ] Signed URL generation for secure access
- [ ] Binary data metadata storage
- [ ] Compression support
- [ ] Encryption at rest
- [ ] Multi-part upload support
- [ ] Progress tracking for uploads/downloads

**Implementation Path**: `budflow/core/binary_data.py`

## Advanced Execution Features

### 5. Partial Execution v2 🔴 **Priority: High**

**Current State**: Basic execution only

**Required Implementation**:
- [ ] Directed graph representation of workflows
- [ ] Graph analysis algorithms:
  - [ ] Strongly connected components detection
  - [ ] Topological sorting
  - [ ] Cycle detection and handling
- [ ] Smart start node detection
- [ ] Dirty node tracking
- [ ] Pinned data support
- [ ] Execution path optimization
- [ ] Parallel branch detection
- [ ] Resource estimation

**Implementation Path**: `budflow/executor/partial_execution.py`

### 6. Execution Recovery Service ⚠️ **Priority: Critical**

**Current State**: No crash recovery

**Required Implementation**:
- [ ] Event log-based recovery system
- [ ] Execution state reconstruction
- [ ] Automatic resume from last successful node
- [ ] UI notifications for recovered executions
- [ ] Recovery modes (simple/extensive)
- [ ] Event sourcing for execution events
- [ ] Zero data loss objective
- [ ] Partial data reconstruction
- [ ] Binary data reference recovery

**Implementation Path**: `budflow/executor/recovery.py`

## Security Enhancements

### 7. External Secrets Management 🟡 **Priority: Medium**

**Current State**: Basic credential encryption only

**Required Implementation**:
- [ ] Provider abstraction interface using Python ABC
- [ ] Hashicorp Vault integration using hvac Python client
- [ ] AWS Secrets Manager integration using boto3
- [ ] Azure Key Vault integration using azure-keyvault-secrets
- [ ] Google Secret Manager integration using google-cloud-secret-manager
- [ ] Automatic secret rotation with APScheduler
- [ ] Provider health monitoring with asyncio
- [ ] Secret caching with TTL using cachetools
- [ ] Audit logging for secret access
- [ ] Secret versioning support

**Technology Choices**:
- hvac for Hashicorp Vault client
- boto3 for AWS integration
- azure-keyvault-secrets for Azure
- google-cloud-secret-manager for GCP

**Implementation Path**: `budflow/security/external_secrets.py`

**Code TODOs**:
- `credentials/service.py:391-402`: Implement actual credential testing

### 8. Task Isolation Security 🔴 **Priority: High**

**Current State**: No code isolation

**Required Implementation**:
- [ ] Secure sandbox environment
- [ ] Network access restrictions
- [ ] File system access limits
- [ ] Environment variable control
- [ ] System call filtering
- [ ] Resource usage monitoring
- [ ] Malicious code detection
- [ ] Audit trail for code execution

**Implementation Path**: `budflow/security/sandbox.py`

## Node System Enhancements

### 9. Additional Node Types 🔴 **Priority: High**

**From code comments in `nodes/actions.py:393-398`**:
- [ ] FileNode - Read/write files with streaming support
- [ ] SlackNode - Send Slack messages with attachments
- [ ] TelegramNode - Send Telegram messages
- [ ] S3Node - AWS S3 operations (upload/download/list)

**From code comments in `nodes/triggers.py:204-209`**:
- [ ] EmailTrigger - IMAP email monitoring
- [ ] FileTrigger - File system monitoring
- [ ] DatabaseTrigger - Database change detection
- [ ] MessageQueueTrigger - Queue message consumption

**From code comments in `nodes/control.py:297-301`**:
- [ ] SwitchNode - Multi-branch based on value
- [ ] TryCatchNode - Error handling with recovery
- [ ] ParallelNode - Parallel execution branches

**Additional n8n-compatible nodes needed**:
- [ ] GraphQL node
- [ ] Redis node
- [ ] MongoDB node
- [ ] PostgreSQL node
- [ ] MySQL node
- [ ] Kafka node
- [ ] RabbitMQ node
- [ ] Elasticsearch node
- [ ] Google Sheets node
- [ ] Airtable node
- [ ] Notion node
- [ ] Discord node
- [ ] GitHub node
- [ ] GitLab node
- [ ] Jira node

### 10. Node Features Enhancement

**Current State**: Basic node execution

**Required Implementation**:
- [ ] Node versioning system
- [ ] Deprecated node handling
- [ ] Node migration tools
- [ ] Custom node properties UI
- [ ] Node documentation generation
- [ ] Node testing framework
- [ ] Node marketplace integration

**Code TODOs**:
- `executor/runner.py:83`: Function node execution not implemented - needs RestrictedPython integration
- `executor/runner.py:112`: Multiplex mode for merge node not implemented

## Data Processing Features

### 11. Advanced Expression Engine 🔴 **Priority: High**

**Current State**: Basic expression evaluation

**Required Implementation**:
- [ ] Workflow data proxy for safe access using Python descriptors
- [ ] Built-in function library:
  - [ ] String manipulation using Python str methods
  - [ ] Array operations with list comprehensions
  - [ ] Object utilities using dataclasses/pydantic
  - [ ] Date/time functions with datetime/pendulum
  - [ ] Math operations using math/numpy
  - [ ] Crypto functions using cryptography/hashlib
- [ ] Custom function registration with decorators
- [ ] Type conversion using typing/pydantic
- [ ] Expression validation with AST parsing
- [ ] Error message improvements with rich tracebacks
- [ ] Expression result caching with functools.lru_cache
- [ ] Security sandboxing using:
  - [ ] RestrictedPython for safe evaluation
  - [ ] AST validation for code safety
  - [ ] Whitelist of allowed operations

**Technology Choices**:
- RestrictedPython for safe expression evaluation
- simpleeval as alternative expression evaluator
- ast module for expression parsing

**Implementation Path**: `budflow/nodes/expression_engine.py`

### 12. Data Transformation Pipeline 🟡 **Priority: Medium**

**Current State**: Basic data passing

**Required Implementation**:
- [ ] Item pairing/unpairing system
- [ ] Data deduplication
- [ ] Batch processing optimization
- [ ] Stream transformations
- [ ] Data validation framework
- [ ] Schema enforcement
- [ ] Data type conversion
- [ ] Custom transformation functions

## Performance and Monitoring

### 13. Execution Metrics & Insights 🟡 **Priority: Medium**

**Current State**: Basic execution tracking

**Required Implementation**:
- [ ] Detailed performance metrics collection
- [ ] Node-level execution timing
- [ ] Resource usage tracking (CPU, memory)
- [ ] Execution success/failure rates
- [ ] Time saved calculations
- [ ] Aggregated analytics
- [ ] Custom dashboards
- [ ] Export capabilities
- [ ] Alerting system

**Implementation Path**: `budflow/monitoring/`

### 14. Caching Layer 🔴 **Priority: High**

**Current State**: No caching

**Required Implementation**:
- [ ] Redis-based caching infrastructure using:
  - [ ] redis-py for direct Redis access
  - [ ] aiocache for async caching abstraction
  - [ ] django-cache-tree patterns for hierarchical caching
- [ ] Webhook cache management with TTL
- [ ] Credential cache with encryption using cryptography
- [ ] Expression result cache with functools.lru_cache
- [ ] Node output cache with size limits
- [ ] Workflow definition cache with versioning
- [ ] Cache invalidation strategies:
  - [ ] Tag-based invalidation
  - [ ] Time-based expiration
  - [ ] Event-driven invalidation
- [ ] Cache warming mechanisms:
  - [ ] Preload on startup
  - [ ] Background refresh
  - [ ] Predictive warming

**Technology Choices**:
- Redis with redis-py client
- aiocache for cache abstraction
- Memcached as alternative option

## Integration Features

### 15. OAuth2 Flow Implementation 🔴 **Priority: High**

**Current State**: Basic OAuth storage only

**Required Implementation**:
- [ ] Full OAuth2 authorization code flow
- [ ] PKCE support for public clients
- [ ] Token refresh automation
- [ ] Multiple provider configurations
- [ ] OAuth2 callback handling
- [ ] State parameter validation
- [ ] Token storage encryption
- [ ] Provider-specific implementations

### 16. LDAP/SAML Integration 🟡 **Priority: Medium**

**Current State**: JWT auth only

**Required Implementation**:
- [ ] LDAP authentication backend using:
  - [ ] python-ldap for LDAP protocol
  - [ ] ldap3 for pure Python implementation
- [ ] LDAP user synchronization with configurable mapping
- [ ] LDAP group mapping to RBAC roles
- [ ] SAML 2.0 SSO support using:
  - [ ] python3-saml for SAML implementation
  - [ ] pysaml2 as alternative
- [ ] IdP metadata handling with XML parsing
- [ ] Attribute mapping with configurable rules
- [ ] Session management with Redis backend
- [ ] Single logout support with SAML SLO

**Technology Choices**:
- ldap3 for LDAP integration
- python3-saml for SAML support
- cryptography for certificate handling

**Implementation Path**: `budflow/auth/enterprise/`

## High Availability Features

### 17. Multi-Main Setup ⚠️ **Priority: Critical for Production**

**Current State**: Single instance only

**Required Implementation**:
- [ ] Leader election using:
  - [ ] Redis with Redlock algorithm
  - [ ] etcd for distributed consensus
  - [ ] Consul for service discovery
- [ ] Instance health monitoring with:
  - [ ] Heartbeat checks using asyncio
  - [ ] Health endpoints for load balancers
  - [ ] Prometheus metrics exposure
- [ ] Webhook distribution using:
  - [ ] Consistent hashing for routing
  - [ ] Redis pub/sub for coordination
- [ ] Schedule distribution with:
  - [ ] Celery Beat for distributed scheduling
  - [ ] APScheduler with Redis backend
- [ ] License check coordination via shared cache
- [ ] Graceful failover mechanisms:
  - [ ] State replication with Redis
  - [ ] Session migration support
- [ ] Split-brain prevention using:
  - [ ] Quorum-based decisions
  - [ ] Network partition detection
- [ ] Shared state management with:
  - [ ] Redis for ephemeral state
  - [ ] PostgreSQL for persistent state
- [ ] Event-based coordination using:
  - [ ] Redis Streams for event sourcing
  - [ ] Kafka for high-throughput events

**Technology Choices**:
- Redis Sentinel for HA Redis
- Consul/etcd for service discovery
- HAProxy/Nginx for load balancing

**Implementation Path**: `budflow/core/multi_main.py`

## Workflow Features

### 18. Sub-workflow Execution 🔴 **Priority: High**

**Current State**: Not implemented

**Required Implementation**:
- [ ] Sub-workflow node type
- [ ] Context isolation between workflows
- [ ] Parameter passing mechanisms
- [ ] Error propagation
- [ ] Sub-workflow versioning
- [ ] Recursive execution limits
- [ ] Performance optimization
- [ ] Debugging support

### 19. Wait Node Implementation 🔴 **Priority: High**

**Current State**: Basic wait in control nodes

**Required Implementation**:
- [ ] Webhook-based resume
- [ ] Time-based waits with persistence
- [ ] Condition monitoring
- [ ] External event waits
- [ ] Wait state persistence
- [ ] Timeout handling
- [ ] Wait queue management

## AI Features

### 20. AI Integration SDK 🟡 **Priority: Medium**

**From README, not implemented**:
- [ ] Universal base class for AI agents using Python ABC
- [ ] LangChain integration with langchain-python
- [ ] OpenAI function calling support using openai-python
- [ ] Anthropic tool use integration using anthropic-python
- [ ] Custom AI provider support with pluggable interface
- [ ] Streaming response handling using:
  - [ ] Server-Sent Events (SSE) with FastAPI
  - [ ] WebSockets for bidirectional streaming
  - [ ] AsyncIO generators for response streaming
- [ ] Token usage tracking with tiktoken
- [ ] Cost estimation with provider-specific calculators

**Technology Choices**:
- langchain for LLM orchestration
- openai Python SDK
- anthropic Python SDK
- tiktoken for token counting
- httpx for async HTTP calls

**Implementation Path**: `budflow/ai/`

### 21. MCP (Model Context Protocol) Support 🟡 **Priority: Medium**

**From README, not implemented**:
- [ ] Native MCP server implementation using:
  - [ ] FastAPI for HTTP endpoints
  - [ ] WebSockets for real-time communication
  - [ ] JSON-RPC for protocol implementation
- [ ] MCP client support with httpx/aiohttp
- [ ] Protocol negotiation with version handling
- [ ] Context management using:
  - [ ] Pydantic for data validation
  - [ ] Context variables for state
- [ ] Tool registration with decorators
- [ ] Event handling with:
  - [ ] asyncio for event loops
  - [ ] Observer pattern for subscriptions

**Technology Choices**:
- FastAPI for server implementation
- httpx for async client
- Pydantic for protocol schemas

**Implementation Path**: `budflow/mcp/`

## Community Features

### 22. Python Package Management for Community Nodes 🟢 **Priority: Low**

**Current State**: No external node support

**Required Implementation**:
- [ ] Python package management system using pip/poetry
- [ ] PyPI-based package distribution
- [ ] Virtual environment isolation per package
- [ ] Package security validation:
  - [ ] Dependency scanning
  - [ ] Code signing verification
  - [ ] Sandboxed execution testing
- [ ] Python package structure:
  ```python
  # Example community node package structure
  budflow-node-custom/
  ├── pyproject.toml
  ├── src/
  │   └── budflow_node_custom/
  │       ├── __init__.py
  │       ├── nodes.py      # Node implementations
  │       ├── manifest.json # Node metadata
  │       └── tests/
  └── README.md
  ```
- [ ] Dynamic loading mechanism
- [ ] Version compatibility checks
- [ ] Automated testing on install
- [ ] Package marketplace integration
- [ ] Update notifications via PyPI
- [ ] Rollback capabilities with pip

**Implementation Path**: `budflow/core/package_manager.py`

## Code-Level TODOs

### Service Layer
- **executions/service.py:140**: Implement worker cancellation signal
- **executions/service.py:291**: Implement actual log retrieval
- **workflows/service.py:615**: Implement credential access validation
- **executor/engine.py:395**: Implement credential decryption

### CLI and Worker
- **cli.py:79**: Implement worker command
- **cli.py:133-138**: Replace placeholder status data with actual health checks
- **worker.py:19-23**: Create missing task modules

## Implementation Priority Matrix

### Phase 1 - Critical Infrastructure (Weeks 1-4)
1. Queue System with Celery/Redis
2. Task Runner with Docker/RestrictedPython
3. Webhook Service with FastAPI
4. Execution Recovery with event sourcing
5. Binary Data Handling with S3/filesystem

### Phase 2 - Core Features (Weeks 5-8)
1. Partial Execution v2
2. Advanced Expression Engine
3. OAuth2 flows
4. Sub-workflows
5. Additional core nodes

### Phase 3 - Enterprise Features (Weeks 9-12)
1. Multi-Main setup
2. External Secrets
3. LDAP/SAML integration
4. Advanced metrics
5. Audit system

### Phase 4 - AI and Community (Weeks 13-16)
1. AI Integration SDK
2. MCP support
3. Community nodes
4. Natural language workflows
5. Templates system

## Python Implementation Guidelines

### Technology Stack Requirements
- **Python 3.11+** for all implementations
- **No Node.js/TypeScript dependencies** - Python equivalents only
- **Async-first design** using asyncio/FastAPI
- **Type hints** required for all public APIs
- **Pydantic** for data validation and serialization

### Python Library Preferences
- **Queue System**: Celery, RQ, or Dramatiq (not Bull)
- **Task Scheduling**: APScheduler or Celery Beat (not node-cron)
- **WebSockets**: python-socketio or FastAPI WebSockets
- **HTTP Client**: httpx or aiohttp (not axios)
- **Testing**: pytest with pytest-asyncio
- **Documentation**: Sphinx with autodoc

### Code Quality Standards
- **Black** for code formatting
- **isort** for import sorting
- **mypy** for type checking
- **pylint/flake8** for linting
- **bandit** for security scanning

## Architecture and Design Pattern Compliance

All implementations must follow these architectural patterns from documentation.md:

### Hexagonal Architecture (Ports & Adapters)
- Domain logic separated from infrastructure
- Port interfaces for all external dependencies
- Adapter implementations for specific technologies
- Dependency injection for loose coupling

### Repository Pattern with Unit of Work
- Repository interfaces in domain layer
- SQLAlchemy implementations in infrastructure
- Unit of Work for transaction management
- Aggregate root pattern for consistency

### Command Query Responsibility Segregation (CQRS)
- Command handlers for write operations
- Query handlers for read operations
- Event bus for decoupling
- Optimized read models for queries

### Strategy Pattern for Execution
- ExecutionStrategy base class
- SequentialStrategy, ParallelStrategy implementations
- Pluggable execution modes
- Context-based strategy selection

### Factory Pattern for Node Creation
- NodeFactory for creating node instances
- Plugin architecture for custom nodes
- Type registration system
- Dependency injection for node services

## Testing Requirements

Each feature implementation must include:
1. Unit tests with >80% coverage
2. Integration tests
3. Performance benchmarks
4. Security validation
5. Documentation updates

## Success Metrics

- Feature parity with n8n core functionality
- Performance benchmarks matching or exceeding n8n
- Security audit compliance
- Scalability to 10,000+ concurrent executions
- 99.9% uptime capability with HA setup