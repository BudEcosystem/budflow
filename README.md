# BudFlow - Enhanced n8n Alternative

An AI-first workflow automation platform built in Python, designed as an enhanced alternative to n8n with advanced AI integration capabilities.

## Features

### Core Features
- 🔄 **Visual Workflow Builder** - Drag-and-drop interface for creating complex workflows
- 🚀 **High Performance** - Async Python architecture with FastAPI
- 🔐 **Enterprise Security** - JWT, MFA, RBAC, LDAP/SAML integration
- 📊 **Multi-Database Support** - PostgreSQL, MongoDB, Redis, Neo4j
- 🎯 **Advanced Execution** - Sequential, parallel, and partial execution modes
- 🔗 **Rich Integrations** - 300+ built-in nodes and easy custom node creation

### AI-First Enhancements
- 🤖 **AI Integration SDK** - Universal base class for AI agents to create integrations
- 🧠 **Semantic Discovery** - Vector-based search for workflows and integrations
- 🛠️ **Model Context Protocol** - Native MCP server/client support
- 📝 **Natural Language Workflows** - Generate workflows from text descriptions
- 🔍 **AI-Powered Testing** - Automated testing framework for integrations

### Enterprise Features
- 🏢 **Multi-Tenancy** - Organization/Department/Team/User hierarchy
- 📈 **Advanced Analytics** - Performance metrics and insights
- 🔒 **Compliance Ready** - SOC2, HIPAA, GDPR compliance
- 🌐 **High Availability** - Active-active multi-region deployment
- 🔄 **Crash Recovery** - Event-sourced state reconstruction

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 13+
- Redis 6+
- Poetry (for dependency management)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd budflow_python

# Install dependencies
poetry install

# Install development dependencies
poetry install --with dev

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Run database migrations
poetry run alembic upgrade head

# Start the development server
poetry run budflow-server
```

### Development

```bash
# Install pre-commit hooks
poetry run pre-commit install

# Run tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=budflow

# Format code
poetry run black .
poetry run isort .

# Type checking
poetry run mypy budflow/

# Security scan
poetry run bandit -r budflow/
```

## Architecture

BudFlow is built with a modular architecture:

- **API Layer** - FastAPI-based REST API with async support
- **Core Engine** - Workflow execution engine with advanced scheduling
- **Node System** - Extensible node architecture for integrations
- **AI Layer** - LangChain integration and semantic discovery
- **Security Layer** - Multi-layered security with encryption and audit
- **Storage Layer** - Multi-database architecture for different data types

## Documentation

- [API Documentation](docs/api.md)
- [Developer Guide](docs/development.md)
- [AI Integration SDK](docs/ai-sdk.md)
- [Deployment Guide](docs/deployment.md)
- [Security Guide](docs/security.md)

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`poetry run pytest`)
5. Format code (`poetry run black . && poetry run isort .`)
6. Commit your changes (`git commit -m 'Add some amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the Fair-code License - see the [LICENSE](LICENSE) file for details.

## Support

- 📧 Email: support@accubits.com
- 💬 Discord: [Join our community](https://discord.gg/budflow)
- 📖 Documentation: [docs.budflow.ai](https://docs.budflow.ai)
- 🐛 Issues: [GitHub Issues](https://github.com/accubits/budflow/issues)