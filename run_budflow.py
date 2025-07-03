#!/usr/bin/env python
"""Run BudFlow Python application with simplified configuration."""

import os
import sys
import uvicorn
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Set minimal environment variables for testing
os.environ.update({
    "SECRET_KEY": "dev-secret-key",
    "ENCRYPTION_KEY": "kN6VmFW0DGJlbXLqQoNvX7RfPjBzQKL/Lf4hYlGzH/s=",
    "DATABASE_URL": "sqlite+aiosqlite:///./budflow.db",
    "REDIS_URL": "redis://localhost:6379",
    "MONGODB_URL": "mongodb://localhost:27017",
    "NEO4J_URI": "bolt://localhost:7687",
    "NEO4J_USERNAME": "neo4j",
    "NEO4J_PASSWORD": "password",
    "CELERY_BROKER_URL": "redis://localhost:6379",
    "CELERY_RESULT_BACKEND": "redis://localhost:6379",
    "STORAGE_TYPE": "filesystem",
    "STORAGE_PATH": "./data/storage",
    "MFA_ENABLED": "false",
    "RATE_LIMIT_ENABLED": "false",
    "METRICS_ENABLED": "false",
    "MCP_ENABLED": "false"
})

# Import app after environment setup
from budflow.main import app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    print("🚀 Starting BudFlow Python...")
    print(f"📍 API Documentation: http://localhost:{port}/docs")
    print(f"📍 Alternative Docs: http://localhost:{port}/redoc")
    print(f"📍 Health Check: http://localhost:{port}/health")
    print("\nPress CTRL+C to stop the server\n")
    
    # Run with minimal configuration
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=True
    )