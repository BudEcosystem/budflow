#!/usr/bin/env python
"""Demo script showcasing BudFlow Python capabilities."""

import requests
import json
from datetime import datetime

# Base URL
BASE_URL = "http://localhost:8080"

def print_header(title):
    """Print a formatted header."""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}\n")

def check_health():
    """Check system health."""
    print_header("System Health Check")
    
    response = requests.get(f"{BASE_URL}/health")
    if response.status_code == 200:
        health = response.json()
        print(f"✅ Status: {health['status']}")
        print(f"📦 Service: {health['service']} v{health['version']}")
        print(f"🌍 Environment: {health['environment']}")
        print(f"\n📊 Database Status:")
        for db, info in health['databases'].items():
            status = "✅" if info['status'] == "healthy" else "❌"
            print(f"  {status} {db}: {info['status']}")
    else:
        print(f"❌ Health check failed: {response.status_code}")

def show_api_docs():
    """Show API documentation URLs."""
    print_header("API Documentation")
    
    print("📚 Interactive API Documentation:")
    print(f"  - Swagger UI: {BASE_URL}/docs")
    print(f"  - ReDoc: {BASE_URL}/redoc")
    print(f"  - OpenAPI JSON: {BASE_URL}/openapi.json")

def list_available_endpoints():
    """List available API endpoints."""
    print_header("Available API Endpoints")
    
    response = requests.get(f"{BASE_URL}/openapi.json")
    if response.status_code == 200:
        openapi = response.json()
        paths = openapi.get('paths', {})
        
        # Group endpoints by category
        categories = {}
        for path, methods in paths.items():
            if path.startswith('/api/v1/'):
                category = path.split('/')[3]  # Extract category from path
                if category not in categories:
                    categories[category] = []
                categories[category].append(path)
        
        # Display grouped endpoints
        for category, endpoints in sorted(categories.items()):
            print(f"\n📁 {category.upper()}")
            for endpoint in sorted(endpoints)[:5]:  # Show first 5 endpoints
                print(f"  - {endpoint}")
            if len(endpoints) > 5:
                print(f"  ... and {len(endpoints) - 5} more")

def show_features():
    """Show BudFlow features."""
    print_header("BudFlow Python Features")
    
    features = {
        "🔄 Workflow Automation": [
            "Visual workflow builder",
            "Node-based execution",
            "Conditional logic & loops",
            "Error handling & retries"
        ],
        "🔌 Integrations": [
            "HTTP/REST API calls",
            "Email sending (SMTP)",
            "Database operations (PostgreSQL, MySQL, MongoDB)",
            "File system operations",
            "Webhook support"
        ],
        "🤖 AI Integration": [
            "LLM integration (OpenAI, Anthropic, Google)",
            "Expression evaluation",
            "Template processing",
            "Intelligent workflow suggestions"
        ],
        "🔐 Security": [
            "JWT authentication",
            "Multi-factor authentication (MFA)",
            "Role-based access control (RBAC)",
            "Credential encryption",
            "OAuth2 support"
        ],
        "⚡ Performance": [
            "Async execution engine",
            "Distributed task processing (Celery)",
            "Binary data handling with S3",
            "Caching & optimization"
        ],
        "🚀 Enterprise Features": [
            "Multi-main high availability",
            "External secrets management",
            "Workflow versioning",
            "Audit logging",
            "Real-time collaboration"
        ]
    }
    
    for category, items in features.items():
        print(f"\n{category}")
        for item in items:
            print(f"  • {item}")

def show_node_types():
    """Show available node types."""
    print_header("Available Node Types")
    
    node_types = {
        "Triggers": [
            "Manual Trigger - Start workflow manually",
            "Webhook Trigger - Start via HTTP webhook",
            "Schedule Trigger - Start on schedule (cron)"
        ],
        "Actions": [
            "HTTP Request - Make API calls",
            "Send Email - Send emails via SMTP",
            "Database Query - Execute SQL queries",
            "File Operations - Read/write files",
            "Execute Code - Run Python/JavaScript"
        ],
        "Control Flow": [
            "If - Conditional branching",
            "Loop - Iterate over items",
            "Wait - Delay execution",
            "Stop - Stop workflow execution"
        ],
        "Data Processing": [
            "Set - Set workflow variables",
            "Function - Transform data",
            "Merge - Combine data streams",
            "Split - Split data into multiple streams"
        ]
    }
    
    for category, nodes in node_types.items():
        print(f"\n🔧 {category}:")
        for node in nodes:
            print(f"  • {node}")

def show_example_workflow():
    """Show an example workflow structure."""
    print_header("Example Workflow: Daily Report Automation")
    
    workflow_example = {
        "name": "Daily Sales Report",
        "description": "Fetch sales data, process it, and send email report",
        "nodes": [
            {
                "type": "Schedule Trigger",
                "config": "Daily at 9:00 AM"
            },
            {
                "type": "Database Query",
                "config": "SELECT * FROM sales WHERE date = TODAY"
            },
            {
                "type": "Function",
                "config": "Process and aggregate sales data"
            },
            {
                "type": "HTTP Request",
                "config": "Generate chart via Chart API"
            },
            {
                "type": "Send Email",
                "config": "Send report to management@company.com"
            }
        ]
    }
    
    print("📋 Workflow:", workflow_example["name"])
    print("📝 Description:", workflow_example["description"])
    print("\n🔄 Workflow Steps:")
    for i, node in enumerate(workflow_example["nodes"], 1):
        print(f"  {i}. {node['type']}")
        print(f"     └─ {node['config']}")

def main():
    """Run the demo."""
    print("\n")
    print("🚀 Welcome to BudFlow Python Demo")
    print("=" * 60)
    print("A powerful workflow automation platform with AI integration")
    print("=" * 60)
    
    # Run demo sections
    check_health()
    show_api_docs()
    list_available_endpoints()
    show_features()
    show_node_types()
    show_example_workflow()
    
    print_header("Getting Started")
    print("1. Visit the API documentation to explore endpoints")
    print("2. Create workflows using the REST API")
    print("3. Execute workflows manually or via triggers")
    print("4. Monitor execution status and results")
    print("\n📚 Full documentation: https://github.com/yourusername/budflow-python")
    print("💬 Support: support@budflow.com")
    print("\n✨ Happy automating with BudFlow Python! ✨\n")

if __name__ == "__main__":
    main()