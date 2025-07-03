#!/usr/bin/env python
"""Demo script showcasing BudFlow workflow composition (DAG) capabilities."""

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

def main():
    """Run the workflow composition demo."""
    print("\n🚀 BudFlow Workflow Composition Demo")
    print("=" * 60)
    print("Demonstrating hierarchical workflow composition with DAG support")
    print("=" * 60)
    
    # Check nodes API to confirm SubWorkflow node is available
    print_header("1. Available Workflow Composition Node")
    
    response = requests.get(f"{BASE_URL}/api/v1/nodes/subworkflow")
    if response.status_code == 200:
        node_def = response.json()
        print(f"✅ Node Type: {node_def['name']}")
        print(f"📝 Description: {node_def['description']}")
        print(f"🎨 Category: {node_def['category']}")
        print(f"\n📋 Parameters:")
        for param in node_def['parameters']:
            required = "Required" if param['required'] else "Optional"
            print(f"  • {param['display_name']} ({param['type']}) - {required}")
            print(f"    {param['description']}")
    
    # Example workflow structures
    print_header("2. Example Workflow Composition Structure")
    
    # Child workflow (Level 2)
    child_workflow = {
        "name": "Data Processing Sub-Workflow",
        "description": "Process and transform data",
        "nodes": [
            {
                "id": "start",
                "name": "Receive Data",
                "type": "manual",
                "parameters": {}
            },
            {
                "id": "transform",
                "name": "Transform Data",
                "type": "function",
                "parameters": {
                    "code": "return [{'processed': True, 'timestamp': new Date().toISOString(), 'data': item} for item in items]"
                }
            },
            {
                "id": "validate",
                "name": "Validate Results",
                "type": "function",
                "parameters": {
                    "code": "return items.filter(item => item.processed === true)"
                }
            }
        ],
        "connections": [
            {"source": "start", "target": "transform"},
            {"source": "transform", "target": "validate"}
        ]
    }
    
    # Parent workflow (Level 1)
    parent_workflow = {
        "name": "Main Processing Workflow",
        "description": "Main workflow that orchestrates sub-workflows",
        "nodes": [
            {
                "id": "trigger",
                "name": "Start Process",
                "type": "manual",
                "parameters": {}
            },
            {
                "id": "prepare",
                "name": "Prepare Data",
                "type": "function",
                "parameters": {
                    "code": "return [{'id': i, 'value': i * 10} for i in range(5)]"
                }
            },
            {
                "id": "subprocess",
                "name": "Execute Sub-Workflow",
                "type": "subworkflow",
                "parameters": {
                    "workflowId": "{{CHILD_WORKFLOW_ID}}",  # Will be replaced with actual ID
                    "inputMapping": {
                        "items": "$.data"
                    },
                    "outputMapping": {
                        "processedResults": "$.results"
                    },
                    "waitForCompletion": True,
                    "maxDepth": 5
                }
            },
            {
                "id": "finalize",
                "name": "Finalize Results",
                "type": "function",
                "parameters": {
                    "code": "return [{'summary': 'Processing complete', 'itemCount': len(items)}]"
                }
            }
        ],
        "connections": [
            {"source": "trigger", "target": "prepare"},
            {"source": "prepare", "target": "subprocess"},
            {"source": "subprocess", "target": "finalize"}
        ]
    }
    
    print("📊 Workflow Hierarchy:")
    print("\n  Main Processing Workflow (Level 1)")
    print("  ├── Start Process")
    print("  ├── Prepare Data")
    print("  ├── Execute Sub-Workflow ──┐")
    print("  │                         │")
    print("  │   Data Processing Sub-Workflow (Level 2)")
    print("  │   ├── Receive Data")
    print("  │   ├── Transform Data")
    print("  │   └── Validate Results")
    print("  │                         │")
    print("  └── Finalize Results ◄────┘")
    
    # Workflow composition features
    print_header("3. Workflow Composition Features")
    
    features = [
        {
            "feature": "Hierarchical Composition",
            "description": "Workflows can include other workflows as sub-components"
        },
        {
            "feature": "Input/Output Mapping",
            "description": "JSONPath-based data mapping between parent and child workflows"
        },
        {
            "feature": "Circular Dependency Detection",
            "description": "Automatic detection and prevention of circular workflow references"
        },
        {
            "feature": "Depth Limiting",
            "description": "Configurable maximum nesting depth to prevent infinite recursion"
        },
        {
            "feature": "Error Handling",
            "description": "Choose to fail or continue when sub-workflows encounter errors"
        },
        {
            "feature": "Async Execution",
            "description": "Option to execute sub-workflows asynchronously without waiting"
        },
        {
            "feature": "Execution Tracking",
            "description": "Parent-child execution relationships are tracked in the database"
        }
    ]
    
    for feature in features:
        print(f"\n🔧 {feature['feature']}")
        print(f"   {feature['description']}")
    
    # API endpoints
    print_header("4. Workflow Composition API Endpoints")
    
    endpoints = [
        {
            "method": "POST",
            "path": "/api/v1/workflows/{workflow_id}/validate-composition",
            "description": "Validate workflow for circular dependencies"
        },
        {
            "method": "GET",
            "path": "/api/v1/workflows/{workflow_id}/dependencies",
            "description": "Get workflows this workflow depends on"
        },
        {
            "method": "GET",
            "path": "/api/v1/workflows/{workflow_id}/dependents",
            "description": "Get workflows that depend on this workflow"
        },
        {
            "method": "GET",
            "path": "/api/v1/workflows/{workflow_id}/composition-graph",
            "description": "Get the complete workflow composition graph"
        },
        {
            "method": "GET",
            "path": "/api/v1/workflows/{workflow_id}/composition-analysis",
            "description": "Analyze composition for optimization opportunities"
        },
        {
            "method": "POST",
            "path": "/api/v1/workflows/validate-compositions",
            "description": "Bulk validate multiple workflow compositions"
        }
    ]
    
    for endpoint in endpoints:
        print(f"\n{endpoint['method']} {endpoint['path']}")
        print(f"   {endpoint['description']}")
    
    # Use cases
    print_header("5. Common Use Cases")
    
    use_cases = [
        {
            "title": "Modular ETL Pipelines",
            "description": "Break complex ETL processes into reusable sub-workflows for extraction, transformation, and loading"
        },
        {
            "title": "Multi-Stage Approval Processes",
            "description": "Create hierarchical approval workflows with different sub-workflows for each approval level"
        },
        {
            "title": "Microservice Orchestration",
            "description": "Compose workflows that coordinate multiple microservice calls in a specific order"
        },
        {
            "title": "Reusable Business Logic",
            "description": "Package common business processes as workflows that can be included in multiple parent workflows"
        },
        {
            "title": "Complex Data Processing",
            "description": "Build multi-level data processing pipelines with specialized sub-workflows for each stage"
        }
    ]
    
    for use_case in use_cases:
        print(f"\n📌 {use_case['title']}")
        print(f"   {use_case['description']}")
    
    # Graph optimization
    print_header("6. Graph Optimization Capabilities")
    
    print("BudFlow analyzes workflow compositions to identify:")
    print("• Critical path through the workflow hierarchy")
    print("• Parallelization opportunities for sub-workflows")
    print("• Redundant or inefficient workflow structures")
    print("• Maximum execution depth and complexity metrics")
    
    print("\n🔍 Example Analysis Output:")
    print(json.dumps({
        "workflow_id": "main-workflow-123",
        "total_nodes": 15,
        "total_workflows": 3,
        "max_depth": 2,
        "critical_path": ["trigger", "prepare", "subprocess", "finalize"],
        "parallelization_opportunities": 2,
        "parallel_groups": [
            ["subprocess-1", "subprocess-2"],
            ["validation-1", "validation-2", "validation-3"]
        ]
    }, indent=2))
    
    print_header("7. Getting Started")
    
    print("To use workflow composition in BudFlow:")
    print("\n1. Create your child workflows first")
    print("2. Add a 'subworkflow' node to your parent workflow")
    print("3. Configure the workflowId parameter with the child workflow ID")
    print("4. Set up input/output mappings using JSONPath expressions")
    print("5. Validate the composition to check for circular dependencies")
    print("6. Execute the parent workflow - child workflows run automatically")
    
    print("\n✨ Workflow composition enables building complex, maintainable automation systems!")
    print("🚀 Start composing your workflows today!\n")

if __name__ == "__main__":
    main()