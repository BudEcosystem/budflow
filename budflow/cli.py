"""Command line interface for BudFlow."""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from budflow.config import settings

app = typer.Typer(
    name="budflow",
    help="BudFlow - Enhanced n8n alternative with AI-first features",
    add_completion=False,
)

console = Console()


@app.command("version")
def version():
    """Show version information."""
    version_info = f"""
BudFlow v{settings.app_version}
Enhanced n8n alternative with AI-first features

Environment: {settings.environment}
Python: {sys.version}
"""
    console.print(
        Panel(
            version_info.strip(),
            title="Version Information",
            border_style="green",
        )
    )


@app.command("server")
def start_server(
    host: Optional[str] = typer.Option(None, "--host", "-h", help="Server host"),
    port: Optional[int] = typer.Option(None, "--port", "-p", help="Server port"),
    reload: bool = typer.Option(False, "--reload", "-r", help="Enable auto-reload"),
    workers: Optional[int] = typer.Option(None, "--workers", "-w", help="Number of workers"),
    debug: bool = typer.Option(False, "--debug", "-d", help="Enable debug mode"),
):
    """Start the BudFlow server."""
    from budflow.server import main as server_main
    
    # Override settings if provided
    if host:
        settings.host = host
    if port:
        settings.port = port
    if reload:
        settings.reload = reload
    if workers:
        settings.workers = workers
    if debug:
        settings.debug = debug
    
    server_main()


@app.command("worker")
def start_worker(
    concurrency: int = typer.Option(4, "--concurrency", "-c", help="Worker concurrency"),
    queues: str = typer.Option("default", "--queues", "-Q", help="Comma-separated queue names"),
    loglevel: str = typer.Option("info", "--loglevel", "-l", help="Log level"),
):
    """Start a Celery worker."""
    console.print("🔧 Starting BudFlow worker...")
    
    # This will be implemented when we create the Celery worker
    console.print("[red]Worker not implemented yet[/red]")
    sys.exit(1)


@app.command("init")
def init_project(
    path: str = typer.Argument(".", help="Project path"),
    force: bool = typer.Option(False, "--force", "-f", help="Force initialization"),
):
    """Initialize a new BudFlow project."""
    project_path = Path(path).resolve()
    
    if project_path.exists() and not force:
        console.print(f"[red]Directory {project_path} already exists. Use --force to overwrite.[/red]")
        sys.exit(1)
    
    console.print(f"🚀 Initializing BudFlow project at {project_path}")
    
    # Create project structure
    directories = [
        "workflows",
        "credentials", 
        "data",
        "logs",
        "uploads",
    ]
    
    for directory in directories:
        (project_path / directory).mkdir(parents=True, exist_ok=True)
        console.print(f"  Created directory: {directory}")
    
    # Create .env file
    env_file = project_path / ".env"
    if not env_file.exists() or force:
        env_content = """# BudFlow Configuration
DATABASE_URL=postgresql://user:password@localhost:5432/budflow
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=your-secret-key-change-in-production
ENCRYPTION_KEY=your-encryption-key-change-in-production
"""
        env_file.write_text(env_content)
        console.print("  Created .env file")
    
    console.print("[green]✅ Project initialized successfully![/green]")


@app.command("status")
def status():
    """Show system status."""
    table = Table(title="BudFlow Status")
    
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Details", style="dim")
    
    # This will be enhanced when we implement health checks
    table.add_row("Server", "Not Running", "Use 'budflow server' to start")
    table.add_row("Database", "Unknown", "Connection not tested")
    table.add_row("Redis", "Unknown", "Connection not tested")
    table.add_row("Worker", "Not Running", "Use 'budflow worker' to start")
    
    console.print(table)


@app.command("config")
def show_config():
    """Show current configuration."""
    config_table = Table(title="BudFlow Configuration")
    
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="green")
    
    # Show key configuration values
    config_items = [
        ("App Name", settings.app_name),
        ("Version", settings.app_version),
        ("Environment", settings.environment),
        ("Debug", str(settings.debug)),
        ("Host", settings.host),
        ("Port", str(settings.port)),
        ("Database URL", settings.database_url[:50] + "..." if len(settings.database_url) > 50 else settings.database_url),
        ("Redis URL", settings.redis_url),
        ("MFA Enabled", str(settings.mfa_enabled)),
        ("Metrics Enabled", str(settings.metrics_enabled)),
    ]
    
    for setting, value in config_items:
        config_table.add_row(setting, value)
    
    console.print(config_table)


def main():
    """Main CLI entry point."""
    try:
        app()
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()