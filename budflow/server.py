"""Server entry point."""

import asyncio
import signal
import sys
from typing import Any, Dict

import structlog
import uvicorn
from rich.console import Console
from rich.panel import Panel

from budflow.config import settings
from budflow.main import app

logger = structlog.get_logger()
console = Console()


def setup_logging() -> None:
    """Setup structured logging."""
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if settings.is_production else structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def create_uvicorn_config() -> Dict[str, Any]:
    """Create Uvicorn configuration."""
    return {
        "app": "budflow.main:app",
        "host": settings.host,
        "port": settings.port,
        "reload": settings.reload and settings.is_development,
        "workers": 1 if settings.reload else settings.workers,
        "log_level": settings.log_level.lower(),
        "access_log": settings.is_development,
        "use_colors": settings.is_development,
        "server_header": False,
        "date_header": False,
    }


def display_startup_info() -> None:
    """Display startup information."""
    startup_info = f"""
🚀 BudFlow Server Starting

Environment: {settings.environment}
Host: {settings.host}
Port: {settings.port}
Debug: {settings.debug}
Workers: {settings.workers}
Reload: {settings.reload and settings.is_development}

🌐 Endpoints:
• API: http://{settings.host}:{settings.port}
• Health: http://{settings.host}:{settings.port}/health
• Metrics: http://{settings.host}:{settings.port}/metrics
"""
    
    if settings.is_development:
        startup_info += f"• Docs: http://{settings.host}:{settings.port}/docs\n"
        startup_info += f"• ReDoc: http://{settings.host}:{settings.port}/redoc\n"

    console.print(
        Panel(
            startup_info.strip(),
            title="BudFlow - Enhanced n8n Alternative",
            border_style="blue",
        )
    )


async def shutdown_handler() -> None:
    """Handle graceful shutdown."""
    logger.info("Received shutdown signal, shutting down gracefully...")
    
    # Give some time for ongoing requests to complete
    await asyncio.sleep(1)
    
    logger.info("Shutdown complete")
    sys.exit(0)


def setup_signal_handlers() -> None:
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum: int, frame: Any) -> None:
        logger.info(f"Received signal {signum}")
        asyncio.create_task(shutdown_handler())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main() -> None:
    """Main server entry point."""
    setup_logging()
    setup_signal_handlers()
    display_startup_info()
    
    try:
        uvicorn_config = create_uvicorn_config()
        uvicorn.run(**uvicorn_config)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error("Server error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()