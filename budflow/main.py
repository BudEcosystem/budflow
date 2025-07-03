"""Main FastAPI application."""

import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest
from starlette.middleware.gzip import GZipMiddleware

from budflow.config import settings
from budflow.database import db_manager

# Metrics
REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"]
)
REQUEST_DURATION = Histogram(
    "http_request_duration_seconds", "HTTP request duration", ["method", "endpoint"]
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager."""
    logger.info("Starting BudFlow application", version=settings.app_version)
    
    # Startup
    try:
        # Initialize database connections
        await db_manager.initialize()
        logger.info("Application startup completed")
        yield
    finally:
        # Cleanup
        logger.info("Shutting down BudFlow application")
        await db_manager.close()


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title=settings.app_name,
        description="Enhanced n8n alternative with AI-first features",
        version=settings.app_version,
        debug=settings.debug,
        lifespan=lifespan,
        docs_url="/docs" if settings.is_development else None,
        redoc_url="/redoc" if settings.is_development else None,
        openapi_url="/openapi.json" if settings.is_development else None,
    )

    # Configure CORS
    if settings.cors_enabled:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=settings.cors_credentials,
            allow_methods=settings.cors_methods,
            allow_headers=settings.cors_headers,
        )

    # Security middleware
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*"] if settings.is_development else [settings.host],
    )

    # Compression middleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # Request logging and metrics middleware
    @app.middleware("http")
    async def logging_middleware(request: Request, call_next) -> Response:
        start_time = time.time()
        
        # Log request
        logger.info(
            "Request started",
            method=request.method,
            url=str(request.url),
            client_host=request.client.host if request.client else None,
        )

        # Process request
        response = await call_next(request)
        
        # Calculate duration
        duration = time.time() - start_time
        
        # Update metrics
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code,
        ).inc()
        
        REQUEST_DURATION.labels(
            method=request.method,
            endpoint=request.url.path,
        ).observe(duration)

        # Log response
        logger.info(
            "Request completed",
            method=request.method,
            url=str(request.url),
            status_code=response.status_code,
            duration=duration,
        )

        return response

    # Health check endpoint
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        # Perform database health checks
        db_health = await db_manager.health_check()
        
        # Determine overall health status (ignore disabled services)
        overall_healthy = all(
            db["status"] in ("healthy", "disabled") for db in db_health.values()
        )
        
        return {
            "status": "healthy" if overall_healthy else "degraded",
            "service": settings.app_name,
            "version": settings.app_version,
            "environment": settings.environment,
            "timestamp": time.time(),
            "databases": db_health,
        }

    # Metrics endpoint
    if settings.metrics_enabled:
        @app.get("/metrics")
        async def metrics():
            """Prometheus metrics endpoint."""
            return Response(
                generate_latest(),
                media_type="text/plain",
            )

    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        """Global exception handler."""
        logger.error(
            "Unhandled exception",
            error=str(exc),
            error_type=type(exc).__name__,
            url=str(request.url),
            method=request.method,
            exc_info=True,
        )
        
        if settings.is_development:
            # In development, include full error details
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal Server Error",
                    "detail": str(exc),
                    "type": type(exc).__name__,
                },
            )
        else:
            # In production, return generic error
            return JSONResponse(
                status_code=500,
                content={"error": "Internal Server Error"},
            )

    # Include routers
    from budflow.auth.routes import router as auth_router
    from budflow.workflows.routes import router as workflows_router
    from budflow.executions.routes import router as executions_router
    from budflow.credentials.routes import router as credentials_router
    from budflow.nodes.routes import router as nodes_router
    
    app.include_router(auth_router, prefix="/api/v1", tags=["Authentication"])
    app.include_router(workflows_router, prefix="/api/v1", tags=["Workflows"])
    app.include_router(executions_router, prefix="/api/v1", tags=["Executions"])
    app.include_router(credentials_router, prefix="/api/v1", tags=["Credentials"])
    app.include_router(nodes_router, tags=["Nodes"])
    
    # Additional routers (to be added)
    # app.include_router(integrations_router, prefix="/api/v1/integrations", tags=["Integrations"])
    # app.include_router(webhooks_router, prefix="/api/v1/webhooks", tags=["Webhooks"])

    return app


# Create the app instance
app = create_app()