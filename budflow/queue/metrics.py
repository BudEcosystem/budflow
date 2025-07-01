"""Queue metrics collection and export."""

import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

try:
    from prometheus_client import Gauge, Counter, Histogram, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("prometheus_client not installed, metrics export disabled")


from .models import QueueMetrics


class MetricsCollector:
    """Collector for queue metrics."""
    
    def __init__(self, namespace: str = "budflow", subsystem: str = "queue"):
        """
        Initialize metrics collector.
        
        Args:
            namespace: Prometheus namespace
            subsystem: Prometheus subsystem
        """
        self.namespace = namespace
        self.subsystem = subsystem
        self._metrics_history: List[QueueMetrics] = []
        self._max_history = 100
        
        if PROMETHEUS_AVAILABLE:
            # Define Prometheus metrics
            self.active_jobs_gauge = Gauge(
                "active_jobs",
                "Number of currently active jobs",
                namespace=namespace,
                subsystem=subsystem
            )
            
            self.waiting_jobs_gauge = Gauge(
                "waiting_jobs",
                "Number of jobs waiting to be processed",
                namespace=namespace,
                subsystem=subsystem
            )
            
            self.completed_jobs_counter = Counter(
                "completed_jobs_total",
                "Total number of completed jobs",
                namespace=namespace,
                subsystem=subsystem
            )
            
            self.failed_jobs_counter = Counter(
                "failed_jobs_total",
                "Total number of failed jobs",
                namespace=namespace,
                subsystem=subsystem
            )
            
            self.job_duration_histogram = Histogram(
                "job_duration_seconds",
                "Job execution duration in seconds",
                namespace=namespace,
                subsystem=subsystem,
                buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)
            )
            
            self.stalled_jobs_counter = Counter(
                "stalled_jobs_total",
                "Total number of stalled jobs",
                namespace=namespace,
                subsystem=subsystem
            )
            
            self.queue_size_gauge = Gauge(
                "queue_size",
                "Total size of the queue (active + waiting)",
                namespace=namespace,
                subsystem=subsystem
            )
    
    def record_metrics(self, metrics: QueueMetrics) -> None:
        """Record queue metrics."""
        # Store in history
        self._metrics_history.append(metrics)
        if len(self._metrics_history) > self._max_history:
            self._metrics_history.pop(0)
        
        if PROMETHEUS_AVAILABLE:
            # Update Prometheus metrics
            self.active_jobs_gauge.set(metrics.active_count)
            self.waiting_jobs_gauge.set(metrics.waiting_count)
            self.queue_size_gauge.set(metrics.active_count + metrics.waiting_count)
            
            # Note: Counters should only increment, so we need to track deltas
            # In a real implementation, we'd track the previous values
    
    def record_job_completed(self, duration_seconds: float) -> None:
        """Record a completed job."""
        if PROMETHEUS_AVAILABLE:
            self.completed_jobs_counter.inc()
            self.job_duration_histogram.observe(duration_seconds)
    
    def record_job_failed(self) -> None:
        """Record a failed job."""
        if PROMETHEUS_AVAILABLE:
            self.failed_jobs_counter.inc()
    
    def record_job_stalled(self) -> None:
        """Record a stalled job."""
        if PROMETHEUS_AVAILABLE:
            self.stalled_jobs_counter.inc()
    
    def get_prometheus_metrics(self) -> bytes:
        """Get metrics in Prometheus format."""
        if PROMETHEUS_AVAILABLE:
            return generate_latest()
        else:
            # Return custom format if Prometheus not available
            if self._metrics_history:
                latest = self._metrics_history[-1]
                return latest.to_prometheus_format().encode()
            return b""
    
    def calculate_statistics(self) -> Dict[str, Any]:
        """Calculate statistics from metrics history."""
        if not self._metrics_history:
            return {}
        
        # Calculate averages
        total_metrics = len(self._metrics_history)
        avg_active = sum(m.active_count for m in self._metrics_history) / total_metrics
        avg_waiting = sum(m.waiting_count for m in self._metrics_history) / total_metrics
        
        # Calculate processing times if available
        processing_times = [
            m.avg_processing_time_ms 
            for m in self._metrics_history 
            if m.avg_processing_time_ms is not None
        ]
        
        stats = {
            "samples": total_metrics,
            "average_active_jobs": round(avg_active, 2),
            "average_waiting_jobs": round(avg_waiting, 2),
            "max_active_jobs": max(m.active_count for m in self._metrics_history),
            "max_waiting_jobs": max(m.waiting_count for m in self._metrics_history),
        }
        
        if processing_times:
            stats.update({
                "average_processing_time_ms": round(
                    sum(processing_times) / len(processing_times), 2
                ),
                "max_processing_time_ms": max(
                    m.max_processing_time_ms 
                    for m in self._metrics_history 
                    if m.max_processing_time_ms is not None
                ),
                "min_processing_time_ms": min(
                    m.min_processing_time_ms 
                    for m in self._metrics_history 
                    if m.min_processing_time_ms is not None
                ),
            })
        
        return stats
    
    def get_time_series_data(
        self,
        metric: str = "active_count",
        points: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get time series data for a specific metric.
        
        Args:
            metric: Metric name to extract
            points: Number of data points to return
            
        Returns:
            List of {timestamp, value} dictionaries
        """
        if not self._metrics_history:
            return []
        
        # Get last N points
        history = self._metrics_history[-points:]
        
        return [
            {
                "timestamp": m.timestamp.isoformat(),
                "value": getattr(m, metric, 0)
            }
            for m in history
        ]


class MetricsExporter:
    """Export metrics to various backends."""
    
    def __init__(self, collector: MetricsCollector):
        self.collector = collector
    
    async def export_to_prometheus(self, port: int = 9090) -> None:
        """Start Prometheus metrics endpoint."""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Prometheus client not available")
            return
        
        from aiohttp import web
        
        async def metrics_handler(request):
            """Handle metrics requests."""
            metrics = self.collector.get_prometheus_metrics()
            return web.Response(
                body=metrics,
                content_type="text/plain; version=0.0.4"
            )
        
        app = web.Application()
        app.router.add_get("/metrics", metrics_handler)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        
        logger.info(f"Prometheus metrics available at http://0.0.0.0:{port}/metrics")
    
    async def export_to_statsd(
        self,
        host: str = "localhost",
        port: int = 8125
    ) -> None:
        """Export metrics to StatsD."""
        # This would implement StatsD protocol
        # For now, just log
        logger.info(f"StatsD export to {host}:{port} not implemented")
    
    async def export_to_influxdb(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str
    ) -> None:
        """Export metrics to InfluxDB."""
        # This would use influxdb-client
        # For now, just log
        logger.info(f"InfluxDB export to {url} not implemented")


async def create_metrics_server(
    config: dict,
    collector: MetricsCollector
) -> Optional[MetricsExporter]:
    """
    Create and start metrics exporter based on configuration.
    
    Args:
        config: Metrics configuration
        collector: Metrics collector instance
        
    Returns:
        MetricsExporter instance if started
    """
    exporter = MetricsExporter(collector)
    
    if config.get("prometheus_enabled"):
        port = config.get("prometheus_port", 9090)
        await exporter.export_to_prometheus(port)
        return exporter
    
    return None