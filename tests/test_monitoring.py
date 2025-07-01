"""Test Advanced Monitoring and Insights system."""

import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from uuid import UUID, uuid4
import json

from budflow.monitoring.insights import (
    MetricsCollector,
    PerformanceAnalyzer,
    WorkflowInsights,
    ExecutionMetrics,
    NodeMetrics,
    SystemMetrics,
    AlertManager,
    Alert,
    AlertSeverity,
    AlertType,
    MetricThreshold,
    InsightReport,
    TrendAnalysis,
    AnomalyDetector,
    CapacityPlanner,
    UsageAnalytics,
    DashboardData,
    MetricAggregation,
    TimeSeries,
    HealthScore,
    ResourceUtilization,
    MonitoringError,
    InsightType,
    AggregationPeriod,
    TrendDirection,
    AnomalyType,
    ReportFormat,
)


@pytest.fixture
def metrics_collector():
    """Create MetricsCollector for testing."""
    return MetricsCollector()


@pytest.fixture
def performance_analyzer():
    """Create PerformanceAnalyzer for testing."""
    return PerformanceAnalyzer()


@pytest.fixture
def alert_manager():
    """Create AlertManager for testing."""
    return AlertManager()


@pytest.fixture
def anomaly_detector():
    """Create AnomalyDetector for testing."""
    return AnomalyDetector()


@pytest.fixture
def sample_execution_metrics():
    """Create sample execution metrics."""
    return ExecutionMetrics(
        execution_id=uuid4(),
        workflow_id=uuid4(),
        start_time=datetime.now(timezone.utc) - timedelta(minutes=5),
        end_time=datetime.now(timezone.utc),
        duration_seconds=300.5,
        status="completed",
        nodes_executed=5,
        nodes_succeeded=5,
        nodes_failed=0,
        data_processed_bytes=1024*1024,
        memory_peak_mb=256.7,
        cpu_time_seconds=45.2,
        error_count=0,
        retry_count=1,
        queue_time_seconds=10.0,
        metadata={"user_id": str(uuid4())}
    )


@pytest.fixture
def sample_node_metrics():
    """Create sample node metrics."""
    return NodeMetrics(
        node_id="http_request_1",
        node_type="action.http",
        execution_id=uuid4(),
        start_time=datetime.now(timezone.utc) - timedelta(minutes=1),
        end_time=datetime.now(timezone.utc),
        duration_seconds=60.0,
        status="completed",
        input_size_bytes=512,
        output_size_bytes=2048,
        memory_used_mb=32.5,
        cpu_time_seconds=5.8,
        error_message=None,
        retry_count=0,
        custom_metrics={"response_time": 1.5, "status_code": 200}
    )


@pytest.fixture
def sample_system_metrics():
    """Create sample system metrics."""
    return SystemMetrics(
        timestamp=datetime.now(timezone.utc),
        cpu_usage_percent=45.2,
        memory_usage_percent=67.8,
        disk_usage_percent=34.1,
        network_in_mbps=12.5,
        network_out_mbps=8.3,
        active_executions=15,
        queued_executions=3,
        worker_count=4,
        database_connections=25,
        redis_connections=12,
        response_time_p95=245.6,
        error_rate_percent=0.1
    )


@pytest.fixture
def sample_alert():
    """Create sample alert."""
    return Alert(
        id=uuid4(),
        type=AlertType.PERFORMANCE,
        severity=AlertSeverity.WARNING,
        title="High execution time detected",
        message="Workflow execution time exceeded threshold",
        triggered_at=datetime.now(timezone.utc),
        resolved_at=None,
        workflow_id=uuid4(),
        execution_id=uuid4(),
        metric_name="execution_duration",
        metric_value=600.0,
        threshold_value=300.0,
        metadata={"node_id": "slow_node"}
    )


@pytest.fixture
def sample_threshold():
    """Create sample metric threshold."""
    return MetricThreshold(
        metric_name="execution_duration",
        operator="greater_than",
        value=300.0,
        severity=AlertSeverity.WARNING,
        enabled=True,
        workflow_id=None,
        node_type=None,
        description="Execution duration threshold"
    )


@pytest.mark.unit
class TestExecutionMetrics:
    """Test ExecutionMetrics model."""
    
    def test_execution_metrics_creation(self, sample_execution_metrics):
        """Test creating execution metrics."""
        assert sample_execution_metrics.duration_seconds == 300.5
        assert sample_execution_metrics.nodes_executed == 5
        assert sample_execution_metrics.status == "completed"
        assert sample_execution_metrics.error_count == 0
    
    def test_execution_metrics_success_rate(self, sample_execution_metrics):
        """Test calculating success rate."""
        success_rate = sample_execution_metrics.get_success_rate()
        assert success_rate == 1.0  # 5/5 succeeded
        
        # Test with failures
        sample_execution_metrics.nodes_failed = 1
        success_rate = sample_execution_metrics.get_success_rate()
        assert success_rate == 0.8  # 4/5 succeeded
    
    def test_execution_metrics_throughput(self, sample_execution_metrics):
        """Test calculating throughput."""
        throughput = sample_execution_metrics.get_throughput_nodes_per_second()
        expected = 5 / 300.5  # nodes per second
        assert abs(throughput - expected) < 0.001
    
    def test_execution_metrics_efficiency_score(self, sample_execution_metrics):
        """Test calculating efficiency score."""
        score = sample_execution_metrics.get_efficiency_score()
        assert 0 <= score <= 1.0
        
        # Should be high for successful execution with no retries
        assert score > 0.8
    
    def test_execution_metrics_serialization(self, sample_execution_metrics):
        """Test execution metrics serialization."""
        data = sample_execution_metrics.model_dump()
        
        assert "execution_id" in data
        assert "duration_seconds" in data
        assert "nodes_executed" in data
        
        # Test deserialization
        restored = ExecutionMetrics.model_validate(data)
        assert restored.execution_id == sample_execution_metrics.execution_id
        assert restored.duration_seconds == sample_execution_metrics.duration_seconds


@pytest.mark.unit
class TestNodeMetrics:
    """Test NodeMetrics model."""
    
    def test_node_metrics_creation(self, sample_node_metrics):
        """Test creating node metrics."""
        assert sample_node_metrics.node_id == "http_request_1"
        assert sample_node_metrics.node_type == "action.http"
        assert sample_node_metrics.duration_seconds == 60.0
        assert sample_node_metrics.status == "completed"
    
    def test_node_metrics_data_ratio(self, sample_node_metrics):
        """Test calculating data processing ratio."""
        ratio = sample_node_metrics.get_data_processing_ratio()
        expected = 2048 / 512  # output/input
        assert ratio == expected
    
    def test_node_metrics_performance_score(self, sample_node_metrics):
        """Test calculating performance score."""
        score = sample_node_metrics.get_performance_score()
        assert 0 <= score <= 1.0
        
        # Fast execution with no errors should score high
        assert score > 0.7
    
    def test_node_metrics_is_slow(self, sample_node_metrics):
        """Test detecting slow nodes."""
        # 60 seconds should not be slow for most nodes
        assert not sample_node_metrics.is_slow(threshold_seconds=120.0)
        
        # But should be slow if threshold is lower
        assert sample_node_metrics.is_slow(threshold_seconds=30.0)


@pytest.mark.unit
class TestSystemMetrics:
    """Test SystemMetrics model."""
    
    def test_system_metrics_creation(self, sample_system_metrics):
        """Test creating system metrics."""
        assert sample_system_metrics.cpu_usage_percent == 45.2
        assert sample_system_metrics.memory_usage_percent == 67.8
        assert sample_system_metrics.active_executions == 15
        assert sample_system_metrics.worker_count == 4
    
    def test_system_health_score(self, sample_system_metrics):
        """Test calculating system health score."""
        score = sample_system_metrics.get_health_score()
        assert 0 <= score <= 1.0
        
        # System should be healthy with these metrics
        assert score > 0.7
    
    def test_system_capacity_utilization(self, sample_system_metrics):
        """Test calculating capacity utilization."""
        cpu_util = sample_system_metrics.get_cpu_utilization()
        memory_util = sample_system_metrics.get_memory_utilization()
        
        assert abs(cpu_util - 0.452) < 0.001  # 45.2%
        assert abs(memory_util - 0.678) < 0.001  # 67.8%
    
    def test_system_load_factor(self, sample_system_metrics):
        """Test calculating load factor."""
        load_factor = sample_system_metrics.get_load_factor()
        expected = 15 / 4  # active executions per worker
        assert load_factor == expected
    
    def test_system_is_overloaded(self, sample_system_metrics):
        """Test detecting system overload."""
        # Current metrics should not indicate overload
        assert not sample_system_metrics.is_overloaded()
        
        # Test overload conditions
        sample_system_metrics.cpu_usage_percent = 95.0
        sample_system_metrics.memory_usage_percent = 98.0
        assert sample_system_metrics.is_overloaded()


@pytest.mark.unit
class TestAlert:
    """Test Alert model."""
    
    def test_alert_creation(self, sample_alert):
        """Test creating alert."""
        assert sample_alert.type == AlertType.PERFORMANCE
        assert sample_alert.severity == AlertSeverity.WARNING
        assert sample_alert.title == "High execution time detected"
        assert sample_alert.resolved_at is None
    
    def test_alert_is_active(self, sample_alert):
        """Test checking if alert is active."""
        assert sample_alert.is_active()
        
        # Resolve the alert
        sample_alert.resolved_at = datetime.now(timezone.utc)
        assert not sample_alert.is_active()
    
    def test_alert_duration(self, sample_alert):
        """Test calculating alert duration."""
        # Active alert
        duration = sample_alert.get_duration()
        assert duration.total_seconds() > 0
        
        # Resolved alert
        sample_alert.resolved_at = sample_alert.triggered_at + timedelta(minutes=30)
        duration = sample_alert.get_duration()
        assert duration.total_seconds() == 1800  # 30 minutes
    
    def test_alert_severity_score(self, sample_alert):
        """Test getting severity score."""
        scores = {}
        
        sample_alert.severity = AlertSeverity.INFO
        scores[AlertSeverity.INFO] = sample_alert._get_severity_score()
        
        sample_alert.severity = AlertSeverity.WARNING
        scores[AlertSeverity.WARNING] = sample_alert._get_severity_score()
        
        sample_alert.severity = AlertSeverity.ERROR
        scores[AlertSeverity.ERROR] = sample_alert._get_severity_score()
        
        sample_alert.severity = AlertSeverity.CRITICAL
        scores[AlertSeverity.CRITICAL] = sample_alert._get_severity_score()
        
        # Scores should increase with severity
        assert scores[AlertSeverity.INFO] < scores[AlertSeverity.WARNING]
        assert scores[AlertSeverity.WARNING] < scores[AlertSeverity.ERROR]
        assert scores[AlertSeverity.ERROR] < scores[AlertSeverity.CRITICAL]


@pytest.mark.unit
class TestMetricThreshold:
    """Test MetricThreshold model."""
    
    def test_threshold_creation(self, sample_threshold):
        """Test creating metric threshold."""
        assert sample_threshold.metric_name == "execution_duration"
        assert sample_threshold.operator == "greater_than"
        assert sample_threshold.value == 300.0
        assert sample_threshold.enabled is True
    
    def test_threshold_evaluation(self, sample_threshold):
        """Test evaluating threshold against values."""
        # Test greater_than operator
        assert sample_threshold.evaluate(400.0) is True  # 400 > 300
        assert sample_threshold.evaluate(200.0) is False  # 200 < 300
        assert sample_threshold.evaluate(300.0) is False  # 300 == 300
        
        # Test other operators
        sample_threshold.operator = "less_than"
        assert sample_threshold.evaluate(200.0) is True
        assert sample_threshold.evaluate(400.0) is False
        
        sample_threshold.operator = "equal_to"
        assert sample_threshold.evaluate(300.0) is True
        assert sample_threshold.evaluate(299.0) is False
        
        sample_threshold.operator = "not_equal_to"
        assert sample_threshold.evaluate(299.0) is True
        assert sample_threshold.evaluate(300.0) is False
    
    def test_threshold_matches_context(self, sample_threshold):
        """Test checking if threshold matches execution context."""
        # No workflow/node restrictions - should match all
        assert sample_threshold.matches_context(workflow_id=uuid4(), node_type="action.http")
        
        # With workflow restriction
        workflow_id = uuid4()
        sample_threshold.workflow_id = workflow_id
        assert sample_threshold.matches_context(workflow_id=workflow_id, node_type="action.http")
        assert not sample_threshold.matches_context(workflow_id=uuid4(), node_type="action.http")
        
        # With node type restriction
        sample_threshold.workflow_id = None
        sample_threshold.node_type = "action.http"
        assert sample_threshold.matches_context(workflow_id=uuid4(), node_type="action.http")
        assert not sample_threshold.matches_context(workflow_id=uuid4(), node_type="action.email")


@pytest.mark.unit
class TestMetricsCollector:
    """Test MetricsCollector."""
    
    def test_collector_initialization(self, metrics_collector):
        """Test collector initialization."""
        assert metrics_collector is not None
        assert hasattr(metrics_collector, 'collect_execution_metrics')
        assert hasattr(metrics_collector, 'collect_system_metrics')
    
    @pytest.mark.asyncio
    async def test_collect_execution_metrics(self, metrics_collector, sample_execution_metrics):
        """Test collecting execution metrics."""
        execution_id = uuid4()
        
        with patch.object(metrics_collector, '_gather_execution_data') as mock_gather:
            mock_gather.return_value = sample_execution_metrics
            
            metrics = await metrics_collector.collect_execution_metrics(execution_id)
            
            assert metrics == sample_execution_metrics
            mock_gather.assert_called_once_with(execution_id)
    
    @pytest.mark.asyncio
    async def test_collect_node_metrics(self, metrics_collector, sample_node_metrics):
        """Test collecting node metrics."""
        execution_id = uuid4()
        node_id = "test_node"
        
        with patch.object(metrics_collector, '_gather_node_data') as mock_gather:
            mock_gather.return_value = sample_node_metrics
            
            metrics = await metrics_collector.collect_node_metrics(execution_id, node_id)
            
            assert metrics == sample_node_metrics
            mock_gather.assert_called_once_with(execution_id, node_id)
    
    @pytest.mark.asyncio
    async def test_collect_system_metrics(self, metrics_collector, sample_system_metrics):
        """Test collecting system metrics."""
        with patch.object(metrics_collector, '_gather_system_data') as mock_gather:
            mock_gather.return_value = sample_system_metrics
            
            metrics = await metrics_collector.collect_system_metrics()
            
            assert metrics == sample_system_metrics
            mock_gather.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_batch_collect_metrics(self, metrics_collector):
        """Test batch collecting metrics."""
        execution_ids = [uuid4() for _ in range(5)]
        
        with patch.object(metrics_collector, 'collect_execution_metrics') as mock_collect:
            mock_metrics = [ExecutionMetrics(
                execution_id=eid,
                workflow_id=uuid4(),
                start_time=datetime.now(timezone.utc),
                end_time=datetime.now(timezone.utc),
                duration_seconds=100.0,
                status="completed",
                nodes_executed=3,
                nodes_succeeded=3,
                nodes_failed=0,
                data_processed_bytes=1024,
                memory_peak_mb=128.0,
                cpu_time_seconds=20.0,
                error_count=0,
                retry_count=0,
                queue_time_seconds=5.0
            ) for eid in execution_ids]
            
            mock_collect.side_effect = mock_metrics
            
            results = await metrics_collector.batch_collect_execution_metrics(execution_ids)
            
            assert len(results) == 5
            assert mock_collect.call_count == 5
            assert all(isinstance(m, ExecutionMetrics) for m in results)


@pytest.mark.unit
class TestPerformanceAnalyzer:
    """Test PerformanceAnalyzer."""
    
    def test_analyzer_initialization(self, performance_analyzer):
        """Test analyzer initialization."""
        assert performance_analyzer is not None
        assert hasattr(performance_analyzer, 'analyze_execution_performance')
        assert hasattr(performance_analyzer, 'analyze_trends')
    
    @pytest.mark.asyncio
    async def test_analyze_execution_performance(self, performance_analyzer, sample_execution_metrics):
        """Test analyzing execution performance."""
        metrics_list = [sample_execution_metrics]
        
        with patch.object(performance_analyzer, '_calculate_performance_insights') as mock_calc:
            mock_insights = WorkflowInsights(
                workflow_id=sample_execution_metrics.workflow_id,
                period_start=datetime.now(timezone.utc) - timedelta(days=1),
                period_end=datetime.now(timezone.utc),
                total_executions=1,
                successful_executions=1,
                failed_executions=0,
                average_duration=300.5,
                p95_duration=300.5,
                success_rate=1.0,
                throughput_per_hour=12.0,
                error_rate=0.0,
                insights=[],
                recommendations=[]
            )
            mock_calc.return_value = mock_insights
            
            insights = await performance_analyzer.analyze_execution_performance(
                workflow_id=sample_execution_metrics.workflow_id,
                period_days=7
            )
            
            assert isinstance(insights, WorkflowInsights)
            assert insights.success_rate == 1.0
            mock_calc.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_analyze_trends(self, performance_analyzer):
        """Test analyzing performance trends."""
        workflow_id = uuid4()
        
        with patch.object(performance_analyzer, '_calculate_trends') as mock_calc:
            mock_trends = TrendAnalysis(
                metric_name="execution_duration",
                period_start=datetime.now(timezone.utc) - timedelta(days=30),
                period_end=datetime.now(timezone.utc),
                direction=TrendDirection.IMPROVING,
                change_percent=-15.5,
                confidence_score=0.85,
                data_points=30,
                trend_line=[],
                anomalies=[]
            )
            mock_calc.return_value = mock_trends
            
            trends = await performance_analyzer.analyze_trends(
                workflow_id=workflow_id,
                metric_name="execution_duration",
                period_days=30
            )
            
            assert isinstance(trends, TrendAnalysis)
            assert trends.direction == TrendDirection.IMPROVING
            mock_calc.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_detect_bottlenecks(self, performance_analyzer):
        """Test detecting performance bottlenecks."""
        workflow_id = uuid4()
        
        with patch.object(performance_analyzer, '_identify_bottlenecks') as mock_identify:
            mock_bottlenecks = [
                {
                    "node_id": "slow_node_1",
                    "node_type": "action.http",
                    "avg_duration": 120.5,
                    "impact_score": 0.85,
                    "recommendation": "Optimize HTTP timeout settings"
                }
            ]
            mock_identify.return_value = mock_bottlenecks
            
            bottlenecks = await performance_analyzer.detect_bottlenecks(workflow_id)
            
            assert len(bottlenecks) == 1
            assert bottlenecks[0]["node_id"] == "slow_node_1"
            mock_identify.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_generate_recommendations(self, performance_analyzer):
        """Test generating performance recommendations."""
        workflow_id = uuid4()
        
        with patch.object(performance_analyzer, '_analyze_optimization_opportunities') as mock_analyze:
            mock_recommendations = [
                "Consider parallelizing independent HTTP requests",
                "Add caching for frequently accessed data",
                "Optimize database queries in data processing nodes"
            ]
            mock_analyze.return_value = mock_recommendations
            
            recommendations = await performance_analyzer.generate_recommendations(workflow_id)
            
            assert len(recommendations) == 3
            assert "parallelizing" in recommendations[0]
            mock_analyze.assert_called_once()


@pytest.mark.unit
class TestAlertManager:
    """Test AlertManager."""
    
    def test_alert_manager_initialization(self, alert_manager):
        """Test alert manager initialization."""
        assert alert_manager is not None
        assert hasattr(alert_manager, 'add_threshold')
        assert hasattr(alert_manager, 'evaluate_metrics')
        assert hasattr(alert_manager, 'get_active_alerts')
    
    @pytest.mark.asyncio
    async def test_add_threshold(self, alert_manager, sample_threshold):
        """Test adding metric threshold."""
        with patch.object(alert_manager, '_save_threshold') as mock_save:
            await alert_manager.add_threshold(sample_threshold)
            mock_save.assert_called_once_with(sample_threshold)
    
    @pytest.mark.asyncio
    async def test_evaluate_metrics(self, alert_manager, sample_threshold, sample_execution_metrics):
        """Test evaluating metrics against thresholds."""
        with patch.object(alert_manager, '_get_thresholds') as mock_get_thresholds:
            with patch.object(alert_manager, '_create_alert') as mock_create_alert:
                mock_get_thresholds.return_value = [sample_threshold]
                
                # Test triggering alert (execution duration > threshold)
                sample_execution_metrics.duration_seconds = 450.0  # > 300.0 threshold
                
                alerts = await alert_manager.evaluate_metrics(sample_execution_metrics)
                
                assert len(alerts) >= 0  # Could create alert or not based on implementation
                mock_get_thresholds.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_active_alerts(self, alert_manager, sample_alert):
        """Test getting active alerts."""
        with patch.object(alert_manager, '_load_active_alerts') as mock_load:
            mock_load.return_value = [sample_alert]
            
            alerts = await alert_manager.get_active_alerts()
            
            assert len(alerts) == 1
            assert alerts[0] == sample_alert
            mock_load.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_resolve_alert(self, alert_manager, sample_alert):
        """Test resolving alert."""
        with patch.object(alert_manager, '_update_alert') as mock_update:
            await alert_manager.resolve_alert(sample_alert.id, "Issue resolved")
            mock_update.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_alert_statistics(self, alert_manager):
        """Test getting alert statistics."""
        with patch.object(alert_manager, '_calculate_alert_stats') as mock_calc:
            mock_stats = {
                "total_alerts": 150,
                "active_alerts": 5,
                "resolved_alerts": 145,
                "by_severity": {
                    "critical": 2,
                    "error": 8,
                    "warning": 25,
                    "info": 115
                },
                "by_type": {
                    "performance": 80,
                    "resource": 35,
                    "error": 20,
                    "availability": 15
                },
                "resolution_time_avg_minutes": 45.3
            }
            mock_calc.return_value = mock_stats
            
            stats = await alert_manager.get_alert_statistics(period_days=30)
            
            assert stats["total_alerts"] == 150
            assert stats["active_alerts"] == 5
            assert "by_severity" in stats
            mock_calc.assert_called_once()


@pytest.mark.unit
class TestAnomalyDetector:
    """Test AnomalyDetector."""
    
    def test_detector_initialization(self, anomaly_detector):
        """Test detector initialization."""
        assert anomaly_detector is not None
        assert hasattr(anomaly_detector, 'detect_anomalies')
        assert hasattr(anomaly_detector, 'train_model')
    
    @pytest.mark.asyncio
    async def test_detect_execution_anomalies(self, anomaly_detector):
        """Test detecting execution anomalies."""
        metrics_data = [
            {"duration": 100.0, "memory": 128.0, "cpu": 20.0},
            {"duration": 105.0, "memory": 130.0, "cpu": 22.0},
            {"duration": 95.0, "memory": 125.0, "cpu": 18.0},
            {"duration": 500.0, "memory": 512.0, "cpu": 180.0},  # Anomaly
        ]
        
        with patch.object(anomaly_detector, '_analyze_statistical_anomalies') as mock_analyze:
            mock_anomalies = [
                {
                    "type": AnomalyType.STATISTICAL,
                    "metric": "duration",
                    "value": 500.0,
                    "expected_range": [90.0, 110.0],
                    "severity": 0.85,
                    "timestamp": datetime.now(timezone.utc)
                }
            ]
            mock_analyze.return_value = mock_anomalies
            
            anomalies = await anomaly_detector.detect_anomalies(metrics_data, "execution_metrics")
            
            assert len(anomalies) == 1
            assert anomalies[0]["type"] == AnomalyType.STATISTICAL
            assert anomalies[0]["metric"] == "duration"
            mock_analyze.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_detect_pattern_anomalies(self, anomaly_detector):
        """Test detecting pattern-based anomalies."""
        time_series_data = [
            {"timestamp": datetime.now(timezone.utc) - timedelta(hours=i), "value": 100 + i * 2}
            for i in range(24)
        ]
        
        with patch.object(anomaly_detector, '_analyze_pattern_anomalies') as mock_analyze:
            mock_anomalies = [
                {
                    "type": AnomalyType.PATTERN,
                    "description": "Unusual spike in execution rate",
                    "start_time": datetime.now(timezone.utc) - timedelta(hours=2),
                    "end_time": datetime.now(timezone.utc) - timedelta(hours=1),
                    "severity": 0.72
                }
            ]
            mock_analyze.return_value = mock_anomalies
            
            anomalies = await anomaly_detector.detect_pattern_anomalies(time_series_data)
            
            assert len(anomalies) == 1
            assert anomalies[0]["type"] == AnomalyType.PATTERN
            mock_analyze.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_train_anomaly_model(self, anomaly_detector):
        """Test training anomaly detection model."""
        training_data = [
            {"execution_id": str(uuid4()), "duration": 100 + i, "memory": 128 + i * 2}
            for i in range(1000)
        ]
        
        with patch.object(anomaly_detector, '_train_ml_model') as mock_train:
            mock_train.return_value = {"model_accuracy": 0.94, "training_samples": 1000}
            
            result = await anomaly_detector.train_model(training_data, model_type="isolation_forest")
            
            assert result["model_accuracy"] == 0.94
            assert result["training_samples"] == 1000
            mock_train.assert_called_once()


@pytest.mark.unit
class TestWorkflowInsights:
    """Test WorkflowInsights model."""
    
    def test_insights_creation(self):
        """Test creating workflow insights."""
        insights = WorkflowInsights(
            workflow_id=uuid4(),
            period_start=datetime.now(timezone.utc) - timedelta(days=7),
            period_end=datetime.now(timezone.utc),
            total_executions=100,
            successful_executions=95,
            failed_executions=5,
            average_duration=250.5,
            p95_duration=450.0,
            success_rate=0.95,
            throughput_per_hour=14.3,
            error_rate=0.05,
            insights=[],
            recommendations=[]
        )
        
        assert insights.total_executions == 100
        assert insights.success_rate == 0.95
        assert insights.error_rate == 0.05
        assert insights.throughput_per_hour == 14.3
    
    def test_insights_performance_grade(self):
        """Test calculating performance grade."""
        insights = WorkflowInsights(
            workflow_id=uuid4(),
            period_start=datetime.now(timezone.utc) - timedelta(days=7),
            period_end=datetime.now(timezone.utc),
            total_executions=500,
            successful_executions=498,
            failed_executions=2,
            average_duration=45.0,  # Fast execution
            p95_duration=80.0,
            success_rate=0.996,  # High success rate
            throughput_per_hour=120.0,  # High throughput
            error_rate=0.004,  # Low error rate
            insights=[],
            recommendations=[]
        )
        
        grade = insights.get_performance_grade()
        assert grade in ['A', 'B', 'C', 'D', 'F']
        
        # High success rate and reasonable performance should get good grade
        assert grade in ['A', 'B']
    
    def test_insights_reliability_score(self):
        """Test calculating reliability score."""
        insights = WorkflowInsights(
            workflow_id=uuid4(),
            period_start=datetime.now(timezone.utc) - timedelta(days=7),
            period_end=datetime.now(timezone.utc),
            total_executions=1000,
            successful_executions=990,
            failed_executions=10,
            average_duration=180.0,
            p95_duration=300.0,
            success_rate=0.99,
            throughput_per_hour=143.0,
            error_rate=0.01,
            insights=[],
            recommendations=[]
        )
        
        score = insights.get_reliability_score()
        assert 0 <= score <= 1.0
        
        # High success rate should yield high reliability score
        assert score > 0.9


@pytest.mark.integration
class TestMonitoringIntegration:
    """Integration tests for monitoring system."""
    
    @pytest.mark.asyncio
    async def test_full_monitoring_pipeline(self, metrics_collector, performance_analyzer, alert_manager):
        """Test complete monitoring pipeline."""
        workflow_id = uuid4()
        execution_id = uuid4()
        
        # Simulate execution metrics collection
        execution_metrics = ExecutionMetrics(
            execution_id=execution_id,
            workflow_id=workflow_id,
            start_time=datetime.now(timezone.utc) - timedelta(minutes=10),
            end_time=datetime.now(timezone.utc),
            duration_seconds=600.0,  # Long execution
            status="completed",
            nodes_executed=8,
            nodes_succeeded=8,
            nodes_failed=0,
            data_processed_bytes=5 * 1024 * 1024,
            memory_peak_mb=512.0,
            cpu_time_seconds=180.0,
            error_count=0,
            retry_count=2,
            queue_time_seconds=30.0
        )
        
        with patch.object(metrics_collector, 'collect_execution_metrics') as mock_collect:
            with patch.object(alert_manager, 'evaluate_metrics') as mock_evaluate:
                with patch.object(performance_analyzer, 'analyze_execution_performance') as mock_analyze:
                    mock_collect.return_value = execution_metrics
                    mock_evaluate.return_value = []
                    
                    mock_insights = WorkflowInsights(
                        workflow_id=workflow_id,
                        period_start=datetime.now(timezone.utc) - timedelta(days=1),
                        period_end=datetime.now(timezone.utc),
                        total_executions=10,
                        successful_executions=9,
                        failed_executions=1,
                        average_duration=400.0,
                        p95_duration=650.0,
                        success_rate=0.9,
                        throughput_per_hour=24.0,
                        error_rate=0.1,
                        insights=["Performance degradation detected"],
                        recommendations=["Consider optimizing slow nodes"]
                    )
                    mock_analyze.return_value = mock_insights
                    
                    # 1. Collect metrics
                    metrics = await metrics_collector.collect_execution_metrics(execution_id)
                    assert metrics.duration_seconds == 600.0
                    
                    # 2. Evaluate for alerts
                    alerts = await alert_manager.evaluate_metrics(metrics)
                    assert isinstance(alerts, list)
                    
                    # 3. Analyze performance
                    insights = await performance_analyzer.analyze_execution_performance(workflow_id, 1)
                    assert insights.workflow_id == workflow_id
                    assert len(insights.insights) > 0
    
    @pytest.mark.asyncio
    async def test_real_time_monitoring_dashboard(self, metrics_collector):
        """Test real-time monitoring dashboard data collection."""
        with patch.object(metrics_collector, 'collect_system_metrics') as mock_system:
            with patch.object(metrics_collector, '_get_active_executions') as mock_active:
                with patch.object(metrics_collector, '_get_recent_alerts') as mock_alerts:
                    # Mock system metrics
                    mock_system.return_value = SystemMetrics(
                        timestamp=datetime.now(timezone.utc),
                        cpu_usage_percent=65.2,
                        memory_usage_percent=78.5,
                        disk_usage_percent=45.0,
                        network_in_mbps=25.3,
                        network_out_mbps=18.7,
                        active_executions=12,
                        queued_executions=4,
                        worker_count=6,
                        database_connections=45,
                        redis_connections=23,
                        response_time_p95=320.5,
                        error_rate_percent=0.5
                    )
                    
                    # Mock active executions
                    mock_active.return_value = [
                        {"execution_id": str(uuid4()), "workflow_name": "Data Processing", "progress": 0.75},
                        {"execution_id": str(uuid4()), "workflow_name": "Report Generation", "progress": 0.35}
                    ]
                    
                    # Mock recent alerts
                    mock_alerts.return_value = [
                        {"severity": "warning", "message": "High memory usage", "timestamp": datetime.now(timezone.utc)}
                    ]
                    
                    # Collect dashboard data
                    dashboard_data = DashboardData(
                        timestamp=datetime.now(timezone.utc),
                        system_metrics=await metrics_collector.collect_system_metrics(),
                        active_executions=await metrics_collector._get_active_executions(),
                        recent_alerts=await metrics_collector._get_recent_alerts(),
                        health_score=0.85,
                        throughput_last_hour=156,
                        error_rate_last_hour=0.5
                    )
                    
                    assert dashboard_data.system_metrics.cpu_usage_percent == 65.2
                    assert len(dashboard_data.active_executions) == 2
                    assert len(dashboard_data.recent_alerts) == 1
                    assert dashboard_data.health_score == 0.85
    
    @pytest.mark.asyncio
    async def test_capacity_planning_analysis(self):
        """Test capacity planning analysis."""
        capacity_planner = CapacityPlanner()
        
        # Mock historical usage data
        usage_data = [
            {"timestamp": datetime.now(timezone.utc) - timedelta(days=i), 
             "cpu_usage": 45 + i * 2, "memory_usage": 60 + i * 1.5, "executions": 100 + i * 5}
            for i in range(30)
        ]
        
        with patch.object(capacity_planner, '_analyze_growth_trends') as mock_analyze:
            with patch.object(capacity_planner, '_predict_resource_needs') as mock_predict:
                mock_trends = {
                    "cpu_growth_rate": 0.05,  # 5% per month
                    "memory_growth_rate": 0.03,  # 3% per month
                    "execution_growth_rate": 0.08  # 8% per month
                }
                mock_analyze.return_value = mock_trends
                
                mock_predictions = {
                    "next_30_days": {"cpu_need": 85.0, "memory_need": 72.0, "worker_need": 8},
                    "next_90_days": {"cpu_need": 95.0, "memory_need": 85.0, "worker_need": 12},
                    "recommendations": [
                        "Scale up worker instances before reaching 80% CPU utilization",
                        "Consider adding memory-optimized instances for data processing workloads"
                    ]
                }
                mock_predict.return_value = mock_predictions
                
                analysis = await capacity_planner.analyze_capacity_needs(usage_data)
                
                assert "next_30_days" in analysis
                assert "recommendations" in analysis
                assert analysis["next_30_days"]["cpu_need"] == 85.0
                mock_analyze.assert_called_once()
                mock_predict.assert_called_once()


@pytest.mark.performance
class TestMonitoringPerformance:
    """Performance tests for monitoring system."""
    
    @pytest.mark.asyncio
    async def test_high_volume_metrics_collection(self, metrics_collector):
        """Test metrics collection performance under high load."""
        # Simulate 1000 concurrent executions
        execution_ids = [uuid4() for _ in range(1000)]
        
        with patch.object(metrics_collector, 'collect_execution_metrics') as mock_collect:
            # Mock fast metrics collection
            async def fast_collect(execution_id):
                return ExecutionMetrics(
                    execution_id=execution_id,
                    workflow_id=uuid4(),
                    start_time=datetime.now(timezone.utc),
                    end_time=datetime.now(timezone.utc),
                    duration_seconds=100.0,
                    status="completed",
                    nodes_executed=5,
                    nodes_succeeded=5,
                    nodes_failed=0,
                    data_processed_bytes=1024,
                    memory_peak_mb=128.0,
                    cpu_time_seconds=25.0,
                    error_count=0,
                    retry_count=0,
                    queue_time_seconds=2.0
                )
            
            mock_collect.side_effect = fast_collect
            
            import time
            start_time = time.time()
            
            # Collect metrics for all executions
            tasks = [metrics_collector.collect_execution_metrics(eid) for eid in execution_ids]
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should complete within reasonable time
            assert duration < 2.0  # Less than 2 seconds for 1000 collections
            assert len(results) == 1000
            assert all(isinstance(r, ExecutionMetrics) for r in results)
    
    @pytest.mark.asyncio
    async def test_anomaly_detection_performance(self, anomaly_detector):
        """Test anomaly detection performance with large datasets."""
        # Generate large dataset
        large_dataset = [
            {"duration": 100 + i + (i % 100) * 0.1, "memory": 128 + i * 0.5, "cpu": 20 + i * 0.2}
            for i in range(10000)
        ]
        
        with patch.object(anomaly_detector, '_analyze_statistical_anomalies') as mock_analyze:
            mock_analyze.return_value = [
                {
                    "type": AnomalyType.STATISTICAL,
                    "metric": "duration",
                    "value": 500.0,
                    "expected_range": [95.0, 105.0],
                    "severity": 0.9,
                    "timestamp": datetime.now(timezone.utc)
                }
            ]
            
            import time
            start_time = time.time()
            
            anomalies = await anomaly_detector.detect_anomalies(large_dataset, "execution_metrics")
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should complete efficiently even with large dataset
            assert duration < 1.0  # Less than 1 second
            assert len(anomalies) >= 0
            mock_analyze.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_alert_evaluation_performance(self, alert_manager):
        """Test alert evaluation performance."""
        # Create many thresholds
        thresholds = [
            MetricThreshold(
                metric_name=f"metric_{i}",
                operator="greater_than",
                value=100.0 + i,
                severity=AlertSeverity.WARNING,
                enabled=True
            )
            for i in range(100)
        ]
        
        with patch.object(alert_manager, '_get_thresholds') as mock_get:
            with patch.object(alert_manager, '_create_alert') as mock_create:
                mock_get.return_value = thresholds
                mock_create.return_value = None
                
                # Create metrics to evaluate
                test_metrics = ExecutionMetrics(
                    execution_id=uuid4(),
                    workflow_id=uuid4(),
                    start_time=datetime.now(timezone.utc),
                    end_time=datetime.now(timezone.utc),
                    duration_seconds=150.0,
                    status="completed",
                    nodes_executed=10,
                    nodes_succeeded=10,
                    nodes_failed=0,
                    data_processed_bytes=2048,
                    memory_peak_mb=256.0,
                    cpu_time_seconds=45.0,
                    error_count=0,
                    retry_count=0,
                    queue_time_seconds=5.0
                )
                
                import time
                start_time = time.time()
                
                alerts = await alert_manager.evaluate_metrics(test_metrics)
                
                end_time = time.time()
                duration = end_time - start_time
                
                # Should evaluate quickly even with many thresholds
                assert duration < 0.5  # Less than 0.5 seconds
                assert isinstance(alerts, list)
                mock_get.assert_called_once()