"""Test Multi-Main high availability setup."""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from uuid import uuid4

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from budflow.core.multi_main import (
    MultiMainConfig,
    NodeInfo,
    NodeStatus,
    LeaderElection,
    InstanceHealthMonitor,
    WebhookDistributor,
    ScheduleDistributor,
    StateReplicator,
    MultiMainManager,
    LeaderElectionError,
    HealthCheckError,
    DistributionError,
    ReplicationError,
)


@pytest.fixture
def multi_main_config():
    """Create multi-main configuration."""
    return MultiMainConfig(
        node_id="node-1",
        redis_url="redis://localhost:6379",
        consul_url="http://localhost:8500",
        instance_name="budflow-main-1",
        region="us-east-1",
        environment="test",
        health_check_interval=5,
        leader_election_ttl=30,
        heartbeat_interval=10,
        webhook_routing_strategy="consistent_hash",
        enable_state_replication=True,
        max_split_brain_timeout=60,
    )


@pytest.fixture
def redis_client():
    """Create mock Redis client."""
    client = AsyncMock()
    client.ping = AsyncMock(return_value=True)
    client.set = AsyncMock(return_value=True)
    client.get = AsyncMock(return_value=None)
    client.delete = AsyncMock(return_value=1)
    client.exists = AsyncMock(return_value=False)
    client.expire = AsyncMock(return_value=True)
    client.eval = AsyncMock(return_value=True)
    client.publish = AsyncMock(return_value=1)
    client.pubsub = Mock()
    client.hset = AsyncMock(return_value=1)
    client.hgetall = AsyncMock(return_value={})
    client.zadd = AsyncMock(return_value=1)
    client.zrange = AsyncMock(return_value=[])
    client.zrem = AsyncMock(return_value=1)
    return client


@pytest.fixture
def consul_client():
    """Create mock Consul client."""
    client = Mock()
    client.health = Mock()
    client.health.service = Mock(return_value=(None, []))
    client.agent = Mock()
    client.agent.service = Mock()
    client.agent.service.register = Mock(return_value=True)
    client.agent.service.deregister = Mock(return_value=True)
    client.kv = Mock()
    client.kv.put = Mock(return_value=True)
    client.kv.get = Mock(return_value=(None, None))
    client.session = Mock()
    client.session.create = Mock(return_value="session-123")
    client.session.renew = Mock(return_value=True)
    return client


@pytest.fixture
def node_info():
    """Create sample node info."""
    return NodeInfo(
        node_id="node-1",
        instance_name="budflow-main-1",
        host="127.0.0.1",
        port=8000,
        region="us-east-1",
        environment="test",
        status=NodeStatus.HEALTHY,
        last_heartbeat=datetime.now(timezone.utc),
        version="1.0.0",
        capabilities=["webhooks", "executions", "schedules"],
        load_metrics={
            "cpu_percent": 45.2,
            "memory_percent": 60.1,
            "active_executions": 12,
            "queue_size": 5,
        },
    )


@pytest.mark.unit
class TestMultiMainConfig:
    """Test multi-main configuration."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = MultiMainConfig(node_id="test-node")
        
        assert config.node_id == "test-node"
        assert config.redis_url == "redis://localhost:6379"
        assert config.health_check_interval == 30
        assert config.leader_election_ttl == 60
        assert config.heartbeat_interval == 15
        assert config.webhook_routing_strategy == "consistent_hash"
        assert config.enable_state_replication is True
    
    def test_custom_config(self, multi_main_config):
        """Test custom configuration."""
        assert multi_main_config.node_id == "node-1"
        assert multi_main_config.health_check_interval == 5
        assert multi_main_config.leader_election_ttl == 30
        assert multi_main_config.environment == "test"


@pytest.mark.unit
class TestNodeInfo:
    """Test node information model."""
    
    def test_node_creation(self, node_info):
        """Test creating node info."""
        assert node_info.node_id == "node-1"
        assert node_info.status == NodeStatus.HEALTHY
        assert node_info.region == "us-east-1"
        assert "webhooks" in node_info.capabilities
        assert node_info.load_metrics["cpu_percent"] == 45.2
    
    def test_is_healthy(self, node_info):
        """Test health check."""
        assert node_info.is_healthy() is True
        
        # Test unhealthy states
        node_info.status = NodeStatus.UNHEALTHY
        assert node_info.is_healthy() is False
        
        node_info.status = NodeStatus.DISCONNECTED
        assert node_info.is_healthy() is False
    
    def test_is_overloaded(self, node_info):
        """Test overload detection."""
        # Normal load
        assert node_info.is_overloaded() is False
        
        # High CPU
        node_info.load_metrics["cpu_percent"] = 95.0
        assert node_info.is_overloaded() is True
        
        # High memory
        node_info.load_metrics["cpu_percent"] = 50.0
        node_info.load_metrics["memory_percent"] = 92.0
        assert node_info.is_overloaded() is True
        
        # High queue size
        node_info.load_metrics["memory_percent"] = 50.0
        node_info.load_metrics["queue_size"] = 150
        assert node_info.is_overloaded() is True
    
    def test_serialization(self, node_info):
        """Test node info serialization."""
        data = node_info.model_dump()
        
        assert "node_id" in data
        assert "status" in data
        assert "load_metrics" in data
        
        # Test deserialization
        restored = NodeInfo.model_validate(data)
        assert restored.node_id == node_info.node_id
        assert restored.status == node_info.status


@pytest.mark.unit
class TestLeaderElection:
    """Test leader election functionality."""
    
    @pytest.mark.asyncio
    async def test_start_election(self, multi_main_config, redis_client):
        """Test starting leader election."""
        election = LeaderElection(multi_main_config, redis_client)
        
        # Mock successful election
        redis_client.eval.return_value = 1  # Lock acquired
        
        result = await election.start_election()
        assert result is True
        assert election.is_leader is True
        
        # Verify Redis calls
        redis_client.eval.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_failed_election(self, multi_main_config, redis_client):
        """Test failed leader election."""
        election = LeaderElection(multi_main_config, redis_client)
        
        # Mock failed election
        redis_client.eval.return_value = 0  # Lock not acquired
        
        result = await election.start_election()
        assert result is False
        assert election.is_leader is False
    
    @pytest.mark.asyncio
    async def test_renew_leadership(self, multi_main_config, redis_client):
        """Test renewing leadership."""
        election = LeaderElection(multi_main_config, redis_client)
        election.is_leader = True
        election.election_token = "token-123"
        
        # Mock successful renewal
        redis_client.eval.return_value = 1
        
        result = await election.renew_leadership()
        assert result is True
        assert election.is_leader is True
    
    @pytest.mark.asyncio
    async def test_step_down(self, multi_main_config, redis_client):
        """Test stepping down from leadership."""
        election = LeaderElection(multi_main_config, redis_client)
        election.is_leader = True
        election.election_token = "token-123"
        
        await election.step_down()
        
        assert election.is_leader is False
        assert election.election_token is None
        # Should delete both leader key and token key
        assert redis_client.delete.call_count == 2
    
    @pytest.mark.asyncio
    async def test_get_current_leader(self, multi_main_config, redis_client):
        """Test getting current leader."""
        election = LeaderElection(multi_main_config, redis_client)
        
        # Mock leader exists
        redis_client.get.return_value = "leader-node-2"
        
        leader = await election.get_current_leader()
        assert leader == "leader-node-2"
        
        # Mock no leader
        redis_client.get.return_value = None
        leader = await election.get_current_leader()
        assert leader is None
    
    @pytest.mark.asyncio
    async def test_redis_connection_error(self, multi_main_config, redis_client):
        """Test handling Redis connection errors."""
        election = LeaderElection(multi_main_config, redis_client)
        
        # Mock Redis connection error
        redis_client.eval.side_effect = RedisConnectionError("Connection failed")
        
        with pytest.raises(LeaderElectionError, match="Redis connection failed"):
            await election.start_election()


@pytest.mark.unit
class TestInstanceHealthMonitor:
    """Test instance health monitoring."""
    
    @pytest.mark.asyncio
    async def test_register_instance(self, multi_main_config, redis_client, node_info):
        """Test registering instance."""
        monitor = InstanceHealthMonitor(multi_main_config, redis_client)
        
        await monitor.register_instance(node_info)
        
        # Verify Redis calls
        redis_client.hset.assert_called_once()
        redis_client.zadd.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_heartbeat(self, multi_main_config, redis_client):
        """Test updating heartbeat."""
        monitor = InstanceHealthMonitor(multi_main_config, redis_client)
        
        await monitor.update_heartbeat()
        
        # Verify heartbeat was recorded
        redis_client.zadd.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_healthy_instances(self, multi_main_config, redis_client):
        """Test getting healthy instances."""
        monitor = InstanceHealthMonitor(multi_main_config, redis_client)
        
        # Mock healthy instances using zrangebyscore
        redis_client.zrangebyscore.return_value = [b"node-1", b"node-2", b"node-3"]
        
        instances = await monitor.get_healthy_instances()
        assert len(instances) == 3
        assert "node-1" in instances
        assert "node-2" in instances
        assert "node-3" in instances
    
    @pytest.mark.asyncio
    async def test_cleanup_stale_instances(self, multi_main_config, redis_client):
        """Test cleaning up stale instances."""
        monitor = InstanceHealthMonitor(multi_main_config, redis_client)
        
        # Mock stale instances using zrangebyscore 
        redis_client.zrangebyscore.return_value = [b"stale-node-1", b"stale-node-2"]
        
        cleaned = await monitor.cleanup_stale_instances()
        assert cleaned == 2
        
        # Verify cleanup calls
        redis_client.zrem.assert_called()
        redis_client.delete.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_instance_info(self, multi_main_config, redis_client, node_info):
        """Test getting instance information."""
        monitor = InstanceHealthMonitor(multi_main_config, redis_client)
        
        # Mock instance data
        instance_data = node_info.model_dump_json()
        redis_client.hget.return_value = instance_data
        
        info = await monitor.get_instance_info("node-1")
        assert info is not None
        assert info.node_id == "node-1"
        assert info.status == NodeStatus.HEALTHY
    
    @pytest.mark.asyncio
    async def test_health_check_endpoint(self, multi_main_config, redis_client):
        """Test health check endpoint."""
        monitor = InstanceHealthMonitor(multi_main_config, redis_client)
        
        # Test healthy status
        health_data = await monitor.get_health_status()
        
        assert "status" in health_data
        assert "node_id" in health_data
        assert "uptime_seconds" in health_data  # Fixed: actual key name
        assert "metrics" in health_data
        assert health_data["node_id"] == multi_main_config.node_id


@pytest.mark.unit
class TestWebhookDistributor:
    """Test webhook distribution logic."""
    
    @pytest.mark.asyncio
    async def test_consistent_hash_routing(self, multi_main_config, redis_client):
        """Test consistent hash routing for webhooks."""
        distributor = WebhookDistributor(multi_main_config, redis_client)
        
        # Mock healthy instances
        instances = ["node-1", "node-2", "node-3"]
        
        # Test routing
        webhook_path = "/api/webhook/test"
        target_node = distributor.get_target_node(webhook_path, instances)
        
        assert target_node in instances
        
        # Test consistency - same path should always route to same node
        target_node_2 = distributor.get_target_node(webhook_path, instances)
        assert target_node == target_node_2
    
    @pytest.mark.asyncio
    async def test_load_balanced_routing(self, multi_main_config, redis_client):
        """Test load-balanced routing."""
        multi_main_config.webhook_routing_strategy = "load_balance"
        distributor = WebhookDistributor(multi_main_config, redis_client)
        
        # Mock instance loads
        redis_client.hgetall.return_value = {
            b"node-1": b"10",
            b"node-2": b"5",
            b"node-3": b"15",
        }
        
        instances = ["node-1", "node-2", "node-3"]
        webhook_path = "/api/webhook/test"
        
        target_node = await distributor.get_least_loaded_node(instances)
        assert target_node == "node-2"  # Lowest load
    
    @pytest.mark.asyncio
    async def test_webhook_registration_sync(self, multi_main_config, redis_client):
        """Test webhook registration synchronization."""
        distributor = WebhookDistributor(multi_main_config, redis_client)
        
        webhook_config = {
            "workflow_id": str(uuid4()),
            "node_id": "webhook_1",
            "path": "/test/webhook",
            "method": "POST",
        }
        
        await distributor.sync_webhook_registration(webhook_config)
        
        # Verify Redis pub/sub for sync
        redis_client.publish.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_handle_node_failure(self, multi_main_config, redis_client):
        """Test handling node failure in webhook distribution."""
        distributor = WebhookDistributor(multi_main_config, redis_client)
        
        failed_node = "node-2"
        remaining_instances = ["node-1", "node-3"]
        
        # Mock webhook reassignment
        redis_client.hgetall.return_value = {
            b"/webhook/1": b"node-2",
            b"/webhook/2": b"node-1", 
            b"/webhook/3": b"node-2",
        }
        
        reassigned = await distributor.reassign_webhooks_from_failed_node(
            failed_node, remaining_instances
        )
        
        assert reassigned == 2  # Two webhooks reassigned
        redis_client.hset.assert_called()


@pytest.mark.unit  
class TestScheduleDistributor:
    """Test schedule distribution for high availability."""
    
    @pytest.mark.asyncio
    async def test_distribute_schedules(self, multi_main_config, redis_client):
        """Test distributing schedules across nodes."""
        distributor = ScheduleDistributor(multi_main_config, redis_client)
        
        schedules = [
            {"id": "schedule-1", "cron": "0 */5 * * *", "workflow_id": str(uuid4())},
            {"id": "schedule-2", "cron": "0 0 * * *", "workflow_id": str(uuid4())},
            {"id": "schedule-3", "cron": "*/10 * * * *", "workflow_id": str(uuid4())},
        ]
        
        instances = ["node-1", "node-2", "node-3"]
        
        distribution = await distributor.distribute_schedules(schedules, instances)
        
        # Verify all schedules are distributed
        total_assigned = sum(len(node_schedules) for node_schedules in distribution.values())
        assert total_assigned == len(schedules)
        
        # Verify each node gets schedules
        for node in instances:
            assert node in distribution
    
    @pytest.mark.asyncio
    async def test_schedule_failover(self, multi_main_config, redis_client):
        """Test schedule failover when node fails."""
        distributor = ScheduleDistributor(multi_main_config, redis_client)
        
        failed_node = "node-2"
        remaining_instances = ["node-1", "node-3"]
        
        # Mock schedules from failed node
        redis_client.smembers.return_value = {b"schedule-1", b"schedule-2"}
        
        reassigned = await distributor.handle_node_failure(failed_node, remaining_instances)
        
        assert reassigned == 2
        # Verify schedules are redistributed
        redis_client.sadd.assert_called()
        redis_client.srem.assert_called()
    
    @pytest.mark.asyncio
    async def test_leader_schedule_coordination(self, multi_main_config, redis_client):
        """Test leader coordination of schedule distribution."""
        distributor = ScheduleDistributor(multi_main_config, redis_client)
        
        # Mock as leader
        redis_client.get.return_value = multi_main_config.node_id
        
        await distributor.coordinate_schedule_distribution()
        
        # Verify leader actions
        redis_client.publish.assert_called()


@pytest.mark.unit
class TestStateReplicator:
    """Test state replication across instances."""
    
    @pytest.mark.asyncio
    async def test_replicate_execution_state(self, multi_main_config, redis_client):
        """Test replicating execution state."""
        replicator = StateReplicator(multi_main_config, redis_client)
        
        execution_state = {
            "execution_id": str(uuid4()),
            "workflow_id": str(uuid4()),
            "status": "running",
            "current_node": "node_1",
            "context": {"variable1": "value1"},
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        
        await replicator.replicate_execution_state(execution_state)
        
        # Verify state is stored in Redis
        redis_client.hset.assert_called_once()
        redis_client.expire.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_execution_state(self, multi_main_config, redis_client):
        """Test retrieving execution state."""
        replicator = StateReplicator(multi_main_config, redis_client)
        
        execution_id = str(uuid4())
        mock_state = {
            b"execution_id": execution_id.encode(),
            b"status": b"running",
            b"context": b'{"variable1": "value1"}',
        }
        
        redis_client.hgetall.return_value = mock_state
        
        state = await replicator.get_execution_state(execution_id)
        
        assert state is not None
        assert state["execution_id"] == execution_id
        assert state["status"] == "running"
    
    @pytest.mark.asyncio
    async def test_cleanup_expired_state(self, multi_main_config, redis_client):
        """Test cleaning up expired execution state."""
        replicator = StateReplicator(multi_main_config, redis_client)
        
        # Mock expired executions
        redis_client.keys.return_value = [
            b"execution_state:exec-1",
            b"execution_state:exec-2",
        ]
        redis_client.ttl.return_value = -1  # Expired
        
        cleaned = await replicator.cleanup_expired_state()
        
        assert cleaned == 2
        redis_client.delete.assert_called()
    
    @pytest.mark.asyncio
    async def test_session_migration(self, multi_main_config, redis_client):
        """Test session migration between nodes."""
        replicator = StateReplicator(multi_main_config, redis_client)
        
        session_data = {
            "user_id": str(uuid4()),
            "session_token": "token-123",
            "expires_at": (datetime.now(timezone.utc)).isoformat(),
            "data": {"authenticated": True},
        }
        
        await replicator.migrate_session("node-1", "node-2", session_data)
        
        # Verify session transfer
        redis_client.hset.assert_called()
        redis_client.publish.assert_called()


@pytest.mark.unit
class TestMultiMainManager:
    """Test main multi-main coordination manager."""
    
    @pytest.mark.asyncio
    async def test_start_multi_main_mode(self, multi_main_config, redis_client, consul_client):
        """Test starting multi-main mode."""
        with patch("budflow.core.multi_main.consul") as mock_consul:
            mock_consul.Consul.return_value = consul_client
            manager = MultiMainManager(multi_main_config, redis_client)
            
            await manager.start()
            
            assert manager.is_running is True
            # Verify components are initialized
            assert manager.leader_election is not None
            assert manager.health_monitor is not None
            assert manager.webhook_distributor is not None
            
            # Cleanup
            await manager.stop()
    
    @pytest.mark.asyncio
    async def test_leader_election_cycle(self, multi_main_config, redis_client, consul_client):
        """Test complete leader election cycle."""
        with patch("budflow.core.multi_main.consul") as mock_consul:
            mock_consul.Consul.return_value = consul_client
            manager = MultiMainManager(multi_main_config, redis_client)
            
            # Mock successful election
            redis_client.eval.return_value = 1
            
            # Mock healthy instances to prevent split-brain detection
            redis_client.zrangebyscore.return_value = [
                multi_main_config.node_id.encode(),
                b"node-2",
                b"node-3"
            ]
            
            await manager.start()
            
            # Simulate election cycle
            await manager._election_cycle()
            
            assert manager.leader_election.is_leader is True
            
            # Cleanup
            await manager.stop()
    
    @pytest.mark.asyncio
    async def test_split_brain_detection(self, multi_main_config, redis_client, consul_client):
        """Test split-brain scenario detection."""
        with patch("budflow.core.multi_main.consul") as mock_consul:
            mock_consul.Consul.return_value = consul_client
            manager = MultiMainManager(multi_main_config, redis_client)
            
            # Set up this node as leader
            manager.leader_election.is_leader = True
            
            # Mock insufficient healthy instances (less than quorum)
            redis_client.zrangebyscore.return_value = [multi_main_config.node_id.encode()]  # Only 1 node visible
            
            split_brain_detected = await manager._detect_split_brain()
            assert split_brain_detected is True
    
    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, multi_main_config, redis_client, consul_client):
        """Test graceful shutdown of multi-main setup."""
        with patch("budflow.core.multi_main.consul") as mock_consul:
            mock_consul.Consul.return_value = consul_client
            manager = MultiMainManager(multi_main_config, redis_client)
            
            await manager.start()
            await manager.stop()
            
            assert manager.is_running is False
            # Verify cleanup
            redis_client.delete.assert_called()
    
    @pytest.mark.asyncio
    async def test_handle_network_partition(self, multi_main_config, redis_client, consul_client):
        """Test handling network partition scenarios."""
        with patch("budflow.core.multi_main.consul") as mock_consul:
            mock_consul.Consul.return_value = consul_client
            manager = MultiMainManager(multi_main_config, redis_client)
            
            # Mock network partition (Redis unavailable)
            redis_client.ping.side_effect = RedisConnectionError("Network partition")
            
            await manager._handle_network_partition()
            
            # Verify fallback to safe mode
            assert manager.safe_mode is True


@pytest.mark.integration
class TestMultiMainIntegration:
    """Integration tests for multi-main setup."""
    
    @pytest.mark.asyncio
    async def test_full_ha_scenario(self, multi_main_config, redis_client, consul_client):
        """Test complete high availability scenario."""
        with patch("budflow.core.multi_main.consul") as mock_consul:
            mock_consul.Consul.return_value = consul_client
            
            # Create a single manager and test leadership
            config = multi_main_config.model_copy()
            config.node_id = "test-leader"
            
            manager = MultiMainManager(config, redis_client)
            
            # Mock enough healthy instances to prevent split-brain
            redis_client.zrangebyscore.return_value = [b"test-leader", b"node-2", b"node-3"]
            
            # Mock successful election
            redis_client.eval.return_value = 1
            
            await manager.start()
            await manager._election_cycle()
            
            # Should become leader
            assert manager.leader_election.is_leader is True
            
            # Should be healthy
            health = await manager.health_monitor.get_health_status()
            assert health["status"] == "healthy"
            
            # Stop manager
            await manager.stop()
    
    @pytest.mark.asyncio
    async def test_leader_failover(self, multi_main_config, redis_client, consul_client):
        """Test leader failover scenario."""
        with patch("budflow.core.multi_main.consul") as mock_consul:
            mock_consul.Consul.return_value = consul_client
            
            # Test simulated failover scenario
            config = multi_main_config.model_copy()
            config.node_id = "failover-node"
            
            manager = MultiMainManager(config, redis_client)
            
            # Mock enough healthy instances
            redis_client.zrangebyscore.return_value = [b"failover-node", b"other-node"]
            
            # Start as non-leader
            redis_client.eval.return_value = 0  # Failed election
            await manager.start()
            await manager._election_cycle()
            assert manager.leader_election.is_leader is False
            
            # Now simulate taking over leadership
            redis_client.eval.return_value = 1  # Successful election
            redis_client.get.return_value = None  # No current leader
            
            await manager._election_cycle()
            assert manager.leader_election.is_leader is True
            
            # Cleanup
            await manager.stop()
    
    @pytest.mark.asyncio 
    async def test_webhook_redistribution_on_failure(self, multi_main_config, redis_client):
        """Test webhook redistribution when node fails."""
        distributor = WebhookDistributor(multi_main_config, redis_client)
        
        # Initial webhook assignments
        initial_assignments = {
            "/webhook/1": "node-1",
            "/webhook/2": "node-2", 
            "/webhook/3": "node-2",
            "/webhook/4": "node-3",
        }
        
        # Mock Redis data
        redis_client.hgetall.return_value = {
            k.encode(): v.encode() for k, v in initial_assignments.items()
        }
        
        # Node-2 fails
        failed_node = "node-2"
        remaining_nodes = ["node-1", "node-3"]
        
        reassigned = await distributor.reassign_webhooks_from_failed_node(
            failed_node, remaining_nodes
        )
        
        # Should reassign 2 webhooks from node-2
        assert reassigned == 2
        
        # Verify redistribution calls
        redis_client.hset.assert_called()
        redis_client.publish.assert_called()


@pytest.mark.performance
class TestMultiMainPerformance:
    """Performance tests for multi-main setup."""
    
    @pytest.mark.asyncio
    async def test_leader_election_performance(self, multi_main_config, redis_client):
        """Test leader election performance under load."""
        election = LeaderElection(multi_main_config, redis_client)
        
        # Mock rapid election attempts
        redis_client.eval.return_value = 1
        
        start_time = time.time()
        
        # Simulate multiple rapid elections
        tasks = []
        for _ in range(100):
            task = election.start_election()
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should complete within reasonable time
        assert duration < 5.0  # 5 seconds for 100 elections
        assert all(r is True for r in results)
    
    @pytest.mark.asyncio
    async def test_webhook_routing_performance(self, multi_main_config, redis_client):
        """Test webhook routing performance."""
        distributor = WebhookDistributor(multi_main_config, redis_client)
        
        instances = [f"node-{i}" for i in range(10)]
        
        start_time = time.time()
        
        # Test routing 1000 webhooks
        for i in range(1000):
            webhook_path = f"/api/webhook/{i}"
            target = distributor.get_target_node(webhook_path, instances)
            assert target in instances
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should route 1000 webhooks quickly
        assert duration < 1.0  # 1 second for 1000 routes
    
    @pytest.mark.asyncio
    async def test_state_replication_performance(self, multi_main_config, redis_client):
        """Test state replication performance."""
        replicator = StateReplicator(multi_main_config, redis_client)
        
        start_time = time.time()
        
        # Replicate 100 execution states
        tasks = []
        for i in range(100):
            state = {
                "execution_id": str(uuid4()),
                "workflow_id": str(uuid4()),
                "status": "running",
                "context": {"step": i},
            }
            task = replicator.replicate_execution_state(state)
            tasks.append(task)
        
        await asyncio.gather(*tasks)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should replicate quickly
        assert duration < 2.0  # 2 seconds for 100 replications